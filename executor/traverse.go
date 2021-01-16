package executor

import (
	"context"
	"runtime"
	"sync"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/atomic"
)

var _ Executor = &TraverseExecutor{}

type traverseTask struct {
	vertexIds  []int64
	chainLevel int64
}

type DirType uint8

const (
	IN DirType = iota
	OUT
	BOTH
)

type condition struct {
	edgeID     int64
	direction  DirType
	cond       expression.Expression
	rowDecoder *rowcodec.ChunkDecoder
	chk        *chunk.Chunk
}

type TraverseExecutor struct {
	baseExecutor
	prepared bool
	done     bool

	startTS     uint64
	txn         kv.Transaction
	snapshot    kv.Snapshot
	workerWg    *sync.WaitGroup
	doneErr     error
	resultTagID int64

	conditions []condition

	codec     *rowcodec.ChunkDecoder
	vidOffset int64

	childExhausted atomic.Bool
	pendingTasks   atomic.Int64

	workerCh chan *traverseTask
	childErr chan error
	results  chan int64
	die      chan struct{}

	tablePlan plannercore.PhysicalPlan
}

func (e *TraverseExecutor) Init(p *plannercore.PointGetPlan, startTs uint64) {
	e.startTS = startTs
}

// Open initializes necessary variables for using this executor.
func (e *TraverseExecutor) Open(ctx context.Context) error {
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	snapshotTS := e.startTS
	var err error

	var (
		pkCols []int64
		cols   = make([]rowcodec.ColInfo, 0, len(e.schema.Columns))
	)
	for _, col := range e.schema.Columns {
		col := rowcodec.ColInfo{
			ID:         col.ID,
			Ft:         col.GetType(),
			IsPKHandle: mysql.HasPriKeyFlag(col.GetType().Flag),
		}
		if col.IsPKHandle {
			pkCols = []int64{col.ID}
		}
		cols = append(cols, col)
	}
	def := func(i int, chk *chunk.Chunk) error {
		// Assume that no default value.
		chk.AppendNull(i)
		return nil
	}
	e.codec = rowcodec.NewChunkDecoder(cols, pkCols, def, nil)

	e.txn, err = e.ctx.Txn(false)
	if err != nil {
		return err
	}
	if e.txn.Valid() && txnCtx.StartTS == txnCtx.GetForUpdateTS() {
		e.snapshot = e.txn.GetSnapshot()
	} else {
		e.snapshot = e.ctx.GetStore().GetSnapshot(kv.Version{Ver: snapshotTS})
	}

	err = e.children[0].Open(ctx)
	if err != nil {
		return err
	}

	e.startWorkers(ctx)
	return nil
}

func (e *TraverseExecutor) runWorker(ctx context.Context) {
	defer e.workerWg.Done()

	for {
		select {
		case task, ok := <-e.workerCh:
			if !ok {
				return
			}
			err := e.handleTask(ctx, task)
			if err != nil {
				e.doneErr = err
				return
			}
		case <-ctx.Done():
			return
		case <-e.die:
			return
		}
	}
}

func (e *TraverseExecutor) startWorkers(ctx context.Context) {
	concurrency := runtime.NumCPU() * 5
	e.workerCh = make(chan *traverseTask, concurrency*1000)
	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.runWorker(ctx)
	}
}

func (e *TraverseExecutor) handleTask(ctx context.Context, task *traverseTask) error {
	level := task.chainLevel
	final := level+1 == int64(len(e.conditions))
	cond := e.conditions[level]

	for _, vid := range task.vertexIds {
		var kvRange kv.KeyRange
		switch e.conditions[level].direction {
		case OUT:
			kvRange.StartKey = tablecodec.ConstructKeyForGraphTraverse(vid, true, e.conditions[level].edgeID)
			kvRange.EndKey = tablecodec.ConstructKeyForGraphTraverse(vid, true, e.conditions[level].edgeID+1)
		case IN:
			kvRange.StartKey = tablecodec.ConstructKeyForGraphTraverse(vid, false, e.conditions[level].edgeID)
			kvRange.EndKey = tablecodec.ConstructKeyForGraphTraverse(vid, false, e.conditions[level].edgeID+1)
		case BOTH:
			kvRange.StartKey = tablecodec.ConstructKeyForGraphTraverse(vid, true, e.conditions[level].edgeID)
			kvRange.EndKey = tablecodec.ConstructKeyForGraphTraverse(vid, true, e.conditions[level].edgeID+1)
			// TODO: cross validate
		}

		iter, err := e.snapshot.Iter(kvRange.StartKey, kvRange.EndKey)
		if err != nil {
			return err
		}
		var newTask *traverseTask
		if !final {
			newTask = &traverseTask{}
			newTask.vertexIds = make([]int64, 0, 100)
			newTask.chainLevel = level + 1
		}
		for iter.Valid() {
			key := iter.Key()
			if cond.cond != nil {
				v := iter.Value()
				handle, err := tablecodec.DecodeGraphEdgeRowKey(key)
				if err != nil {
					return err
				}
				cond.chk.Reset()
				err = cond.rowDecoder.DecodeToChunk(v, handle, cond.chk)
				if err != nil {
					return err
				}
				val, isNull, err := cond.cond.EvalInt(e.ctx, cond.chk.GetRow(0))
				if err != nil {
					return err
				}
				if val <= 0 || isNull {
					continue
				}
			}

			resultID, err := tablecodec.DecodeLastIDOfGraphEdge(key)
			if err != nil {
				return err
			}

			if final {
				e.results <- resultID
			} else {
				newTask.vertexIds = append(newTask.vertexIds, resultID)
			}

			err = iter.Next()
			if err != nil {
				return err
			}
		}
		if newTask != nil {
			e.pushTask(newTask)
		}
	}

	e.pendingTasks.Sub(1)

	// All workers should be closed if the child exhausted and all pending traverse tasks finished.
	if e.childExhausted.Load() && e.pendingTasks.Load() == 0 {
		close(e.results)
		close(e.workerCh)
	}

	return nil
}

func (e *TraverseExecutor) fetchFromChild(ctx context.Context) {
	defer func() {
		e.workerWg.Done()
		e.childExhausted.Store(true)
	}()

	chk := newFirstChunk(e.children[0])

	for {
		select {
		case <-e.die:
			return
		default:
			chk.Reset()
			if err := Next(ctx, e.children[0], chk); err != nil {
				e.childErr <- err
				return
			}
			if chk.NumRows() == 0 {
				return
			}

			task := &traverseTask{
				vertexIds: make([]int64, 0, chk.NumRows()),
			}
			for i := 0; i < chk.NumRows(); i++ {
				vid := chk.GetRow(i).GetInt64(int(e.vidOffset))
				task.vertexIds = append(task.vertexIds, vid)
			}
			e.pushTask(task)
		}
	}
}

func (e *TraverseExecutor) pushTask(task *traverseTask) {
	e.workerCh <- task
	e.pendingTasks.Add(1)
}

func (e *TraverseExecutor) appendResult(ctx context.Context, vid int64, req *chunk.Chunk) error {
	key := tablecodec.EncodeGraphTag(vid, e.resultTagID)
	value, err := e.snapshot.Get(ctx, key)
	if err != nil {
		if kv.ErrNotExist.Equal(err) {
			return nil
		}
		return err
	}

	return e.codec.DecodeToChunk(value, kv.IntHandle(vid), req)
}

func (e *TraverseExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.prepared {
		e.workerWg.Add(1)
		go e.fetchFromChild(ctx)
		e.prepared = true
	}

	req.Reset()
	if e.done {
		return nil
	}

	for {
		select {
		case err := <-e.childErr:
			return err
		case vid, ok := <-e.results:
			if !ok {
				e.done = true
				return nil
			}
			err := e.appendResult(ctx, vid, req)
			if err != nil {
				return err
			}
			if req.IsFull() {
				return nil
			}
		}
	}
}

func (e *TraverseExecutor) Close() error {
	close(e.die)

	for range e.results {
	}

	e.workerWg.Wait()
	return nil
}
