package executor

import (
	"context"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/rowcodec"
	"sync"
)

var _ Executor = &TraverseExecutor{}

const workerConcurrency = 5

type tempResult struct {
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

	startTS     uint64
	txn         kv.Transaction
	snapshot    kv.Snapshot
	workerWg    *sync.WaitGroup
	doneErr     error
	resultTagID int64

	conditionChain []condition

	*rowcodec.ChunkDecoder
	vertexIdOffsetInChild int64
	prepared              bool
	done                  bool

	mu struct {
		sync.Mutex
		childFinish bool
		taskNum     int
	}

	workerChan          chan *tempResult
	fetchFromChildErr   chan error
	traverseResultVIDCh chan int64
	closeCh             chan struct{}

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
	e.ChunkDecoder = rowcodec.NewChunkDecoder(cols, pkCols, def, nil)

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

func (e *TraverseExecutor) runNewWorker(ctx context.Context) {
	defer func() {
		e.workerWg.Done()
	}()

	var task *tempResult
	for ok := true; ok; {
		select {
		case task, ok = <-e.workerChan:
			if !ok {
				return
			}
			err := e.handleTraverseTask(ctx, task)
			if e.checkFinishAfterHandleTask() {
				e.done = true
				close(e.traverseResultVIDCh)
				return
			}
			if err != nil {
				e.doneErr = err
			}
		case <-ctx.Done():
			return
		case <-e.closeCh:
			return
		}
	}
}

func (e *TraverseExecutor) startWorkers(ctx context.Context) {
	e.workerChan = make(chan *tempResult, workerConcurrency)

	for i := 0; i < workerConcurrency; i++ {
		e.workerWg.Add(1)
		go e.runNewWorker(ctx)
	}
}

func (e *TraverseExecutor) checkFinishAfterHandleTask() bool {
	finish := false
	e.mu.Lock()
	e.mu.taskNum--
	finish = e.mu.childFinish && e.mu.taskNum == 0
	e.mu.Unlock()
	return finish
}

func (e *TraverseExecutor) handleTraverseTask(ctx context.Context, task *tempResult) error {
	level := task.chainLevel
	finish := false
	var newTask tempResult
	if level+1 == int64(len(e.conditionChain)) {
		finish = true
	}
	for _, vertexId := range task.vertexIds {
		var kvRange kv.KeyRange
		switch e.conditionChain[level].direction {
		case OUT:
			kvRange.StartKey = tablecodec.ConstructKeyForGraphTraverse(vertexId, true, e.conditionChain[level].edgeID)
			kvRange.EndKey = tablecodec.ConstructKeyForGraphTraverse(vertexId, true, e.conditionChain[level].edgeID+1)
		case IN:
			kvRange.StartKey = tablecodec.ConstructKeyForGraphTraverse(vertexId, false, e.conditionChain[level].edgeID)
			kvRange.EndKey = tablecodec.ConstructKeyForGraphTraverse(vertexId, false, e.conditionChain[level].edgeID+1)
		case BOTH:
			kvRange.StartKey = tablecodec.ConstructKeyForGraphTraverse(vertexId, true, e.conditionChain[level].edgeID)
			kvRange.EndKey = tablecodec.ConstructKeyForGraphTraverse(vertexId, true, e.conditionChain[level].edgeID+1)
			// TODO: cross validate
		}
		cond := e.conditionChain[level]
		iter, err := e.snapshot.Iter(kvRange.StartKey, kvRange.EndKey)
		if err != nil {
			return err
		}
		if !finish {
			newTask = tempResult{}
			newTask.vertexIds = make([]int64, 0, 100)
			newTask.chainLevel = level + 1
		}
		for iter.Valid() {
			k := iter.Key()
			used := true
			if cond.cond != nil {
				v := iter.Value()
				handle, err := tablecodec.DecodeGraphEdgeRowKey(k)
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
					used = false
				}
			}

			if !used {
				continue
			}

			resultID, err := tablecodec.DecodeLastIDOfGraphEdge(k)
			if err != nil {
				return err
			}

			if finish {
				e.traverseResultVIDCh <- resultID
			} else {
				newTask.vertexIds = append(newTask.vertexIds, resultID)
			}

			err = iter.Next()
			if err != nil {
				return err
			}
		}
		if !finish {
			e.sendTaskToWorker(&newTask)
		}
	}
	return nil
}

func (e *TraverseExecutor) sendTaskToWorker(task *tempResult) {
	e.mu.Lock()
	e.mu.taskNum += 1
	e.mu.Unlock()
	e.workerChan <- task
}

func (e *TraverseExecutor) fetchFromChildAndBuildFirstTask(ctx context.Context) {
	defer func() {
		e.workerWg.Done()
		e.mu.Lock()
		e.mu.childFinish = true
		e.mu.Unlock()
	}()

	chk := newFirstChunk(e.children[0])

	for {
		select {
		case <-e.closeCh:
			return
		default:
			newTask := tempResult{}
			newTask.chainLevel = 0
			newTask.vertexIds = make([]int64, 0, 100)
			chk.Reset()
			if err := Next(ctx, e.children[0], chk); err != nil {
				e.fetchFromChildErr <- err
				return
			}
			if chk.NumRows() == 0 {
				return
			}
			for i := 0; i < chk.NumRows(); i++ {
				vid := chk.GetRow(i).GetInt64(int(e.vertexIdOffsetInChild))
				newTask.vertexIds = append(newTask.vertexIds, vid)
			}
			e.sendTaskToWorker(&newTask)
		}
	}
}

func (e *TraverseExecutor) ConstructResultRow(ctx context.Context, vid int64, req *chunk.Chunk) error {
	key := tablecodec.EncodeGraphTag(vid, e.resultTagID)
	value, err := e.snapshot.Get(ctx, key)
	if err != nil {
		if kv.ErrNotExist.Equal(err) {
			return nil
		}
		return err
	}

	return e.ChunkDecoder.DecodeToChunk(value, kv.IntHandle(vid), req)
}

func (e *TraverseExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.prepared {
		e.workerWg.Add(1)
		go e.fetchFromChildAndBuildFirstTask(ctx)
		e.prepared = true
	}

	req.Reset()
	if e.done {
		return nil
	}

	for {
		select {
		case err := <-e.fetchFromChildErr:
			return err
		case vid, ok := <-e.traverseResultVIDCh:
			if !ok {
				return nil
			}
			err := e.ConstructResultRow(ctx, vid, req)
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
	close(e.closeCh)
	close(e.workerChan)
	e.workerWg.Wait()
	return nil
}
