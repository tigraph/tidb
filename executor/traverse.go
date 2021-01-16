package executor

import (
	"context"
	"fmt"
	"sync"
	goatomic "sync/atomic"
	"time"

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

type traverseLevel struct {
	edgeID     int64
	direction  DirType
	cond       expression.Expression
	rowDecoder *rowcodec.ChunkDecoder
	chk        *chunk.Chunk
	dedup      struct {
		mu   sync.Mutex
		data map[int64]struct{}
	}
}

type TraverseExecutorStats struct {
	concurrency   int
	taskNum       int64
	edgeScanRows  int64
	totalTaskTime int64
	maxTaskTime   int64
	pushTaskTime  int64
	fetchRootTime int64

	batchGet            int64
	batchGetTotalKey    int64
	batchGetTotalResult int64
	batchGetExecCount   int64
}

func (s *TraverseExecutorStats) String() string {
	return fmt.Sprintf("concur: %v, fetch_root: %v, task:{num: %v, time: %v,avg: %v, max: %v, push_wait: %v, batch_get: %v, edge_scan_rows: %v, batch_get:{total_key: %v,total_result: %v, exec_count: %v, avg_get_keys: %v}}", s.concurrency,
		time.Duration(s.fetchRootTime), s.taskNum, time.Duration(s.totalTaskTime), time.Duration(float64(s.totalTaskTime)/float64(s.taskNum)),
		time.Duration(s.maxTaskTime), time.Duration(s.pushTaskTime), time.Duration(float64(s.batchGet)/float64(s.batchGetExecCount)), s.edgeScanRows,
		s.batchGetTotalKey, s.batchGetTotalResult, s.batchGetExecCount, s.batchGetTotalKey/s.batchGetExecCount)
}

type TraverseExecutor struct {
	baseExecutor
	concurrency int
	prepared    bool
	done        bool

	startTS     uint64
	txn         kv.Transaction
	snapshot    kv.Snapshot
	workerWg    *sync.WaitGroup
	doneErr     error
	resultTagID int64

	traverseLevels []*traverseLevel

	codec     *rowcodec.ChunkDecoder
	vidOffset int64

	childExhausted atomic.Bool
	pendingTasks   atomic.Int64

	workerCh chan *traverseTask
	childErr chan error
	results  chan *chunk.Chunk
	die      chan struct{}

	tablePlan plannercore.PhysicalPlan

	stats TraverseExecutorStats
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

	for i := range e.traverseLevels {
		e.traverseLevels[i].dedup.data = make(map[int64]struct{})
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
	e.stats.concurrency = e.concurrency
	e.workerWg.Add(e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		go e.runWorker(ctx)
	}
}

func (e *TraverseExecutor) handleTask(ctx context.Context, task *traverseTask) error {
	level := task.chainLevel
	final := level+1 == int64(len(e.traverseLevels))
	cond := e.traverseLevels[level]

	startTime := time.Now()
	edgeScanRows := 0
	defer func() {
		cost := time.Since(startTime)
		goatomic.AddInt64(&e.stats.totalTaskTime, int64(cost))
		goatomic.AddInt64(&e.stats.edgeScanRows, int64(edgeScanRows))
		if goatomic.LoadInt64(&e.stats.maxTaskTime) < int64(cost) {
			goatomic.StoreInt64(&e.stats.maxTaskTime, int64(cost))
		}
	}()
	goatomic.AddInt64(&e.stats.taskNum, 1)

	batchSize := 1024
	vids := make([]int64, 0, batchSize)
	for _, vid := range task.vertexIds {
		var kvRange kv.KeyRange
		switch e.traverseLevels[level].direction {
		case OUT:
			kvRange.StartKey = tablecodec.ConstructKeyForGraphTraverse(vid, true, e.traverseLevels[level].edgeID)
			kvRange.EndKey = tablecodec.ConstructKeyForGraphTraverse(vid, true, e.traverseLevels[level].edgeID+1)
		case IN:
			kvRange.StartKey = tablecodec.ConstructKeyForGraphTraverse(vid, false, e.traverseLevels[level].edgeID)
			kvRange.EndKey = tablecodec.ConstructKeyForGraphTraverse(vid, false, e.traverseLevels[level].edgeID+1)
		case BOTH:
			kvRange.StartKey = tablecodec.ConstructKeyForGraphTraverse(vid, true, e.traverseLevels[level].edgeID)
			kvRange.EndKey = tablecodec.ConstructKeyForGraphTraverse(vid, true, e.traverseLevels[level].edgeID+1)
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
		for err = nil; iter.Valid(); err = iter.Next() {
			edgeScanRows++
			if err != nil {
				return err
			}
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

			cond.dedup.mu.Lock()
			_, found := cond.dedup.data[resultID]
			if !found {
				cond.dedup.data[resultID] = struct{}{}
			}
			cond.dedup.mu.Unlock()
			if found {
				continue
			}

			if final {
				vids = append(vids, resultID)
				if len(vids) == batchSize {
					chk, err := e.batchGetVertex(ctx, vids)
					if err != nil {
						return err
					}
					if chk.NumRows() > 0 {
						e.results <- chk
					}
					vids = make([]int64, 0, batchSize)
				}
			} else {
				newTask.vertexIds = append(newTask.vertexIds, resultID)
			}
		}
		if newTask != nil {
			e.pushTask(newTask)
		}
	}
	if final && len(vids) > 0 {
		chk, err := e.batchGetVertex(ctx, vids)
		if err != nil {
			return err
		}
		if chk.NumRows() > 0 {
			e.results <- chk
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
	start := time.Now()
	defer func() {
		e.workerWg.Done()
		e.childExhausted.Store(true)
		goatomic.AddInt64(&e.stats.fetchRootTime, int64(time.Since(start)))
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
	start := time.Now()
	e.workerCh <- task
	e.pendingTasks.Add(1)
	goatomic.AddInt64(&e.stats.pushTaskTime, int64(time.Since(start)))
}

func (e *TraverseExecutor) batchGetVertex(ctx context.Context, vids []int64) (*chunk.Chunk, error) {
	base := e.base()
	chk := chunk.New(base.retFieldTypes, len(vids), len(vids))

	cacheKeys := make([]kv.Key, 0, len(vids))
	for _, vid := range vids {
		key := tablecodec.EncodeGraphTag(vid, e.resultTagID)
		cacheKeys = append(cacheKeys, key)
	}
	start := time.Now()
	values, err := e.snapshot.BatchGet(ctx, cacheKeys)
	goatomic.AddInt64(&e.stats.batchGet, int64(time.Since(start)))
	goatomic.AddInt64(&e.stats.batchGetExecCount, 1)
	goatomic.AddInt64(&e.stats.batchGetTotalKey, int64(len(cacheKeys)))
	goatomic.AddInt64(&e.stats.batchGetTotalResult, int64(len(values)))
	if err != nil {
		if kv.ErrNotExist.Equal(err) {
			return chk, nil
		}
		return chk, err
	}
	// keep order
	for idx, key := range cacheKeys {
		value, ok := values[string(key)]
		if !ok {
			continue
		}
		err = e.codec.DecodeToChunk(value, kv.IntHandle(vids[idx]), chk)
		if err != nil {
			return chk, err
		}
	}

	return chk, nil
}

func (e *TraverseExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.prepared {
		e.workerWg.Add(1)
		go e.fetchFromChild(ctx)
		e.prepared = true
	}

	req.Reset()
	if e.done {
		return e.doneErr
	}

	for {
		select {
		case err := <-e.childErr:
			return err
		case chk, ok := <-e.results:
			if !ok {
				e.done = true
				return nil
			}
			req.SwapColumns(chk)
			return nil
		}
	}
}

func (e *TraverseExecutor) Close() error {
	//start := time.Now()
	close(e.die)

	for range e.results {
	}

	e.workerWg.Wait()
	//closeWait := time.Since(start)
	//fmt.Printf("traverse stats: %v, close_wait: %v \n------\n", e.stats.String(), closeWait.String())
	return nil
}
