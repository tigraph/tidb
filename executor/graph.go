// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/codec"
	"sync"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/atomic"
	goatomic "sync/atomic"
)

var _ Executor = &GraphEdgeScanExecutor{}

type DirType uint8

const (
	IN DirType = iota
	OUT
	BOTH
)

type GraphEdgeScanExecutorStats struct {
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

func (s *GraphEdgeScanExecutorStats) String() string {
	return fmt.Sprintf("concur: %v, fetch_root: %v, task:{num: %v, time: %v,avg: %v, max: %v, push_wait: %v, batch_get: %v, edge_scan_rows: %v, batch_get:{total_key: %v,total_result: %v, exec_count: %v, avg_get_keys: %v}}", s.concurrency,
		time.Duration(s.fetchRootTime), s.taskNum, time.Duration(s.totalTaskTime), time.Duration(float64(s.totalTaskTime)/float64(s.taskNum)),
		time.Duration(s.maxTaskTime), time.Duration(s.pushTaskTime), time.Duration(float64(s.batchGet)/float64(s.batchGetExecCount)), s.edgeScanRows,
		s.batchGetTotalKey, s.batchGetTotalResult, s.batchGetExecCount, s.batchGetTotalKey/s.batchGetExecCount)
}

type GraphEdgeScanExecutor struct {
	baseExecutor
	concurrency int
	prepared    bool
	done        bool

	startTS  uint64
	txn      kv.Transaction
	snapshot kv.Snapshot
	workerWg *sync.WaitGroup
	doneErr  error

	direction      ast.GraphEdgeDirection
	edgeTableInfo  *model.TableInfo
	edgeRowDecoder *rowcodec.ChunkDecoder
	edgeChunk      *chunk.Chunk
	destTableInfo  *model.TableInfo
	destRowDecoder *rowcodec.ChunkDecoder
	destChunk      *chunk.Chunk

	codec *rowcodec.ChunkDecoder

	childExhausted atomic.Bool
	pendingTasks   atomic.Int64

	// Channel to send vertex identifiers.
	sourceVerticesCh chan []int64
	childErr         chan error
	results          chan *chunk.Chunk
	die              chan struct{}

	stats GraphEdgeScanExecutorStats
}

// Open initializes necessary variables for using this executor.
func (e *GraphEdgeScanExecutor) Open(ctx context.Context) error {
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

func (e *GraphEdgeScanExecutor) runWorker(ctx context.Context) {
	defer e.workerWg.Done()

	for {
		select {
		case sourceVertices, ok := <-e.sourceVerticesCh:
			if !ok {
				return
			}
			err := e.handleTask(ctx, sourceVertices)
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

func (e *GraphEdgeScanExecutor) startWorkers(ctx context.Context) {
	e.stats.concurrency = e.concurrency
	e.workerWg.Add(e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		go e.runWorker(ctx)
	}
}

func (e *GraphEdgeScanExecutor) handleTask(ctx context.Context, sourceVertices []int64) error {
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
	destVertices := make([]int64, 0, batchSize)
	for _, vid := range sourceVertices {
		var indices []*model.IndexInfo
		switch e.direction {
		case ast.GraphEdgeDirectionOut:
			indices = append(indices, e.edgeTableInfo.FindIndexByName(mysql.PrimaryKeyName))
		case ast.GraphEdgeDirectionIn:
			indices = append(indices, e.edgeTableInfo.FindIndexByName(mysql.GraphEdgeKeyName))
		case ast.GraphEdgeDirectionBoth:
			indices = append(indices, e.edgeTableInfo.FindIndexByName(mysql.PrimaryKeyName))
			indices = append(indices, e.edgeTableInfo.FindIndexByName(mysql.GraphEdgeKeyName))
		}

		for _, idx := range indices {
			startKey := tablecodec.EncodeIndexSeekKey(e.edgeTableInfo.ID, idx.ID, codec.EncodeInt(nil, vid))
			endKey := tablecodec.EncodeIndexSeekKey(e.edgeTableInfo.ID, idx.ID, codec.EncodeInt(nil, vid+1))
			// TODO: coprocessor index scanner
			iter, err := e.snapshot.Iter(startKey, endKey)
			if err != nil {
				return err
			}
			for err = nil; iter.Valid(); err = iter.Next() {
				edgeScanRows++
				if err != nil {
					return err
				}
				key := iter.Key()
				destVertexID, err := tablecodec.DecodeGraphEdgeDestID(key)
				if err != nil {
					return err
				}
				destVertices = append(destVertices, destVertexID)
			}
		}
	}

	// Fetch all vertex information
	for verticesLen := len(destVertices); verticesLen > 0; {
		count := batchSize
		if verticesLen < batchSize {
			count = verticesLen
		}
		chk, err := e.batchFillChunk(ctx, destVertices[:count])
		if err != nil {
			return err
		}
		if chk.NumRows() > 0 {
			e.results <- chk
		}
		destVertices = destVertices[count:]
	}

	e.pendingTasks.Sub(1)

	// All workers should be closed if the child exhausted and all pending traverse tasks finished.
	if e.childExhausted.Load() && e.pendingTasks.Load() == 0 {
		close(e.results)
		close(e.sourceVerticesCh)
	}

	return nil
}

func (e *GraphEdgeScanExecutor) fetchFromChild(ctx context.Context) {
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

			var vertices []int64
			for i := 0; i < chk.NumRows(); i++ {
				// FIXME: use correct child primary key offset
				vid := chk.GetRow(i).GetInt64(0)
				vertices = append(vertices, vid)
			}
			e.pushTask(vertices)
		}
	}
}

func (e *GraphEdgeScanExecutor) pushTask(vertices []int64) {
	start := time.Now()
	e.sourceVerticesCh <- vertices
	e.pendingTasks.Add(1)
	goatomic.AddInt64(&e.stats.pushTaskTime, int64(time.Since(start)))
}

func (e *GraphEdgeScanExecutor) batchFillChunk(ctx context.Context, destVertices []int64) (*chunk.Chunk, error) {
	base := e.base()
	chk := chunk.New(base.retFieldTypes, len(destVertices), len(destVertices))

	cacheKeys := make([]kv.Key, 0, len(destVertices))
	for _, vid := range destVertices {
		key := tablecodec.EncodeRowKey(e.destTableInfo.ID, codec.EncodeInt(nil, vid))
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
		err = e.destRowDecoder.DecodeToChunk(value, kv.IntHandle(destVertices[idx]), chk)
		if err != nil {
			return chk, err
		}
	}

	return chk, nil
}

func (e *GraphEdgeScanExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
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

func (e *GraphEdgeScanExecutor) Close() error {
	close(e.die)

	for range e.results {
	}

	e.workerWg.Wait()
	return nil
}
