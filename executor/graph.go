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
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/atomic"
)

const chunkBatchSize = 1024

var _ Executor = &GraphEdgeScanExecutor{}

type GraphEdgeScanExecutorStats struct {
	concurrency   int
	taskNum       atomic.Int64
	edgeScanRows  atomic.Int64
	totalTaskTime atomic.Int64
	maxTaskTime   atomic.Int64
	pushTaskTime  atomic.Int64
	fetchRootTime atomic.Int64

	batchGet            atomic.Int64
	batchGetTotalKey    atomic.Int64
	batchGetTotalResult atomic.Int64
	batchGetExecCount   atomic.Int64
}

func (s *GraphEdgeScanExecutorStats) String() string {
	return fmt.Sprintf("concur: %v, fetch_root: %v, task:{num: %v, time: %v,avg: %v, max: %v, push_wait: %v, batch_get: %v, edge_scan_rows: %v, batch_get:{total_key: %v,total_result: %v, exec_count: %v, avg_get_keys: %v}}",
		s.concurrency,
		time.Duration(s.fetchRootTime.Load()),
		s.taskNum,
		time.Duration(s.totalTaskTime.Load()),
		time.Duration(float64(s.totalTaskTime.Load())/float64(s.taskNum.Load())),
		time.Duration(s.maxTaskTime.Load()),
		time.Duration(s.pushTaskTime.Load()), time.Duration(float64(s.batchGet.Load())/float64(s.batchGetExecCount.Load())),
		s.edgeScanRows,
		s.batchGetTotalKey,
		s.batchGetTotalResult,
		s.batchGetExecCount,
		s.batchGetTotalKey.Load()/s.batchGetExecCount.Load())
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
	edgeSchema     *expression.Schema
	edgeRowDecoder *rowcodec.ChunkDecoder
	edgeRetFields  []*types.FieldType
	edgeChunk      *chunk.Chunk
	eliminateEdge  bool
	destSchema     *expression.Schema
	destTableInfo  *model.TableInfo
	destRowDecoder *rowcodec.ChunkDecoder
	destRetFields  []*types.FieldType
	destChunk      *chunk.Chunk
	eliminateDest  bool

	pendingTasks sync.WaitGroup

	// Channel to send vertex identifiers.
	childChunkCh chan *chunk.Chunk
	childErr     chan error
	results      chan *chunk.Chunk
	die          chan struct{}

	stats GraphEdgeScanExecutorStats
}

// Open initializes necessary variables for using this executor.
func (e *GraphEdgeScanExecutor) Open(ctx context.Context) error {
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	snapshotTS := e.startTS
	var err error
	e.txn, err = e.ctx.Txn(false)
	if err != nil {
		return err
	}
	if e.txn.Valid() && txnCtx.StartTS == txnCtx.GetForUpdateTS() {
		e.snapshot = e.txn.GetSnapshot()
	} else {
		e.snapshot = e.ctx.GetStore().GetSnapshot(kv.Version{Ver: snapshotTS})
	}

	e.eliminateEdge = e.edgeSchema.Len() == 0
	e.eliminateDest = e.destSchema.Len() == 1 && mysql.HasPriKeyFlag(e.destSchema.Columns[0].RetType.Flag)

	return e.children[0].Open(ctx)
}

func (e *GraphEdgeScanExecutor) runWorker(ctx context.Context) {
	defer e.workerWg.Done()

	for {
		select {
		case childChunk, ok := <-e.childChunkCh:
			if !ok {
				return
			}
			err := e.handleTask(ctx, childChunk)
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

func mustEncodeKey(v ...types.Datum) []byte {
	k, err := codec.EncodeKey(nil, nil, v...)
	if err != nil {
		panic(err)
	}
	return k
}

func decodeSecondInt64(data []byte) (int64, error) {
	remain, _, err := codec.DecodeOne(data)
	if err != nil {
		return 0, err
	}
	_, datum, err := codec.DecodeOne(remain)
	if err != nil {
		return 0, err
	}
	return datum.GetInt64(), nil
}

func (e *GraphEdgeScanExecutor) iterInboundEdge(vid int64, idxInfo *model.IndexInfo, f func(edgeRowHandle kv.Handle, destRowHandle kv.Handle) error) error {
	startKey := tablecodec.EncodeIndexSeekKey(e.edgeTableInfo.ID, idxInfo.ID, mustEncodeKey(types.NewIntDatum(vid)))
	endKey := tablecodec.EncodeIndexSeekKey(e.edgeTableInfo.ID, idxInfo.ID, mustEncodeKey(types.NewIntDatum(vid+1)))
	iter, err := e.snapshot.Iter(startKey, endKey)
	if err != nil {
		return err
	}
	defer iter.Close()

	for err = nil; err == nil && iter.Valid(); err = iter.Next() {
		// Index key format is t{table_id}_i{index_id}{index_values}, this prefix length is equal with tablecodec.RecordRowKeyLen.
		destVid, err := decodeSecondInt64(iter.Key()[tablecodec.RecordRowKeyLen:])
		if err != nil {
			return err
		}
		edgeRowHandle, err := tablecodec.DecodeHandleInUniqueIndexValue(iter.Value(), true)
		if err != nil {
			return err
		}
		if err := f(edgeRowHandle, kv.IntHandle(destVid)); err != nil {
			return err
		}
	}
	return nil
}

func (e *GraphEdgeScanExecutor) iterOutboundEdge(vid int64, f func(edgeRowHandle kv.Handle, edgeRowData []byte, destRowHandle kv.Handle) error) error {
	startKey := tablecodec.EncodeRowKey(e.edgeTableInfo.ID, mustEncodeKey(types.NewIntDatum(vid)))
	endKey := tablecodec.EncodeRowKey(e.edgeTableInfo.ID, mustEncodeKey(types.NewIntDatum(vid+1)))
	iter, err := e.snapshot.Iter(startKey, endKey)
	if err != nil {
		return err
	}
	defer iter.Close()
	for err = nil; err == nil && iter.Valid(); err = iter.Next() {
		edgeRowHandle, err := tablecodec.DecodeRowKey(iter.Key())
		if err != nil {
			return err
		}
		destVid, err := decodeSecondInt64(edgeRowHandle.Encoded())
		if err != nil {
			return err
		}
		if err := f(edgeRowHandle, iter.Value(), kv.IntHandle(destVid)); err != nil {
			return err
		}
	}
	return nil
}

type chunkBatch struct {
	childRows      []chunk.Row
	edgeRowHandles []kv.Handle
	edgeRowData    [][]byte
	destRowHandles []kv.Handle
	destRowData    [][]byte

	e *GraphEdgeScanExecutor

	edgeChunk     *chunk.Chunk
	destChunk     *chunk.Chunk
	resultChunk   *chunk.Chunk
	resultChunkCh chan<- *chunk.Chunk
}

func (b *chunkBatch) append(
	ctx context.Context,
	childRow chunk.Row,
	edgeRowHandle kv.Handle,
	destRowHandle kv.Handle,
) error {
	b.childRows = append(b.childRows, childRow)
	b.edgeRowHandles = append(b.edgeRowHandles, edgeRowHandle)
	b.destRowHandles = append(b.destRowHandles, destRowHandle)
	if len(b.childRows) < chunkBatchSize {
		return nil
	}
	return b.flush(ctx)
}

func (b *chunkBatch) appendWithEdgeRowData(
	ctx context.Context,
	childRow chunk.Row,
	edgeRowHandle kv.Handle,
	edgeRowData []byte,
	destRowHandle kv.Handle,
) error {
	b.childRows = append(b.childRows, childRow)
	b.edgeRowHandles = append(b.edgeRowHandles, edgeRowHandle)
	b.edgeRowData = append(b.edgeRowData, edgeRowData)
	b.destRowHandles = append(b.destRowHandles, destRowHandle)
	if len(b.childRows) < chunkBatchSize {
		return nil
	}
	return b.flush(ctx)
}

func (b *chunkBatch) rows() int {
	return len(b.childRows)
}

func (b *chunkBatch) reset() {
	b.resultChunk.Reset()
	b.childRows = b.childRows[:0]
	b.edgeRowHandles = b.edgeRowHandles[:0]
	b.edgeRowData = b.edgeRowData[:0]
	b.destRowHandles = b.destRowHandles[:0]
	b.destRowData = b.destRowData[:0]
}

func (b *chunkBatch) flush(ctx context.Context) error {
	if b.rows() == 0 {
		return nil
	}

	e := b.e
	if !e.eliminateEdge && len(b.edgeRowData) < len(b.edgeRowHandles) {
		var keys []kv.Key
		for _, h := range b.edgeRowHandles {
			keys = append(keys, tablecodec.EncodeRowKeyWithHandle(e.edgeTableInfo.ID, h))
		}
		values, err := b.e.snapshot.BatchGet(ctx, keys)
		if err != nil {
			return err
		}
		b.edgeRowData = b.edgeRowData[:0]
		for _, key := range keys {
			val, ok := values[string(hack.String(key))]
			if !ok {
				return errors.Errorf("value for key %#v not found", key)
			}
			b.edgeRowData = append(b.edgeRowData, val)
		}
	}
	if !e.eliminateDest && len(b.destRowData) < len(b.destRowHandles) {
		var keys []kv.Key
		for _, h := range b.destRowHandles {
			keys = append(keys, tablecodec.EncodeRowKeyWithHandle(e.destTableInfo.ID, h))
		}
		values, err := b.e.snapshot.BatchGet(ctx, keys)
		if err != nil {
			return err
		}
		b.destRowData = b.destRowData[:0]
		for _, key := range keys {
			val, ok := values[string(hack.String(key))]
			if !ok {
				return errors.Errorf("value for key %#v not found", key)
			}
			b.destRowData = append(b.destRowData, val)
		}
	}

	childCols := b.childRows[0].Len()
	edgeCols := b.e.edgeSchema.Len()
	for i := 0; i < len(b.childRows); i++ {
		b.resultChunk.AppendPartialRow(0, b.childRows[i])

		if !e.eliminateEdge {
			b.edgeChunk.Reset()
			e.edgeRowDecoder.DecodeToChunk(b.edgeRowData[i], b.edgeRowHandles[i], b.edgeChunk)
			b.resultChunk.AppendPartialRow(childCols, b.edgeChunk.GetRow(0))
		}

		if !e.eliminateDest {
			b.destChunk.Reset()
			e.destRowDecoder.DecodeToChunk(b.destRowData[i], b.destRowHandles[i], b.destChunk)
			b.resultChunk.AppendPartialRow(childCols+edgeCols, b.destChunk.GetRow(0))
		} else {
			b.resultChunk.AppendInt64(childCols+edgeCols, b.destRowHandles[i].IntValue())
		}
	}

	select {
	case b.resultChunkCh <- b.resultChunk.CopyConstruct():
		b.reset()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *GraphEdgeScanExecutor) newChunkBatch() *chunkBatch {
	return &chunkBatch{
		e:             e,
		edgeChunk:     e.edgeChunk.CopyConstruct(),
		destChunk:     e.destChunk.CopyConstruct(),
		resultChunk:   chunk.New(e.base().retFieldTypes, chunkBatchSize, chunkBatchSize),
		resultChunkCh: e.results,
	}
}

func (e *GraphEdgeScanExecutor) handleTask(ctx context.Context, childChunk *chunk.Chunk) error {
	startTime := time.Now()
	edgeScanRows := 0
	defer func() {
		cost := time.Since(startTime)
		e.stats.totalTaskTime.Add(int64(cost))
		e.stats.edgeScanRows.Add(int64(edgeScanRows))
		if e.stats.maxTaskTime.Load() < int64(cost) {
			e.stats.maxTaskTime.Add(int64(cost))
		}
		e.pendingTasks.Done()
	}()

	e.stats.taskNum.Inc()

	lastVidIdx := 0
	childColLen := len(e.schema.Columns) - len(e.edgeSchema.Columns) - len(e.destSchema.Columns)
	for i := 0; i < childColLen; i++ {
		if mysql.HasPriKeyFlag(e.schema.Columns[i].GetType().Flag) {
			lastVidIdx = i
		}
	}
	idxInfo := e.edgeTableInfo.FindIndexByName(strings.ToLower(mysql.GraphEdgeKeyName))
	inboundChunkBatch := e.newChunkBatch()
	outboundChunkBatch := e.newChunkBatch()

	for i := 0; i < childChunk.NumRows(); i++ {
		childRow := childChunk.GetRow(i)
		vid := childRow.GetInt64(lastVidIdx)

		var iterErr error
		switch e.direction {
		case ast.GraphEdgeDirectionIn:
			iterErr = e.iterInboundEdge(vid, idxInfo, func(edgeRowHandle kv.Handle, destRowHandle kv.Handle) error {
				return inboundChunkBatch.append(ctx, childRow, edgeRowHandle, destRowHandle)
			})
		case ast.GraphEdgeDirectionOut:
			iterErr = e.iterOutboundEdge(vid, func(edgeRowHandle kv.Handle, edgeRowData []byte, destRowHandle kv.Handle) error {
				return outboundChunkBatch.appendWithEdgeRowData(ctx, childRow, edgeRowHandle, edgeRowData, destRowHandle)
			})
		case ast.GraphEdgeDirectionBoth:
			iterErr = e.iterInboundEdge(vid, idxInfo, func(edgeRowHandle kv.Handle, destRowHandle kv.Handle) error {
				return inboundChunkBatch.append(ctx, childRow, edgeRowHandle, destRowHandle)
			})
			if iterErr == nil {
				iterErr = e.iterOutboundEdge(vid, func(edgeRowHandle kv.Handle, edgeRowData []byte, destRowHandle kv.Handle) error {
					return outboundChunkBatch.appendWithEdgeRowData(ctx, childRow, edgeRowHandle, edgeRowData, destRowHandle)
				})
			}
		}
		if iterErr != nil {
			return iterErr
		}
	}
	if err := inboundChunkBatch.flush(ctx); err != nil {
		return err
	}
	if err := outboundChunkBatch.flush(ctx); err != nil {
		return err
	}
	return nil
}

func (e *GraphEdgeScanExecutor) fetchFromChild(ctx context.Context) {
	start := time.Now()
	defer func() {
		e.workerWg.Done()
		e.pendingTasks.Wait()
		close(e.results)
		e.stats.fetchRootTime.Add(int64(time.Since(start)))
	}()

	for {
		select {
		case <-e.die:
			return
		default:
			chk := newFirstChunk(e.children[0])
			if err := Next(ctx, e.children[0], chk); err != nil {
				e.childErr <- err
				return
			}
			if chk.NumRows() == 0 {
				return
			}
			e.pushTask(chk)
		}
	}
}

func (e *GraphEdgeScanExecutor) pushTask(chk *chunk.Chunk) {
	start := time.Now()
	e.pendingTasks.Add(1)
	e.childChunkCh <- chk
	e.stats.pushTaskTime.Add(int64(time.Since(start)))
}

func (e *GraphEdgeScanExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.prepared {
		e.workerWg.Add(1)
		go e.fetchFromChild(ctx)
		e.stats.concurrency = e.concurrency
		e.workerWg.Add(e.concurrency)
		for i := 0; i < e.concurrency; i++ {
			go e.runWorker(ctx)
		}
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
