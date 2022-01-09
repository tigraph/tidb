package executor

import (
	"context"
	"runtime"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

type GraphAnyShortestExec struct {
	baseExecutor

	srcTableInfo  *model.TableInfo
	dstTableInfo  *model.TableInfo
	edgeTableInfo *model.TableInfo
	srcRowDecoder *rowcodec.ChunkDecoder
	dstRowDecoder *rowcodec.ChunkDecoder

	startTS  uint64
	snapshot kv.Snapshot

	computeStart  atomic.Bool
	resultChunkCh chan *chunk.Chunk

	execCtx    context.Context
	execCancel context.CancelFunc
}

func (e *GraphAnyShortestExec) Open(ctx context.Context) error {
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	snapshotTS := e.startTS
	var err error
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	if txn.Valid() && txnCtx.StartTS == txnCtx.GetForUpdateTS() {
		e.snapshot = txn.GetSnapshot()
	} else {
		e.snapshot = e.ctx.GetStore().GetSnapshot(kv.Version{Ver: snapshotTS})
	}
	if err := e.children[0].Open(ctx); err != nil {
		return err
	}
	if err := e.children[1].Open(ctx); err != nil {
		return err
	}
	e.execCtx, e.execCancel = context.WithCancel(ctx)
	e.resultChunkCh = make(chan *chunk.Chunk, 1024)
	e.srcRowDecoder = NewRowDecoder(e.ctx, e.children[0].Schema(), e.srcTableInfo)
	e.dstRowDecoder = NewRowDecoder(e.ctx, e.children[0].Schema(), e.dstTableInfo)
	return nil
}

func (e *GraphAnyShortestExec) iterOutboundEdge(srcID int64, f func(dstID int64) error) error {
	startKey := tablecodec.EncodeRowKey(e.edgeTableInfo.ID, mustEncodeKey(types.NewIntDatum(srcID)))
	endKey := tablecodec.EncodeRowKey(e.edgeTableInfo.ID, mustEncodeKey(types.NewIntDatum(srcID+1)))
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
		dstID, err := decodeSecondInt64(edgeRowHandle.Encoded())
		if err != nil {
			return err
		}
		if err := f(dstID); err != nil {
			return err
		}
	}
	return nil
}

func (e *GraphAnyShortestExec) deliverRes(ctx context.Context, srcID, dstID int64, shortestPath []int64, unreachable bool) error {
	resultChunk := chunk.New(e.base().retFieldTypes, 1, 1)

	sb := &strings.Builder{}
	if unreachable {
		sb.WriteString("Unreachable")
	} else {
		sb.WriteByte('[')
		for i, id := range shortestPath {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(strconv.Itoa(int(id)))
		}
		sb.WriteByte(']')
	}
	pathStr := sb.String()

	srcRowKey := tablecodec.EncodeRowKeyWithHandle(e.srcTableInfo.ID, kv.IntHandle(srcID))
	dstRowKey := tablecodec.EncodeRowKeyWithHandle(e.dstTableInfo.ID, kv.IntHandle(dstID))
	srcRowData, err := e.snapshot.Get(ctx, srcRowKey)
	if err != nil {
		return err
	}
	dstRowData, err := e.snapshot.Get(ctx, dstRowKey)
	if err != nil {
		return err
	}
	srcChunk := chunk.New(e.children[0].base().retFieldTypes, 1, 1)
	dstChunk := chunk.New(e.children[1].base().retFieldTypes, 1, 1)
	if err := e.srcRowDecoder.DecodeToChunk(srcRowData, kv.IntHandle(srcID), srcChunk); err != nil {
		return err
	}
	if err := e.dstRowDecoder.DecodeToChunk(dstRowData, kv.IntHandle(dstID), dstChunk); err != nil {
		return err
	}

	srcRow := srcChunk.GetRow(0)
	dstRow := dstChunk.GetRow(0)
	resultChunk.AppendPartialRow(0, srcRow)
	resultChunk.AppendString(srcRow.Len(), pathStr)
	resultChunk.AppendPartialRow(srcRow.Len()+1, dstRow)

	select {
	case e.resultChunkCh <- resultChunk:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *GraphAnyShortestExec) computeShortest(ctx context.Context, srcID int64, dstIDs map[int64]struct{}) error {
	prevIDs := make(map[int64]int64)
	queue := []int64{srcID}
	prevIDs[srcID] = -1

	getShortestPath := func(dstID int64) []int64 {
		paths := []int64{dstID}
		curID := dstID
		for prevIDs[curID] != -1 {
			paths = append(paths, prevIDs[curID])
			curID = prevIDs[curID]
		}
		for i, j := 0, len(paths)-1; i < j; i, j = i+1, j-1 {
			paths[i], paths[j] = paths[j], paths[i]
		}
		return paths
	}

	for len(queue) > 0 && len(dstIDs) > 0 {
		l := len(queue)
		var first int64
		for i := 0; i < l; i++ {
			first, queue = queue[0], queue[1:]
			if err := e.iterOutboundEdge(first, func(dstID int64) error {
				if _, exist := prevIDs[dstID]; !exist {
					prevIDs[dstID] = first
					queue = append(queue, dstID)
				}

				if _, ok := dstIDs[dstID]; ok {
					if err := e.deliverRes(ctx, srcID, dstID, getShortestPath(dstID), false); err != nil {
						return err
					}
					delete(dstIDs, dstID)
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}
	if len(dstIDs) > 0 {
		for dstID := range dstIDs {
			if err := e.deliverRes(ctx, srcID, dstID, nil, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *GraphAnyShortestExec) computeShortestAll() {
	defer close(e.resultChunkCh)

	dstIDIdx := -1
	for i, col := range e.children[1].Schema().Columns {
		if mysql.HasPriKeyFlag(col.GetType().Flag) {
			dstIDIdx = i
			break
		}
	}
	if dstIDIdx < 0 {
		return
	}
	dstIDs := make(map[int64]struct{})
	chk := newFirstChunk(e.children[1])
loop1:
	for {
		select {
		case <-e.execCtx.Done():
			return
		default:
			chk.Reset()
			if err := Next(e.execCtx, e.children[1], chk); err != nil {
				return
			}
			if chk.NumRows() == 0 {
				break loop1
			}
			for i := 0; i < chk.NumRows(); i++ {
				dstIDs[chk.GetRow(i).GetInt64(dstIDIdx)] = struct{}{}
			}
		}
	}

	srcIDIdx := -1
	for i, col := range e.children[0].Schema().Columns {
		if mysql.HasPriKeyFlag(col.GetType().Flag) {
			srcIDIdx = i
			break
		}
	}
	if srcIDIdx < 0 {
		return
	}
	pool := utils.NewWorkerPool(uint(runtime.NumCPU()), "compute shortest")
	eg, gCtx := errgroup.WithContext(e.execCtx)
loop2:
	for {
		select {
		case <-e.execCtx.Done():
			return
		default:
			chk.Reset()
			if err := Next(e.execCtx, e.children[0], chk); err != nil {
				return
			}
			if chk.NumRows() == 0 {
				break loop2
			}
			for i := 0; i < chk.NumRows(); i++ {
				srcID := chk.GetRow(i).GetInt64(srcIDIdx)
				pool.ApplyOnErrorGroup(eg, func() error {
					newDstIDs := make(map[int64]struct{}, len(dstIDs))
					for dstID := range dstIDs {
						newDstIDs[dstID] = struct{}{}
					}
					return e.computeShortest(gCtx, srcID, newDstIDs)
				})
			}
		}
	}
	_ = eg.Wait()
}

func (e *GraphAnyShortestExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.computeStart.CAS(false, true) {
		go e.computeShortestAll()
	}
	req.Reset()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case chk, ok := <-e.resultChunkCh:
		if !ok {
			return nil
		}
		req.SwapColumns(chk)
		return nil
	}
}

func (e *GraphAnyShortestExec) Close() error {
	e.children[0].Close()
	e.children[1].Close()
	e.execCancel()
	return nil
}
