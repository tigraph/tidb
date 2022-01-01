package tables

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type GraphCommon struct {
	TableCommon
}

func (t *GraphCommon) recordKey(r []types.Datum) kv.Key {
	switch t.meta.Type {
	case model.TableTypeIsVertex:
		vertexID := r[0].GetInt64()
		return tablecodec.EncodeGraphVertex(vertexID, t.tableID)
	case model.TableTypeIsEdge:
		srcVertexID := r[0].GetInt64()
		dstVertexID := r[1].GetInt64()
		return tablecodec.EncodeGraphOutEdge(srcVertexID, dstVertexID, t.tableID)
	}
	panic("unreachable")
}

func RecordKeyFromHandle(h kv.Handle, tid int64, tp model.TableType) (kv.Key, error) {
	switch tp {
	case model.TableTypeIsVertex:
		return tablecodec.EncodeGraphVertex(h.IntValue(), tid), nil
	case model.TableTypeIsEdge:
		if h.NumCols() != 2 {
			return nil, nil
		}
		_, src, err := codec.DecodeOne(h.EncodedCol(0))
		if err != nil {
			return nil, err
		}
		_, dst, err := codec.DecodeOne(h.EncodedCol(1))
		if err != nil {
			return nil, err
		}
		srcVertexID := src.GetInt64()
		dstVertexID := dst.GetInt64()
		return tablecodec.EncodeGraphOutEdge(srcVertexID, dstVertexID, tid), nil
	}
	panic("unreachable")
}

func (t *GraphCommon) RecordKeyFromHandle(h kv.Handle) (kv.Key, error) {
	return RecordKeyFromHandle(h, t.tableID, t.meta.Type)
}

func (t *GraphCommon) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	key, err := t.RecordKeyFromHandle(h)
	if err != nil {
		return err
	}
	return txn.Delete(key)
}

func (t *GraphCommon) AddRecord(sctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (_ kv.Handle, err error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, err
	}

	var opt table.AddRecordOpt
	for _, fn := range opts {
		fn.ApplyOn(&opt)
	}
	tblInfo := t.Meta()
	txn.CacheTableInfo(t.physicalTableID, tblInfo)

	var colIDs []int64
	var row []types.Datum
	if recordCtx, ok := sctx.Value(addRecordCtxKey).(*CommonAddRecordCtx); ok {
		colIDs = recordCtx.colIDs[:0]
		row = recordCtx.row[:0]
	} else {
		colIDs = make([]int64, 0, len(r))
		row = make([]types.Datum, 0, len(r))
	}
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	sessVars := sctx.GetSessionVars()
	for _, col := range t.WritableCols() {
		var value types.Datum
		if col.ChangeStateInfo != nil && col.State != model.StatePublic {
			// TODO: Check overflow or ignoreTruncate.
			value, err = table.CastValue(sctx, r[col.DependencyColumnOffset], col.ColumnInfo, false, false)
			if err != nil {
				return nil, err
			}
			if len(r) < len(t.WritableCols()) {
				r = append(r, value)
			} else {
				r[col.Offset] = value
			}
			row = append(row, value)
			colIDs = append(colIDs, col.ID)
			continue
		}
		if col.State != model.StatePublic &&
			// Update call `AddRecord` will already handle the write only column default value.
			// Only insert should add default value for write only column.
			!opt.IsUpdate {
			// If col is in write only or write reorganization state, we must add it with its default value.
			value, err = table.GetColOriginDefaultValue(sctx, col.ToInfo())
			if err != nil {
				return nil, err
			}
			// add value to `r` for dirty db in transaction.
			// Otherwise when update will panic cause by get value of column in write only state from dirty db.
			if col.Offset < len(r) {
				r[col.Offset] = value
			} else {
				r = append(r, value)
			}
		} else {
			value = r[col.Offset]
		}
		if !t.canSkip(col, &value) {
			colIDs = append(colIDs, col.ID)
			row = append(row, value)
		}
	}
	writeBufs := sessVars.GetWriteStmtBufs()
	adjustRowValuesBuf(writeBufs, len(row))
	key := t.recordKey(r)
	logutil.BgLogger().Debug("addRecord",
		zap.Stringer("key", key))
	sc, rd := sessVars.StmtCtx, &sessVars.RowEncoder
	writeBufs.RowValBuf, err = tablecodec.EncodeRow(sc, row, colIDs, writeBufs.RowValBuf, writeBufs.AddRowValues, rd)
	if err != nil {
		return nil, err
	}
	value := writeBufs.RowValBuf

	err = memBuffer.Set(key, value)
	if err != nil {
		return nil, err
	}
	memBuffer.Release(sh)
	sc.AddAffectedRows(1)
	return nil, nil
}
