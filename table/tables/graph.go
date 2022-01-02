package tables

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type EdgeCommon struct {
	TableCommon
}

func (t *EdgeCommon) recordKey(r []types.Datum) kv.Key {
	srcVertexID := r[t.Meta().GetSourceKeyColInfo().Offset].GetInt64()
	dstVertexID := r[t.Meta().GetDestinationKeyColInfo().Offset].GetInt64()
	return tablecodec.EncodeGraphOutEdge(srcVertexID, dstVertexID, t.tableID)
}

func (t *EdgeCommon) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	key, err := tablecodec.EncodeEdgeKeyWithHandle(t.tableID, h)
	if err != nil {
		return err
	}
	return txn.Delete(key)
}

func (t *EdgeCommon) AddRecord(sctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (_ kv.Handle, err error) {
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
