// Copyright 2016 PingCAP, Inc.
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

package infoschema

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/domainutil"
	"github.com/pingcap/tidb/util/sqlexec"
)

// Builder builds a new InfoSchema.
type Builder struct {
	is *infoSchema
	// dbInfos do not need to be copied everytime applying a diff, instead,
	// they can be copied only once over the whole lifespan of Builder.
	// This map will indicate which DB has been copied, so that they
	// don't need to be copied again.
	dirtyDB map[string]bool
	// TODO: store is only used by autoid allocators
	// detach allocators from storage, use passed transaction in the feature
	store kv.Storage

	factory func() (pools.Resource, error)
}

// ApplyDiff applies SchemaDiff to the new InfoSchema.
// Return the detail updated table IDs that are produced from SchemaDiff and an error.
func (b *Builder) ApplyDiff(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	b.is.schemaMetaVersion = diff.Version
	switch diff.Type {
	case model.ActionCreateSchema:
		return nil, b.applyCreateSchema(m, diff)
	case model.ActionDropSchema:
		return b.applyDropSchema(diff.SchemaID), nil
	case model.ActionModifySchemaCharsetAndCollate:
		return nil, b.applyModifySchemaCharsetAndCollate(m, diff)
	case model.ActionModifySchemaDefaultPlacement:
		return nil, b.applyModifySchemaDefaultPlacement(m, diff)
	case model.ActionCreatePlacementPolicy:
		return nil, b.applyCreatePolicy(m, diff)
	case model.ActionDropPlacementPolicy:
		return b.applyDropPolicy(diff.SchemaID), nil
	case model.ActionAlterPlacementPolicy:
		return b.applyAlterPolicy(m, diff)
	case model.ActionTruncateTablePartition, model.ActionTruncateTable:
		return b.applyTruncateTableOrPartition(m, diff)
	case model.ActionDropTable, model.ActionDropTablePartition:
		return b.applyDropTableOrPartition(m, diff)
	case model.ActionRecoverTable:
		return b.applyRecoverTable(m, diff)
	case model.ActionCreateTables:
		return b.applyCreateTables(m, diff)
	case model.ActionCreateGraph:
		return nil, b.applyCreateGraph(m, diff)
	case model.ActionDropGraph:
		return nil, b.applyDropGraph(diff)
	default:
		return b.applyDefaultAction(m, diff)
	}
}

func (b *Builder) applyCreateTables(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	tblIDs := make([]int64, 0, len(diff.AffectedOpts))
	if diff.AffectedOpts != nil {
		for _, opt := range diff.AffectedOpts {
			affectedDiff := &model.SchemaDiff{
				Version:     diff.Version,
				Type:        model.ActionCreateTable,
				SchemaID:    opt.SchemaID,
				TableID:     opt.TableID,
				OldSchemaID: opt.OldSchemaID,
				OldTableID:  opt.OldTableID,
			}
			affectedIDs, err := b.ApplyDiff(m, affectedDiff)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tblIDs = append(tblIDs, affectedIDs...)
		}
	}
	return tblIDs, nil
}

func (b *Builder) applyTruncateTableOrPartition(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	tblIDs, err := b.applyTableUpdate(m, diff)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, opt := range diff.AffectedOpts {
		if diff.Type == model.ActionTruncateTablePartition {
			// Reduce the impact on DML when executing partition DDL. eg.
			// While session 1 performs the DML operation associated with partition 1,
			// the TRUNCATE operation of session 2 on partition 2 does not cause the operation of session 1 to fail.
			tblIDs = append(tblIDs, opt.OldTableID)
		}
		b.applyPlacementDelete(placement.GroupID(opt.OldTableID))
		err := b.applyPlacementUpdate(placement.GroupID(opt.TableID))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return tblIDs, nil
}

func (b *Builder) applyDropTableOrPartition(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	tblIDs, err := b.applyTableUpdate(m, diff)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, opt := range diff.AffectedOpts {
		b.applyPlacementDelete(placement.GroupID(opt.OldTableID))
	}
	return tblIDs, nil
}

func (b *Builder) applyRecoverTable(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	tblIDs, err := b.applyTableUpdate(m, diff)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, opt := range diff.AffectedOpts {
		err := b.applyPlacementUpdate(placement.GroupID(opt.TableID))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return tblIDs, nil
}

func (b *Builder) applyDefaultAction(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	tblIDs, err := b.applyTableUpdate(m, diff)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, opt := range diff.AffectedOpts {
		var err error
		affectedDiff := &model.SchemaDiff{
			Version:     diff.Version,
			Type:        diff.Type,
			SchemaID:    opt.SchemaID,
			TableID:     opt.TableID,
			OldSchemaID: opt.OldSchemaID,
			OldTableID:  opt.OldTableID,
		}
		affectedIDs, err := b.ApplyDiff(m, affectedDiff)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tblIDs = append(tblIDs, affectedIDs...)
	}

	return tblIDs, nil
}

func (b *Builder) applyTableUpdate(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	roDBInfo, ok := b.is.SchemaByID(diff.SchemaID)
	if !ok {
		return nil, ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	dbInfo := b.getSchemaAndCopyIfNecessary(roDBInfo.Name.L)
	var oldTableID, newTableID int64
	switch diff.Type {
	case model.ActionCreateTable, model.ActionCreateSequence, model.ActionRecoverTable:
		newTableID = diff.TableID
	case model.ActionDropTable, model.ActionDropView, model.ActionDropSequence:
		oldTableID = diff.TableID
	case model.ActionTruncateTable, model.ActionCreateView, model.ActionExchangeTablePartition:
		oldTableID = diff.OldTableID
		newTableID = diff.TableID
	default:
		oldTableID = diff.TableID
		newTableID = diff.TableID
	}
	// handle placement rule cache
	switch diff.Type {
	case model.ActionCreateTable:
		if err := b.applyPlacementUpdate(placement.GroupID(newTableID)); err != nil {
			return nil, errors.Trace(err)
		}
	case model.ActionDropTable:
		b.applyPlacementDelete(placement.GroupID(oldTableID))
	case model.ActionTruncateTable:
		b.applyPlacementDelete(placement.GroupID(oldTableID))
		if err := b.applyPlacementUpdate(placement.GroupID(newTableID)); err != nil {
			return nil, errors.Trace(err)
		}
	case model.ActionRecoverTable:
		if err := b.applyPlacementUpdate(placement.GroupID(newTableID)); err != nil {
			return nil, errors.Trace(err)
		}
	case model.ActionExchangeTablePartition:
		if err := b.applyPlacementUpdate(placement.GroupID(newTableID)); err != nil {
			return nil, errors.Trace(err)
		}
	}
	b.copySortedTables(oldTableID, newTableID)

	tblIDs := make([]int64, 0, 2)
	// We try to reuse the old allocator, so the cached auto ID can be reused.
	var allocs autoid.Allocators
	if tableIDIsValid(oldTableID) {
		if oldTableID == newTableID && (diff.Type != model.ActionRenameTable && diff.Type != model.ActionRenameTables) &&
			diff.Type != model.ActionExchangeTablePartition &&
			// For repairing table in TiDB cluster, given 2 normal node and 1 repair node.
			// For normal node's information schema, repaired table is existed.
			// For repair node's information schema, repaired table is filtered (couldn't find it in `is`).
			// So here skip to reserve the allocators when repairing table.
			diff.Type != model.ActionRepairTable &&
			// Alter sequence will change the sequence info in the allocator, so the old allocator is not valid any more.
			diff.Type != model.ActionAlterSequence {
			oldAllocs, _ := b.is.AllocByID(oldTableID)
			allocs = filterAllocators(diff, oldAllocs)
		}

		tmpIDs := tblIDs
		if (diff.Type == model.ActionRenameTable || diff.Type == model.ActionRenameTables) && diff.OldSchemaID != diff.SchemaID {
			oldRoDBInfo, ok := b.is.SchemaByID(diff.OldSchemaID)
			if !ok {
				return nil, ErrDatabaseNotExists.GenWithStackByArgs(
					fmt.Sprintf("(Schema ID %d)", diff.OldSchemaID),
				)
			}
			oldDBInfo := b.getSchemaAndCopyIfNecessary(oldRoDBInfo.Name.L)
			tmpIDs = b.applyDropTable(oldDBInfo, oldTableID, tmpIDs)
		} else {
			tmpIDs = b.applyDropTable(dbInfo, oldTableID, tmpIDs)
		}

		if oldTableID != newTableID {
			// Update tblIDs only when oldTableID != newTableID because applyCreateTable() also updates tblIDs.
			tblIDs = tmpIDs
		}
	}
	if tableIDIsValid(newTableID) {
		// All types except DropTableOrView.
		var err error
		tblIDs, err = b.applyCreateTable(m, dbInfo, newTableID, allocs, diff.Type, tblIDs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return tblIDs, nil
}

func filterAllocators(diff *model.SchemaDiff, oldAllocs autoid.Allocators) autoid.Allocators {
	var newAllocs autoid.Allocators
	switch diff.Type {
	case model.ActionRebaseAutoID, model.ActionModifyTableAutoIdCache:
		// Only drop auto-increment allocator.
		newAllocs = oldAllocs.Filter(func(a autoid.Allocator) bool {
			tp := a.GetType()
			return tp != autoid.RowIDAllocType && tp != autoid.AutoIncrementType
		})
	case model.ActionRebaseAutoRandomBase:
		// Only drop auto-random allocator.
		newAllocs = oldAllocs.Filter(func(a autoid.Allocator) bool {
			tp := a.GetType()
			return tp != autoid.AutoRandomType
		})
	default:
		// Keep all allocators.
		newAllocs = oldAllocs
	}
	return newAllocs
}

func appendAffectedIDs(affected []int64, tblInfo *model.TableInfo) []int64 {
	affected = append(affected, tblInfo.ID)
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			affected = append(affected, def.ID)
		}
	}
	return affected
}

// copySortedTables copies sortedTables for old table and new table for later modification.
func (b *Builder) copySortedTables(oldTableID, newTableID int64) {
	if tableIDIsValid(oldTableID) {
		b.copySortedTablesBucket(tableBucketIdx(oldTableID))
	}
	if tableIDIsValid(newTableID) && newTableID != oldTableID {
		b.copySortedTablesBucket(tableBucketIdx(newTableID))
	}
}

func (b *Builder) applyCreatePolicy(m *meta.Meta, diff *model.SchemaDiff) error {
	po, err := m.GetPolicy(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if po == nil {
		return ErrPlacementPolicyExists.GenWithStackByArgs(
			fmt.Sprintf("(Policy ID %d)", diff.SchemaID),
		)
	}
	b.is.setPolicy(po)
	return nil
}

func (b *Builder) applyAlterPolicy(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	po, err := m.GetPolicy(diff.SchemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if po == nil {
		return nil, ErrPlacementPolicyExists.GenWithStackByArgs(
			fmt.Sprintf("(Policy ID %d)", diff.SchemaID),
		)
	}

	b.is.setPolicy(po)
	// TODO: return the policy related table ids
	return []int64{}, nil
}

func (b *Builder) applyCreateSchema(m *meta.Meta, diff *model.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// When we apply an old schema diff, the database may has been dropped already, so we need to fall back to
		// full load.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	b.is.schemaMap[di.Name.L] = newSchemaMetas(di)
	return nil
}

func (b *Builder) applyModifySchemaCharsetAndCollate(m *meta.Meta, diff *model.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// This should never happen.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	newDbInfo := b.getSchemaAndCopyIfNecessary(di.Name.L)
	newDbInfo.Charset = di.Charset
	newDbInfo.Collate = di.Collate
	return nil
}

func (b *Builder) applyModifySchemaDefaultPlacement(m *meta.Meta, diff *model.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// This should never happen.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	newDbInfo := b.getSchemaAndCopyIfNecessary(di.Name.L)
	newDbInfo.PlacementPolicyRef = di.PlacementPolicyRef
	return nil
}

func (b *Builder) applyDropPolicy(PolicyID int64) []int64 {
	po, ok := b.is.PolicyByID(PolicyID)
	if !ok {
		return nil
	}
	b.is.deletePolicy(po.Name.L)
	// TODO: return the policy related table ids
	return []int64{}
}

func (b *Builder) applyDropSchema(schemaID int64) []int64 {
	di, ok := b.is.SchemaByID(schemaID)
	if !ok {
		return nil
	}
	delete(b.is.schemaMap, di.Name.L)
	b.applyPlacementDelete(placement.GroupID(schemaID))

	// Copy the sortedTables that contain the table we are going to drop.
	tableIDs := make([]int64, 0, len(di.Tables))
	bucketIdxMap := make(map[int]struct{}, len(di.Tables))
	for _, tbl := range di.Tables {
		bucketIdxMap[tableBucketIdx(tbl.ID)] = struct{}{}
		// TODO: If the table ID doesn't exist.
		tableIDs = appendAffectedIDs(tableIDs, tbl)
	}
	for bucketIdx := range bucketIdxMap {
		b.copySortedTablesBucket(bucketIdx)
	}

	di = di.Clone()
	for _, id := range tableIDs {
		b.applyPlacementDelete(placement.GroupID(id))
		b.applyDropTable(di, id, nil)
	}
	return tableIDs
}

func (b *Builder) copySortedTablesBucket(bucketIdx int) {
	oldSortedTables := b.is.sortedTablesBuckets[bucketIdx]
	newSortedTables := make(sortedTables, len(oldSortedTables))
	copy(newSortedTables, oldSortedTables)
	b.is.sortedTablesBuckets[bucketIdx] = newSortedTables
}

func (b *Builder) applyCreateTable(m *meta.Meta, dbInfo *model.DBInfo, tableID int64, allocs autoid.Allocators, tp model.ActionType, affected []int64) ([]int64, error) {
	tblInfo, err := m.GetTable(dbInfo.ID, tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tblInfo == nil {
		// When we apply an old schema diff, the table may has been dropped already, so we need to fall back to
		// full load.
		return nil, ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", dbInfo.ID),
			fmt.Sprintf("(Table ID %d)", tableID),
		)
	}

	switch tp {
	case model.ActionDropTablePartition:
	case model.ActionTruncateTablePartition:
	default:
		pi := tblInfo.GetPartitionInfo()
		if pi != nil {
			for _, partition := range pi.Definitions {
				err = b.applyPlacementUpdate(placement.GroupID(partition.ID))
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if tp != model.ActionTruncateTablePartition {
		affected = appendAffectedIDs(affected, tblInfo)
	}

	// Failpoint check whether tableInfo should be added to repairInfo.
	// Typically used in repair table test to load mock `bad` tableInfo into repairInfo.
	failpoint.Inject("repairFetchCreateTable", func(val failpoint.Value) {
		if val.(bool) {
			if domainutil.RepairInfo.InRepairMode() && tp != model.ActionRepairTable && domainutil.RepairInfo.CheckAndFetchRepairedTable(dbInfo, tblInfo) {
				failpoint.Return(nil, nil)
			}
		}
	})

	ConvertCharsetCollateToLowerCaseIfNeed(tblInfo)
	ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)

	if len(allocs) == 0 {
		allocs = autoid.NewAllocatorsFromTblInfo(b.store, dbInfo.ID, tblInfo)
	} else {
		tblVer := autoid.AllocOptionTableInfoVersion(tblInfo.Version)
		switch tp {
		case model.ActionRebaseAutoID, model.ActionModifyTableAutoIdCache:
			newAlloc := autoid.NewAllocator(b.store, dbInfo.ID, tblInfo.ID, tblInfo.IsAutoIncColUnsigned(), autoid.RowIDAllocType, tblVer)
			allocs = append(allocs, newAlloc)
		case model.ActionRebaseAutoRandomBase:
			newAlloc := autoid.NewAllocator(b.store, dbInfo.ID, tblInfo.ID, tblInfo.IsAutoRandomBitColUnsigned(), autoid.AutoRandomType, tblVer)
			allocs = append(allocs, newAlloc)
		case model.ActionModifyColumn:
			// Change column attribute from auto_increment to auto_random.
			if tblInfo.ContainsAutoRandomBits() && allocs.Get(autoid.AutoRandomType) == nil {
				// Remove auto_increment allocator.
				allocs = allocs.Filter(func(a autoid.Allocator) bool {
					return a.GetType() != autoid.AutoIncrementType && a.GetType() != autoid.RowIDAllocType
				})
				newAlloc := autoid.NewAllocator(b.store, dbInfo.ID, tblInfo.ID, tblInfo.IsAutoRandomBitColUnsigned(), autoid.AutoRandomType, tblVer)
				allocs = append(allocs, newAlloc)
			}
		}
	}
	tbl, err := b.tableFromMeta(allocs, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	metas := b.is.schemaMap[dbInfo.Name.L]
	metas.tables[tblInfo.Name.L] = tbl
	bucketIdx := tableBucketIdx(tableID)
	sortedTbls := b.is.sortedTablesBuckets[bucketIdx]
	sortedTbls = append(sortedTbls, tbl)
	sort.Sort(sortedTbls)
	b.is.sortedTablesBuckets[bucketIdx] = sortedTbls

	newTbl, ok := b.is.TableByID(tableID)
	if ok {
		dbInfo.Tables = append(dbInfo.Tables, newTbl.Meta())
	}
	return affected, nil
}

// ConvertCharsetCollateToLowerCaseIfNeed convert the charset / collation of table and its columns to lower case,
// if the table's version is prior to TableInfoVersion3.
func ConvertCharsetCollateToLowerCaseIfNeed(tbInfo *model.TableInfo) {
	if tbInfo.Version >= model.TableInfoVersion3 {
		return
	}
	tbInfo.Charset = strings.ToLower(tbInfo.Charset)
	tbInfo.Collate = strings.ToLower(tbInfo.Collate)
	for _, col := range tbInfo.Columns {
		col.Charset = strings.ToLower(col.Charset)
		col.Collate = strings.ToLower(col.Collate)
	}
}

// ConvertOldVersionUTF8ToUTF8MB4IfNeed convert old version UTF8 to UTF8MB4 if config.TreatOldVersionUTF8AsUTF8MB4 is enable.
func ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo *model.TableInfo) {
	if tbInfo.Version >= model.TableInfoVersion2 || !config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 {
		return
	}
	if tbInfo.Charset == charset.CharsetUTF8 {
		tbInfo.Charset = charset.CharsetUTF8MB4
		tbInfo.Collate = charset.CollationUTF8MB4
	}
	for _, col := range tbInfo.Columns {
		if col.Version < model.ColumnInfoVersion2 && col.Charset == charset.CharsetUTF8 {
			col.Charset = charset.CharsetUTF8MB4
			col.Collate = charset.CollationUTF8MB4
		}
	}
}

func (b *Builder) applyDropTable(dbInfo *model.DBInfo, tableID int64, affected []int64) []int64 {
	bucketIdx := tableBucketIdx(tableID)
	sortedTbls := b.is.sortedTablesBuckets[bucketIdx]
	idx := sortedTbls.searchTable(tableID)
	if idx == -1 {
		return affected
	}
	if metas, ok := b.is.schemaMap[dbInfo.Name.L]; ok {
		tblInfo := sortedTbls[idx].Meta()
		delete(metas.tables, tblInfo.Name.L)
		affected = appendAffectedIDs(affected, tblInfo)
	}
	// Remove the table in sorted table slice.
	b.is.sortedTablesBuckets[bucketIdx] = append(sortedTbls[0:idx], sortedTbls[idx+1:]...)

	// The old DBInfo still holds a reference to old table info, we need to remove it.
	for i, tblInfo := range dbInfo.Tables {
		if tblInfo.ID == tableID {
			if i == len(dbInfo.Tables)-1 {
				dbInfo.Tables = dbInfo.Tables[:i]
			} else {
				dbInfo.Tables = append(dbInfo.Tables[:i], dbInfo.Tables[i+1:]...)
			}
			break
		}
	}
	return affected
}

func (b *Builder) applyPlacementDelete(id string) {
	b.is.deleteBundle(id)
}

func (b *Builder) applyPlacementUpdate(id string) error {
	bundle, err := infosync.GetRuleBundle(context.TODO(), id)
	if err != nil {
		return err
	}

	if !bundle.IsEmpty() {
		b.is.SetBundle(bundle)
	} else {
		b.applyPlacementDelete(id)
	}
	return nil
}

func (b *Builder) applyCreateGraph(m *meta.Meta, diff *model.SchemaDiff) error {
	roDBInfo, ok := b.is.SchemaByID(diff.SchemaID)
	if !ok {
		return ErrDatabaseNotExists.GenWithStackByArgs(fmt.Sprintf("(Schema ID %d)", diff.SchemaID))
	}

	graphInfo, err := m.GetGraph(roDBInfo.ID, diff.GraphID)
	if err != nil {
		if ErrTableNotExists.Equal(err) {
			err = ErrGraphNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
				fmt.Sprintf("(Graph ID %d)", diff.GraphID),
			)
		}
		return err
	}

	metas := b.is.schemaMap[roDBInfo.Name.L]
	metas.graphs[graphInfo.Name.L] = graphInfo
	b.is.sortedGraphs = append(b.is.sortedGraphs, graphInfo)
	sort.SliceStable(b.is.sortedGraphs, func(i, j int) bool {
		return b.is.sortedGraphs[i].ID < b.is.sortedGraphs[j].ID
	})

	dbInfo := b.getSchemaAndCopyIfNecessary(roDBInfo.Name.L)
	dbInfo.Graphs = append(dbInfo.Graphs, graphInfo)

	return nil
}

func (b *Builder) applyDropGraph(diff *model.SchemaDiff) error {
	roDBInfo, ok := b.is.SchemaByID(diff.SchemaID)
	if !ok {
		return ErrDatabaseNotExists.GenWithStackByArgs(fmt.Sprintf("(Schema ID %d)", diff.SchemaID))
	}

	idx := sort.Search(len(b.is.sortedGraphs), func(i int) bool {
		return b.is.sortedGraphs[i].ID >= diff.GraphID
	})
	if idx >= len(b.is.sortedGraphs) || b.is.sortedGraphs[idx].ID != diff.GraphID {
		return ErrGraphNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
			fmt.Sprintf("(Graph ID %d)", diff.GraphID),
		)
	}

	if metas, ok := b.is.schemaMap[roDBInfo.Name.L]; ok {
		delete(metas.graphs, b.is.sortedGraphs[idx].Name.L)
	}
	b.is.sortedGraphs = append(b.is.sortedGraphs[:idx], b.is.sortedGraphs[idx+1:]...)

	dbInfo := b.getSchemaAndCopyIfNecessary(roDBInfo.Name.L)
	// The old DBInfo still holds a reference to old graph info, we need to remove it.
	for i, graph := range dbInfo.Graphs {
		if graph.ID == diff.GraphID {
			dbInfo.Graphs = append(dbInfo.Graphs[:i], dbInfo.Graphs[i+1:]...)
			break
		}
	}

	return nil
}

// Build builds and returns the built infoschema.
func (b *Builder) Build() InfoSchema {
	return b.is
}

// InitWithOldInfoSchema initializes an empty new InfoSchema by copies all the data from old InfoSchema.
func (b *Builder) InitWithOldInfoSchema(oldSchema InfoSchema) *Builder {
	oldIS := oldSchema.(*infoSchema)
	b.is.schemaMetaVersion = oldIS.schemaMetaVersion
	b.copySchemasMap(oldIS)
	b.copyBundlesMap(oldIS)
	b.copyPoliciesMap(oldIS)

	copy(b.is.sortedTablesBuckets, oldIS.sortedTablesBuckets)

	b.is.sortedGraphs = make([]*model.GraphInfo, len(oldIS.sortedGraphs))
	copy(b.is.sortedGraphs, oldIS.sortedGraphs)

	return b
}

func (b *Builder) copySchemasMap(oldIS *infoSchema) {
	for k, v := range oldIS.schemaMap {
		b.is.schemaMap[k] = v
	}
}

func (b *Builder) copyBundlesMap(oldIS *infoSchema) {
	is := b.is
	for _, v := range oldIS.RuleBundles() {
		is.SetBundle(v)
	}
}

func (b *Builder) copyPoliciesMap(oldIS *infoSchema) {
	is := b.is
	for _, v := range oldIS.AllPlacementPolicies() {
		is.policyMap[v.Name.L] = v
	}
}

// getSchemaAndCopyIfNecessary creates a new schemaMetas instance when a table in the database has changed.
// It also does modifications on the new one because old schemaMetas must be read-only.
// And it will only copy the changed database once in the lifespan of the Builder.
// NOTE: please make sure the dbName is in lowercase.
func (b *Builder) getSchemaAndCopyIfNecessary(dbName string) *model.DBInfo {
	if !b.dirtyDB[dbName] {
		b.dirtyDB[dbName] = true
		oldMetas := b.is.schemaMap[dbName]
		newMetas := newSchemaMetas(oldMetas.dbInfo.Copy())
		for k, v := range oldMetas.tables {
			newMetas.tables[k] = v
		}
		for k, v := range oldMetas.graphs {
			newMetas.graphs[k] = v
		}
		b.is.schemaMap[dbName] = newMetas
		return newMetas.dbInfo
	}
	return b.is.schemaMap[dbName].dbInfo
}

// InitWithDBInfos initializes an empty new InfoSchema with a slice of DBInfo, all placement rules, and schema version.
func (b *Builder) InitWithDBInfos(dbInfos []*model.DBInfo, bundles []*placement.Bundle, policies []*model.PolicyInfo, schemaVersion int64) (*Builder, error) {
	info := b.is
	info.schemaMetaVersion = schemaVersion
	for _, bundle := range bundles {
		info.SetBundle(bundle)
	}
	// build the policies.
	for _, policy := range policies {
		info.setPolicy(policy)
	}

	for _, di := range dbInfos {
		err := b.createSchemaMetasForDB(di, b.tableFromMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Initialize virtual tables.
	for _, driver := range drivers {
		err := b.createSchemaMetasForDB(driver.DBInfo, driver.TableFromMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Sort all tables by `ID`
	for _, v := range info.sortedTablesBuckets {
		sort.Sort(v)
	}
	return b, nil
}

func (b *Builder) tableFromMeta(alloc autoid.Allocators, tblInfo *model.TableInfo) (table.Table, error) {
	ret, err := tables.TableFromMeta(alloc, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t, ok := ret.(table.CachedTable); ok {
		var tmp pools.Resource
		tmp, err = b.factory()
		if err != nil {
			return nil, errors.Trace(err)
		}

		err = t.Init(tmp.(sqlexec.SQLExecutor))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return ret, nil
}

type tableFromMetaFunc func(alloc autoid.Allocators, tblInfo *model.TableInfo) (table.Table, error)

func (b *Builder) createSchemaMetasForDB(di *model.DBInfo, tableFromMeta tableFromMetaFunc) error {
	schMetas := newSchemaMetas(di)
	b.is.schemaMap[di.Name.L] = schMetas

	for _, t := range di.Tables {
		allocs := autoid.NewAllocatorsFromTblInfo(b.store, di.ID, t)
		var tbl table.Table
		tbl, err := tableFromMeta(allocs, t)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Build table `%s`.`%s` schema failed", di.Name.O, t.Name.O))
		}
		schMetas.tables[t.Name.L] = tbl
		sortedTbls := b.is.sortedTablesBuckets[tableBucketIdx(t.ID)]
		b.is.sortedTablesBuckets[tableBucketIdx(t.ID)] = append(sortedTbls, tbl)
	}

	for _, g := range di.Graphs {
		schMetas.graphs[g.Name.L] = g
		b.is.sortedGraphs = append(b.is.sortedGraphs, g)
	}
	sort.SliceStable(b.is.sortedGraphs, func(i, j int) bool {
		return b.is.sortedGraphs[i].ID < b.is.sortedGraphs[j].ID
	})

	return nil
}

type virtualTableDriver struct {
	*model.DBInfo
	TableFromMeta tableFromMetaFunc
}

var drivers []*virtualTableDriver

// RegisterVirtualTable register virtual tables to the builder.
func RegisterVirtualTable(dbInfo *model.DBInfo, tableFromMeta tableFromMetaFunc) {
	drivers = append(drivers, &virtualTableDriver{dbInfo, tableFromMeta})
}

// NewBuilder creates a new Builder with a Handle.
func NewBuilder(store kv.Storage, factory func() (pools.Resource, error)) *Builder {
	return &Builder{
		store: store,
		is: &infoSchema{
			schemaMap:           map[string]*schemaMetas{},
			policyMap:           map[string]*model.PolicyInfo{},
			ruleBundleMap:       map[string]*placement.Bundle{},
			sortedTablesBuckets: make([]sortedTables, bucketCount),
		},
		dirtyDB: make(map[string]bool),
		factory: factory,
	}
}

func tableBucketIdx(tableID int64) int {
	return int(tableID % bucketCount)
}

func tableIDIsValid(tableID int64) bool {
	return tableID != 0
}
