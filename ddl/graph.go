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

package ddl

import (
	"fmt"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/dbterror"
)

func onCreateGraph(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID

	graphInfo := &model.GraphInfo{}
	if err := job.DecodeArgs(graphInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, err
	}
	graphInfo.State = model.StateNone

	err := checkGraphNotExists(d, t, schemaID, graphInfo.Name.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrGraphExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, err
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, err
	}

	switch graphInfo.State {
	case model.StateNone:
		graphInfo.State = model.StatePublic
		if err := createGraphWithCheck(t, job, schemaID, graphInfo); err != nil {
			return ver, err
		}
		job.State = model.JobStateDone
		job.SchemaState = model.StatePublic
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("graph", graphInfo.State)
	}
}

func checkGraphNotExists(d *ddlCtx, t *meta.Meta, schemaID int64, graphName string) error {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoCache.GetLatest()
	if is.SchemaMetaVersion() == currVer {
		return checkGraphNotExistsFromInfoSchema(is, schemaID, graphName)
	}
	return checkGraphNotExistsFromStore(t, schemaID, graphName)
}

func checkGraphNotExistsFromInfoSchema(is infoschema.InfoSchema, schemaID int64, graphName string) error {
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
	}
	if _, ok := is.GraphByName(schema.Name, model.NewCIStr(graphName)); ok {
		return infoschema.ErrGraphExists.GenWithStackByArgs(graphName)
	}
	return nil
}

func checkGraphNotExistsFromStore(t *meta.Meta, schemaID int64, graphName string) error {
	graphs, err := t.ListGraphs(schemaID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		return err
	}
	for _, graph := range graphs {
		if graph.Name.L == graphName {
			return infoschema.ErrGraphExists.GenWithStackByArgs(graphName)
		}
	}
	return nil
}

func createGraphWithCheck(t *meta.Meta, job *model.Job, schemaID int64, graphInfo *model.GraphInfo) error {
	err := checkGraphInfoValid(graphInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return err
	}
	return t.CreateGraph(schemaID, graphInfo)
}

func onDropGraph(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var graphID int64
	if err := job.DecodeArgs(&graphID); err != nil {
		return ver, err
	}
	graphInfo, err := checkGraphExistAndCancelNonExistJob(t, job, graphID)
	if err != nil {
		return ver, err
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, err
	}

	switch graphInfo.State {
	case model.StatePublic:
		// public -> write only
		graphInfo.State = model.StateWriteOnly
		err = t.UpdateGraph(job.SchemaID, graphInfo)
		if err != nil {
			return ver, err
		}
		// Update the job state when all affairs done.
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> delete only
		graphInfo.State = model.StateDeleteOnly
		err = t.UpdateGraph(job.SchemaID, graphInfo)
		if err != nil {
			return ver, err
		}
		// Update the job state when all affairs done.
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> none
		graphInfo.State = model.StateNone
		err = t.UpdateGraph(job.SchemaID, graphInfo)
		if err != nil {
			return ver, err
		}
		if err = t.DropGraph(job.SchemaID, graphInfo.ID); err != nil {
			break
		}
		// Finish this job.
		job.State = model.JobStateDone
		job.SchemaState = model.StateNone
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("graph", graphInfo.State)
	}

	return ver, nil
}

func checkGraphExistAndCancelNonExistJob(t *meta.Meta, job *model.Job, graphID int64) (*model.GraphInfo, error) {
	graphInfo, err := t.GetGraph(job.SchemaID, graphID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			err = infoschema.ErrDatabaseNotExists.GenWithStackByArgs(fmt.Sprintf("(Schema ID %d)", job.SchemaID))
		} else if meta.ErrGraphNotExists.Equal(err) {
			err = infoschema.ErrGraphNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", job.SchemaID),
				fmt.Sprintf("(Graph ID %d)", graphID),
			)
		}
		job.State = model.JobStateCancelled
		return nil, err
	}
	return graphInfo, nil
}
