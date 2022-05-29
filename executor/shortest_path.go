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

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
)

// ShortestPathExec is the executor to compute the shortest path.
type ShortestPathExec struct {
	baseExecutor

	SrcVertex *plannercore.Vertex
	DstVertex *plannercore.Vertex
	Path      *plannercore.VariableLengthPath
}

// Open implements the Executor Open interface.
func (e *ShortestPathExec) Open(ctx context.Context) error {
	return nil
}

// Next implements the Executor Next interface.
func (e *ShortestPathExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	return nil
}
