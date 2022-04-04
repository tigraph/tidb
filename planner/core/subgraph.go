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

package core

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/slicesext"
	"golang.org/x/exp/maps"
)

// vertexVar represents a vertex variable.
type vertexVar struct {
	name      model.CIStr
	anonymous bool
	tables    []*model.GraphTable
}

func (v *vertexVar) copy() *vertexVar {
	nv := *v
	return &nv
}

// filterTables filter tables which match the label filter.
func (v *vertexVar) filterTables(labels []model.CIStr) {
	v.tables = slicesext.FilterFunc(v.tables, func(tbl *model.GraphTable) bool {
		return slicesext.ContainsFunc(labels, func(label model.CIStr) bool {
			return tbl.Label.Equal(label)
		})
	})
}

// edgeVar represents an edge variable.
type edgeVar struct {
	name       model.CIStr
	anonymous  bool
	tables     []*model.GraphTable
	srcVarName string
	dstVarName string
	// anyDirected indicates the source vertex and destination vertex can
	// swap position when they are connected with this edge.
	anyDirected bool
	group       *edgeGroup
}

func (e *edgeVar) copy() *edgeVar {
	ne := *e
	return &ne
}

func (e *edgeVar) isGroup() bool {
	return e.group != nil
}

// edgeGroup holds group information for a group edge variable.
// See https://pgql-lang.org/spec/1.4/#group-variables for more details.
type edgeGroup struct {
	tp        ast.PathPatternType
	topk      uint64
	hopSrcVar *vertexVar
	hopDstVar *vertexVar
	minHops   uint64
	maxHops   uint64
	where     ast.ExprNode
	cost      ast.ExprNode
}

type subgraph struct {
	graphInfo    *model.GraphInfo
	vertexLabels []model.CIStr
	edgeLabels   []model.CIStr
	vertexVars   map[string]*vertexVar
	edgeVars     map[string]*edgeVar
}

func newSubgraph(graphInfo *model.GraphInfo) *subgraph {
	return &subgraph{
		graphInfo:    graphInfo,
		vertexLabels: graphInfo.VertexLabels(),
		edgeLabels:   graphInfo.EdgeLabels(),
		vertexVars:   make(map[string]*vertexVar),
		edgeVars:     make(map[string]*edgeVar),
	}
}

func (s *subgraph) clone() *subgraph {
	ns := newSubgraph(s.graphInfo)
	ns.vertexVars = maps.Clone(s.vertexVars)
	ns.edgeVars = maps.Clone(s.edgeVars)
	return ns
}

func (s *subgraph) addVertex(astVar *ast.VariableSpec) {
	varName := astVar.Name
	if v, ok := s.vertexVars[varName.L]; ok {
		if len(astVar.Labels) > 0 {
			v.filterTables(astVar.Labels)
		}
	} else {
		s.vertexVars[varName.L] = s.newVertex(astVar)
	}
}

func (s *subgraph) addEdge(astVar *ast.VariableSpec, srcVarName, dstVarName string, anyDirected bool) {
	varName := astVar.Name
	if _, ok := s.edgeVars[varName.L]; ok {
		panic(fmt.Sprintf("subgraph: duplicate edge variable %v", varName))
	}
	labels := astVar.Labels
	if len(labels) == 0 {
		labels = s.edgeLabels
	}
	s.edgeVars[varName.L] = &edgeVar{
		name:        varName,
		anonymous:   astVar.Anonymous,
		tables:      s.graphInfo.EdgeTablesByLabels(labels...),
		srcVarName:  srcVarName,
		dstVarName:  dstVarName,
		anyDirected: anyDirected,
	}
}

func (s *subgraph) addGroupEdge(astVar *ast.VariableSpec, srcVarName, dstVarName string, anyDirected bool, group *edgeGroup) {
	varName := astVar.Name
	s.addEdge(astVar, srcVarName, dstVarName, anyDirected)
	ev := s.edgeVars[varName.L]
	ev.group = group
}

func (s *subgraph) newVertex(astVar *ast.VariableSpec) *vertexVar {
	varName := astVar.Name
	labels := astVar.Labels
	if len(labels) == 0 {
		labels = s.vertexLabels
	}
	return &vertexVar{
		name:      varName,
		anonymous: astVar.Anonymous,
		tables:    s.graphInfo.VertexTablesByLabels(labels...),
	}
}

// propagate generates multiple simple subgraphs. The union matching results of multiple subgraphs is equivalent to
// the original subgraph matching results.
// A simple subgraph has the following properties:
// 1. All of its simple singleton variables holds *exactly one* graph table.
// 2. A singleton edge variable is always directed if source key and destination key reference different vertex tables.
func (s *subgraph) propagate() []*subgraph {
	ctx := &propagateCtx{
		parent: s.clone(),
		child:  newSubgraph(s.graphInfo),
	}
	propagateSubgraph(ctx)
	return ctx.children
}

type propagateCtx struct {
	parent   *subgraph
	child    *subgraph
	children []*subgraph
}

func propagateSubgraph(ctx *propagateCtx) {
	if len(ctx.parent.edgeVars) == 0 {
		if len(ctx.parent.vertexVars) == 0 {
			ctx.children = append(ctx.children, ctx.child.clone())
			return
		}
		var vv *vertexVar
		for _, vv = range ctx.parent.vertexVars {
			break
		}
		propagateVertex(ctx, vv)
		return
	}

	var ev *edgeVar
	for _, ev = range ctx.parent.edgeVars {
		break
	}
	propagateEdge(ctx, ev)
}

func propagateEdge(ctx *propagateCtx, ev *edgeVar) {
	srcVar, ok := ctx.child.vertexVars[ev.srcVarName]
	if !ok {
		vv := ctx.parent.vertexVars[ev.srcVarName]
		propagateVertex(ctx, vv)
		return
	}
	dstVar, ok := ctx.child.vertexVars[ev.dstVarName]
	if !ok {
		vv := ctx.parent.vertexVars[ev.dstVarName]
		propagateVertex(ctx, vv)
		return
	}
	srcTblName := srcVar.tables[0].Name
	dstTblName := dstVar.tables[0].Name

	if ev.isGroup() {
		propagateEdgeGroup(ctx, ev, srcTblName, dstTblName)
		return
	}

	delete(ctx.parent.edgeVars, ev.name.L)
	defer func() { ctx.parent.edgeVars[ev.name.L] = ev }()
	for _, tbl := range ev.tables {
		if tbl.Source.Name.Equal(srcTblName) && tbl.Destination.Name.Equal(dstTblName) ||
			ev.anyDirected && tbl.Source.Name.Equal(dstTblName) && tbl.Destination.Name.Equal(srcTblName) {
			nv := ev.copy()
			nv.tables = []*model.GraphTable{tbl}
			if nv.anyDirected && !srcTblName.Equal(dstTblName) {
				if tbl.Source.Name.Equal(dstTblName) {
					nv.srcVarName = dstVar.name.L
					nv.dstVarName = srcVar.name.L
				}
				nv.anyDirected = false
			}
			ctx.child.edgeVars[ev.name.L] = nv
			propagateSubgraph(ctx)
			delete(ctx.child.edgeVars, ev.name.L)
		}
	}
}

func propagateEdgeGroup(ctx *propagateCtx, ev *edgeVar, srcTblName, dstTblName model.CIStr) {
	eg := ev.group
	if eg.minHops > eg.maxHops {
		return
	}

	queue := []model.CIStr{srcTblName}
	visited := set.NewStringSet(srcTblName.L)
	minHops := -1
bfsLoop:
	for hops := 0; len(queue) > 0; hops++ {
		l := len(queue)
		for i := 0; i < l; i++ {
			curTblName := queue[0]
			queue = queue[1:]
			if curTblName.Equal(dstTblName) {
				minHops = hops
				break bfsLoop
			}
			if eg.hopSrcVar != nil {
				if _, ok := slicesext.FindFunc(eg.hopSrcVar.tables, func(tbl *model.GraphTable) bool {
					return tbl.Name.Equal(curTblName)
				}); !ok {
					continue
				}
			}
			for _, tbl := range ev.tables {
				var nextTblName model.CIStr
				if tbl.Source.Name.Equal(curTblName) {
					nextTblName = tbl.Destination.Name
				} else if ev.anyDirected && tbl.Destination.Name.Equal(curTblName) {
					nextTblName = tbl.Source.Name
				} else {
					continue
				}
				if visited.Exist(nextTblName.L) {
					continue
				}
				if eg.hopDstVar != nil {
					if _, ok := slicesext.FindFunc(eg.hopDstVar.tables, func(tbl *model.GraphTable) bool {
						return tbl.Name.Equal(nextTblName)
					}); !ok {
						continue
					}
				}
				visited.Insert(nextTblName.L)
				queue = append(queue, nextTblName)
			}
		}
	}

	if minHops != -1 && uint64(minHops) <= eg.maxHops {
		nv := ev.copy()
		delete(ctx.parent.edgeVars, ev.name.L)
		ctx.child.edgeVars[ev.name.L] = nv
		propagateSubgraph(ctx)
		delete(ctx.child.edgeVars, ev.name.L)
		ctx.parent.edgeVars[ev.name.L] = ev
	}
}

func propagateVertex(ctx *propagateCtx, vv *vertexVar) {
	delete(ctx.parent.vertexVars, vv.name.L)
	for _, tbl := range vv.tables {
		nv := vv.copy()
		nv.tables = []*model.GraphTable{tbl}
		ctx.child.vertexVars[vv.name.L] = nv
		propagateSubgraph(ctx)
		delete(ctx.child.vertexVars, vv.name.L)
	}
	ctx.parent.vertexVars[vv.name.L] = vv
}

// tableAsNameForVar combines variable name and table name.
// It guarantees the two result strings are equal if and only if both varName and tblName are equal.
//
// This is achieved by encoding the varName first and then concat it with tblName.
// The encoding rule is similar to codec.EncodeBytes, but with group 4 and padding char '0':
//  [group1][marker1]...[groupN][markerN]
//  group is 4 bytes slice which is padding with '0'.
//  marker is `'0' + char count`
// For example:
//   "" -> "00000"
//   "a" -> "a0001"
//   "ab" -> "ab002"
//   "abc" -> "abc03"
//   "abcd" -> "abcd400000"
//   "abcde" -> "abcd4e0001"
func tableAsNameForVar(varName model.CIStr, tblName model.CIStr) model.CIStr {
	const (
		encGroupSize = 4
		paddingChar  = '0'
	)
	sb := strings.Builder{}
	for i := 0; i <= len(varName.O); i += encGroupSize {
		s := varName.O[i:]
		if len(s) > encGroupSize {
			s = s[:encGroupSize]
		}
		sb.WriteString(s)
		for j := 0; j < encGroupSize-len(s); j++ {
			sb.WriteByte(paddingChar)
		}
		sb.WriteByte(paddingChar + byte(len(s)))
	}
	sb.WriteByte('_')
	sb.WriteString(tblName.O)
	return model.NewCIStr(sb.String())
}
