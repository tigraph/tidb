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

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/slicesext"
	"golang.org/x/exp/maps"
)

// vertexVar represents a vertex variable.
type vertexVar struct {
	name      model.CIStr
	anonymous bool
	tables    []*model.VertexTable
	// labels is the original labels specified in match clause.
	// It is used to build Plan.Schema and Plan.OutputNames.
	labels []model.CIStr
}

func (v *vertexVar) copy() *vertexVar {
	nv := *v
	return &nv
}

// tableMap returns a map of all vertex tables.
func (v *vertexVar) tableMap() map[string]*model.VertexTable {
	m := make(map[string]*model.VertexTable)
	for _, tbl := range v.tables {
		m[tbl.Name.L] = tbl
	}
	return m
}

// filterTables filter tables which match the label filter.
func (v *vertexVar) filterTables(labels []model.CIStr) {
	v.tables = slicesext.FilterFunc(v.tables, func(tbl *model.VertexTable) bool {
		return slicesext.ContainsFunc(labels, func(label model.CIStr) bool {
			return tbl.Label.Equal(label)
		})
	})
}

// edgeVar represents an edge variable.
type edgeVar struct {
	name      model.CIStr
	anonymous bool
	tables    []*model.EdgeTable
	// labels is the original labels specified in match clause.
	// It is used to build Plan.Schema and Plan.OutputNames.
	labels       []model.CIStr
	srcVertexVar string
	dstVertexVar string
	// anyDirected indicates the source vertex and destination vertex can
	// swap position when they are connected with this edge.
	anyDirected bool
}

func (e *edgeVar) copy() *edgeVar {
	ne := *e
	return &ne
}

type subgraph struct {
	graphInfo  *model.GraphInfo
	vertexVars map[string]*vertexVar
	edgeVars   map[string]*edgeVar
}

func newSubgraph(graphInfo *model.GraphInfo) *subgraph {
	return &subgraph{
		graphInfo:  graphInfo,
		vertexVars: make(map[string]*vertexVar),
		edgeVars:   make(map[string]*edgeVar),
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
	labels := astVar.Labels
	if len(labels) == 0 {
		labels = s.graphInfo.VertexLabels()
	}
	if v, ok := s.vertexVars[varName.L]; ok {
		v.filterTables(labels)
	} else {
		s.vertexVars[varName.L] = &vertexVar{
			name:      varName,
			anonymous: astVar.Anonymous,
			tables:    s.graphInfo.VertexTablesByLabels(labels...),
			labels:    labels,
		}
	}
}

func (s *subgraph) addEdge(astVar *ast.VariableSpec, srcVertexVar, dstVertexVar string, anyDirected bool) {
	varName := astVar.Name
	if _, ok := s.edgeVars[varName.L]; ok {
		panic(fmt.Sprintf("subgraph: duplicate edge variable %v", varName))
	}
	labels := astVar.Labels
	if len(labels) == 0 {
		labels = s.graphInfo.EdgeLabels()
	}
	s.edgeVars[varName.L] = &edgeVar{
		name:         varName,
		anonymous:    astVar.Anonymous,
		tables:       s.graphInfo.EdgeTablesByLabels(labels...),
		labels:       labels,
		srcVertexVar: srcVertexVar,
		dstVertexVar: dstVertexVar,
		anyDirected:  anyDirected,
	}
}

// propagate propagates multiple subgraphs, with each variable in each subgraph holding exactly one table.
// For each edge variable in each result subgraph, it's always directed if source variable and destination
// variable hold different tables.
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
		delete(ctx.parent.vertexVars, vv.name.L)
		for _, tbl := range vv.tables {
			nv := vv.copy()
			nv.tables = []*model.VertexTable{tbl}
			ctx.child.vertexVars[vv.name.L] = nv
			propagateSubgraph(ctx)
			delete(ctx.child.vertexVars, vv.name.L)
		}
		ctx.parent.vertexVars[vv.name.L] = vv
		return
	}

	var ev *edgeVar
	for _, ev = range ctx.parent.edgeVars {
		break
	}

	joinEdge := func(eTbl *model.EdgeTable, srcVarName, dstVarName string) {
		if _, ok := ctx.child.vertexVars[srcVarName]; !ok {
			srcVar := ctx.parent.vertexVars[srcVarName]
			srcTbl, ok := slicesext.SearchFunc(srcVar.tables, func(vTbl *model.VertexTable) bool {
				return vTbl.Name.Equal(eTbl.Source.Name)
			})
			if !ok {
				return
			}
			nv := srcVar.copy()
			nv.tables = []*model.VertexTable{srcTbl}
			delete(ctx.parent.vertexVars, srcVarName)
			ctx.child.vertexVars[srcVarName] = nv
			defer func() {
				delete(ctx.child.vertexVars, srcVarName)
				ctx.parent.vertexVars[srcVarName] = srcVar
			}()
		}
		if _, ok := ctx.child.vertexVars[dstVarName]; !ok {
			dstVar := ctx.parent.vertexVars[dstVarName]
			dstTbl, ok := slicesext.SearchFunc(dstVar.tables, func(vTbl *model.VertexTable) bool {
				return vTbl.Name.Equal(eTbl.Destination.Name)
			})
			if !ok {
				return
			}
			nv := dstVar.copy()
			nv.tables = []*model.VertexTable{dstTbl}
			delete(ctx.parent.vertexVars, dstVarName)
			ctx.child.vertexVars[dstVarName] = nv
			defer func() {
				delete(ctx.child.vertexVars, dstVarName)
				ctx.parent.vertexVars[dstVarName] = dstVar
			}()
		}
		nv := ev.copy()
		nv.tables = []*model.EdgeTable{eTbl}
		nv.srcVertexVar = srcVarName
		nv.dstVertexVar = dstVarName
		nv.anyDirected = ev.anyDirected && eTbl.Source.Name.Equal(eTbl.Destination.Name)
		delete(ctx.parent.edgeVars, ev.name.L)
		ctx.child.edgeVars[nv.name.L] = nv
		propagateSubgraph(ctx)
		delete(ctx.child.edgeVars, nv.name.L)
		ctx.parent.edgeVars[ev.name.L] = ev
	}

	for _, eTbl := range ev.tables {
		joinEdge(eTbl, ev.srcVertexVar, ev.dstVertexVar)
		if ev.anyDirected && ev.srcVertexVar != ev.dstVertexVar && !eTbl.Source.Name.Equal(eTbl.Destination.Name) {
			joinEdge(eTbl, ev.dstVertexVar, ev.srcVertexVar)
		}
	}
}
