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
	"math"
	"sort"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/slicesext"
	"golang.org/x/exp/maps"
)

type Vertex struct {
	Name  model.CIStr
	Table *model.GraphTable
}

type VertexPairConnection interface {
	Name() model.CIStr
	SrcTblName() model.CIStr
	DstTblName() model.CIStr
	AnyDirected() bool
	SetAnyDirected(anyDirected bool)
	SelfConnected() bool
	SrcVarName() model.CIStr
	SetSrcVarName(name model.CIStr)
	DstVarName() model.CIStr
	SetDstVarName(name model.CIStr)
	Copy() VertexPairConnection
}

var (
	_ VertexPairConnection = &Edge{}
	_ VertexPairConnection = &CommonPathExpression{}
	_ VertexPairConnection = &VariableLengthPath{}
)

type baseVertexPairConnection struct {
	name        model.CIStr
	srcVarName  model.CIStr
	dstVarName  model.CIStr
	anyDirected bool
}

func (b *baseVertexPairConnection) Name() model.CIStr {
	return b.name
}

func (b *baseVertexPairConnection) SetName(name model.CIStr) {
	b.name = name
}

func (b *baseVertexPairConnection) SrcVarName() model.CIStr {
	return b.srcVarName
}

func (b *baseVertexPairConnection) SetSrcVarName(name model.CIStr) {
	b.srcVarName = name
}

func (b *baseVertexPairConnection) DstVarName() model.CIStr {
	return b.dstVarName
}

func (b *baseVertexPairConnection) SetDstVarName(name model.CIStr) {
	b.dstVarName = name
}

func (b *baseVertexPairConnection) AnyDirected() bool {
	return b.anyDirected
}

func (b *baseVertexPairConnection) SetAnyDirected(anyDirected bool) {
	b.anyDirected = anyDirected
}

type Edge struct {
	baseVertexPairConnection

	Table *model.GraphTable
}

func (e *Edge) Copy() VertexPairConnection {
	ne := *e
	return &ne
}

func (e *Edge) SrcTblName() model.CIStr {
	return e.Table.Source.Name
}

func (e *Edge) DstTblName() model.CIStr {
	return e.Table.Destination.Name
}

func (e *Edge) SelfConnected() bool {
	return false
}

type CommonPathExpression struct {
	baseVertexPairConnection

	Leftmost    *Vertex
	Rightmost   *Vertex
	Vertices    map[string]*Vertex
	Connections map[string]VertexPairConnection
	Constraints ast.ExprNode
}

func (c *CommonPathExpression) Copy() VertexPairConnection {
	nc := *c
	return &nc
}

func (c *CommonPathExpression) SrcTblName() model.CIStr {
	return c.Leftmost.Table.Name
}

func (c *CommonPathExpression) DstTblName() model.CIStr {
	return c.Rightmost.Table.Name
}

func (c *CommonPathExpression) SelfConnected() bool {
	return false
}

type PathFindingGoal int

const (
	PathFindingAll PathFindingGoal = iota
	PathFindingReaches
	PathFindingShortest
	PathFindingCheapest
)

type VariableLengthPath struct {
	baseVertexPairConnection

	srcTblName model.CIStr
	dstTblName model.CIStr

	Conns       []VertexPairConnection
	Goal        PathFindingGoal
	MinHops     int64
	MaxHops     int64
	TopK        int64
	WithTies    bool
	Constraints ast.ExprNode
	Cost        ast.ExprNode
	HopSrc      []*Vertex
	HopDst      []*Vertex
}

func (v *VariableLengthPath) Copy() VertexPairConnection {
	nv := *v
	return &nv
}

func (v *VariableLengthPath) SrcTblName() model.CIStr {
	return v.srcTblName
}

func (v *VariableLengthPath) SetSrcTblName(name model.CIStr) {
	v.srcTblName = name
}

func (v *VariableLengthPath) DstTblName() model.CIStr {
	return v.dstTblName
}

func (v *VariableLengthPath) SetDstTblName(name model.CIStr) {
	v.dstTblName = name
}

func (v *VariableLengthPath) SelfConnected() bool {
	return len(v.Conns) == 0
}

type Subgraph struct {
	Vertices    map[string]*Vertex
	Connections map[string]VertexPairConnection
}

func (s *Subgraph) Clone() *Subgraph {
	return &Subgraph{
		Vertices:    maps.Clone(s.Vertices),
		Connections: maps.Clone(s.Connections),
	}
}

type GraphVar struct {
	Name          model.CIStr
	Anonymous     bool
	PropertyNames []model.CIStr
}

type Subgraphs struct {
	Matched       []*Subgraph
	SingletonVars []*GraphVar
	GroupVars     []*GraphVar
}

type SubgraphBuilder struct {
	graph        *model.GraphInfo
	vertexLabels []model.CIStr
	edgeLabels   []model.CIStr
	macros       []*ast.PathPatternMacro
	paths        []*ast.PathPattern

	cpes          map[string][]*CommonPathExpression
	vertices      map[string][]*Vertex
	connections   map[string][]VertexPairConnection
	singletonVars []*GraphVar
	groupVars     []*GraphVar
	subgraphs     []*Subgraph
}

func NewSubgraphBuilder(graph *model.GraphInfo) *SubgraphBuilder {
	return &SubgraphBuilder{
		graph:        graph,
		vertexLabels: graph.VertexLabels(),
		edgeLabels:   graph.EdgeLabels(),
		cpes:         make(map[string][]*CommonPathExpression),
		vertices:     make(map[string][]*Vertex),
		connections:  make(map[string][]VertexPairConnection),
	}
}

func (s *SubgraphBuilder) AddPathPatterns(paths ...*ast.PathPattern) *SubgraphBuilder {
	s.paths = append(s.paths, paths...)
	return s
}

func (s *SubgraphBuilder) AddPathPatternMacros(macros ...*ast.PathPatternMacro) *SubgraphBuilder {
	s.macros = append(s.macros, macros...)
	return s
}

func (s *SubgraphBuilder) Build() (*Subgraphs, error) {
	s.buildVertices()
	if err := s.buildCommonPathExpressions(); err != nil {
		return nil, err
	}
	if err := s.buildConnections(); err != nil {
		return nil, err
	}
	s.buildSubgraphs(&Subgraph{
		Vertices:    make(map[string]*Vertex),
		Connections: make(map[string]VertexPairConnection),
	})
	sort.Slice(s.singletonVars, func(i, j int) bool {
		return s.singletonVars[i].Name.L < s.singletonVars[j].Name.L
	})
	sort.Slice(s.groupVars, func(i, j int) bool {
		return s.groupVars[i].Name.L < s.groupVars[j].Name.L
	})
	sgs := &Subgraphs{
		Matched:       s.subgraphs,
		SingletonVars: s.singletonVars,
		GroupVars:     s.groupVars,
	}
	return sgs, nil
}

func (s *SubgraphBuilder) buildSubgraphs(sg *Subgraph) {
	stepVertex := func(name string) {
		vertices := s.vertices[name]
		if len(vertices) == 0 {
			return
		}
		delete(s.vertices, name)
		for _, v := range vertices {
			sg.Vertices[name] = v
			s.buildSubgraphs(sg)
			delete(sg.Vertices, name)
		}
		s.vertices[name] = vertices
	}

	stepConn := func(name string) {
		conns := s.connections[name]
		if len(conns) == 0 {
			return
		}
		srcVarName := conns[0].SrcVarName()
		srcVertex, ok := sg.Vertices[srcVarName.L]
		if !ok {
			stepVertex(srcVarName.L)
			return
		}
		dstVarName := conns[0].DstVarName()
		dstVertex, ok := sg.Vertices[dstVarName.L]
		if !ok {
			stepVertex(dstVarName.L)
			return
		}
		delete(s.connections, name)
		for _, conn := range conns {
			if srcVertex.Table.Name.Equal(conn.SrcTblName()) &&
				dstVertex.Table.Name.Equal(conn.DstTblName()) {
				sg.Connections[name] = conn
				s.buildSubgraphs(sg)
				delete(sg.Connections, name)
			}
		}
		s.connections[name] = conns
	}

	if len(s.connections) == 0 {
		if len(s.vertices) == 0 {
			s.subgraphs = append(s.subgraphs, sg.Clone())
			return
		}
		var name string
		for name = range s.vertices {
			break
		}
		stepVertex(name)
		return
	}
	var name string
	for name = range s.connections {
		break
	}
	stepConn(name)
}

func (s *SubgraphBuilder) buildCommonPathExpressions() error {
	for _, m := range s.macros {
		result, err := s.buildPathPatternMacro(m)
		if err != nil {
			return err
		}
		s.cpes[m.Name.L] = result
	}
	return nil
}

func (s *SubgraphBuilder) buildPathPatternMacro(macro *ast.PathPatternMacro) ([]*CommonPathExpression, error) {
	if macro.Path.Tp != ast.PathPatternSimple {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("Non-simple Path in Path Pattern Macro")
	}
	subgraphs, err := NewSubgraphBuilder(s.graph).AddPathPatterns(macro.Path).Build()
	if err != nil {
		return nil, err
	}
	if len(subgraphs.Matched) == 0 {
		return nil, nil
	}

	leftmostVarName := macro.Path.Vertices[0].Variable.Name.L
	rightmostVarName := macro.Path.Vertices[len(macro.Path.Vertices)-1].Variable.Name.L

	var cpes []*CommonPathExpression
	for _, sg := range subgraphs.Matched {
		cpes = append(cpes, &CommonPathExpression{
			Leftmost:    sg.Vertices[leftmostVarName],
			Rightmost:   sg.Vertices[rightmostVarName],
			Vertices:    sg.Vertices,
			Connections: sg.Connections,
			Constraints: macro.Where,
		})
	}
	return cpes, nil
}

func (s *SubgraphBuilder) buildVertices() {
	astVars := make(map[string]*ast.VariableSpec)
	for _, path := range s.paths {
		for _, astVertex := range path.Vertices {
			astVar := astVertex.Variable
			labels := astVar.Labels
			if vs, ok := s.vertices[astVar.Name.L]; ok {
				if len(labels) > 0 {
					vs = slicesext.FilterFunc(vs, func(v *Vertex) bool {
						return slicesext.ContainsFunc(labels, func(label model.CIStr) bool {
							return label.Equal(v.Name)
						})
					})
					s.vertices[astVar.Name.L] = vs
				}
			} else {
				s.vertices[astVar.Name.L] = s.buildVertex(astVar)
				astVars[astVar.Name.L] = astVar
			}
		}
	}
	for name, vs := range s.vertices {
		astVar := astVars[name]
		s.singletonVars = append(s.singletonVars, buildVertexVar(astVar, vs))
	}
}

func (s *SubgraphBuilder) buildVertex(astVar *ast.VariableSpec) []*Vertex {
	labels := astVar.Labels
	if len(labels) == 0 {
		labels = s.vertexLabels
	}
	tables := s.graph.VertexTablesByLabels(labels...)
	var vs []*Vertex
	for _, table := range tables {
		vs = append(vs, &Vertex{
			Name:  astVar.Name,
			Table: table,
		})
	}
	return vs
}

func (s *SubgraphBuilder) buildConnections() error {
	allConns := s.connections
	for _, path := range s.paths {
		for i, astConn := range path.Connections {
			var (
				conns []VertexPairConnection
				err   error
			)
			switch path.Tp {
			case ast.PathPatternSimple:
				conns, err = s.buildSimplePath(astConn)
			case ast.PathPatternAny, ast.PathPatternAnyShortest, ast.PathPatternAllShortest, ast.PathPatternTopKShortest,
				ast.PathPatternAnyCheapest, ast.PathPatternAllCheapest, ast.PathPatternTopKCheapest, ast.PathPatternAll:
				topK := int64(path.TopK & math.MaxInt64) // FIXME: use int64 in TopK
				conns, err = s.buildVariableLengthPath(path.Tp, topK, astConn)
			default:
				return ErrUnsupportedType.GenWithStack("Unsupported PathPatternType %d", path.Tp)
			}

			connName, direction, err := extractConnNameAndDirection(astConn)
			if err != nil {
				return err
			}
			leftVarName := path.Vertices[i].Variable.Name
			rightVarName := path.Vertices[i+1].Variable.Name
			srcVarName, dstVarName, anyDirected, err := resolveSrcDstVarName(leftVarName, rightVarName, direction)
			if err != nil {
				return err
			}

			// Try to set or eliminate any directed connection.
			var newConns []VertexPairConnection
			for _, conn := range conns {
				var revConn VertexPairConnection
				if anyDirected {
					if conn.SrcTblName().Equal(conn.DstTblName()) {
						conn.SetAnyDirected(true)
					} else {
						revConn = conn.Copy()
						revConn.SetSrcVarName(dstVarName)
						revConn.SetDstVarName(srcVarName)
						revConn.SetAnyDirected(false)
						conn.SetAnyDirected(false)
						newConns = append(newConns, revConn)
					}
				}
				conn.SetSrcVarName(srcVarName)
				conn.SetDstVarName(dstVarName)
				newConns = append(newConns, conn)
			}
			allConns[connName.L] = newConns
		}
	}
	return nil
}

func (s *SubgraphBuilder) buildSimplePath(astConn ast.VertexPairConnection) ([]VertexPairConnection, error) {
	var conns []VertexPairConnection
	switch x := astConn.(type) {
	case *ast.EdgePattern:
		varName := x.Variable.Name
		labels := x.Variable.Labels
		if len(labels) == 0 {
			labels = s.edgeLabels
		}
		tables := s.graph.EdgeTablesByLabels(labels...)
		for _, table := range tables {
			edge := &Edge{Table: table}
			edge.SetName(varName)
			conns = append(conns, edge)
		}
		s.singletonVars = append(s.singletonVars, buildGraphVarWithTables(x.Variable, tables))
		return conns, nil
	case *ast.ReachabilityPathExpr:
		vlp := s.buildBasicVariableLengthPath(x.AnonymousName, x.Labels)
		vlp.Goal = PathFindingReaches
		if x.Quantifier != nil {
			vlp.MinHops = int64(x.Quantifier.N & math.MaxInt64) // FIXME: use int64 in Quantifier
			vlp.MaxHops = int64(x.Quantifier.M & math.MaxInt64) // FIXME: use int64 in Quantifier
		} else {
			vlp.MinHops = 1
			vlp.MaxHops = 1
		}
		for _, conn := range s.expandVariableLengthPaths(vlp) {
			conns = append(conns, conn)
		}
		s.groupVars = append(s.groupVars, &GraphVar{
			Name:      x.AnonymousName,
			Anonymous: true,
		})
		return conns, nil
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.VertexPairConnection(%T) in simple path pattern", x)
	}
}

func (s *SubgraphBuilder) buildVariableLengthPath(
	pathTp ast.PathPatternType, topK int64, astConn ast.VertexPairConnection,
) ([]VertexPairConnection, error) {
	x, ok := astConn.(*ast.QuantifiedPathExpr)
	if !ok {
		return nil, ErrUnsupportedType.GenWithStack(
			"Unsupported ast.VertexPairConnection(%T) for variable-length path pattern", astConn)
	}
	varName := x.Edge.Variable.Name
	labels := x.Edge.Variable.Labels
	vlp := s.buildBasicVariableLengthPath(varName, labels)

	switch pathTp {
	case ast.PathPatternAny, ast.PathPatternAnyShortest:
		vlp.Goal = PathFindingShortest
		vlp.TopK = 1
	case ast.PathPatternAllShortest:
		vlp.Goal = PathFindingShortest
		vlp.WithTies = true
	case ast.PathPatternTopKShortest:
		vlp.Goal = PathFindingShortest
		vlp.TopK = topK
	case ast.PathPatternAnyCheapest:
		vlp.Goal = PathFindingCheapest
		vlp.TopK = 1
	case ast.PathPatternAllCheapest:
		vlp.Goal = PathFindingCheapest
		vlp.WithTies = true
	case ast.PathPatternTopKCheapest:
		vlp.Goal = PathFindingCheapest
		vlp.TopK = topK
	case ast.PathPatternAll:
		vlp.Goal = PathFindingAll
	}
	if x.Quantifier != nil {
		vlp.MinHops = int64(x.Quantifier.N & math.MaxInt64) // FIXME: use int64 in Quantifier
		vlp.MaxHops = int64(x.Quantifier.M & math.MaxInt64) // FIXME: use int64 in Quantifier
	} else {
		vlp.MinHops = 1
		vlp.MaxHops = 1
	}
	if x.Quantifier != nil {
		vlp.MinHops = int64(x.Quantifier.N & math.MaxInt64) // FIXME: use int64 in Quantifier
		vlp.MaxHops = int64(x.Quantifier.M & math.MaxInt64) // FIXME: use int64 in Quantifier
	} else {
		vlp.MinHops = 1
		vlp.MaxHops = 1
	}
	vlp.Constraints = x.Where
	vlp.Cost = x.Cost

	var hopSrcVar, hopDstVar *ast.VariableSpec
	if x.Source != nil {
		hopSrcVar = x.Source.Variable
	}
	if x.Destination != nil {
		hopDstVar = x.Destination.Variable
	}
	if x.Edge.Direction == ast.EdgeDirectionIncoming {
		hopSrcVar, hopDstVar = hopDstVar, hopSrcVar
	}
	if hopSrcVar != nil {
		vlp.HopSrc = s.buildVertex(hopSrcVar)
		if len(vlp.HopSrc) == 0 {
			vlp.Conns = nil
		}
		s.groupVars = append(s.groupVars, buildVertexVar(hopSrcVar, vlp.HopSrc))
	}
	if hopDstVar != nil {
		vlp.HopDst = s.buildVertex(hopDstVar)
		if len(vlp.HopDst) == 0 {
			vlp.Conns = nil
		}
		s.groupVars = append(s.groupVars, buildVertexVar(hopDstVar, vlp.HopDst))
	}
	var tables []*model.GraphTable
	for _, conn := range vlp.Conns {
		if e, ok := conn.(*Edge); ok {
			tables = append(tables, e.Table)
		}
	}
	s.groupVars = append(s.groupVars, buildGraphVarWithTables(x.Edge.Variable, tables))

	var conns []VertexPairConnection
	for _, conn := range s.expandVariableLengthPaths(vlp) {
		conns = append(conns, conn)
	}
	return conns, nil
}

func (s *SubgraphBuilder) buildBasicVariableLengthPath(varName model.CIStr, labels []model.CIStr) *VariableLengthPath {
	var conns []VertexPairConnection
	if len(labels) == 0 {
		labels = s.edgeLabels
	} else {
		var newLabels []model.CIStr
		for _, label := range labels {
			if matchedCpes, ok := s.cpes[label.L]; ok {
				for _, cpe := range matchedCpes {
					newCpe := cpe.Copy().(*CommonPathExpression)
					newCpe.SetName(varName)
					conns = append(conns, newCpe)
				}
			} else {
				newLabels = append(labels, label)
			}
		}
		labels = newLabels
	}
	if len(labels) > 0 {
		tables := s.graph.EdgeTablesByLabels(labels...)
		for _, table := range tables {
			edge := &Edge{Table: table}
			edge.SetName(varName)
			conns = append(conns, edge)
		}
	}
	vlp := &VariableLengthPath{Conns: conns}
	vlp.SetName(varName)
	return vlp
}

func (s *SubgraphBuilder) expandVariableLengthPaths(vlp *VariableLengthPath) []*VariableLengthPath {
	var vlps []*VariableLengthPath
	selfConnectable := set.NewStringSet()
	if vlp.MinHops == 0 {
		for _, tbl := range s.graph.VertexTables {
			newVlp := vlp.Copy().(*VariableLengthPath)
			newVlp.SetSrcTblName(tbl.Name)
			newVlp.SetDstTblName(tbl.Name)
			vlps = append(vlps, newVlp)
			selfConnectable.Insert(tbl.Name.L)
		}
	}
	if len(vlp.Conns) == 0 {
		return vlps
	}

	leftMostConns := make(map[string]VertexPairConnection)
	rightMostConns := make(map[string]VertexPairConnection)
	for _, conn := range vlp.Conns {
		leftMostConns[conn.SrcTblName().L] = conn
		rightMostConns[conn.DstTblName().L] = conn
	}
	var hopSrcTblNames, hopDstTblNames set.StringSet
	if len(vlp.HopSrc) > 0 {
		hopSrcTblNames = set.NewStringSet()
		for _, v := range vlp.HopSrc {
			hopSrcTblNames.Insert(v.Name.L)
		}
	}
	if len(vlp.HopDst) > 0 {
		hopDstTblNames = set.NewStringSet()
		for _, v := range vlp.HopDst {
			hopDstTblNames.Insert(v.Name.L)
		}
	}

	for _, leftMostConn := range leftMostConns {
		for _, rightMostConn := range rightMostConns {
			if leftMostConn.SrcTblName().Equal(rightMostConn.DstTblName()) &&
				selfConnectable.Exist(leftMostConn.SrcTblName().L) {
				continue
			}
			if hopSrcTblNames != nil && !hopSrcTblNames.Exist(leftMostConn.SrcTblName().L) {
				continue
			}
			if hopDstTblNames != nil && !hopDstTblNames.Exist(rightMostConn.DstTblName().L) {
				continue
			}
			newVlp := vlp.Copy().(*VariableLengthPath)
			newVlp.SetSrcTblName(leftMostConn.SrcTblName())
			newVlp.SetDstTblName(rightMostConn.DstTblName())
			vlps = append(vlps, newVlp)
		}
	}
	return vlps
}

func buildVertexVar(astVar *ast.VariableSpec, vs []*Vertex) *GraphVar {
	var tables []*model.GraphTable
	for _, v := range vs {
		tables = append(tables, v.Table)
	}
	return buildGraphVarWithTables(astVar, tables)
}

func buildGraphVarWithTables(astVar *ast.VariableSpec, tables []*model.GraphTable) *GraphVar {
	if astVar.Anonymous {
		return &GraphVar{
			Name:      astVar.Name,
			Anonymous: true,
		}
	} else {
		ss := set.NewStringSet()
		var propertyNames []model.CIStr
		for _, table := range tables {
			for _, p := range table.Properties {
				if !ss.Exist(p.Name.L) {
					propertyNames = append(propertyNames, p.Name)
					ss.Insert(p.Name.L)
				}
			}
		}
		sort.Slice(propertyNames, func(i, j int) bool {
			return propertyNames[i].L < propertyNames[j].L
		})
		return &GraphVar{
			Name:          astVar.Name,
			PropertyNames: propertyNames,
		}
	}
}

func resolveSrcDstVarName(
	leftVarName, rightVarName model.CIStr, direction ast.EdgeDirection,
) (srcVarName, dstVarName model.CIStr, anyDirected bool, err error) {
	switch direction {
	case ast.EdgeDirectionOutgoing:
		srcVarName = leftVarName
		dstVarName = rightVarName
	case ast.EdgeDirectionIncoming:
		srcVarName = rightVarName
		dstVarName = leftVarName
	case ast.EdgeDirectionAnyDirected:
		srcVarName = leftVarName
		dstVarName = rightVarName
		anyDirected = true
	default:
		err = ErrUnsupportedType.GenWithStack("Unsupported EdgeDirection %d", direction)
	}
	return
}

func extractConnNameAndDirection(conn ast.VertexPairConnection) (model.CIStr, ast.EdgeDirection, error) {
	switch x := conn.(type) {
	case *ast.EdgePattern:
		return x.Variable.Name, x.Direction, nil
	case *ast.ReachabilityPathExpr:
		return x.AnonymousName, x.Direction, nil
	case *ast.QuantifiedPathExpr:
		return x.Edge.Variable.Name, x.Edge.Direction, nil
	default:
		return model.CIStr{}, 0, ErrUnsupportedType.GenWithStack("Unsupported ast.VertexPairConnection(%T)", x)
	}
}
