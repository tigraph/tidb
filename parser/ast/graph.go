// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"fmt"

	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
)

var _ Node = &GraphPattern{}

type (
	GraphPathPatternType int

	// GraphEdgeDirection is used to represent the traverse direction. It can be IN/OUT/BOTH.
	GraphEdgeDirection byte

	GraphVariableSpec struct {
		node

		Name   *TableName
		AsName model.CIStr
		Where  ExprNode
	}

	GraphEdgePattern struct {
		node

		Direction GraphEdgeDirection
		Targets   []*GraphVariableSpec
	}

	// GraphPathPattern is used to represent a path of traversal.
	GraphPathPattern struct {
		node

		Type        GraphPathPatternType
		Source      *GraphVariableSpec
		Edges       []*GraphEdgePattern
		Destination *GraphVariableSpec
		TopK        int64
	}

	// GraphPattern ise used to represent a group of action to match a graph
	// reference: https://pgql-lang.org/spec/1.4/#match
	GraphPattern struct {
		node

		Paths []*GraphPathPattern
	}
)

const (
	GraphPathPatternTypeSimple GraphPathPatternType = iota
	GraphPathPatternTypeAnyPath
	GraphPathPatternTypeAnyShortestPath
	GraphPathPatternTypeAllShortestPath
	GraphPathPatternTypeTopKShortestPath
	GraphPathPatternTypeAllPath
)

const (
	GraphEdgeDirectionIn GraphEdgeDirection = iota
	GraphEdgeDirectionOut
	GraphEdgeDirectionBoth
)

// String implements the fmt.Stringer interface
func (d GraphEdgeDirection) String() string {
	switch d {
	case GraphEdgeDirectionIn:
		return "IN"
	case GraphEdgeDirectionOut:
		return "OUT"
	case GraphEdgeDirectionBoth:
		return "BOTH"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", d)
	}
}

func (*GraphPattern) resultSet() {}

// Restore implements Node Accept interface.
func (t *GraphPattern) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("MATCH ")
	for i, v := range t.Paths {
		if err := v.Restore(ctx); err != nil {
			return err
		}
		if i != len(t.Paths)-1 {
			ctx.WritePlain(", ")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (t *GraphPattern) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(t)
	if skipChildren {
		return v.Leave(newNode)
	}

	n := newNode.(*GraphPattern)
	for i, p := range n.Paths {
		node, ok := p.Accept(v)
		if !ok {
			return n, false
		}
		n.Paths[i] = node.(*GraphPathPattern)
	}

	return v.Leave(n)
}

// Restore implements Node Accept interface.
func (t *GraphPathPattern) Restore(ctx *format.RestoreCtx) error {
	switch t.Type {
	case GraphPathPatternTypeSimple:
	case GraphPathPatternTypeAnyPath:
		ctx.WriteKeyWord("ANY ")
	case GraphPathPatternTypeAnyShortestPath:
		ctx.WriteKeyWord("ANY SHORTEST ")
	case GraphPathPatternTypeAllShortestPath:
		ctx.WriteKeyWord("ALL SHORTEST ")
	case GraphPathPatternTypeTopKShortestPath:
		ctx.WriteKeyWord("TOP ")
		ctx.WritePlainf("%d", t.TopK)
	case GraphPathPatternTypeAllPath:
		ctx.WriteKeyWord("ALL ")
	}

	if err := t.Source.Restore(ctx); err != nil {
		return err
	}

	for _, e := range t.Edges {
		ctx.WritePlain(".")
		if err := e.Restore(ctx); err != nil {
			return err
		}
	}

	if t.Destination != nil {
		if err := t.Destination.Restore(ctx); err != nil {
			return err
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (t *GraphPathPattern) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(t)
	if skipChildren {
		return v.Leave(newNode)
	}

	n := newNode.(*GraphPathPattern)

	if n.Source != nil {
		node, ok := n.Source.Accept(v)
		if !ok {
			return n, false
		}
		n.Source = node.(*GraphVariableSpec)
	}
	for i, e := range n.Edges {
		node, ok := e.Accept(v)
		if !ok {
			return n, false
		}
		n.Edges[i] = node.(*GraphEdgePattern)
	}
	if n.Destination != nil {
		node, ok := n.Destination.Accept(v)
		if !ok {
			return n, false
		}
		n.Destination = node.(*GraphVariableSpec)
	}

	return v.Leave(n)
}

// Restore implements Node Accept interface.
func (t *GraphEdgePattern) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlainf("%s(", t.Direction.String())
	for j, n := range t.Targets {
		if err := n.Name.Restore(ctx); err != nil {
			return err
		}
		if err := n.Where.Restore(ctx); err != nil {
			return err
		}
		if j != len(t.Targets)-1 {
			ctx.WritePlain(", ")
		}
	}
	ctx.WritePlain(")")

	return nil
}

// Accept implements Node Accept interface.
func (t *GraphEdgePattern) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(t)
	if skipChildren {
		return v.Leave(newNode)
	}

	n := newNode.(*GraphEdgePattern)

	for i, e := range n.Targets {
		node, ok := e.Accept(v)
		if !ok {
			return n, false
		}
		n.Targets[i] = node.(*GraphVariableSpec)
	}

	return v.Leave(n)
}

// Restore implements Node Accept interface.
func (t *GraphVariableSpec) Restore(ctx *format.RestoreCtx) error {
	if err := t.Name.Restore(ctx); err != nil {
		return err
	}

	if t.AsName.O != "" {
		ctx.WritePlainf("AS %s", t.AsName.O)
	}

	return t.Where.Restore(ctx)
}

// Accept implements Node Accept interface.
func (t *GraphVariableSpec) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(t)
	if skipChildren {
		return v.Leave(newNode)
	}

	n := newNode.(*GraphVariableSpec)
	if n.Name != nil {
		node, ok := n.Name.Accept(v)
		if !ok {
			return n, false
		}
		n.Name = node.(*TableName)
	}

	if n.Where != nil {
		node, ok := n.Where.Accept(v)
		if !ok {
			return n, false
		}
		n.Where = node.(ExprNode)
	}

	return v.Leave(n)
}
