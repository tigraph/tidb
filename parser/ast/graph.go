package ast

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
)

var (
	_ DDLNode = &CreatePropertyGraphStmt{}
	_ DDLNode = &DropPropertyGraphStmt{}

	_ Node = &VertexTable{}
	_ Node = &EdgeTable{}
	_ Node = &GraphName{}
	_ Node = &KeyClause{}
	_ Node = &LabelClause{}
	_ Node = &PropertiesClause{}
	_ Node = &VertexTableRef{}
	_ Node = &Property{}
)

type CreatePropertyGraphStmt struct {
	ddlNode

	Graph        *GraphName
	VertexTables []*VertexTable
	EdgeTables   []*EdgeTable
}

func (n *CreatePropertyGraphStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE PROPERTY GRAPH ")
	if err := n.Graph.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreatePropertyGraphStmt.GraphName")
	}

	ctx.WriteKeyWord(" VERTEX TABLES ")
	ctx.WritePlain("(")
	for i, tbl := range n.VertexTables {
		if i > 0 {
			ctx.WritePlain(",")
		}
		if err := tbl.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CreatePropertyGraphStmt.VertexTables")
		}
	}
	ctx.WritePlain(")")

	if len(n.EdgeTables) > 0 {
		ctx.WriteKeyWord(" EDGE TABLES ")
		ctx.WritePlain("(")
		for i, tbl := range n.EdgeTables {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := tbl.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore CreatePropertyGraphStmt.EdgeTables")
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

func (n *CreatePropertyGraphStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*CreatePropertyGraphStmt)
	if nn.Graph != nil {
		node, ok := nn.Graph.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Graph = node.(*GraphName)
	}
	for i, tbl := range nn.VertexTables {
		node, ok := tbl.Accept(v)
		if !ok {
			return nn, false
		}
		nn.VertexTables[i] = node.(*VertexTable)
	}
	for i, tbl := range nn.EdgeTables {
		node, ok := tbl.Accept(v)
		if !ok {
			return nn, false
		}
		nn.EdgeTables[i] = node.(*EdgeTable)
	}
	return v.Leave(nn)
}

type DropPropertyGraphStmt struct {
	ddlNode

	Graph *GraphName
}

func (n *DropPropertyGraphStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP PROPERTY GRAPH ")
	if err := n.Graph.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore DropPropertyGraphStmt.GraphName")
	}
	return nil
}

func (n *DropPropertyGraphStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*DropPropertyGraphStmt)
	if nn.Graph != nil {
		node, ok := nn.Graph.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Graph = node.(*GraphName)
	}
	return v.Leave(nn)
}

type GraphName struct {
	node

	Schema model.CIStr
	Name   model.CIStr
}

func (n *GraphName) Restore(ctx *format.RestoreCtx) error {
	if n.Schema.String() != "" {
		ctx.WriteName(n.Schema.String())
		ctx.WritePlain(".")
	}
	ctx.WriteName(n.Name.String())
	return nil
}

func (n *GraphName) Accept(v Visitor) (node Node, ok bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type VertexTable struct {
	node

	Table      *TableName
	AsName     model.CIStr
	Key        *KeyClause
	Label      *LabelClause
	Properties *PropertiesClause
}

func (n *VertexTable) Restore(ctx *format.RestoreCtx) error {
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore VertexTable.Table")
	}
	if asName := n.AsName.String(); asName != "" {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteName(asName)
	}
	if n.Key != nil {
		ctx.WritePlain(" ")
		if err := n.Key.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore VertexTable.Key")
		}
	}
	if n.Label != nil {
		ctx.WritePlain(" ")
		if err := n.Label.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore VertexTable.Label")
		}
	}
	if n.Properties != nil {
		ctx.WritePlain(" ")
		if err := n.Properties.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore VertexTable.Properties")
		}
	}
	return nil
}

func (n *VertexTable) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*VertexTable)
	node, ok := nn.Table.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Table = node.(*TableName)
	if nn.Key != nil {
		node, ok = nn.Key.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Key = node.(*KeyClause)
	}
	if nn.Label != nil {
		node, ok = nn.Label.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Label = node.(*LabelClause)
	}
	if nn.Properties != nil {
		node, ok = nn.Properties.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Properties = node.(*PropertiesClause)
	}
	return v.Leave(nn)
}

type EdgeTable struct {
	node

	Table       *TableName
	AsName      model.CIStr
	Key         *KeyClause
	Source      *VertexTableRef
	Destination *VertexTableRef
	Label       *LabelClause
	Properties  *PropertiesClause
}

func (n *EdgeTable) Restore(ctx *format.RestoreCtx) error {
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore EdgeTable.Table")
	}
	if asName := n.AsName.String(); asName != "" {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteName(asName)
	}
	if n.Key != nil {
		ctx.WritePlain(" ")
		if err := n.Key.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore EdgeTable.Key")
		}
	}

	ctx.WriteKeyWord(" SOURCE ")
	if err := n.Source.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore EdgeTable.Source")
	}
	ctx.WriteKeyWord(" DESTINATION ")
	if err := n.Destination.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore EdgeTable.Destination")
	}

	if n.Label != nil {
		ctx.WritePlain(" ")
		if err := n.Label.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore EdgeTable.Label")
		}
	}
	if n.Properties != nil {
		ctx.WritePlain(" ")
		if err := n.Properties.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore EdgeTable.Properties")
		}
	}
	return nil
}

func (n *EdgeTable) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*EdgeTable)
	node, ok := nn.Table.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Table = node.(*TableName)
	if nn.Key != nil {
		node, ok = nn.Key.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Key = node.(*KeyClause)
	}
	node, ok = nn.Source.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Source = node.(*VertexTableRef)
	node, ok = nn.Destination.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Destination = node.(*VertexTableRef)
	if nn.Label != nil {
		node, ok = nn.Label.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Label = node.(*LabelClause)
	}
	if nn.Properties != nil {
		node, ok = nn.Properties.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Properties = node.(*PropertiesClause)
	}
	return v.Leave(nn)
}

type KeyClause struct {
	node

	Cols []*ColumnName
}

func (n *KeyClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("KEY ")
	ctx.WritePlain("(")
	for i, col := range n.Cols {
		if i > 0 {
			ctx.WritePlain(",")
		}
		if err := col.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore KeyClause.Cols")
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *KeyClause) Accept(v Visitor) (node Node, ok bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type VertexTableRef struct {
	node

	Key   *KeyClause
	Table *TableName
}

func (n *VertexTableRef) Restore(ctx *format.RestoreCtx) error {
	if n.Key != nil {
		if err := n.Key.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore VertexTableRef.Key")
		}
		ctx.WriteKeyWord(" REFERENCES ")
	}
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore VertexTableRef.Table")
	}
	return nil
}

func (n *VertexTableRef) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*VertexTableRef)
	if nn.Key != nil {
		node, ok := nn.Key.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Key = node.(*KeyClause)
	}
	node, ok := nn.Table.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Table = node.(*TableName)
	return v.Leave(nn)
}

type LabelClause struct {
	node

	Name model.CIStr
}

func (n *LabelClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LABEL ")
	ctx.WriteName(n.Name.String())
	return nil
}

func (n *LabelClause) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type PropertiesClause struct {
	node

	AllCols      bool
	ExceptCols   []*ColumnName
	Properties   []*Property
	NoProperties bool
}

func (n *PropertiesClause) Restore(ctx *format.RestoreCtx) error {
	switch {
	case n.AllCols:
		ctx.WriteKeyWord("PROPERTIES ARE ALL COLUMNS")
		if len(n.ExceptCols) > 0 {
			ctx.WriteKeyWord(" EXCEPT ")
			ctx.WritePlain("(")
			for i, col := range n.ExceptCols {
				if i > 0 {
					ctx.WritePlain(",")
				}
				if err := col.Restore(ctx); err != nil {
					return errors.Annotate(err, "An error occurred while restore PropertiesClause.ExceptCols")
				}
			}
			ctx.WritePlain(")")
		}
	case len(n.Properties) > 0:
		ctx.WriteKeyWord("PROPERTIES ")
		ctx.WritePlain("(")
		for i, prop := range n.Properties {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := prop.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore PropertiesClause.Properties")
			}
		}
		ctx.WritePlain(")")
	case n.NoProperties:
		ctx.WriteKeyWord("NO PROPERTIES")
	default:
		return errors.New("PropertiesClause is not properly set")
	}
	return nil
}

func (n *PropertiesClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*PropertiesClause)
	switch {
	case nn.AllCols:
		for i, col := range n.ExceptCols {
			node, ok := col.Accept(v)
			if !ok {
				return nn, false
			}
			nn.ExceptCols[i] = node.(*ColumnName)
		}
	case len(nn.Properties) > 0:
		for i, prop := range nn.Properties {
			node, ok := prop.Accept(v)
			if !ok {
				return nn, false
			}
			nn.Properties[i] = node.(*Property)
		}
	}
	return v.Leave(nn)
}

type Property struct {
	node

	Expr   ExprNode
	AsName model.CIStr
}

func (n *Property) Restore(ctx *format.RestoreCtx) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore Property.Expr")
	}
	if asName := n.AsName.String(); asName != "" {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteName(asName)
	}
	return nil
}

func (n *Property) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*Property)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Expr = node.(ExprNode)
	return v.Leave(nn)
}
