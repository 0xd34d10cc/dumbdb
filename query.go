//nolint:govet
package dumbdb

import (
	"errors"
	"fmt"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var queryLexer = lexer.MustSimple([]lexer.Rule{
	{Name: `Ident`, Pattern: `[a-zA-Z][a-zA-Z_\d]*`},
	{Name: `String`, Pattern: `"(?:\\.|[^"])*"`},
	{Name: `Int`, Pattern: `\d+`},
	{Name: `Float`, Pattern: `\d+(?:\.\d+)?`},
	{Name: `Operators`, Pattern: `<>|!=|<=|>=|[-+*/%,.()=<>]`},
	{Name: "comment", Pattern: `[#;][^\n]*`},
	{Name: "whitespace", Pattern: `\s+`},
})

type Type struct {
	Integer bool `@"int"`
	Bool    bool `| @"bool"`
	Varchar int  `| "varchar" "(" @Int ")"`
}

type FieldDescription struct {
	Name string `@Ident`
	Type *Type  `@@`
}

type Create struct {
	Table  string             `"create" "table" @Ident`
	Fields []FieldDescription `"(" @@ ("," @@)*  ")"`
}

type Drop struct {
	Table string `"drop" "table" @Ident`
}

type BoolVal bool

func (val *BoolVal) Capture(s []string) error {
	switch s[0] {
	case "true":
		*val = true
	case "false":
		*val = false
	}

	return errors.New("bool can only be either true or false")
}

// Same as Value, but based on pointers
type Literal struct {
	Int  *int32   `@Int`
	Bool *BoolVal `| @("true" | "false")`
	Str  *string  `| @String`
}

func (val *Literal) ToValue() Value {
	switch {
	case val.Int != nil:
		return Value{
			TypeID: TypeInt,
			Int:    *val.Int,
		}
	case val.Str != nil:
		return Value{
			TypeID: TypeVarchar,
			Str:    *val.Str,
		}
	}

	panic("unhandled type")
}

type Tuple struct {
	Values []Literal `"(" @@ ("," @@)* ")"`
}

func (row *Tuple) ToRow() Row {
	values := make([]Value, 0, len(row.Values))
	for _, val := range row.Values {
		values = append(values, val.ToValue())
	}
	return values
}

func ConvertRows(ptrs []Tuple) []Row {
	rows := make([]Row, 0, len(ptrs))
	for _, row := range ptrs {
		rows = append(rows, row.ToRow())
	}
	return rows
}

type Insert struct {
	Table string  `"insert" "into" @Ident`
	Rows  []Tuple `"values" @@ ("," @@)*`
}

type Projection struct {
	All    bool     `@"*"`
	Fields []string `| @Ident ("," @Ident)*`
}

type Op int

const (
	OpAdd Op = iota
	OpSub
	OpMul
	OpDiv

	OpEq
	OpNotEq
	OpLess
	OpLessOrEq
	OpGreater
	OpGreaterOrEq

	OpOr
	OpAnd
)

func (o Op) IsArithmetic() bool {
	switch o {
	case OpAdd, OpSub, OpMul, OpDiv:
		return true
	default:
		return false
	}
}

func (o Op) String() string {
	switch o {
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpEq:
		return "="
	case OpNotEq:
		return "!="
	case OpLess:
		return "<"
	case OpLessOrEq:
		return "<="
	case OpGreater:
		return ">"
	case OpGreaterOrEq:
		return ">="
	case OpOr:
		return "or"
	case OpAnd:
		return "and"
	default:
		return "<unknown op>"
	}
}

func (o *Op) Capture(s []string) error {
	switch s[0] {
	case "+":
		*o = OpAdd
	case "-":
		*o = OpSub
	case "*":
		*o = OpMul
	case "/":
		*o = OpDiv

	case "=":
		*o = OpEq
	case "!=":
		*o = OpNotEq
	case "<":
		*o = OpLess
	case "<=":
		*o = OpLessOrEq
	case ">":
		*o = OpGreater
	case ">=":
		*o = OpGreaterOrEq

	case "or":
		*o = OpOr
	case "and":
		*o = OpAnd

	default:
		return fmt.Errorf("unexpected op %v", s)
	}
	return nil
}

type ComplexValue struct {
	Const   *Literal    `@@`
	Field   string      `| @Ident`
	Subexpr *Expression `| "(" @@ ")"`
}

type Factor struct {
	Left *ComplexValue `@@`
	Rest []*OpFactor   `@@*`
}

type OpFactor struct {
	Op    Op      `@("*" | "/")`
	Right *Factor `@@`
}

type Term struct {
	Left *Factor   `@@`
	Rest []*OpTerm `@@*`
}

type OpTerm struct {
	Op    Op    `@("+" | "-")`
	Right *Term `@@`
}

type Comp struct {
	Left *Term     `@@`
	Rest []*OpComp `@@*`
}

type OpComp struct {
	Op    Op    `@("<" | "<=" | ">" | ">=" | "=" | "!=")`
	Right *Comp `@@`
}

type Conj struct {
	Left *Comp     `@@`
	Rest []*OpConj `@@*`
}

type OpConj struct {
	Op    Op    `@"and"`
	Right *Conj `@@`
}

type Disj struct {
	Left *Conj     `@@`
	Rest []*OpDisj `@@*`
}

type OpDisj struct {
	Op    Op    `@"or"`
	Right *Disj `@@`
}

// Expr ::= Disj
// Disj ::= Conj ('!!' Conj)*
// Conj ::= Comp ('&&' Comp)*
// Comp ::= Arithm ( '<'  Arithm
//                 | '<=' Arithm
//                 | '>'  Arithm
//                 | '>=' Arithm
//                 | '==' Arithm
//                 | '!=' Arithm)*
//
// Arithm ::= Term ('+' Term | '-' Term)*
// Term ::= Factor ('*' Factor | '/' Factor | '%' Factor)*
// Factor ::= ['-'] (Var | Number | '(' Expr ')')
type Expression struct {
	Left *Disj     `@@`
	Rest []*OpDisj `@@*`
}

type BinOpNode struct {
	Op    Op
	Left  *BinOpTree
	Right *BinOpTree
}

type BinOpTree struct {
	val     *ComplexValue
	subtree *BinOpNode
}

func (e *ComplexValue) ToBinOp() *BinOpTree {
	if e.Subexpr != nil {
		return e.Subexpr.ToBinOp()
	}

	return &BinOpTree{
		val: e,
	}
}

func (e *Factor) ToBinOp() *BinOpTree {
	if len(e.Rest) == 0 {
		return e.Left.ToBinOp()
	}

	current := &BinOpTree{
		subtree: &BinOpNode{
			Left:  e.Left.ToBinOp(),
			Right: nil,
		},
	}

	for _, rhs := range e.Rest {
		current.subtree.Op = rhs.Op
		current.subtree.Right = rhs.Right.ToBinOp()
		current = &BinOpTree{
			subtree: &BinOpNode{
				Left: current,
			},
		}
	}

	return current.subtree.Left
}

func (e *Term) ToBinOp() *BinOpTree {
	if len(e.Rest) == 0 {
		return e.Left.ToBinOp()
	}

	current := &BinOpTree{
		subtree: &BinOpNode{
			Left:  e.Left.ToBinOp(),
			Right: nil,
		},
	}

	for _, rhs := range e.Rest {
		current.subtree.Op = rhs.Op
		current.subtree.Right = rhs.Right.ToBinOp()
		current = &BinOpTree{
			subtree: &BinOpNode{
				Left: current,
			},
		}
	}

	return current.subtree.Left
}

func (e *Comp) ToBinOp() *BinOpTree {
	if len(e.Rest) == 0 {
		return e.Left.ToBinOp()
	}

	current := &BinOpTree{
		subtree: &BinOpNode{
			Left:  e.Left.ToBinOp(),
			Right: nil,
		},
	}

	for _, rhs := range e.Rest {
		current.subtree.Op = rhs.Op
		current.subtree.Right = rhs.Right.ToBinOp()
		current = &BinOpTree{
			subtree: &BinOpNode{
				Left: current,
			},
		}
	}

	return current.subtree.Left
}

func (e *Conj) ToBinOp() *BinOpTree {
	if len(e.Rest) == 0 {
		return e.Left.ToBinOp()
	}

	current := &BinOpTree{
		subtree: &BinOpNode{
			Left:  e.Left.ToBinOp(),
			Right: nil,
		},
	}

	for _, rhs := range e.Rest {
		current.subtree.Op = rhs.Op
		current.subtree.Right = rhs.Right.ToBinOp()
		current = &BinOpTree{
			subtree: &BinOpNode{
				Left: current,
			},
		}
	}

	return current.subtree.Left
}

func (e *Disj) ToBinOp() *BinOpTree {
	if len(e.Rest) == 0 {
		return e.Left.ToBinOp()
	}

	current := &BinOpTree{
		subtree: &BinOpNode{
			Left:  e.Left.ToBinOp(),
			Right: nil,
		},
	}

	for _, rhs := range e.Rest {
		current.subtree.Op = rhs.Op
		current.subtree.Right = rhs.Right.ToBinOp()
		current = &BinOpTree{
			subtree: &BinOpNode{
				Left: current,
			},
		}
	}

	return current.subtree.Left
}

func (e *Expression) ToBinOp() *BinOpTree {
	if len(e.Rest) == 0 {
		return e.Left.ToBinOp()
	}

	current := &BinOpTree{
		subtree: &BinOpNode{
			Left:  e.Left.ToBinOp(),
			Right: nil,
		},
	}

	for _, rhs := range e.Rest {
		current.subtree.Op = rhs.Op
		current.subtree.Right = rhs.Right.ToBinOp()
		current = &BinOpTree{
			subtree: &BinOpNode{
				Left: current,
			},
		}
	}

	return current.subtree.Left
}

type Select struct {
	Projection Projection  `"select" @@`
	Table      string      `"from" @Ident`
	Where      *Expression `["where" @@]`
}

// see https://sqlite.org/syntaxdiagrams.html
type Query struct {
	Create *Create `@@`
	Drop   *Drop   `| @@`
	Insert *Insert `| @@`
	Select *Select `| @@`
}

var parser = participle.MustBuild(&Query{},
	participle.Lexer(queryLexer),
	participle.Unquote("String"),
)

func ParseQuery(query string) (*Query, error) {
	q := &Query{}
	err := parser.ParseString("", query, q)
	if err != nil {
		return nil, err
	}
	return q, nil
}
