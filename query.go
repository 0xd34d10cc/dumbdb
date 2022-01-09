//nolint:govet
package main

import (
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

// Same as Value, but based on pointers
type ValuePtr struct {
	Int *int32  `@Int`
	Str *string `| @String`
}

func (val *ValuePtr) ToValue() Value {
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

type RowPtr struct {
	Values []ValuePtr `"(" @@ ("," @@)* ")"`
}

func (row *RowPtr) ToRow() Row {
	values := make([]Value, 0, len(row.Values))
	for _, val := range row.Values {
		values = append(values, val.ToValue())
	}
	return values
}

func ConvertRows(ptrs []RowPtr) []Row {
	rows := make([]Row, 0, len(ptrs))
	for _, row := range ptrs {
		rows = append(rows, row.ToRow())
	}
	return rows
}

type Insert struct {
	Table string   `"insert" "into" @Ident`
	Rows  []RowPtr `"values" @@ ("," @@)*`
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
)

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

	default:
		return fmt.Errorf("unexpected op %v", s)
	}
	return nil
}

type ComplexValue struct {
	Const   *ValuePtr   `@@`
	Field   string      `| @Ident`
	Subexpr *Expression `| "(" @@ ")"`
}

type Factor struct {
	Value *ComplexValue `@@`
}

type OpFactor struct {
	Op     Op      `@("*" | "/")`
	Factor *Factor `@@`
}

type Term struct {
	Left  *Factor     `@@`
	Right []*OpFactor `@@*`
}

type OpTerm struct {
	Op   Op    `@("+" | "-")`
	Term *Term `@@`
}

type Expression struct {
	Left  *Term     `@@`
	Right []*OpTerm `@@*`
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
