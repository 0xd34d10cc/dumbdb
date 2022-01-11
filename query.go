//nolint:govet
package dumbdb

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
type Literal struct {
	Int *int32  `@Int`
	Str *string `| @String`
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
	Op   Op    `@("+" | "-")`
	Term *Term `@@`
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
	Left  *Disj     `@@`
	Right []*OpDisj `@@*`
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
