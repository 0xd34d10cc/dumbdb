//nolint:govet
package main

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// A custom lexer for INI files. This illustrates a relatively complex Regexp lexer, as well
// as use of the Unquote filter, which unquotes string tokens.
var iniLexer = lexer.MustSimple([]lexer.Rule{
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
	Name   string             `"create" "table" @Ident`
	Fields []FieldDescription `"(" @@ ("," @@)*  ")"`
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
	Name string   `"insert" "into" @Ident`
	Rows []RowPtr `"values" @@ ("," @@)*`
}

type Query struct {
	Create *Create `@@`
	Insert *Insert `| @@`
}

var parser = participle.MustBuild(&Query{},
	participle.Lexer(iniLexer),
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
