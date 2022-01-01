package main

import (
	"errors"
	"fmt"
	"io"

	"github.com/olekukonko/tablewriter"
)

type Result struct {
	schema Schema
	rows   []Row
}

func (result *Result) FormatTable(w io.Writer) {
	writer := tablewriter.NewWriter(w)
	writer.SetHeader(result.schema.ColumnNames())

	text := make([]string, 0, 3)
	for _, row := range result.rows {
		for _, field := range row {
			text = append(text, field.String())
		}

		writer.Append(text)
		text = text[:0]
	}
	writer.Render()
}

type Database struct {
	tables map[string]*Table
}

func NewDatabase() *Database {
	return &Database{
		tables: make(map[string]*Table),
	}
}

func (db *Database) Close() {
	for _, table := range db.tables {
		table.Close()
	}
}

func (db *Database) Execute(query *Query) (*Result, error) {
	switch {
	case query.Create != nil:
		create := query.Create
		_, ok := db.tables[create.Table]
		if ok {
			return nil, errors.New("table with such name already exist")
		}

		table, err := NewTable(create.Table, create.Fields)
		if err != nil {
			return nil, err
		}

		db.tables[create.Table] = table
	case query.Insert != nil:
		insert := query.Insert
		table, ok := db.tables[insert.Table]
		if !ok {
			return nil, errors.New("no table with such name")
		}

		rows := ConvertRows(insert.Rows)
		for i, row := range rows {
			err := table.schema.Typecheck(row)
			if err != nil {
				return nil, fmt.Errorf("row #%d %v", i, err)
			}
		}

		err := table.Insert(rows)
		if err != nil {
			return nil, err
		}
	case query.Select != nil:
		q := query.Select
		table, ok := db.tables[q.Table]
		if !ok {
			return nil, errors.New("no table with such name")
		}

		rows, err := table.SelectAll()
		if err != nil {
			return nil, err
		}

		return &Result{
			schema: table.schema,
			rows:   rows,
		}, nil
	default:
		return nil, errors.New("unhandled query")
	}

	return nil, nil
}
