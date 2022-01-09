package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/olekukonko/tablewriter"
)

var (
	ErrTableAlreadyExist = errors.New("table with such name already exist")
	ErrTableDoesNotExist = errors.New("table does not exist")
	ErrNoSuchTable       = errors.New("no table with such name")
	ErrUnhandledQuery    = errors.New("unhandled query")
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

const MetadataFilename string = "metadata.json"

type Database struct {
	dataDir string
	tables  map[string]*Table
}

func NewDatabase(dataDir string) (*Database, error) {
	db := &Database{
		dataDir: dataDir,
		tables:  make(map[string]*Table),
	}

	data, err := ioutil.ReadFile(filepath.Join(dataDir, MetadataFilename))
	if os.IsNotExist(err) {
		return db, nil
	}

	if err != nil {
		return nil, err
	}

	var metadata map[string]Schema
	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return nil, err
	}

	for name, schema := range metadata {
		table, err := OpenTable(filepath.Join(dataDir, name), schema)
		if err != nil {
			return nil, err
		}
		db.tables[name] = table
	}

	return db, nil
}

func (db *Database) Close() error {
	err := db.saveMetadata()
	if err != nil {
		return err
	}

	for _, table := range db.tables {
		err = table.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) saveMetadata() error {
	metadata := make(map[string]Schema)
	for name, table := range db.tables {
		metadata[name] = table.schema
	}

	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filepath.Join(db.dataDir, MetadataFilename), data, 0600)
}

func (db *Database) Execute(query *Query) (*Result, error) {
	switch {
	case query.Create != nil:
		create := query.Create
		_, ok := db.tables[create.Table]
		if ok {
			return nil, ErrTableAlreadyExist
		}

		schema := NewSchema(create.Fields)
		table, err := NewTable(create.Table, schema)
		if err != nil {
			return nil, err
		}

		db.tables[create.Table] = table
		err = db.saveMetadata()
		if err != nil {
			delete(db.tables, create.Table)
			return nil, err
		}
	case query.Drop != nil:
		drop := query.Drop
		table, ok := db.tables[drop.Table]
		if !ok {
			return nil, ErrTableDoesNotExist
		}

		delete(db.tables, drop.Table)
		filename := table.file.Name()
		// FIXME: this flushes all caches to disk, which is unnecessary
		//        because we are going to delete the file anyway
		err := table.Close()
		if err != nil {
			return nil, err
		}

		err = os.Remove(filename)
		if err != nil {
			return nil, err
		}

		err = db.saveMetadata()
		return nil, err
	case query.Insert != nil:
		insert := query.Insert
		table, ok := db.tables[insert.Table]
		if !ok {
			return nil, ErrNoSuchTable
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
			return nil, ErrNoSuchTable
		}

		if q.Where != nil {
			return nil, errors.New("where clause is not supported yet")
		}

		// FIXME: we are basically loading whole table in the memory here
		result := Result{
			rows:   make([]Row, 0),
			schema: table.schema,
		}

		if q.Projection.All {
			err := table.Scan(func(r Row) error {
				result.rows = append(result.rows, r)
				return nil
			})

			if err != nil {
				return nil, err
			}
		} else {
			newSchema, indexes, err := table.schema.Project(q.Projection.Fields)
			if err != nil {
				return nil, err
			}

			err = table.Scan(func(r Row) error {
				projectedRow := r.Project(indexes)
				result.rows = append(result.rows, projectedRow)
				return nil
			})

			if err != nil {
				return nil, err
			}

			result.schema = newSchema
		}

		return &result, nil
	default:
		return nil, ErrUnhandledQuery
	}

	return nil, nil
}
