package dumbdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrTableAlreadyExist = errors.New("table with such name already exist")
	ErrTableDoesNotExist = errors.New("table does not exist")
	ErrNoSuchTable       = errors.New("no table with such name")
	ErrUnhandledQuery    = errors.New("unhandled query")
)

type Result struct {
	Schema Schema
	Rows   <-chan Row
}

const MetadataFilename string = "metadata.json"

type Database struct {
	// read-only
	dataDir string

	// protects tables map
	m      sync.RWMutex
	tables map[string]*Table
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
	db.m.RLock()
	defer db.m.RUnlock()

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

func (db *Database) doCreate(create *Create) (*Result, error) {
	db.m.Lock()
	defer db.m.Unlock()

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

	return nil, nil
}

func (db *Database) doDrop(drop *Drop) (*Result, error) {
	db.m.Lock()
	defer db.m.Unlock()

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
}

func (db *Database) doInsert(insert *Insert) (*Result, error) {
	db.m.RLock()
	defer db.m.RUnlock()

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
	return nil, err
}

func exprType(expr *BinOpTree, schema *Schema) (TypeID, error) {
	switch {
	case expr.val != nil:
		switch {
		case expr.val.Const != nil:
			switch {
			case expr.val.Const.Int != nil:
				return TypeInt, nil
			case expr.val.Const.Bool != nil:
				return TypeBool, nil
			case expr.val.Const.Str != nil:
				return TypeVarchar, nil
			}
		case expr.val.Field != "":
			idx, field := schema.GetField(expr.val.Field)
			if idx == -1 {
				return TypeInt, fmt.Errorf("no field named %v in table", expr.val.Field)
			}

			return field.TypeID, nil
		case expr.val.Subexpr != nil:
			panic("subexpr should always be nil")
		default:
			panic("empty value node")
		}
	case expr.subtree != nil:
		left, err := exprType(expr.subtree.Left, schema)
		if err != nil {
			return left, err
		}

		right, err := exprType(expr.subtree.Right, schema)
		if err != nil {
			return right, err
		}

		op := expr.subtree.Op
		if left != right {
			return TypeInt, fmt.Errorf("%v op types mismatch: left is %v, right is %v", op, left, right)
		}

		isArithmetic := op.IsArithmetic()
		isStrConcat := op == OpAdd && left == TypeVarchar
		if isArithmetic && !isStrConcat && left != TypeInt {
			return TypeInt, fmt.Errorf("attempt to perform arithmetic op %v on type %v", op, left)
		}

		if isStrConcat {
			return TypeVarchar, nil
		} else if isArithmetic {
			return TypeInt, nil
		} else {
			// logic op otherwise
			return TypeBool, nil
		}
	}

	return TypeInt, fmt.Errorf("unhandled expr: %v", expr)
}

// |expr| should be typechecked before calling this function
func evalExpr(expr *BinOpTree, fieldToIdx map[string]int, row Row) Value {
	switch {
	case expr.val != nil:
		switch {
		case expr.val.Const != nil:
			switch {
			case expr.val.Const.Int != nil:
				return Value{
					TypeID: TypeInt,
					Int:    *expr.val.Const.Int,
				}
			case expr.val.Const.Bool != nil:
				return Value{
					TypeID: TypeBool,
					Int:    expr.val.Const.Bool.ToInt(),
				}
			case expr.val.Const.Str != nil:
				return Value{
					TypeID: TypeVarchar,
					Str:    *expr.val.Const.Str,
				}
			}
		case expr.val.Field != "":
			idx, ok := fieldToIdx[expr.val.Field]
			if !ok {
				panic("unknown field")
			}
			return row[idx]
		case expr.val.Subexpr != nil:
			panic("subexpr should always be nil")
		default:
			panic("empty value node")
		}
	case expr.subtree != nil:
		left := evalExpr(expr.subtree.Left, fieldToIdx, row)
		right := evalExpr(expr.subtree.Right, fieldToIdx, row)
		op := expr.subtree.Op
		return op.Apply(left, right)
	}

	panic("unhandled binop node")
}

func (db *Database) doSelect(ctx context.Context, q *Select) (*Result, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	table, ok := db.tables[q.Table]
	if !ok {
		return nil, ErrNoSuchTable
	}

	filter := func(row Row) bool {
		return true
	}

	if q.Where != nil {
		filterTree := q.Where.ToBinOp()
		t, err := exprType(filterTree, &table.schema)
		if err != nil {
			return nil, err
		}

		if t != TypeBool {
			return nil, errors.New("where clause expression should eval to bool")
		}

		fieldToIdx := make(map[string]int)
		fields := table.schema.ColumnNames()
		for i, name := range fields {
			fieldToIdx[name] = i
		}

		filter = func(row Row) bool {
			return evalExpr(filterTree, fieldToIdx, row).Int != 0
		}
	}

	project := func(row Row) Row {
		return row
	}

	schema := table.schema
	if !q.Projection.All {
		newSchema, indexes, err := table.schema.Project(q.Projection.Fields)
		if err != nil {
			return nil, err
		}

		project = func(row Row) Row {
			return row.Project(indexes)
		}

		schema = newSchema
	}

	result := Result{
		Rows:   FullScan(ctx, table, filter, project),
		Schema: schema,
	}

	return &result, nil
}

func (db *Database) Execute(ctx context.Context, query *Query) (*Result, error) {
	switch {
	case query.Create != nil:
		return db.doCreate(query.Create)
	case query.Drop != nil:
		return db.doDrop(query.Drop)
	case query.Insert != nil:
		return db.doInsert(query.Insert)
	case query.Select != nil:
		return db.doSelect(ctx, query.Select)
	default:
		return nil, ErrUnhandledQuery
	}
}
