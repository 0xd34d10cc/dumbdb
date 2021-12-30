package main

import (
	"encoding/binary"
	"log"
	"os"
)

// TODO: actually aquire a lock here
type LockedPage struct {
	initialRows uint16
	wasDirty    bool

	nRows uint16
	page  *Page
}

func LockPage(page *Page) LockedPage {
	nRows := binary.LittleEndian.Uint16(page.Data()[:2])
	return LockedPage{
		initialRows: nRows,
		wasDirty:    page.IsDirty(),

		nRows: nRows,
		page:  page,
	}
}

func (p *LockedPage) Unlock() {
	// TODO: implement
}

// Returns true on success
// NOTE: inserts are not applied until Commit() is called
func (p *LockedPage) TryInsert(row Row, schema *Schema) bool {
	offset := 2 + schema.RowSize()*int(p.nRows)
	if offset+schema.RowSize() > len(p.page.Data()) {
		return false
	}

	err := schema.WriteRow(p.page.Data()[offset:], row)
	if err != nil {
		return false
	}

	p.nRows += 1
	return true
}

// Commit inserts into memory
func (p *LockedPage) Commit() {
	if p.nRows != p.initialRows {
		binary.LittleEndian.PutUint16(p.page.Data(), p.nRows)
		p.page.MarkDirty()
	}
}

func (p *LockedPage) Rollback() {
	if p.nRows != p.initialRows {
		binary.LittleEndian.PutUint16(p.page.Data(), p.initialRows)
		if !p.wasDirty {
			p.page.MarkClean()
		}
	}
}

type Table struct {
	schema Schema
	file   *os.File
	pager  *Pager
}

// Create a new table
func NewTable(name string, fields []FieldDescription) (*Table, error) {
	return initTable(name, fields, true)
}

// Open existing table
func OpenTable(name string, fields []FieldDescription) (*Table, error) {
	return initTable(name, fields, false)
}

func initTable(name string, fields []FieldDescription, isNew bool) (*Table, error) {
	schema := NewSchema(fields)
	flags := os.O_RDWR | os.O_CREATE
	if isNew {
		flags |= os.O_EXCL
	}

	file, err := os.OpenFile(name+".bin", flags, 0600)
	if err != nil {
		return nil, err
	}

	pager, err := NewPager(file)
	if err != nil {
		return nil, err
	}

	return &Table{
		schema: schema,
		file:   file,
		pager:  pager,
	}, nil
}

// Returns number of pages successfully inserted
func (table *Table) insertInto(id PageID, rows []Row) (int, error) {
	page, err := table.pager.FetchPage(id)
	if err != nil {
		return 0, err
	}

	i := 0
	lockedPage := LockPage(page)
	defer lockedPage.Unlock()
	for i < len(rows) && lockedPage.TryInsert(rows[i], &table.schema) {
		i++
	}

	if i != 0 {
		lockedPage.Commit()
		err := table.pager.SyncPage(id, page)
		if err != nil {
			lockedPage.Rollback()
			return 0, err
		}
	}

	return i, nil
}

// TODO: make it atomic globally, not only inside a single page
func (table *Table) Insert(rows []Row) error {
	i := 0
	// first try inserting into existing pages
	for id := table.pager.FirstPage(); id != InvalidPageID; id = table.pager.NextPage(id) {
		n, err := table.insertInto(id, rows[i:])
		if err != nil {
			return err
		}

		i += n
		if i == len(rows) {
			return nil
		}
	}

	// no space on existing pages, allocate new pages
	for {
		id, err := table.pager.AllocatePage()
		if err != nil {
			return err
		}

		n, err := table.insertInto(id, rows[i:])
		if err != nil {
			return err
		}

		i += n
		if i == len(rows) {
			return nil
		}
	}
}

func (table *Table) Close() {
	err := table.pager.SyncAll()
	if err != nil {
		log.Println("Failed to sync:", err)
	}
	table.file.Close()
}
