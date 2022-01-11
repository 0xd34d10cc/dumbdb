package main

import (
	"encoding/binary"
	"os"
)

type RowListPage struct {
	initialRows uint16
	wasDirty    bool

	nRows uint16
	page  *Page
}

func NewRowListPage(page *Page) RowListPage {
	nRows := binary.LittleEndian.Uint16(page.Data()[:2])
	return RowListPage{
		initialRows: nRows,
		wasDirty:    page.IsDirty(),

		nRows: nRows,
		page:  page,
	}
}

func (p *RowListPage) NumRows() int {
	return int(p.nRows)
}

func (p *RowListPage) ReadRow(idx int, schema *Schema) Row {
	offset := 2 + schema.RowSize()*idx
	if offset+schema.RowSize() > len(p.page.Data()) {
		return nil
	}

	row := make(Row, 0, len(schema.Fields))
	err := schema.ReadRow(p.page.Data()[offset:], &row)
	if err != nil {
		return nil
	}

	return row
}

// Returns true on success
// NOTE: inserts are not applied until Commit() is called
func (p *RowListPage) TryInsert(row Row, schema *Schema) bool {
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
func (p *RowListPage) Commit() {
	if p.nRows != p.initialRows {
		binary.LittleEndian.PutUint16(p.page.Data(), p.nRows)
		p.page.MarkDirty()
	}
}

func (p *RowListPage) Rollback() {
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
func NewTable(path string, schema Schema) (*Table, error) {
	return initTable(path, schema, true)
}

// Open existing table
func OpenTable(path string, schema Schema) (*Table, error) {
	return initTable(path, schema, false)
}

func initTable(path string, schema Schema, isNew bool) (*Table, error) {
	// TODO: consider O_DIRECT, see https://github.com/ncw/directio
	// TODO: check whether WriteAt() is atomic if writes are aligned to page size
	flags := os.O_RDWR | os.O_CREATE | os.O_SYNC
	if isNew {
		flags |= os.O_EXCL
	}

	file, err := os.OpenFile(path+".bin", flags, 0600)
	if err != nil {
		return nil, err
	}

	pager, err := NewPager(4096, file)
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
	defer page.Unpin()

	i := 0
	page.Lock()
	lockedPage := NewRowListPage(page)
	defer page.Unlock()
	for i < len(rows) && lockedPage.TryInsert(rows[i], &table.schema) {
		i++
	}

	if i != 0 {
		lockedPage.Commit()
		// TODO: remove this sync() after implementing WAL
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

func (table *Table) ScanPage(id PageID, onRow func(Row) error) error {
	page, err := table.pager.FetchPage(id)
	if err != nil {
		return err
	}
	defer page.Unpin()

	page.RLock()
	lockedPage := NewRowListPage(page)
	defer page.RUnlock()
	for i := 0; i < lockedPage.NumRows(); i++ {
		row := lockedPage.ReadRow(i, &table.schema)
		err := onRow(row)
		if err != nil {
			return err
		}
	}

	return nil
}

func (table *Table) Scan(onRow func(Row) error) error {
	for id := table.pager.FirstPage(); id != InvalidPageID; id = table.pager.NextPage(id) {
		err := table.ScanPage(id, onRow)
		if err != nil {
			return err
		}
	}
	return nil
}

func (table *Table) Close() error {
	err := table.pager.SyncAll()
	if err != nil {
		return err
	}
	return table.file.Close()
}
