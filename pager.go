package main

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

type Storage interface {
	io.ReaderAt
	io.WriterAt
	io.Seeker

	Truncate(size int64) error
}

type PageID uint32

const PageSize uint32 = 4096
const InvalidPageID PageID = PageID(0xf0000000)

var (
	ErrInvalidStorageSize = errors.New("storage size should be multiple of page size")
	ErrNoFreePages        = errors.New("no free pages")
	ErrPageNotAllocated   = errors.New("page not allocated")
)

func (id PageID) String() string {
	if id == InvalidPageID {
		return "PageID(invalid)"
	}
	return fmt.Sprintf("PageID(%d)", uint32(id))
}

type Page struct {
	m     sync.Mutex
	dirty bool
	_data [PageSize]byte
}

func (page *Page) Lock() {
	page.m.Lock()
}

func (page *Page) Unlock() {
	page.m.Unlock()
}

func (page *Page) Data() []byte {
	return page._data[:]
}

func (page *Page) IsDirty() bool {
	return page.dirty
}

func (page *Page) MarkDirty() {
	page.dirty = true
}

func (page *Page) MarkClean() {
	page.dirty = false
}

// NOTE: cache methods have to be thread-safe
type PageCache interface {
	Get(id PageID) *Page
	Put(id PageID, page *Page) (PageID, *Page)
	Remove(id PageID) *Page
	ForEach(f func(PageID, *Page) bool)
}

// Manages pool of pages in memory abstracting away details of file storage
type Pager struct {
	storage     Storage
	storageSize int64

	cache PageCache
	index *Page
}

// Create a new pager backed by storage
func NewPager(maxPages int, storage Storage) (*Pager, error) {
	storageSize, err := storage.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	if storageSize%int64(PageSize) != 0 {
		return nil, ErrInvalidStorageSize
	}

	cache := NewLRUCache(maxPages)
	pager := &Pager{
		storage:     storage,
		storageSize: storageSize,
		cache:       &cache,
		index:       nil,
	}

	err = pager.ensureSize(int64(PageSize))
	if err != nil {
		return nil, err
	}

	pager.index, err = pager.readPageAt(0)
	if err != nil {
		return nil, err
	}

	return pager, nil
}

// Obtain a page by id
func (pager *Pager) FetchPage(id PageID) (*Page, error) {
	// first check the memory cache
	page := pager.cache.Get(id)
	if page != nil {
		return page, nil
	}

	// read from the disk
	page, err := pager.readPage(id)
	if err != nil {
		return nil, err
	}

	pager.putInPool(id, page)
	return page, nil
}

// Allocate a new page on the disk, this only changes the metadata
func (pager *Pager) AllocatePage() (PageID, error) {
	index := pager.index
	index.Lock()
	defer index.Unlock()

	// Check metadata for free pages to reuse
	nEntries := len(index.Data())
	for id := 0; id < nEntries; id++ {
		if !pager.isPageAllocated(index, PageID(id)) {
			pager.markAllocated(index, PageID(id))

			offset, err := pager.offsetFromID(index, PageID(id))
			if err != nil {
				// should only fail if page is not allocated
				panic(err)
			}

			err = pager.ensureSize(offset + int64(PageSize))
			if err != nil {
				pager.markDeallocated(index, PageID(id))
				continue
			}
			return PageID(id), nil
		}
	}

	return InvalidPageID, ErrNoFreePages
}

// Mark page as deallocated, this only changes the metadata
func (pager *Pager) DeallocatePage(id PageID) {
	// remove from memory
	pager.cache.Remove(id)

	index := pager.index
	index.Lock()
	defer index.Unlock()
	// mark as deallocated
	pager.markDeallocated(index, id)
}

// Flush page to disk by id
func (pager *Pager) SyncPageByID(id PageID) error {
	page := pager.cache.Get(id)
	if page == nil {
		// page is not cached, nothing to sync
		return nil
	}

	return pager.SyncPage(id, page)
}

// Flush page to disk, page have to be locked
func (pager *Pager) SyncPage(id PageID, page *Page) error {
	if !page.IsDirty() {
		// no changes in page, nothing to sync
		return nil
	}

	err := pager.writePage(id, page)
	if err != nil {
		return err
	}

	page.MarkClean()
	return nil
}

// Flush all metadata pages to the disk
func (pager *Pager) SyncMetadata() error {
	index := pager.index
	index.Lock()
	defer index.Unlock()
	if !index.IsDirty() {
		return nil
	}

	err := pager.writePageAt(0, index)
	if err != nil {
		return err
	}

	index.MarkClean()
	return nil
}

// Flush all the pages to the disk
func (pager *Pager) SyncAll() error {
	err := pager.SyncMetadata()
	if err != nil {
		return err
	}

	pager.cache.ForEach(func(id PageID, page *Page) bool {
		err = pager.SyncPage(id, page)
		return err == nil
	})

	return err
}

// Get ID of the first page. Returns InvalidPageID if db is empty
func (pager *Pager) FirstPage() PageID {
	id := PageID(^uint32(0)) // uint32(-1)
	return pager.NextPage(id)
}

func (pager *Pager) NextPage(pid PageID) PageID {
	index := pager.index
	index.Lock()
	defer index.Unlock()
	nEntries := len(index.Data())
	for id := int(pid + 1); id < nEntries; id++ {
		if pager.isPageAllocated(index, PageID(id)) {
			return PageID(id)
		}
	}
	return InvalidPageID
}

// Check whether or not page is allocated according to the metadata
func (pager *Pager) isPageAllocated(index *Page, id PageID) bool {
	slotOffset := int(id)
	nEntries := len(index.Data())

	if slotOffset >= nEntries {
		// Out of bounds
		return false
	}
	return index.Data()[slotOffset] == 1
}

// Mark page as allocated
func (pager *Pager) markAllocated(index *Page, id PageID) {
	if !pager.isPageAllocated(index, id) {
		index.Data()[int(id)] = 1
		index.MarkDirty()
	}
}

// Mark page as free
func (pager *Pager) markDeallocated(index *Page, id PageID) {
	if pager.isPageAllocated(index, id) {
		index.Data()[int(id)] = 0
		index.MarkDirty()
	}
}

// Put a page in memory pager, evicting if neccesarry
func (pager *Pager) putInPool(id PageID, page *Page) {
	evictedID, evictedPage := pager.cache.Put(id, page)
	if evictedID != InvalidPageID {
		pager.SyncPage(evictedID, evictedPage)
	}
}

// Map page id to file offset
func (pager *Pager) offsetFromID(index *Page, id PageID) (int64, error) {
	if !pager.isPageAllocated(index, id) {
		return 0, ErrPageNotAllocated
	}

	// TODO: store offsets in the metadata page
	return (int64(id) + 1) * int64(PageSize), nil
}

func (pager *Pager) ensureSize(requiredSize int64) error {
	currentSize := pager.storageSize
	if currentSize >= requiredSize {
		return nil
	}

	err := pager.storage.Truncate(requiredSize)
	if err != nil {
		return err
	}

	pager.storageSize = requiredSize
	return nil
}

// Read page at offset
func (pager *Pager) readPageAt(offset int64) (*Page, error) {
	page := &Page{}
	_, err := pager.storage.ReadAt(page.Data(), offset)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// Read page from the disk
func (pager *Pager) readPage(id PageID) (*Page, error) {
	index := pager.index
	index.Lock()
	offset, err := pager.offsetFromID(index, id)
	if err != nil {
		index.Unlock()
		return nil, err
	}
	index.Unlock()

	return pager.readPageAt(offset)
}

// Write page at offset
func (pager *Pager) writePageAt(offset int64, page *Page) error {
	_, err := pager.storage.WriteAt(page.Data(), offset)
	return err
}

// Write page to the disk
func (pager *Pager) writePage(id PageID, page *Page) error {
	index := pager.index
	index.Lock()
	offset, err := pager.offsetFromID(index, id)
	if err != nil {
		index.Unlock()
		return err
	}
	index.Unlock()

	return pager.writePageAt(offset, page)
}
