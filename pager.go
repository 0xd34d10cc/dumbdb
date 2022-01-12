package dumbdb

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

const (
	IndexHeaderSize        = 4
	IndexMaxEntriesPerPage = (PageSize - IndexHeaderSize) * 8
)

type AllocationIndex struct {
	nEntires uint32
	root     *Page
}

func ReadAllocationIndex(storage Storage) (*AllocationIndex, error) {
	root := &Page{}
	_, err := storage.ReadAt(root.Data(), 0)
	if err != nil {
		return nil, err
	}

	nEntries := binary.LittleEndian.Uint32(root.Data())
	return &AllocationIndex{
		nEntires: nEntries,
		root:     root,
	}, nil
}

func (index *AllocationIndex) RLock() {
	index.root.RLock()
}

func (index *AllocationIndex) RUnlock() {
	index.root.RUnlock()
}

func (index *AllocationIndex) Lock() {
	index.root.Lock()
}

func (index *AllocationIndex) Unlock() {
	index.root.Unlock()
}

func (index *AllocationIndex) SyncPages(storage Storage) error {
	if !index.root.IsDirty() {
		return nil
	}

	binary.LittleEndian.PutUint32(index.root.Data(), index.nEntires)
	_, err := storage.WriteAt(index.root.Data(), 0)
	if err != nil {
		index.root.MarkClean()
	}
	return err
}

// returns number of pages allocated
func (index *AllocationIndex) NumEntries() uint32 {
	return index.nEntires
}

func (index *AllocationIndex) GetOffset(id PageID) int64 {
	if uint32(id) >= index.NumEntries() {
		return -1
	}

	return (1 + int64(id)) * int64(PageSize)
}

func (index *AllocationIndex) IsAllocated(id PageID) bool {
	idx := uint32(id)
	if idx >= index.NumEntries() {
		return false
	}

	nByte := idx / 8
	nBit := idx % 8
	return index.root.Data()[IndexHeaderSize:][nByte]&(1<<nBit) != 0
}

func (index *AllocationIndex) Allocate() PageID {
	idx := index.NumEntries()
	if idx >= IndexMaxEntriesPerPage {
		return InvalidPageID
	}

	nByte := idx / 8
	nBit := idx % 8
	index.root.Data()[IndexHeaderSize:][nByte] |= (1 << nBit)
	index.nEntires++
	index.root.MarkDirty()
	return PageID(idx)
}

type Storage interface {
	io.ReaderAt
	io.WriterAt
	io.Seeker

	Truncate(size int64) error
}

// NOTE: all cache methods have to be thread-safe
type PageCache interface {
	// get page from cache by id
	Get(id PageID) *Page

	// try put page into cache
	// returns:
	//    (InvalidPageID, nil) if cache is not full
	//    (evictedID, evictedPage) otherwise
	//
	Put(id PageID, page *Page) (PageID, *Page)

	// remove page from cache
	// returns removed page, if any
	Remove(id PageID) *Page

	// run function f for each page in cache
	// and stop if f() returns false
	ForEach(f func(PageID, *Page) bool)
}

var (
	ErrInvalidStorageSize = errors.New("storage size should be multiple of page size")
	ErrNoFreePages        = errors.New("no free pages")
	ErrPageNotAllocated   = errors.New("page not allocated")
)

// Manages pool of pages in memory abstracting away details of file storage
type Pager struct {
	storage     Storage
	storageSize int64

	pageLocks [128]sync.Mutex
	cache     PageCache
	index     *AllocationIndex
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

	pager.index, err = ReadAllocationIndex(storage)
	if err != nil {
		return nil, err
	}

	return pager, nil
}

// Obtain a page by id
// returns pinned page or error, if any
func (pager *Pager) FetchPage(id PageID) (*Page, error) {
	pager.lockPageID(id)

	// first check the memory cache
	page := pager.cache.Get(id)
	if page != nil {
		pager.unlockPageID(id)
		return page, nil
	}

	// read from the disk
	page, err := pager.readPage(id)
	if err != nil {
		pager.unlockPageID(id)
		return nil, err
	}
	page.Pin()

	// put page in cache
	evictedID, evictedPage := pager.cache.Put(id, page)
	pager.unlockPageID(id)

	if evictedID != InvalidPageID {
		// NOTE: this can't deadlock, because evictedPage is unpinned
		evictedPage.RLock()
		err = pager.SyncPage(evictedID, evictedPage)
		evictedPage.RUnlock()
		if err != nil {
			page.Unpin()
			return nil, err
		}
	}

	return page, nil
}

// Allocate a new page on the disk, this only changes the metadata
func (pager *Pager) AllocatePage() (PageID, error) {
	index := pager.index
	index.Lock()
	defer index.Unlock()
	// FIXME: sync changed metadata page
	id := index.Allocate()
	if id == InvalidPageID {
		return InvalidPageID, ErrNoFreePages
	}

	offset := index.GetOffset(id)
	err := pager.ensureSize(offset + int64(PageSize))
	return id, err
}

// Flush page to disk, page have to be locked
func (pager *Pager) SyncPage(id PageID, page *Page) error {
	if !page.IsDirty() {
		// no changes in page, nothing to sync
		return nil
	}

	pager.lockPageID(id)
	defer pager.unlockPageID(id)

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
	index.RLock()
	defer index.RUnlock()
	return index.SyncPages(pager.storage)
}

// Flush all the pages to the disk
func (pager *Pager) SyncAll() error {
	err := pager.SyncMetadata()
	if err != nil {
		return err
	}

	pager.cache.ForEach(func(id PageID, page *Page) bool {
		page.RLock()
		err = pager.SyncPage(id, page)
		page.RUnlock()
		return err == nil
	})

	return err
}

// Get ID of the first page. Returns InvalidPageID if db is empty
func (pager *Pager) FirstPage() PageID {
	id := PageID(^uint32(0)) // uint32(-1)
	return pager.NextPage(id)
}

func (pager *Pager) NextPage(id PageID) PageID {
	index := pager.index
	index.RLock()
	defer index.RUnlock()
	next := PageID(uint32(id) + 1)
	if index.IsAllocated(next) {
		return next
	}
	return InvalidPageID
}

func (pager *Pager) lockPageID(id PageID) {
	idx := id.Hash() % uint32(len(pager.pageLocks))
	pager.pageLocks[idx].Lock()
}

func (pager *Pager) unlockPageID(id PageID) {
	idx := id.Hash() % uint32(len(pager.pageLocks))
	pager.pageLocks[idx].Unlock()
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
	index.RLock()
	offset := index.GetOffset(id)
	if offset == -1 {
		index.RUnlock()
		return nil, ErrPageNotAllocated
	}
	index.RUnlock()

	// FIXME: it is possible for id -> offset mapping to change while we are doing IO
	//        we'll have to LockPageID(id) in DeallocPage() to fix that
	page, err := pager.readPageAt(offset)
	if err != nil {
		return nil, err
	}

	page.id = id
	return page, nil
}

// Write page at offset
func (pager *Pager) writePageAt(offset int64, page *Page) error {
	_, err := pager.storage.WriteAt(page.Data(), offset)
	return err
}

// Write page to the disk
func (pager *Pager) writePage(id PageID, page *Page) error {
	index := pager.index
	index.RLock()
	offset := index.GetOffset(id)
	if offset == -1 {
		index.RUnlock()
		return ErrPageNotAllocated
	}
	index.RUnlock()

	return pager.writePageAt(offset, page)
}
