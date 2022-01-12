package dumbdb

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type PageID uint32

const PageSize uint32 = 4096

// any page > InvalidPageID is also considered invalid
// so max file size is (InvalidPageID - 1) * PageSize = ~64GB
const InvalidPageID PageID = PageID(0x00ffffff)

func (id PageID) Hash() uint32 {
	// fnv32 on first 3 bytes
	val := uint32(id)
	hash := uint32(2166136261)
	hash *= 16777619
	hash ^= val & 0xff

	hash *= 16777619
	hash ^= (val >> 8) & 0xff

	hash *= 16777619
	hash ^= (val >> 16) & 0xff
	return hash
}

func (id PageID) String() string {
	if id == InvalidPageID {
		return "PageID(invalid)"
	}
	return fmt.Sprintf("PageID(%d)", uint32(id))
}

type Page struct {
	// Number of threads which use this page currently (pager references don't count)
	// When pinCount > 0 page cannot be evicted from page cache
	pinCount int32

	// This field is read-only
	id PageID

	// Protects access to data
	m sync.RWMutex
	// true if data was modified and doesn't match what's in persistent storage
	dirty bool
	data  [PageSize]byte
}

func (page *Page) IsPinned() bool {
	return atomic.LoadInt32(&page.pinCount) != 0
}

func (page *Page) Pin() {
	// fmt.Fprintln(os.Stderr, "==================================")
	// fmt.Fprintln(os.Stderr, page.id, "pinned at")
	// debug.PrintStack()

	atomic.AddInt32(&page.pinCount, 1)
}

func (page *Page) Unpin() {
	// fmt.Fprintln(os.Stderr, "==================================")
	// fmt.Fprintln(os.Stderr, page.id, "unpinned at")
	// debug.PrintStack()

	if atomic.AddInt32(&page.pinCount, -1) < 0 {
		panic("Unpin() called on page that is not pinned")
	}
}

func (page *Page) RLock() {
	// fmt.Fprintln(os.Stderr, "==================================")
	// fmt.Fprintln(os.Stderr, page.id, "r-locked at")
	// debug.PrintStack()

	page.m.RLock()
}

func (page *Page) RUnlock() {
	// fmt.Fprintln(os.Stderr, "==================================")
	// fmt.Fprintln(os.Stderr, page.id, "r-unlocked at")
	// debug.PrintStack()

	page.m.RUnlock()
}

func (page *Page) Lock() {
	// fmt.Fprintln(os.Stderr, "==================================")
	// fmt.Fprintln(os.Stderr, page.id, "w-locked at")
	// debug.PrintStack()

	page.m.Lock()
}

func (page *Page) Unlock() {
	// fmt.Fprintln(os.Stderr, "==================================")
	// fmt.Fprintln(os.Stderr, page.id, "w-unlocked at")
	// debug.PrintStack()

	page.m.Unlock()
}

func (page *Page) Data() []byte {
	return page.data[:]
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
