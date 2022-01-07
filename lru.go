package main

import "sync"

type LRUNode struct {
	id   PageID
	page *Page

	next *LRUNode
	prev *LRUNode
}

type LRUCache struct {
	// NOTE: we can't take advantage of RWLock here, because Get() modifies recentlyUsed
	m            sync.Mutex
	values       map[PageID]*LRUNode
	recentlyUsed *LRUNode // most recently used node
	leastUsed    *LRUNode // least recently used node
	capacity     int      // maximum number of nodes in cache
}

func NewLRUCache(capacity int) LRUCache {
	return LRUCache{
		values:       make(map[PageID]*LRUNode),
		recentlyUsed: nil,
		leastUsed:    nil,
		capacity:     capacity,
	}
}

func (cache *LRUCache) Get(id PageID) *Page {
	cache.m.Lock()
	defer cache.m.Unlock()
	node, ok := cache.values[id]
	if ok {
		cache.markUsed(node)
		node.page.Pin()
		return node.page
	}
	return nil
}

func (cache *LRUCache) markUsed(node *LRUNode) {
	if node == cache.recentlyUsed {
		// already most recently used
		return
	}

	// empty cache case
	if cache.recentlyUsed == nil {
		cache.recentlyUsed = node
		cache.leastUsed = node
		return
	}

	// detach node from the list
	cache.detachNode(node)

	// push to the end of list
	node.next = nil
	node.prev = cache.recentlyUsed
	cache.recentlyUsed.next = node
	cache.recentlyUsed = node
}

func (cache *LRUCache) detachNode(node *LRUNode) {
	// new least recently used node
	if node == cache.leastUsed {
		cache.leastUsed = cache.leastUsed.next
		if cache.leastUsed != nil {
			cache.leastUsed.prev = nil
		}
	}

	// new recently used node
	if node == cache.recentlyUsed {
		cache.recentlyUsed = cache.recentlyUsed.prev
	}

	// update next and previous node, if they exist
	if node.next != nil {
		node.next.prev = node.prev
	}

	if node.prev != nil {
		node.prev.next = node.next
	}
}

func (cache *LRUCache) Put(id PageID, page *Page) (evictedID PageID, evictedPage *Page) {
	cache.m.Lock()
	defer cache.m.Unlock()
	node, ok := cache.values[id]
	if ok {
		// entry already exist, reuse it
		// generally this should never happen
		evictedID = node.id
		evictedPage = node.page
		if evictedPage.IsPinned() {
			panic("Attempt to replace a pinned page")
		}

	} else if len(cache.values) >= cache.capacity {
		// reached max capacity
		// reuse evicted node allocation
		node = cache.leastUsed
		for node != nil && node.page.IsPinned() {
			node = node.next
		}

		if node == nil {
			// TODO: handle this case normally
			panic("All cache pages are pinned")
		}

		delete(cache.values, node.id)
		evictedID = node.id
		evictedPage = node.page
	} else {
		// no eviction
		node = &LRUNode{}
		evictedID = InvalidPageID
		evictedPage = nil
	}

	// insert a new value and make it most recently used
	node.id = id
	node.page = page
	cache.values[id] = node
	cache.markUsed(node)
	return
}

func (cache *LRUCache) Remove(id PageID) *Page {
	cache.m.Lock()
	defer cache.m.Unlock()
	node, ok := cache.values[id]
	if !ok {
		return nil
	}

	cache.detachNode(node)
	return node.page
}

func (cache *LRUCache) ForEach(f func(id PageID, page *Page) bool) {
	cache.m.Lock()
	defer cache.m.Unlock()
	for node := cache.recentlyUsed; node != nil; node = node.prev {
		if !f(node.id, node.page) {
			break
		}
	}
}
