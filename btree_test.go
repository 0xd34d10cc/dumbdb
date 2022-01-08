package main

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

var (
	ErrPartialRead  = errors.New("partial read")
	ErrPartialWrite = errors.New("partial write")
)

type MemoryStorage struct {
	pages [][PageSize]byte
	off   int64
}

func (s *MemoryStorage) TotalLen() int64 {
	return int64(len(s.pages)) * int64(PageSize)
}

func (s *MemoryStorage) Truncate(size int64) error {
	for s.TotalLen() < size {
		s.pages = append(s.pages, [PageSize]byte{})
	}
	return nil
}

func (s *MemoryStorage) ReadAt(buf []byte, off int64) (n int, err error) {
	idx := off / int64(PageSize)
	n = copy(buf, s.pages[idx][:])
	if n != len(buf) {
		err = ErrPartialRead
	}
	return
}

func (s *MemoryStorage) WriteAt(buf []byte, off int64) (n int, err error) {
	idx := off / int64(PageSize)
	n = copy(s.pages[idx][:], buf)
	if n != len(buf) {
		err = ErrPartialWrite
	}
	return
}

func (s *MemoryStorage) Seek(diff int64, whence int) (newOff int64, err error) {
	switch whence {
	case io.SeekCurrent:
		s.off = s.off + diff
	case io.SeekStart:
		s.off = diff
	case io.SeekEnd:
		s.off = s.TotalLen() + diff
	}
	newOff = s.off
	return
}

func debugLeaf(node *BTreeNode) {
	if node.len() > 10 {
		for idx := 0; idx < 5; idx++ {
			k, v := node.getLeaf(idx)
			fmt.Printf("(%v, %v), ", k, v)
		}

		fmt.Printf(" ... ")
		for idx := node.len() - 5; idx < node.len(); idx++ {
			k, v := node.getLeaf(idx)
			fmt.Printf(", (%v, %v)", k, v)
		}
	} else {
		for idx := 0; idx < node.len(); idx++ {
			k, v := node.getLeaf(idx)
			fmt.Printf("(%v, %v), ", k, v)
		}
	}
}

func debugTree(t *testing.T, root *BTreeNode, pager *Pager, depth int) {
	printPage := func(id PageID) {
		page, err := pager.FetchPage(id)
		if err != nil {
			t.Fatal(err)
		}
		defer page.Unpin()

		node := readNode(page)
		if !node.isLeaf {
			fmt.Printf("subtree (right=%v): \n", node.next)
			debugTree(t, &node, pager, depth+1)
		} else {
			fmt.Printf("(prev: %v, next: %v): ", node.prev, node.next)
			debugLeaf(&node)
			fmt.Println()
		}
	}

	for i := 0; i < root.len(); i++ {
		key, id := root.getBranch(i)
		fmt.Printf("%v", strings.Repeat("\t", depth))
		fmt.Printf("%v) %v -> %v: ", i, key, id)
		printPage(id)
	}

	if root.next != InvalidPageID {
		page, err := pager.FetchPage(root.next)
		if err != nil {
			t.Fatal(err)
		}
		defer page.Unpin()

		fmt.Printf("%v", strings.Repeat("\t", depth))
		node := readNode(page)
		if !node.isLeaf {
			fmt.Printf("rightmost subtree (right=%v): \n", node.next)
			debugTree(t, &node, pager, depth+1)
		} else {
			fmt.Printf("%v) above -> %v (prev: %v, next: %v): ", root.len(), root.next, node.prev, node.next)
			debugLeaf(&node)
			fmt.Println()
		}
	}
}

func checkValid(t *testing.T, tree *BTree, searchFrom int, nEntries int, tillEnd bool) {
	c := tree.Search(BTreeKey(searchFrom))
	defer c.Close()
	for i := searchFrom; i < nEntries; i++ {
		if c.Err() != nil {
			t.Fatal(c.Err())
		}

		key, val := c.Get()
		if key != BTreeKey(i) || val != BTreeValue(i*2) {
			t.Fatalf("Unexpected key at %v: %v %v\n", i, key, val)
		}

		innerCursor := tree.Search(BTreeKey(i))
		if innerCursor.Err() != nil {
			t.Fatal(c.Err())
		}
		k, v := innerCursor.Get()
		if k != key || v != val {
			t.Fatalf("Search results don't match %v: (%v %v) vs (%v %v)\n", i, key, val, k, v)
		}
		innerCursor.Close()

		isNotLast := i+1 != nEntries
		movedForward := c.Forward()
		if isNotLast != movedForward {
			if !movedForward {
				t.Fatalf("Unexpected end of cursor: %v at %v\n", c.Err(), i)
			} else if tillEnd {
				t.Fatalf("Cursor moved past the end: %v at %v\n", c.Err(), i)
			}
		}
	}
}

func TestInsert(t *testing.T) {
	storage := &MemoryStorage{
		pages: make([][PageSize]byte, 0),
		off:   0,
	}

	pager, err := NewPager(20, storage)
	if err != nil {
		t.Fatal(err)
	}
	defer pager.SyncAll()

	tree, err := NewBTree(pager)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	const nEntries = 32

	// insert high keys
	for key := nEntries - 1; key >= nEntries/2; key-- {
		val := key * 2
		err = tree.Insert(BTreeKey(key), BTreeValue(val))
		if err != nil {
			t.Fatal(err)
		}

		debugTree(t, &tree.root, pager, 0)
		fmt.Println("---------------------------------")
		checkValid(t, tree, key, nEntries, true)
	}

	// insert low keys
	for key := 0; key < nEntries/2; key++ {
		val := key * 2
		err = tree.Insert(BTreeKey(key), BTreeValue(val))
		if err != nil {
			t.Fatal(err)
		}

		debugTree(t, &tree.root, pager, 0)
		fmt.Println("---------------------------------")
		checkValid(t, tree, 0, key+1, false)
		checkValid(t, tree, nEntries/2, nEntries, true)
	}
}
