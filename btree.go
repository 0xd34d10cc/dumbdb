package main

import "encoding/binary"

// TODO: move to a different file
type RowID PageID

func (id RowID) PageID() PageID {
	val := uint32(id)
	return PageID(val & 0x00ffffff)
}

func (id RowID) RowIndex() uint8 {
	val := uint32(id)
	return uint8(val >> 24)
}

// B+ tree is
// 1) m-way search tree (for each node there is up to m children nodes)
// 2) Perfectly balanced (every leaf node is at same depth)
// 3) Every node (except root) is at least half full, i.t. m/2-1 <= keys <= m-1
// 4) Every inner node with k keys has k+1 non-null children
//
// for database M is generally set to (PageSize-HeaderSize)/(KeySize+PageIDSize)
//
// LeafNode {
//      // header:
// 		level  uint32  // depth of the node
//      nSlots uint16  // number of slots taken
//      prev   PageID  // id of the next leaf page
//      next   PageID  // id of the previous leaf page
//
//      Page
//      // data in page:
//      // keys   []Key       // sorted
//      // values []RecordID
// }
//
// see cmudb.io/btree for visualization
//
// insert(key, value) -> err
// Search(key) -> Cursor
type BTree struct {
	root  BTreeNode
	pager *Pager
}

type BTreeKey uint32
type BTreeValue RowID

const (
	// isLeaf (1) + pad (1) +  slotsTaken (2) + prev (4) + next (4)
	NodeHeaderSize  = 2 + 2 + 4 + 4
	KeySize         = 4 // sizeof(uint32)
	ValueSize       = 4 // sizeof(RowID)
	PageIDSize      = 4 // sizeof(PageID)
	BranchEntrySize = KeySize + PageIDSize
	LeafEntrySize   = KeySize + ValueSize
)

type BTreeNode struct {
	isLeaf     bool   // leaf when true, branch otherwise
	slotsTaken uint16 // number of slots taken
	prev       PageID // id of the previous leaf page, not set for branch nodes
	next       PageID // id of the next leaf page, id of the rightmost branch for branch nodes

	page *Page
}

func readNode(page *Page) BTreeNode {
	data := page.Data()
	node := BTreeNode{
		isLeaf:     data[0] != 0,
		slotsTaken: binary.LittleEndian.Uint16(data[2:]),
		prev:       InvalidPageID,
		next:       InvalidPageID,

		page: page,
	}

	if node.isLeaf {
		node.prev = PageID(binary.LittleEndian.Uint32(data[4:]))
	}

	node.next = PageID(binary.LittleEndian.Uint32(data[8:]))
	return node
}

func (node *BTreeNode) writeHeader() {
	data := node.page.Data()
	if node.isLeaf {
		data[0] = 1
	} else {
		data[0] = 0
	}

	binary.LittleEndian.PutUint16(data[2:], node.slotsTaken)
	binary.LittleEndian.PutUint32(data[4:], uint32(node.prev))
	binary.LittleEndian.PutUint32(data[8:], uint32(node.next))
	node.page.MarkDirty()
}

func initLeafNode(page *Page, prev PageID, next PageID) BTreeNode {
	node := BTreeNode{
		isLeaf:     true,
		slotsTaken: 0,
		prev:       prev,
		next:       next,

		page: page,
	}
	node.writeHeader()
	return node
}

func (node *BTreeNode) IsLeaf() bool {
	return node.isLeaf
}

func (node *BTreeNode) len() int {
	return int(node.slotsTaken)
}

func (node *BTreeNode) branchCap() int {
	return (int(PageSize) - NodeHeaderSize) / BranchEntrySize
}

func (node *BTreeNode) leafCap() int {
	return (int(PageSize) - NodeHeaderSize) / LeafEntrySize
}

// requires IsLeaf() && idx < Len()
func (node *BTreeNode) getBranch(idx int) (key BTreeKey, id PageID) {
	offset := NodeHeaderSize + BranchEntrySize*idx
	data := node.page.Data()
	key = BTreeKey(binary.LittleEndian.Uint32(data[offset:]))
	id = PageID(binary.LittleEndian.Uint32(data[offset+KeySize:]))
	return
}

func (node *BTreeNode) searchBranch(key BTreeKey) (int, PageID) {
	len := node.len()
	for idx := 0; idx < len; idx++ {
		k, id := node.getBranch(idx)
		if key <= k {
			return idx, id
		}
	}
	return len, node.next
}

func (node *BTreeNode) insertBranch(key BTreeKey, id PageID) int {
	idx := 0
	len := node.len()
	for {
		if idx >= len {
			break
		}
		k, _ := node.getBranch(idx)
		if k > key {
			break
		}
		idx++
	}

	data := node.page.Data()
	offset := NodeHeaderSize + BranchEntrySize*idx
	restSize := (len - idx) * BranchEntrySize
	copy(data[offset+BranchEntrySize:], data[offset:offset+restSize])
	binary.LittleEndian.PutUint32(data[offset:], uint32(key))
	binary.LittleEndian.PutUint32(data[offset+KeySize:], uint32(id))
	node.slotsTaken++
	return idx
}

func (node *BTreeNode) searchLeaf(key BTreeKey) (int, BTreeValue) {
	len := node.len()
	for idx := 0; idx < len; idx++ {
		k, v := node.getLeaf(idx)
		if key <= k {
			return idx, v
		}
	}
	return len, BTreeValue(0)
}

// requires IsLeaf() && idx < Len()
func (node *BTreeNode) getLeaf(idx int) (key BTreeKey, value BTreeValue) {
	offset := NodeHeaderSize + LeafEntrySize*idx
	data := node.page.Data()
	key = BTreeKey(binary.LittleEndian.Uint32(data[offset:]))
	value = BTreeValue(binary.LittleEndian.Uint32(data[offset+KeySize:]))
	return
}

// requires IsLeaf() && Len() < Cap()
// returns position at which key value pair was inserted
func (node *BTreeNode) insertLeaf(key BTreeKey, value BTreeValue) int {
	idx := 0
	len := node.len()
	for {
		if idx >= len {
			break
		}
		k, _ := node.getLeaf(idx)
		if k > key {
			break
		}
		idx++
	}

	data := node.page.Data()
	offset := NodeHeaderSize + LeafEntrySize*idx
	restSize := (len - idx) * LeafEntrySize
	copy(data[offset+LeafEntrySize:], data[offset:offset+restSize])
	binary.LittleEndian.PutUint32(data[offset:], uint32(key))
	binary.LittleEndian.PutUint32(data[offset+KeySize:], uint32(value))
	node.slotsTaken++
	return idx
}

func ReadBTree(rootID PageID, pager *Pager) (*BTree, error) {
	root, err := pager.FetchPage(rootID)
	if err != nil {
		return nil, err
	}

	root.Lock()
	rootNode := readNode(root)
	root.Unlock()
	return &BTree{
		root:  rootNode,
		pager: pager,
	}, nil
}

func NewBTree(page *Page, pager *Pager) *BTree {
	tree := &BTree{
		root: BTreeNode{
			isLeaf:     false,
			slotsTaken: 0,
			prev:       InvalidPageID,
			next:       InvalidPageID,

			page: page,
		},
		pager: pager,
	}
	tree.root.writeHeader()
	return tree
}

func (tree *BTree) Insert(key BTreeKey, value BTreeValue) error {
	node := &tree.root

	var path [4]BTreeNode
	depth := 0
	for {
		if node.IsLeaf() {
			if node.len() < node.leafCap() {
				node.insertLeaf(key, value)
				node.writeHeader()
				return nil
			} else {
				// TODO: allocate new node, backtrack & rebalance
				panic("out of space")
			}
		}

		idx, id := node.searchBranch(key)
		if id == InvalidPageID {
			// no valid path, which means we have not allocated node yet
			allocatedID, err := tree.pager.AllocatePage()
			if err != nil {
				return err
			}

			page, err := tree.pager.FetchPage(allocatedID)
			if err != nil {
				return err
			}

			len := node.len()
			prev := InvalidPageID
			// FIXME: previous leaf node might be in a different subtree
			if len > 0 {
				_, prev = node.getBranch(idx - 1)
			}
			next := InvalidPageID
			// FIXME: next leaf node might be in a different subtree
			if idx+1 < len {
				_, next = node.getBranch(idx + 1)
			}

			if len == 0 {
				node.insertBranch(key, allocatedID)
			} else {
				node.next = allocatedID
			}
			node.writeHeader()
			path[depth] = initLeafNode(page, prev, next)
			node = &path[depth]
			depth++
			continue
		}

		page, err := tree.pager.FetchPage(id)
		if err != nil {
			return err
		}

		path[depth] = readNode(page)
		node = &path[depth]
		depth++
	}
}

// leaf nodes iterator
type Cursor struct {
	pager *Pager
	idx   int
	node  BTreeNode
	err   error
}

func (cursor *Cursor) Forward() bool {
	if cursor.err != nil {
		return false
	}

	cursor.idx++
	if cursor.idx >= cursor.node.len() {
		if cursor.node.next == InvalidPageID {
			return false
		}

		page, err := cursor.pager.FetchPage(cursor.node.next)
		if err != nil {
			cursor.err = err
			return false
		}

		cursor.node = readNode(page)
		cursor.idx = 0
		return true
	}
	return true
}

func (cursor *Cursor) Get() (BTreeKey, BTreeValue) {
	return cursor.node.getLeaf(cursor.idx)
}

func (cursor *Cursor) Err() error {
	return cursor.err
}

func (tree *BTree) Search(key BTreeKey) Cursor {
	node := tree.root
	for {
		if node.IsLeaf() {
			idx, _ := node.searchLeaf(key)
			return Cursor{
				pager: tree.pager,
				idx:   idx,
				node:  node,
				err:   nil,
			}
		}

		_, next := node.searchBranch(key)
		if next == InvalidPageID {
			// TODO: in which cases could this happen?
			panic("no valid branch")
		}

		page, err := tree.pager.FetchPage(next)
		if err != nil {
			return Cursor{
				err: err,
			}
		}

		node = readNode(page)
	}
}
