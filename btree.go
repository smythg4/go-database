package godatabase

// const T = 1000 // minimum degree of the B-Tree (higher T -> shorter tree -> fewer disk reads)

// type PageID uint32
// type KeyCount uint16
// type Key uint64
// type Value []byte

// type BNode struct {
// 	PageID   PageID
// 	IsLeaf   bool
// 	Keys     []Key
// 	Values   []Value
// 	Children []PageID
// }

// func (node *BNode) NumKeys() KeyCount {
// 	return KeyCount(len(node.Keys))
// }

// type DiskManager interface {
// 	DiskRead(pageID PageID) (*BNode, error)
// 	DiskWrite(node *BNode) error
// 	AllocateNode() (*BNode, error)
// }

// type BTree struct {
// 	Root *BNode
// 	Disk DiskManager
// }

// func (node *BNode) BTreeSearch(key Key, disk DiskManager) (*BNode, Key, error) {
// 	i := 0
// 	for i < int(node.NumKeys()) && key < node.Keys[i] {
// 		// linear search for smallest i such that k <= node.key[i]
// 		// update this for binary search
// 		i++
// 	}
// 	// check if we found the key
// 	if i < int(node.NumKeys()) && node.Keys[i] == key {
// 		return node, node.Keys[i], nil
// 	}
// 	// if leaf node and key not found
// 	if node.IsLeaf {
// 		return nil, 0, nil
// 	}
// 	// search in child with disk-read
// 	child, err := disk.DiskRead(node.Children[i])
// 	if err != nil {
// 		return nil, 0, err
// 	}
// 	return child.BTreeSearch(key, disk)
// }

// func NewBTree(disk DiskManager) (*BTree, error) {
// 	root, err := disk.AllocateNode()
// 	if err != nil {
// 		return nil, err
// 	}
// 	root.IsLeaf = true
// 	err = disk.DiskWrite(root)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &BTree{Root: root, Disk: disk}, nil
// }

// func (node *BNode) BTreeSplitChild(i int, disk DiskManager) error {
// 	y, err := disk.DiskRead(node.Children[i]) // full node to split
// 	if err != nil {
// 		return err
// 	}
// 	z, err := disk.AllocateNode() // z will take half of y
// 	if err != nil {
// 		return err
// 	}
// 	z.IsLeaf = y.IsLeaf

// 	// z gets upper half of y's keys (T-1 keys)
// 	z.Keys = make([]Key, T-1)
// 	z.Values = make([]Value, T-1)
// 	for j := 0; j < T-1; j++ {
// 		z.Keys[j] = y.Keys[j+T]     // z gets y's greatest keys...
// 		z.Values[j] = y.Values[j+T] // ... and values
// 	}

// 	// if y isn't a leaf, copy the upper half of children to z
// 	if !y.IsLeaf {
// 		z.Children = make([]PageID, T)
// 		for j := 0; j < T; j++ {
// 			z.Children[j] = y.Children[j+T] // ... and its corresponding children
// 		}
// 	}
// 	// truncate y to keep only the first T-1 keys
// 	y.Keys = y.Keys[:T-1]
// 	y.Values = y.Values[:T-1]
// 	if !y.IsLeaf {
// 		y.Children = y.Children[:T]
// 	}

// 	// shift parents childen to make room for z
// 	for j := int(node.NumKeys()); j >= i+1; j-- {
// 		if j+1 >= len(node.Children) {
// 			node.Children = append(node.Children, 0)
// 		}
// 		node.Children[j+1] = node.Children[j]
// 	}

// 	node.Children[i+1] = z.PageID

// 	// shift parent's keys to make room for median key
// 	for j := int(node.NumKeys()) - 1; j >= i; j-- {
// 		if j+1 >= len(node.Keys) {
// 			node.Keys = append(node.Keys, 0)
// 			node.Values = append(node.Values, nil)
// 		}
// 		node.Keys[j+1] = node.Keys[j]
// 		node.Values[j+1] = node.Values[j]
// 	}

// 	// insert the median key from y into parent
// 	if i >= len(node.Keys) {
// 		node.Keys = append(node.Keys, y.Keys[T-1])
// 		node.Values = append(node.Values, y.Values[T-1])
// 	} else {
// 		node.Keys[i] = y.Keys[T]
// 		node.Values[i] = y.Values[T]
// 	}

// 	// write all modified nodes back to disk
// 	err = disk.DiskWrite(y)
// 	if err != nil {
// 		return err
// 	}
// 	err = disk.DiskWrite(z)
// 	if err != nil {
// 		return err
// 	}
// 	err = disk.DiskWrite(node)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (tree *BTree) BTreeInsert(key Key) error {
// 	root := tree.Root
// 	if root.NumKeys() == 2*T-1 {
// 		s, err := tree.BTreeSplitRoot()
// 		if err != nil {
// 			return err
// 		}
// 		err = s.BTreeInsertNonFull(key, tree.Disk)
// 		if err != nil {
// 			return err
// 		}
// 	} else {
// 		err := root.BTreeInsertNonFull(key, tree.Disk)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (tree *BTree) BTreeSplitRoot() (*BNode, error) {
// 	s, err := tree.Disk.AllocateNode()
// 	if err != nil {
// 		return nil, err
// 	}
// 	s.IsLeaf = false
// 	s.Keys = make([]Key, 0)
// 	s.Children = append(s.Children, tree.Root.PageID)
// 	tree.Root.PageID = s.PageID
// 	err = s.BTreeSplitChild(1, tree.Disk)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return s, nil
// }

// func (node *BNode) BTreeInsertNonFull(key Key, disk DiskManager) error {
// 	i := node.NumKeys()
// 	if node.IsLeaf { // inserting into a leaf?
// 		for i >= 0 && key < node.Keys[i] { // shift keys in node to make room for k
// 			node.Keys[i+1] = node.Keys[i]
// 			node.Values[i+1] = node.Values[i]
// 		}
// 		node.Keys = append(node.Keys, key) // insert key into node
// 		disk.DiskWrite(node)
// 	} else {
// 		for i >= 0 && key < node.Keys[i] {
// 			i-- // find child where key belongs
// 		}
// 		i++
// 		c, err := disk.DiskRead(node.Children[i])
// 		if err != nil {
// 			return err
// 		}
// 		if c.NumKeys() == 2*T-1 {
// 			node.BTreeSplitChild(int(i), disk) // split child if it's full
// 			if key > node.Keys[i] {
// 				i++
// 			}
// 			err = c.BTreeInsertNonFull(key, disk)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 	}
// 	return nil
// }
