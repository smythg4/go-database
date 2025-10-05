package godatabase

// import "fmt"

// // MemoryDiskManager - simple map-based implementation for testing/learning
// type MemoryDiskManager struct {
// 	pages      map[PageID]*BNode
// 	nextPageID PageID
// }

// func NewMemoryDiskManager() *MemoryDiskManager {
// 	return &MemoryDiskManager{
// 		pages:      make(map[PageID]*BNode),
// 		nextPageID: 1, // Start at 1, reserve 0 as invalid
// 	}
// }

// func (m *MemoryDiskManager) DiskRead(pageID PageID) (*BNode, error) {
// 	if pageID == 0 {
// 		return nil, fmt.Errorf("invalid pageID: 0")
// 	}
// 	node, exists := m.pages[pageID]
// 	if !exists {
// 		return nil, fmt.Errorf("page %d not found", pageID)
// 	}
// 	// Return a copy to simulate disk read
// 	return m.copyNode(node), nil
// }

// func (m *MemoryDiskManager) DiskWrite(node *BNode) error {
// 	if node.PageID == 0 {
// 		return fmt.Errorf("cannot write node with invalid pageID: 0")
// 	}
// 	// Store a copy to simulate disk write
// 	m.pages[node.PageID] = m.copyNode(node)
// 	return nil
// }

// func (m *MemoryDiskManager) AllocateNode() (*BNode, error) {
// 	pageID := m.nextPageID
// 	m.nextPageID++

// 	node := &BNode{
// 		PageID:   pageID,
// 		IsLeaf:   false, // Caller will set this
// 		Keys:     make([]Key, 0, 2*T-1),     // Max keys = 2t-1
// 		Values:   make([]Value, 0, 2*T-1),   // Max values = 2t-1
// 		Children: make([]PageID, 0, 2*T),    // Max children = 2t
// 	}

// 	return node, nil
// }

// // Helper to copy nodes (simulates serialization/deserialization)
// func (m *MemoryDiskManager) copyNode(node *BNode) *BNode {
// 	if node == nil {
// 		return nil
// 	}

// 	nodeCopy := &BNode{
// 		PageID: node.PageID,
// 		IsLeaf: node.IsLeaf,
// 		Keys:   make([]Key, len(node.Keys)),
// 		Values: make([]Value, len(node.Values)),
// 		Children: make([]PageID, len(node.Children)),
// 	}

// 	copy(nodeCopy.Keys, node.Keys)
// 	copy(nodeCopy.Children, node.Children)

// 	// Deep copy Values (since they're []byte slices)
// 	for i, val := range node.Values {
// 		nodeCopy.Values[i] = make([]byte, len(val))
// 		copy(nodeCopy.Values[i], val)
// 	}

// 	return nodeCopy
// }
