package bowl

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Should return:
//
// 1. -1 if a < b
//
// 2. 0 if a == b
//
// 3. 1 if a > b
//
// and NOT anything else
type Comparator func(a, b interface{}) int

const (
	NODE_SIZE int = 32
)

var errNodeIsFull = errors.New("Node is already full")
var errNodeIsEmpty = errors.New("Node is empty")
var errDataNotFound = errors.New("Given data is not in this node")
var errHeightOutsideRange = errors.New("This node's height is lower than given height")

// ItemHandle wraps key-value pair into single object
//
// Separating key and value into individual interface cause memory usage to go twice (cause 2 pointers instead of one),
// but this also makes the usage much clearer
type ItemHandle struct {
	key   interface{}
	value interface{}
}

// Node holds a slice of at most NODE_SIZE data
//
// To be accessed, each node should be lock first before doing anything.
//
// For deletion, the node is MARKED_REMOVAL, for now
//
// For now, it uses sync.Mutex for simplicity.
// As algorithm and implementation becomes more settled,
// will change to single int for lock, among others
//
// Another note is that I still haven't found good way to enforce data is a sort.Interface.
// Implementing sort.Interface wouldh have the benefit that the user can easily insert batched, ordered data at once
type Node struct {
	mu        sync.Mutex
	state     State
	cmp       Comparator
	dataCount int
	data      []ItemHandle
	height    int
	nextNodes []*Node
}

// NewEmptyNode creates Node with height h and given comparator
func NewEmptyNode(h int, cmp Comparator) *Node {
	return &Node{
		mu:        sync.Mutex{},
		state:     ACTIVE,
		cmp:       cmp,
		dataCount: 0,
		data:      make([]ItemHandle, NODE_SIZE),
		height:    h,
		nextNodes: make([]*Node, h),
	}
}

// NewNodeWithOrderedSlice creates Node with height h, given initial data and comparator
func NewNodeWithOrderedSlice(h int, data []ItemHandle, cmp Comparator) *Node {
	n := &Node{
		mu:        sync.Mutex{},
		state:     ACTIVE,
		cmp:       cmp,
		dataCount: 0,
		data:      make([]ItemHandle, NODE_SIZE),
		height:    h,
		nextNodes: make([]*Node, h),
	}
	copy(n.data, data)
	n.dataCount = len(data)
	return n
}

// Lock this node for change
//
// If already ready for removal, mutes is not taken, and will return false
func (n *Node) lock() bool {
	if atomic.LoadInt32((*int32)(&n.state)) == int32(ACTIVE) {
		n.mu.Lock()
		// 2nd check, probably MARKED_REMOVED after this lock is taken
		if atomic.LoadInt32((*int32)(&n.state)) == int32(MARKED_REMOVED) {
			n.mu.Unlock()
			return false
		}
		return true
	}
	return false
}

// Unlock this node
//
// Should not be called if `Lock()` returns false
func (n *Node) unlock() {
	n.mu.Unlock()
}

// markRemoval should only be called when Lock is held
func (n *Node) markRemoval() {
	atomic.StoreInt32(((*int32)(&n.state)), int32(MARKED_REMOVED))
}

// getBiggestPositionLessThan returns the position of the key in the node
func (n *Node) getBiggestPositionLessThan(key interface{}) int {
	idx := -1
	for i := 0; i < n.dataCount; i++ {
		if n.cmp(key, n.data[i].key) == -1 {
			idx = i
			break
		}
	}
	return idx
}

// getPositionExact returns the position of the key in the node
func (n *Node) getPositionExact(key interface{}) int {
	idx := -1
	for i := 0; i < n.dataCount; i++ {
		if n.cmp(n.data[i].key, key) == 0 {
			idx = i
			break
		}
	}
	return idx
}

// insert should only be called when Lock is held
func (n *Node) insert(ih ItemHandle) error {
	if n.dataCount == NODE_SIZE {
		return errNodeIsFull
	}
	idx := n.getBiggestPositionLessThan(ih.key)
	if idx == -1 {
		n.data[n.dataCount] = ih
	} else {
		copy(n.data[idx+1:n.dataCount+1], n.data[idx:n.dataCount])
		n.data[idx] = ih
	}
	n.dataCount++
	return nil
}

// delete should only be called when Lock is held
func (n *Node) delete(key interface{}) error {
	if n.dataCount == 0 {
		return errNodeIsEmpty
	}
	idx := n.getPositionExact(key)
	if idx == -1 {
		return errDataNotFound
	}
	n.dataCount--
	copy(n.data[idx:n.dataCount], n.data[idx+1:n.dataCount+1])
	return nil
}

// update should only be called when Lock is held
func (n *Node) update(d ItemHandle) error {
	if n.dataCount == 0 {
		return errNodeIsEmpty
	}
	idx := n.getPositionExact(d.key)
	if idx == -1 {
		return errDataNotFound
	}
	n.data[idx].value = d.value
	return nil
}

// get should only be called when Lock is held
//
// Returns the value for the specified key, if any
func (n *Node) get(key interface{}) (interface{}, error) {
	if n.dataCount == 0 {
		return nil, errNodeIsEmpty
	}
	idx := n.getPositionExact(key)
	if idx == -1 {
		return nil, errDataNotFound
	}
	return n.data[idx].value, nil
}

// exist should only be called when Lock is held
//
// check when the given key is in this node
func (n *Node) exist(key interface{}) bool {
	if n.dataCount == 0 {
		return false
	}
	idx := n.getPositionExact(key)
	return idx != -1
}

// checkKeyStrictlyLessThanMax should only be called when Lock is held
func (n *Node) checkKeyStrictlyLessThanMax(key interface{}) (bool, error) {
	if n.dataCount == 0 {
		return false, errNodeIsEmpty
	}
	return n.cmp(key, n.data[n.dataCount-1].key) == -1, nil
}

// checkKeyStrictlyLessThanMin should only be called when Lock is held
func (n *Node) checkKeyStrictlyLessThanMin(key interface{}) (bool, error) {
	if n.dataCount == 0 {
		return false, errNodeIsEmpty
	}
	return n.cmp(key, n.data[0].key) == -1, nil
}

// connectNode should only be called when Lock is held
func (n *Node) connectNode(atHeight int, next *Node) error {
	if atHeight < 0 || atHeight > n.height {
		return errHeightOutsideRange
	}
	n.nextNodes[atHeight-1] = next
	return nil
}

// disconnectNode should only be called either when Lock is held
// or when already marked for removal
func (n *Node) disconnectNode(atHeight int) error {
	if atHeight < 0 || atHeight > n.height {
		return errHeightOutsideRange
	}
	n.nextNodes[atHeight-1] = nil
	return nil
}
