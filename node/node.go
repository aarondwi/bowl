package node

import (
	"errors"
	"sync"

	"github.com/aarondwi/bowl/common"
)

const (
	NODE_SIZE int = 32
)

var ErrKeyAlreadyExist = errors.New("Given key is already exist")
var ErrNodeIsFull = errors.New("Node is already full")
var ErrNodeIsEmpty = errors.New("Node is empty")
var ErrDataNotFound = errors.New("Given data is not in this node")
var ErrHeightOutsideRange = errors.New("This node's height is lower than given height")

// ItemHandle wraps key-value pair into single object
//
// Separating key and value into individual interface cause memory usage to go twice (cause 2 pointers instead of one),
// but this also makes the usage much clearer
type ItemHandle struct {
	Key   interface{}
	Value interface{}
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
	state     common.State
	cmp       common.Comparator
	dataCount int
	data      []ItemHandle
	height    int
	nextNodes []*Node
}

// NewEmptyNode creates Node with height h and given common.comparator
func NewEmptyNode(h int, cmp common.Comparator) *Node {
	return &Node{
		mu:        sync.Mutex{},
		state:     common.ACTIVE,
		cmp:       cmp,
		dataCount: 0,
		data:      make([]ItemHandle, NODE_SIZE),
		height:    h,
		nextNodes: make([]*Node, h),
	}
}

// NewNodeWithOrderedSlice creates Node with height h, given initial data and common.comparator
func NewNodeWithOrderedSlice(
	h int, data []ItemHandle, size int, cmp common.Comparator) *Node {
	n := &Node{
		mu:        sync.Mutex{},
		state:     common.ACTIVE,
		cmp:       cmp,
		dataCount: 0,
		data:      make([]ItemHandle, NODE_SIZE),
		height:    h,
		nextNodes: make([]*Node, h),
	}
	copy(n.data, data[:size])
	n.dataCount = size
	return n
}

// GetHeight returns n.height
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) GetHeight() int {
	return n.height
}

// Lock this node for change/write
func (n *Node) WriteLock() {
	n.mu.Lock()
}

// Unlock this node
func (n *Node) Unlock() {
	n.mu.Unlock()
}

// MarkRemoval mark this node as REMOVED
//
// Should only be called when WriteLock is held, or when no concurrency is guaranteed
func (n *Node) MarkRemoval() {
	n.state = common.MARKED_REMOVED
}

// MarkRemoval returns whether this node is alrady marked-removal
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) MarkedRemoval() bool {
	return n.state == common.MARKED_REMOVED
}

// GetCount returns the number of items in this node
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) GetCount() int {
	return n.dataCount
}

// GetPositionLessThanEqual returns the position of the key
// less than or equal the given key
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) GetPositionLessThanEqual(key interface{}) int {
	for i := 0; i < n.dataCount; i++ {
		if n.cmp(key, n.data[i].Key) <= 0 {
			return i
		}
	}
	return -1
}

// GetPositionGreaterThanEqual returns the position
// of at least equal to the given key
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) GetPositionGreaterThanEqual(key interface{}) int {
	for i := 0; i < n.dataCount; i++ {
		if n.cmp(n.data[i].Key, key) >= 0 {
			return i
		}
	}
	return -1
}

// GetPositionExact returns the position of the key in the node
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) GetPositionExact(key interface{}) int {
	if n.dataCount == 0 {
		return -1
	}
	low := 0
	high := n.dataCount - 1
	for high >= low {
		mid := low + ((high - low) / 2)
		cmp := n.cmp(n.data[mid].Key, key)
		if cmp == 0 {
			return mid
		}
		if cmp == -1 { // cause key in node checked first
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return -1
}

// Insert ih into current node.
// Whether this node is the correct node, is left for the upper layer
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) Insert(ih ItemHandle) error {
	idx := n.GetPositionExact(ih.Key)
	if idx != -1 {
		return ErrKeyAlreadyExist
	}
	if n.dataCount == NODE_SIZE {
		return ErrNodeIsFull
	}
	idx = n.GetPositionLessThanEqual(ih.Key)
	if idx == -1 {
		n.data[n.dataCount] = ih
	} else {
		copy(n.data[idx+1:n.dataCount+1], n.data[idx:n.dataCount])
		n.data[idx] = ih
	}
	n.dataCount++
	return nil
}

// Delete the specified key, if any
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) Delete(key interface{}) error {
	if n.dataCount == 0 {
		return ErrNodeIsEmpty
	}
	idx := n.GetPositionExact(key)
	if idx == -1 {
		return ErrDataNotFound
	}
	n.dataCount--
	copy(n.data[idx:n.dataCount], n.data[idx+1:n.dataCount+1])
	return nil
}

// Update the itemHandle for d.Key into d
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) Update(d ItemHandle) error {
	if n.dataCount == 0 {
		return ErrNodeIsEmpty
	}
	idx := n.GetPositionExact(d.Key)
	if idx == -1 {
		return ErrDataNotFound
	}
	n.data[idx].Value = d.Value
	return nil
}

// Get returns the value for the specified key, if any
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) Get(key interface{}) (interface{}, error) {
	if n.dataCount == 0 {
		return nil, ErrNodeIsEmpty
	}
	idx := n.GetPositionExact(key)
	if idx == -1 {
		return nil, ErrDataNotFound
	}
	return n.data[idx].Value, nil
}

// Exist checks when the given key is in this node
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) Exist(key interface{}) bool {
	if n.dataCount == 0 {
		return false
	}
	idx := n.GetPositionExact(key)
	return idx != -1
}

// CheckKeyStrictlyLessThanMax checks whether key is less than the biggest value in this node
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) CheckKeyStrictlyLessThanMax(key interface{}) (bool, error) {
	if n.dataCount == 0 {
		return false, ErrNodeIsEmpty
	}
	return n.cmp(key, n.data[n.dataCount-1].Key) == -1, nil
}

// CheckKeyStrictlyLessThanMin checks whether key is less than the smallest value in this node
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) CheckKeyStrictlyLessThanMin(key interface{}) (bool, error) {
	if n.dataCount == 0 {
		return false, ErrNodeIsEmpty
	}
	return n.cmp(key, n.data[0].Key) == -1, nil
}

// ConnectNode set nextNodes at height `atHeight` to `next`
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node) ConnectNode(atHeight int, next *Node) error {
	if atHeight < 0 || atHeight >= n.height {
		return ErrHeightOutsideRange
	}
	n.nextNodes[atHeight] = next
	return nil
}

// DisconnectNode set nextNodes at height `atHeight` to nil
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
// or when already marked for removal
func (n *Node) DisconnectNode(atHeight int) error {
	if atHeight < 0 || atHeight >= n.height {
		return ErrHeightOutsideRange
	}
	n.nextNodes[atHeight] = nil
	return nil
}

// GetNextNodeAt returns the next node at the given `atHeight`
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
func (n *Node) GetNextNodeAt(atHeight int) (*Node, error) {
	if atHeight < 0 || atHeight >= n.height {
		return nil, ErrHeightOutsideRange
	}
	return n.nextNodes[atHeight], nil
}

// ScanAll pass each data to fn
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
func (n *Node) ScanAll(fn func(ItemHandle)) {
	for i := 0; i < n.GetCount(); i++ {
		fn(n.data[i])
	}
}

// ScanGreaterThanEqual pass each data greater than `key` to fn
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
func (n *Node) ScanGreaterThanEqual(key interface{}, fn func(ItemHandle)) {
	ok, _ := n.CheckKeyStrictlyLessThanMax(key)
	if !ok {
		return
	}
	idx := n.GetPositionGreaterThanEqual(key)
	if idx != -1 {
		for i := idx; i < n.GetCount(); i++ {
			fn(n.data[i])
		}
	} else {
		//much lower than min
		n.ScanAll(fn)
	}
}

// ScanStrictlyLessThan pass each data strictly less than `key` to fn
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
func (n *Node) ScanStrictlyLessThan(key interface{}, fn func(ItemHandle)) {
	ok, _ := n.CheckKeyStrictlyLessThanMin(key)
	if ok {
		return
	}
	idx := n.GetPositionLessThanEqual(key)
	if idx != -1 {
		for i := 0; i < idx; i++ {
			fn(n.data[i])
		}
	} else {
		// much bigger than contents
		n.ScanAll(fn)
	}
}

// SplitIntoNewNode split current node's contents with the first half still in current node
// and second half into returned node (may be empty)
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
func (n *Node) SplitIntoNewNode(h int) *Node {
	posToSplit := n.dataCount / 2
	newNode := NewNodeWithOrderedSlice(h, n.data[posToSplit:], n.dataCount-posToSplit, n.cmp)
	n.dataCount = posToSplit
	return newNode
}

// GetMinKey returns the key at pos 0, if any
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
func (n *Node) GetMinKey() (interface{}, error) {
	if n.dataCount == 0 {
		return nil, ErrNodeIsEmpty
	}
	return n.data[0].Key, nil
}
