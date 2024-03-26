package bowl

import (
	"errors"
)

const (
	NODE_SIZE int = 256
)

var ErrKeyAlreadyExist = errors.New("Given key is already exist")
var ErrNodeIsFull = errors.New("Node is already full")
var ErrNodeIsEmpty = errors.New("Node is empty")
var ErrDataNotFound = errors.New("Given data is not in this node")
var ErrHeightOutsideRange = errors.New("This node's height is lower than given height")

// Item wraps key-value pair into single object
type Item[k comparable, v any] struct {
	Key   k
	Value v
}

// Node holds a slice of at most NODE_SIZE data
//
// For deletion, the node is MARKED_REMOVAL, for now
//
// For now, it uses sync.Mutex for simplicity.
// As algorithm and implementation becomes more settled,
// will change to single int for lock, among others
//
// Another note is that I still haven't found good way to enforce data is a sort.Interface.
// Implementing sort.Interface wouldh have the benefit that the user can easily insert batched, ordered data at once
type Node[k comparable, v any] struct {
	state     State
	cmp       Comparator[k]
	dataCount int
	data      []Item[k, v]
	height    int
	nextNodes []*Node[k, v]
}

// NewEmptyNode creates Node with height h and given comparator
func NewEmptyNode[k comparable, v any](h int, cmp Comparator[k]) *Node[k, v] {
	return &Node[k, v]{
		state:     ACTIVE,
		cmp:       cmp,
		dataCount: 0,
		data:      make([]Item[k, v], NODE_SIZE),
		height:    h,
		nextNodes: make([]*Node[k, v], h),
	}
}

// NewNodeWithOrderedSlice creates Node with height h, given initial data and comparator
func NewNodeWithOrderedSlice[k comparable, v any](
	h int, data []Item[k, v], size int, cmp Comparator[k]) *Node[k, v] {
	n := &Node[k, v]{
		state:     ACTIVE,
		cmp:       cmp,
		dataCount: 0,
		data:      make([]Item[k, v], NODE_SIZE),
		height:    h,
		nextNodes: make([]*Node[k, v], h),
	}
	copy(n.data, data[:size])
	n.dataCount = size
	return n
}

// GetHeight returns n.height
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) GetHeight() int {
	return n.height
}

// MarkRemoval mark this node as REMOVED
//
// Should only be called when WriteLock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) MarkRemoval() {
	n.state = MARKED_REMOVED
}

// MarkRemoval returns whether this node is alrady marked-removal
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) MarkedRemoval() bool {
	return n.state == MARKED_REMOVED
}

// GetCount returns the number of items in this node
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) GetCount() int {
	return n.dataCount
}

// GetPositionLessThanEqual returns the position of the key
// less than or equal the given key
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) GetPositionLessThanEqual(key k) int {
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
func (n *Node[k, v]) GetPositionGreaterThanEqual(key k) int {
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
func (n *Node[k, v]) GetPositionExact(key k) int {
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
func (n *Node[k, v]) Insert(ih Item[k, v]) error {
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
func (n *Node[k, v]) Delete(key k) error {
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
func (n *Node[k, v]) Update(d Item[k, v]) error {
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
func (n *Node[k, v]) Get(key k, notFoundDefaultValue v) (v, error) {
	if n.dataCount == 0 {
		return notFoundDefaultValue, ErrNodeIsEmpty
	}
	idx := n.GetPositionExact(key)
	if idx == -1 {
		return notFoundDefaultValue, ErrDataNotFound
	}
	return n.data[idx].Value, nil
}

// Exist checks when the given key is in this node
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) Exist(key k) bool {
	if n.dataCount == 0 {
		return false
	}
	idx := n.GetPositionExact(key)
	return idx != -1
}

// CheckKeyStrictlyLessThanMax checks whether key is less than the biggest value in this node
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) CheckKeyStrictlyLessThanMax(key k) (bool, error) {
	if n.dataCount == 0 {
		return false, ErrNodeIsEmpty
	}
	return n.cmp(key, n.data[n.dataCount-1].Key) == -1, nil
}

// CheckKeyStrictlyLessThanMin checks whether key is less than the smallest value in this node
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) CheckKeyStrictlyLessThanMin(key k) (bool, error) {
	if n.dataCount == 0 {
		return false, ErrNodeIsEmpty
	}
	return n.cmp(key, n.data[0].Key) == -1, nil
}

// ConnectNode set nextNodes at height `atHeight` to `next`
//
// Should only be called when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) ConnectNode(atHeight int, next *Node[k, v]) error {
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
func (n *Node[k, v]) DisconnectNode(atHeight int) error {
	if atHeight < 0 || atHeight >= n.height {
		return ErrHeightOutsideRange
	}
	n.nextNodes[atHeight] = nil
	return nil
}

// GetNextNodeAt returns the next node at the given `atHeight`
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) GetNextNodeAt(atHeight int) (*Node[k, v], error) {
	if atHeight < 0 || atHeight >= n.height {
		return nil, ErrHeightOutsideRange
	}
	return n.nextNodes[atHeight], nil
}

// ScanAll pass each data to fn
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) ScanAll(fn func(Item[k, v])) {
	for i := 0; i < n.GetCount(); i++ {
		fn(n.data[i])
	}
}

// ScanGreaterThanEqual pass each data greater than `key` to fn
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) ScanGreaterThanEqual(key k, fn func(Item[k, v])) {
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
func (n *Node[k, v]) ScanStrictlyLessThan(key k, fn func(Item[k, v])) {
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
func (n *Node[k, v]) SplitIntoNewNode(h int) *Node[k, v] {
	posToSplit := n.dataCount / 2
	newNode := NewNodeWithOrderedSlice(h, n.data[posToSplit:], n.dataCount-posToSplit, n.cmp)
	n.dataCount = posToSplit
	return newNode
}

// GetMinKey returns the key at pos 0, if any
//
// Should only be called either when Lock is held, or when no concurrency is guaranteed
func (n *Node[k, v]) GetMinKey(notFoundDefaultValue k) (k, error) {
	if n.dataCount == 0 {
		return notFoundDefaultValue, ErrNodeIsEmpty
	}
	return n.data[0].Key, nil
}
