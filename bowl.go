package bowl

import (
	"fmt"
	"sync"
)

const (
	MAX_HEIGHT int = 64
)

// Bowl is an unrolled skip list where every operation grabs single mutex,
// reducing concurrency possibility, but gain simplicity of development,
// as the API can be set to be totally one-pass, even on reconnections
//
// Deletes are still deferred for next access. While actually not needed,
// it makes the implementation uniform with the others.
//
// It has `STRICT SERIALIZABLE` isolation level, as everything goes through a single mutex
type Bowl[k comparable, v any] struct {
	sync.Mutex
	head *Node[k, v]
	ch   <-chan int
	cmp  Comparator[k]

	// this variables would hold all the latest pointing nodes for all height
	// the goal is not to scan from the beginning just to connect pointers on any new nodes
	//
	// this is only used for Insert
	// we update this value on any traversals
	// but can safely ignore them for any other operations
	latestPointingNodes []*Node[k, v]
}

// NewBOWL creates our new empty BOWL, with given Comparator
func NewBOWL[k comparable, v any](cmp Comparator[k]) *Bowl[k, v] {
	// empty node for head, so can skip logic for removing head if empty
	head := NewEmptyNode[k, v](MAX_HEIGHT, cmp)
	ch := RandomLevelGenerator(MAX_HEIGHT)
	latestPointingNodes := make([]*Node[k, v], MAX_HEIGHT)

	return &Bowl[k, v]{head: head, ch: ch, cmp: cmp, latestPointingNodes: latestPointingNodes}
}

func (b *Bowl[k, v]) resetLatestPointingNodes() {
	for i := 0; i < MAX_HEIGHT; i++ {
		b.latestPointingNodes[i] = b.head
	}
}

func (b *Bowl[k, v]) setLatestPointingNodes(n *Node[k, v]) {
	for i := 0; i < n.GetHeight(); i++ {
		b.latestPointingNodes[i] = n
	}
}

// Get returns all values for the given keys
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *Bowl[k, v]) Get(keys []k, notFoundDefaultValue v) []v {
	result := make([]v, len(keys))

	b.Lock()
	defer b.Unlock()

	currentNode := b.getNextNodeFromHead(keys[0])

	for i, k := range keys {
		currentNode = b.getCorrectNode(k, currentNode)
		v, _ := currentNode.Get(k, notFoundDefaultValue)
		result[i] = v
	}
	return result
}

// Update updates ih[i].Value when mathing ih[i].Key found
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *Bowl[k, v]) Update(ihs []Item[k, v]) []error {
	errs := make([]error, len(ihs))

	b.Lock()
	defer b.Unlock()

	currentNode := b.getNextNodeFromHead(ihs[0].Key)
	for i, ih := range ihs {
		currentNode = b.getCorrectNodeFromItemHandle(ih, currentNode)
		errs[i] = currentNode.Update(ih)
	}
	return errs
}

// Delete removes all matching keys
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *Bowl[k, v]) Delete(keys []k) []error {
	errs := make([]error, len(keys))

	b.Lock()
	defer b.Unlock()

	currentNode := b.getNextNodeFromHead(keys[0])

	for i, k := range keys {
		currentNode = b.getCorrectNode(k, currentNode)
		errs[i] = currentNode.Delete(k)
		if currentNode.GetCount() == 0 {
			currentNode.MarkRemoval()
		}
	}
	return errs
}

func (b *Bowl[k, v]) insertFastPathConnectNewNodeFromCurrent(
	currentNode *Node[k, v], newNode *Node[k, v], height int) {
	for j := 0; j < height; j++ {
		n, _ := currentNode.GetNextNodeAt(j)
		newNode.ConnectNode(j, n)
		currentNode.ConnectNode(j, newNode)
	}
}

func (b *Bowl[k, v]) connectUntil(
	targetNode *Node[k, v], fromHeight, toHeight int) {
	for h := fromHeight; h >= toHeight; h-- {
		err := b.latestPointingNodes[h].ConnectNode(h, targetNode)
		if err != nil {
			panic(fmt.Sprintf("Should be no error in `connectUntil` function, means something is broken: %v", err))
		}
	}
}

// Insert returns all values for the given keys
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *Bowl[k, v]) Insert(ihs []Item[k, v]) []error {
	errs := make([]error, len(ihs))

	b.Lock()
	defer b.Unlock()

	b.resetLatestPointingNodes()
	currentNode := b.getNextNodeFromHead(ihs[0].Key)

	for i, ih := range ihs {
		currentNode = b.getCorrectNodeFromItemHandle(ih, currentNode)
		err := currentNode.Insert(ih)
		if err != nil && err == ErrNodeIsFull {
			newHeight := <-b.ch
			newNode := currentNode.SplitIntoNewNode(newHeight)

			minHeight := newHeight
			if minHeight > currentNode.GetHeight() {
				minHeight = currentNode.GetHeight()
			}
			b.insertFastPathConnectNewNodeFromCurrent(currentNode, newNode, minHeight)
			b.setLatestPointingNodes(currentNode)
			if newHeight > currentNode.GetHeight() {
				b.connectUntil(newNode, newHeight, currentNode.GetHeight())
			}

			if ok, _ := currentNode.CheckKeyStrictlyLessThanMax(ih.Key); ok {
				err = currentNode.Insert(ih)
			} else {
				err = newNode.Insert(ih)
				currentNode = newNode
				b.setLatestPointingNodes(currentNode)
			}
			if err != nil {
				panic(fmt.Sprintf("Should be no error here, means something is broken: %v", err))
			}
		}
		errs[i] = err
	}
	return errs
}

func (b *Bowl[k, v]) scanNextNodeNotMarkedRemoval(
	prev, next *Node[k, v]) (bool, *Node[k, v]) {
	for next.MarkedRemoval() {
		afterNext, _ := next.GetNextNodeAt(0)
		if afterNext == nil {
			return false, next
		}
		prev.ConnectNode(0, afterNext)
		next.DisconnectNode(0)
		next = afterNext
	}
	return true, next
}

func (b *Bowl[k, v]) getNextNodeAtHeightNotMarkedRemoval(
	h int, prev, next *Node[k, v]) (bool, *Node[k, v]) {
	atLeast1NotMarkedRemovalAtThisHeight := true
	for next.MarkedRemoval() {
		afterNext, _ := next.GetNextNodeAt(h)
		if afterNext == nil {
			atLeast1NotMarkedRemovalAtThisHeight = false
			break
		}
		prev.ConnectNode(h, afterNext)
		next.DisconnectNode(h)
		next = afterNext
	}
	return atLeast1NotMarkedRemovalAtThisHeight, next
}

func (b *Bowl[k, v]) getValidNodeToStartScan() *Node[k, v] {
	node, _ := b.head.GetNextNodeAt(0)
	if node == nil {
		return nil
	}
	ok, node := b.getNextNodeAtHeightNotMarkedRemoval(0, b.head, node)
	if !ok {
		return nil
	}
	return node
}

// ScanAll pass each data to fn
func (b *Bowl[k, v]) ScanAll(fn func(Item[k, v])) {
	b.Lock()
	defer b.Unlock()

	node := b.getValidNodeToStartScan()
	if node == nil {
		return
	}

	for {
		node.ScanAll(fn)
		next, _ := node.GetNextNodeAt(0)
		if next == nil {
			break
		}
		ok, next := b.scanNextNodeNotMarkedRemoval(node, next)
		if !ok {
			return
		}
		node = next // can be nil
	}
}

// ScanGreaterThanEqual pass each data greater than `key` to fn
func (b *Bowl[k, v]) ScanGreaterThanEqual(
	key k, fn func(Item[k, v])) {
	b.Lock()
	defer b.Unlock()

	node := b.getNextNodeFromHead(key)
	node = b.getCorrectNode(key, node)
	node.ScanGreaterThanEqual(key, fn)
	for {
		next, _ := node.GetNextNodeAt(0)
		if next == nil {
			break
		}
		ok, next := b.scanNextNodeNotMarkedRemoval(node, next)
		if !ok {
			return
		}
		node = next
		node.ScanAll(fn)
	}
}

// ScanGreaterThanEqual pass each data until `key` to fn
func (b *Bowl[k, v]) ScanStrictlyLessThan(
	key k, fn func(Item[k, v])) {
	b.Lock()
	defer b.Unlock()

	node := b.getValidNodeToStartScan()
	if node == nil {
		return
	}

	for {
		ok, _ := node.CheckKeyStrictlyLessThanMax(key)
		if ok {
			node.ScanStrictlyLessThan(key, fn)
			break
		} else { // bigger than max
			node.ScanAll(fn)
		}
		next, _ := node.GetNextNodeAt(0)
		if next == nil {
			break
		}
		ok, next = b.scanNextNodeNotMarkedRemoval(node, next)
		if !ok {
			return
		}
		ok, _ = next.CheckKeyStrictlyLessThanMin(key)
		if ok {
			break
		}
		node = next
	}
}

// ScanRange pass each data between fromKey <= data <= toKey
func (b *Bowl[k, v]) ScanRange(
	fromKey k, toKey k, fn func(Item[k, v])) {
	b.Lock()
	defer b.Unlock()

	node := b.getNextNodeFromHead(fromKey)
	node = b.getCorrectNode(fromKey, node)

	// when all the values are all contained in the node
	ok, _ := node.CheckKeyStrictlyLessThanMax(toKey)
	if ok {
		node.ScanRange(fromKey, toKey, fn)
		return
	}

	// possibly more than current node only
	node.ScanGreaterThanEqual(fromKey, fn)
	for {
		next, _ := node.GetNextNodeAt(0)
		if next == nil {
			break
		}
		ok, next := b.scanNextNodeNotMarkedRemoval(node, next)
		if !ok {
			return
		}
		node = next

		ok, _ = node.CheckKeyStrictlyLessThanMax(toKey)
		if ok {
			node.ScanStrictlyLessThan(toKey, fn)
			break
		} else { // bigger than max
			node.ScanAll(fn)
		}

		ok, _ = next.CheckKeyStrictlyLessThanMin(toKey)
		if ok {
			break
		}
	}
}

// getNextNodeFromHead returns only the next node, and should be fast
//
// Separating this checked from normal flow easily guarantee that
// there will always be at least one data node beside head
//
// The node returned will never be nil, and is already locked
func (b *Bowl[k, v]) getNextNodeFromHead(key k) *Node[k, v] {
	for h := MAX_HEIGHT - 1; h > 0; {
		next, _ := b.head.GetNextNodeAt(h)
		if next == nil {
			h--
			continue
		}

		ok, next := b.getNextNodeAtHeightNotMarkedRemoval(h, b.head, next)
		if !ok {
			h--
			continue
		}

		ok, err := next.CheckKeyStrictlyLessThanMin(key)
		if err != nil && !ok { // meaning bigger than next min
			b.setLatestPointingNodes(next)
			return next
		}
		h--
	}

	// now check at height 0, cause already checked till height 1
	// and still no matches
	n, _ := b.head.GetNextNodeAt(0)
	if n == nil {
		// meaning this BOWL is empty, create new
		nextHeight := <-b.ch
		newNode := NewEmptyNode[k, v](nextHeight, b.cmp)
		for i := 0; i < nextHeight; i++ {
			b.head.ConnectNode(i, newNode)
		}
		b.setLatestPointingNodes(newNode)
		return newNode
	}
	return n
}

// getCorrectNode returns the node that should has the key
//
// splitting this function from `getNextNodeFromHead` cause we have some logic skipping on head
func (b *Bowl[k, v]) getCorrectNode(
	key k, currentNode *Node[k, v]) *Node[k, v] {
	h := currentNode.GetHeight()
	for {
		ok, _ := currentNode.CheckKeyStrictlyLessThanMax(key)
		if ok {
			b.setLatestPointingNodes(currentNode)
			return currentNode
		}

		// ---------------------------------------------------
		// Either way, find correct node
		// at least it needs to check one next node, else it is done
		//
		// We try from the highest one,
		// as each node contains lots of data
		// ---------------------------------------------------
		atLeastCheck1NextNode := false
		for h >= 0 {
			next, _ := currentNode.GetNextNodeAt(h)
			if next == nil {
				h--
				continue
			}

			ok, next = b.getNextNodeAtHeightNotMarkedRemoval(h, currentNode, next)
			if !ok {
				h--
				continue
			}

			atLeastCheck1NextNode = true
			ok, _ = next.CheckKeyStrictlyLessThanMin(key)
			if ok {
				h--
				continue
			}

			currentNode = next
			b.setLatestPointingNodes(currentNode)
			break
		}

		if !atLeastCheck1NextNode {
			break
		}
	}
	return currentNode
}

// getCorrectNode returns the node that should has the key
//
// splitting this function from `getNextNodeFromHead` cause we have some logic skipping on head
func (b *Bowl[k, v]) getCorrectNodeFromItemHandle(
	ih Item[k, v], currentNode *Node[k, v]) *Node[k, v] {
	return b.getCorrectNode(ih.Key, currentNode)
}
