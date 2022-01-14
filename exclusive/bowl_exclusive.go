package exclusive

import (
	"fmt"
	"sync"

	"github.com/aarondwi/bowl/common"
	"github.com/aarondwi/bowl/node"
)

const (
	MAX_HEIGHT int = 32
)

// BowlExclusive is a BOWL where every operation grabs single mutex,
// reducing concurrency possibility, but gain simplicity of development,
// as the API can be set to be totally one-pass, even on reconnections
//
// Deletes are still deferred for next access. While actually not needed,
// it makes the implementation uniform with the others.
//
// It has `STRICT SERIALIZABLE` isolation level, as everything goes through a single mutex
type BowlExclusive struct {
	sync.Mutex
	head *node.Node
	ch   <-chan int
	cmp  common.Comparator
}

// NewBOWL creates our new empty BOWL, with given common.Comparator
func NewBOWL(cmp common.Comparator) *BowlExclusive {
	// empty node for head, so can skip logic for removing head if empty
	head := node.NewEmptyNode(32, cmp)
	ch := common.RandomLevelGenerator(MAX_HEIGHT)

	return &BowlExclusive{head: head, ch: ch, cmp: cmp}
}

// Get returns all values for the given keys
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *BowlExclusive) Get(keys []interface{}) []interface{} {
	result := make([]interface{}, len(keys))

	b.Lock()
	defer b.Unlock()

	currentNode := b.getNextNodeFromHead(keys[0])

	for i, k := range keys {
		currentNode = b.getCorrectNode(k, currentNode)
		v, _ := currentNode.Get(k)
		result[i] = v
	}
	return result
}

// Update updates ih[i].Value when mathing ih[i].Key found
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *BowlExclusive) Update(ihs []node.ItemHandle) []error {
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
func (b *BowlExclusive) Delete(keys []interface{}) []error {
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

func (b *BowlExclusive) insertFastPathConnectNewNodeFromCurrent(
	currentNode *node.Node, newNode *node.Node, height int) {
	for j := 0; j < height; j++ {
		n, _ := currentNode.GetNextNodeAt(j)
		newNode.ConnectNode(j, n)
		currentNode.ConnectNode(j, newNode)
	}
}

func (b *BowlExclusive) connectUntil(
	targetNode *node.Node, fromHeight, toHeight int) {
	targetMinKey, _ := targetNode.GetMinKey()
	currentNode := b.head
	for h := fromHeight; h >= toHeight; h-- {
		for {
			n, _ := currentNode.GetNextNodeAt(h)
			if n == nil {
				currentNode.ConnectNode(h, targetNode)
				break
			}
			ok, n := b.getNextNodeAtHeightNotMarkedRemoval(h, currentNode, n)
			if !ok {
				currentNode.ConnectNode(h, targetNode)
				break
			}
			ok, _ = n.CheckKeyStrictlyLessThanMin(targetMinKey)
			if ok {
				currentNode.ConnectNode(h, targetNode)
				targetNode.ConnectNode(h, n)
				break
			}
			currentNode = n
		}
	}
}

// Insert returns all values for the given keys
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *BowlExclusive) Insert(ihs []node.ItemHandle) []error {
	errs := make([]error, len(ihs))

	b.Lock()
	defer b.Unlock()

	currentNode := b.getNextNodeFromHead(ihs[0].Key)

	for i, ih := range ihs {
		currentNode = b.getCorrectNodeFromItemHandle(ih, currentNode)
		err := currentNode.Insert(ih)
		if err != nil && err == node.ErrNodeIsFull {
			newHeight := <-b.ch
			newNode := currentNode.SplitIntoNewNode(newHeight)

			minHeight := newHeight
			if minHeight > currentNode.GetHeight() {
				minHeight = currentNode.GetHeight()
			}
			b.insertFastPathConnectNewNodeFromCurrent(currentNode, newNode, minHeight)

			if newHeight > currentNode.GetHeight() {
				b.connectUntil(newNode, newHeight, currentNode.GetHeight())
			}

			if ok, _ := currentNode.CheckKeyStrictlyLessThanMax(ih.Key); ok {
				err = currentNode.Insert(ih)
			} else {
				err = newNode.Insert(ih)
				currentNode = newNode
			}
			if err != nil {
				panic(fmt.Sprintf("Should be no error here, means something is broken: %v", err))
			}
		}
		errs[i] = err
	}
	return errs
}

func (b *BowlExclusive) scanNextNodeNotMarkedRemoval(
	prev, next *node.Node) (bool, *node.Node) {
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

func (b *BowlExclusive) getNextNodeAtHeightNotMarkedRemoval(
	h int, prev, next *node.Node) (bool, *node.Node) {
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

func (b *BowlExclusive) getValidNodeToStartScan() *node.Node {
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
func (b *BowlExclusive) ScanAll(fn func(node.ItemHandle)) {
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
func (b *BowlExclusive) ScanGreaterThanEqual(
	key interface{}, fn func(node.ItemHandle)) {
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
func (b *BowlExclusive) ScanStrictlyLessThan(
	key interface{}, fn func(node.ItemHandle)) {
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
func (b *BowlExclusive) ScanRange(
	fromKey interface{}, toKey interface{}, fn func(node.ItemHandle)) {
	b.Lock()
	defer b.Unlock()

	node := b.getNextNodeFromHead(fromKey)
	node = b.getCorrectNode(fromKey, node)
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
func (b *BowlExclusive) getNextNodeFromHead(key interface{}) *node.Node {
	var n *node.Node
	for h := MAX_HEIGHT - 1; h >= 0; {
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

		ok, _ = next.CheckKeyStrictlyLessThanMin(key)
		if !ok { // meaning bigger than next min
			return next
		}
		h--
	}

	// meaning this BOWL is empty, create new
	if n == nil {
		nextHeight := <-b.ch
		next := node.NewEmptyNode(nextHeight, b.cmp)
		for i := 0; i < nextHeight; i++ {
			b.head.ConnectNode(i, next)
		}
	}
	return n
}

// getCorrectNode returns the node that should has the key
//
// splitting this function from `getNextNodeFromHead` cause we have some logic skipping on head
func (b *BowlExclusive) getCorrectNode(
	key interface{}, currentNode *node.Node) *node.Node {
	h := currentNode.GetHeight()
	for {
		ok, _ := currentNode.CheckKeyStrictlyLessThanMax(key)
		if ok {
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
func (b *BowlExclusive) getCorrectNodeFromItemHandle(
	ih node.ItemHandle, currentNode *node.Node) *node.Node {
	return b.getCorrectNode(ih.Key, currentNode)
}
