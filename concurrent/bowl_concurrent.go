package concurrent

import (
	"fmt"

	"github.com/aarondwi/bowl/common"
	"github.com/aarondwi/bowl/node"
)

const (
	MAX_HEIGHT int = 32
)

// BowlConcurrent is a BOWL where multiple insert/update/delete/read can run in parallel
//
// It has `READ COMMITTED` isolation level, and only guarantee per key linearizability
type BowlConcurrent struct {
	head *node.Node
	ch   <-chan int
	cmp  common.Comparator
}

// NewBOWL creates our new empty BOWL, with given common.Comparator
func NewBOWL(cmp common.Comparator) *BowlConcurrent {
	// empty node for head, so can skip logic for removing head if empty
	head := node.NewEmptyNode(32, cmp)
	ch := common.RandomLevelGenerator(MAX_HEIGHT)

	return &BowlConcurrent{head: head, ch: ch, cmp: cmp}
}

// Get returns all values for the given keys
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *BowlConcurrent) Get(keys []interface{}) []interface{} {
	result := make([]interface{}, len(keys))
	currentNode := b.getNextNodeFromHead(keys[0])

	for i, k := range keys {
		b.getCorrectNode(k, currentNode)
		v, _ := currentNode.Get(k)
		result[i] = v
	}
	currentNode.Unlock()
	return result
}

// Update updates ih[i].Value when mathing ih[i].Key found
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *BowlConcurrent) Update(ihs []node.ItemHandle) []error {
	errs := make([]error, len(ihs))
	currentNode := b.getNextNodeFromHead(ihs[0].Key)

	for i, ih := range ihs {
		b.getCorrectNodeFromItemHandle(ih, currentNode)
		errs[i] = currentNode.Update(ih)
	}
	currentNode.Unlock()
	return errs
}

// Delete removes all matching keys
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *BowlConcurrent) Delete(keys []interface{}) []error {
	errs := make([]error, len(keys))
	currentNode := b.getNextNodeFromHead(keys[0])

	for i, k := range keys {
		b.getCorrectNode(k, currentNode)
		errs[i] = currentNode.Delete(k)
		if currentNode.GetCount() == 0 {
			currentNode.MarkRemoval()
		}
	}
	currentNode.Unlock()
	return errs
}

func (b *BowlConcurrent) insertFastPathConnectNewNodeFromCurrent(
	currentNode *node.Node, newNode *node.Node, height int) {
	for j := 0; j < height; j++ {
		n, _ := currentNode.GetNextNodeAt(j)
		newNode.ConnectNode(j, n)
		currentNode.ConnectNode(j, newNode)
	}
}

// Insert returns all values for the given keys
//
// Note that keys should already be ascending-sorted, or else the result is NOT guaranteed
func (b *BowlConcurrent) Insert(ihs []node.ItemHandle) []error {
	errs := make([]error, len(ihs))

	// currentNode is already locked
	currentNode := b.getNextNodeFromHead(ihs[0].Key)

	for i, ih := range ihs {
		b.getCorrectNodeFromItemHandle(ih, currentNode)
		err := currentNode.Insert(ih)
		if err != nil && err == node.ErrNodeIsFull {
			newHeight := <-b.ch
			newNode := currentNode.SplitIntoNewNode(newHeight)

			newNode.WriteLock()
			minHeight := newHeight
			if minHeight > currentNode.GetHeight() {
				minHeight = currentNode.GetHeight()
			}
			b.insertFastPathConnectNewNodeFromCurrent(currentNode, newNode, minHeight)

			if newHeight > currentNode.GetHeight() {
				// heightDiff := newHeight - currentNode.GetHeight()
				// the rest? should start from head, but how to avoid deadlock?
			}

			if ok, _ := currentNode.CheckKeyStrictlyLessThanMax(ih.Key); ok {
				err = currentNode.Insert(ih)
				newNode.Unlock()
			} else {
				err = newNode.Insert(ih)
				currentNode.Unlock()
				currentNode = newNode
			}
			if err != nil {
				panic(fmt.Sprintf("Should be no error here, means something is brokenL %v", err))
			}
		}
		errs[i] = err
	}
	currentNode.Unlock()
	return errs
}

func (b *BowlConcurrent) scanNextNodeNotMarkedRemoval(prev, next *node.Node) bool {
	for next.MarkedRemoval() {
		afterNext, _ := next.GetNextNodeAt(0)
		if afterNext == nil {
			prev.Unlock()
			next.Unlock()
			return false
		}
		prev.ConnectNode(0, afterNext)
		next.DisconnectNode(0)
		afterNext.WriteLock()
		next.Unlock()
		next = afterNext
	}
	return true
}

func (b *BowlConcurrent) getNextNodeAtHeightNotMarkedRemoval(h int, prev, next *node.Node) bool {
	atLeast1NotMarkedRemovalAtThisHeight := true
	for next.MarkedRemoval() {
		afterNext, _ := next.GetNextNodeAt(h)
		if afterNext == nil {
			next.Unlock()
			atLeast1NotMarkedRemovalAtThisHeight = false
			break
		}
		prev.ConnectNode(h, afterNext)
		next.DisconnectNode(h)
		afterNext.WriteLock()
		next.Unlock()
		next = afterNext
	}
	return atLeast1NotMarkedRemovalAtThisHeight
}

func (b *BowlConcurrent) getValidNodeToStartScan() *node.Node {
	b.head.WriteLock()
	node, _ := b.head.GetNextNodeAt(0)
	if node == nil {
		b.head.Unlock()
		return nil
	}
	node.WriteLock()
	ok := b.getNextNodeAtHeightNotMarkedRemoval(0, b.head, node)
	if !ok {
		b.head.Unlock()
		node.Unlock()
		return nil
	}
	b.head.Unlock()
	return node
}

// ScanAll pass each data to fn
func (b *BowlConcurrent) ScanAll(fn func(node.ItemHandle)) {
	node := b.getValidNodeToStartScan()
	if node == nil {
		return
	}

	for {
		node.ScanAll(fn)
		next, _ := node.GetNextNodeAt(0)
		if next == nil {
			node.Unlock()
			break
		}
		next.WriteLock()
		ok := b.scanNextNodeNotMarkedRemoval(node, next)
		node.Unlock()
		if !ok {
			next.Unlock()
			return
		}
		node = next // can be nil
	}
}

// ScanGreaterThanEqual pass each data greater than `key` to fn
func (b *BowlConcurrent) ScanGreaterThanEqual(key interface{}, fn func(node.ItemHandle)) {
	node := b.getNextNodeFromHead(key)
	b.getCorrectNode(key, node)
	node.ScanGreaterThanEqual(key, fn)
	for {
		next, _ := node.GetNextNodeAt(0)
		if next == nil {
			node.Unlock()
			break
		}
		next.WriteLock()
		ok := b.scanNextNodeNotMarkedRemoval(node, next)
		node.Unlock()
		if !ok {
			next.Unlock()
			return
		}
		node = next
		node.ScanAll(fn)
	}
}

// ScanGreaterThanEqual pass each data until `key` to fn
func (b *BowlConcurrent) ScanStrictlyLessThan(key interface{}, fn func(node.ItemHandle)) {
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
			node.Unlock()
			break
		}
		next.WriteLock()
		ok = b.scanNextNodeNotMarkedRemoval(node, next)
		node.Unlock()
		if !ok {
			next.Unlock()
			return
		}
		ok, _ = next.CheckKeyStrictlyLessThanMin(key)
		if ok {
			next.Unlock()
			break
		}
		node = next
	}
}

// ScanRange pass each data between fromKey <= data <= toKey
func (b *BowlConcurrent) ScanRange(fromKey interface{}, toKey interface{}, fn func(node.ItemHandle)) {
	node := b.getNextNodeFromHead(fromKey)
	b.getCorrectNode(fromKey, node)
	node.ScanGreaterThanEqual(fromKey, fn)

	for {
		next, _ := node.GetNextNodeAt(0)
		if next == nil {
			node.Unlock()
			break
		}
		next.WriteLock()
		ok := b.scanNextNodeNotMarkedRemoval(node, next)
		node.Unlock()
		if !ok {
			next.Unlock()
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
			node.Unlock()
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
func (b *BowlConcurrent) getNextNodeFromHead(key interface{}) *node.Node {
	var n *node.Node
	b.head.WriteLock()
	for h := MAX_HEIGHT - 1; h >= 0; {
		next, _ := b.head.GetNextNodeAt(h)
		if next == nil {
			h--
			continue
		}

		next.WriteLock()
		ok := b.getNextNodeAtHeightNotMarkedRemoval(h, b.head, next)
		if !ok {
			h--
			continue
		}

		ok, _ = next.CheckKeyStrictlyLessThanMin(key)
		if !ok { // meaning bigger than next min
			n = next // still locked when returned
			b.head.Unlock()
			return n
		}
		next.Unlock() // wrong one, check others
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
	n.WriteLock()
	b.head.Unlock()
	return n
}

// getCorrectNode returns the node that should has the key
//
// splitting this function from `getNextNodeFromHead` cause we have some logic skipping on head
func (b *BowlConcurrent) getCorrectNode(key interface{}, currentNode *node.Node) {
	h := currentNode.GetHeight()
	for {
		ok, _ := currentNode.CheckKeyStrictlyLessThanMax(key)
		if ok {
			return
		}

		// Either way, find correct node
		// at least it needs to check one next node, else it is done
		//
		// We try from the highest one,
		// as each node contains lots of data
		atLeastCheck1NextNode := false
		for h >= 0 {
			n, _ := currentNode.GetNextNodeAt(h)
			if n == nil {
				h--
				continue
			}

			n.WriteLock()
			ok = b.getNextNodeAtHeightNotMarkedRemoval(h, currentNode, n)
			if !ok {
				h--
				continue
			}

			atLeastCheck1NextNode = true
			ok, _ = n.CheckKeyStrictlyLessThanMin(key)
			if ok {
				n.Unlock()
				h--
				continue
			}

			currentNode.Unlock()
			currentNode = n // already locked
			break
		}

		if !atLeastCheck1NextNode {
			break
		}
	}
}

// getCorrectNode returns the node that should has the key
//
// splitting this function from `getNextNodeFromHead` cause we have some logic skipping on head
func (b *BowlConcurrent) getCorrectNodeFromItemHandle(ih node.ItemHandle, currentNode *node.Node) {
	b.getCorrectNode(ih.Key, currentNode)
}
