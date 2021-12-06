package node

import (
	"testing"
)

func cmpTest(a, b interface{}) int {
	if a.(int) == b.(int) {
		return 0
	}
	if a.(int) < b.(int) {
		return -1
	}
	return 1
}

func TestBOWLNode(t *testing.T) {
	bn := NewEmptyNode(16, cmpTest)
	bn.WriteLock()
	defer bn.Unlock()

	ok := bn.Exist(1)
	if ok {
		t.Fatal("It should be false, cause still empty, but it is not")
	}

	_, err := bn.Get(1)
	if err == nil || err != ErrNodeIsEmpty {
		t.Fatalf("err should be errNodeIsEmpty, but instead we got %v", err)
	}

	err = bn.Insert(ItemHandle{Key: 1, Value: 1})
	if err != nil {
		t.Fatalf("It should be okay to insert, but instead we got %v", err)
	}
	for i := 32; i > 1; i-- {
		err = bn.Insert(ItemHandle{Key: i, Value: i})
		if err != nil {
			t.Fatalf("It should be okay to insert, but instead we got %v", err)
		}
	}

	cnt := bn.GetCount()
	if cnt != 32 {
		t.Fatalf("We should have 32 data, but instead we only have %d", cnt)
	}

	err = bn.Insert(ItemHandle{Key: 250})
	if err == nil || err != ErrNodeIsFull {
		t.Fatalf("err should be errNodeIsFull, but instead we got %v", err)
	}

	val, err := bn.Get(16)
	if err != nil {
		t.Fatalf("err should be nil, caused 16 exists, but instead we got %v", err)
	}
	if val.(int) != 16 {
		t.Fatalf("Val should be 16, but instead we got %d", val.(int))
	}

	_, err = bn.Get(33)
	if err == nil || err != ErrDataNotFound {
		t.Fatalf("err should be errDataNotFound, but instead we got %v", err)
	}
	ok = bn.Exist(33)
	if ok {
		t.Fatalf("It shouldn't be found, cause `get` already return errDataNotFound, but instead it is")
	}

	ok, err = bn.CheckKeyStrictlyLessThanMin(4)
	if err != nil {
		t.Fatalf("It shouldn't be an error, cause node has data, but instead we got %v", err)
	}
	if ok {
		t.Fatal("It should be false, cause current max is key `1`")
	}

	ok, err = bn.CheckKeyStrictlyLessThanMin(0)
	if err != nil {
		t.Fatalf("It shouldn't be an error, cause node has data, but instead we got %v", err)
	}
	if !ok {
		t.Fatal("It should be true, cause current max is key `1`")
	}

	ok, err = bn.CheckKeyStrictlyLessThanMax(35)
	if err != nil {
		t.Fatalf("It shouldn't be an error, cause node has data, but instead we got %v", err)
	}
	if ok {
		t.Fatal("It should be false, cause current max is key `32`")
	}

	err = bn.Update(ItemHandle{Key: 16, Value: 50})
	if err != nil {
		t.Fatalf("It should be okay to update value of key 16, cause it exists, but instead we got %v", err)
	}
	val, _ = bn.Get(16)
	if val.(int) != 50 {
		t.Fatalf("It should be 50 after we updated it, but instead we got %d", val.(int))
	}

	err = bn.Update(ItemHandle{Key: 34, Value: 34})
	if err == nil || err != ErrDataNotFound {
		t.Fatalf("updating 34 should fail with errDataNotFound, but instead we got %v", err)
	}

	err = bn.Delete(41)
	if err == nil || err != ErrDataNotFound {
		t.Fatalf("deleting 41 should fail with errDataNotFound, but instead we got %v", err)
	}

	err = bn.Delete(17)
	if err != nil {
		t.Fatalf("deleting 17 should be ok cause it exists, but instead we got %v", err)
	}
	ok = bn.Exist(17)
	if ok {
		t.Fatal("It should be false, cause 17 already got deleted, but it is not")
	}
	cnt = bn.GetCount()
	if cnt != 31 {
		t.Fatalf("We should now only have 31 data, cause 17 already got deleted, but instead we only have %d", cnt)
	}

	err = bn.Insert(ItemHandle{Key: 11})
	if err == nil || err != ErrKeyAlreadyExist {
		t.Fatalf("err should be ErrKeyAlreadyExist, but instead we got %v", err)
	}

	err = bn.Insert(ItemHandle{Key: 38, Value: 38})
	if err != nil {
		t.Fatalf("It should be inserted cause we have slot, but instead we got %v", err)
	}

	ok, err = bn.CheckKeyStrictlyLessThanMax(35)
	if err != nil {
		t.Fatalf("It shouldn't be an error, cause node has data, but instead we got %v", err)
	}
	if !ok {
		t.Fatal("It should be true, cause current max is key `38`")
	}

	newNode := bn.SplitIntoNewNode(10)
	if !bn.Exist(16) || !newNode.Exist(18) {
		t.Fatalf("It should be split evenly, with 16 and 18 as the cut position, but instead we got %v and %v", bn.data, newNode.data)
	}
}

func TestBOWLNewOrderedNodeAndSplitting(t *testing.T) {
	ihs := make([]ItemHandle, 2)
	ihs[0] = ItemHandle{Key: 1, Value: 1}
	ihs[1] = ItemHandle{Key: 2, Value: 2}
	bn := NewNodeWithOrderedSlice(5, ihs, cmpTest)

	if len(bn.data) != NODE_SIZE {
		t.Fatalf("It should still have len 32, but instead we got %d", len(bn.data))
	}

	bn.WriteLock()
	defer bn.Unlock()

	if bn.dataCount != 2 {
		t.Fatalf("It should only have 2 data, but instead we got %d", bn.dataCount)
	}

	if !bn.Exist(1) || !bn.Exist(2) {
		t.Fatal("Both should be exist but instead it is not")
	}

	newNode := bn.SplitIntoNewNode(4)
	if !bn.Exist(1) || !newNode.Exist(2) {
		t.Fatalf("It should be split evenly, but instead we got %v and %v", bn.data, newNode.data)
	}

	if len(bn.data) != NODE_SIZE || cap(bn.data) != NODE_SIZE {
		t.Fatalf("Both should still have len and cap of 32, but instead we got len:%d and cap:%d", len(bn.data), cap(bn.data))
	}

	if len(newNode.data) != NODE_SIZE || cap(newNode.data) != NODE_SIZE {
		t.Fatalf("Both should still have len and cap of 32, but instead we got len:%d and cap:%d", len(newNode.data), cap(newNode.data))
	}
}

func TestBOWLNodeMarkRemoval(t *testing.T) {
	bn := NewEmptyNode(5, cmpTest)

	bn.WriteLock()
	defer bn.Unlock()

	bn.MarkRemoval()
	if !bn.MarkedRemoval() {
		t.Fatalf("Should be already MARKED_REMOVAL, but it is not")
	}
}

func TestBOWLNodeConnect(t *testing.T) {
	bn := NewEmptyNode(5, cmpTest)  // 0-4
	bn2 := NewEmptyNode(4, cmpTest) // 0-3
	bn.WriteLock()
	defer bn.Unlock()

	h := bn.GetHeight()
	if h != 5 {
		t.Fatalf("bn should have height 5, but instead we got %d", h)
	}
	_, err := bn.GetNextNodeAt(7)
	if err == nil || err != ErrHeightOutsideRange {
		t.Fatalf("err should be ErrHeightOutsideRange, but instead we got %v", err)
	}

	err = bn.ConnectNode(3, bn2)
	if err != nil {
		t.Fatalf("err should be nil, but instead we got %v", err)
	}
	for i := 0; i < 5; i++ {
		n, _ := bn.GetNextNodeAt(i)
		if i == 3 {
			if n != bn2 {
				t.Fatalf("It should be bn2, but instead we got %v", bn.nextNodes[3])
			}
		} else if n != nil {
			t.Fatalf("It should be nil, cause none connected, but instead we got %v", n)
		}
	}

	err = bn.ConnectNode(5, bn2)
	if err == nil || err != ErrHeightOutsideRange {
		t.Fatalf("err should be errHeightOutsideRange, but instead we got %v", err)
	}

	err = bn.DisconnectNode(3)
	if err != nil {
		t.Fatalf("err should be nil, but instead we got %v", err)
	}
	for i := 0; i < 5; i++ {
		n, _ := bn.GetNextNodeAt(i)
		if n != nil {
			t.Fatalf("It should be nil, cause none connected, but instead we got %v", n)
		}
	}
}

func TestBOWLNodeScan(t *testing.T) {
	bn := NewEmptyNode(16, cmpTest)
	bn.WriteLock()
	defer bn.Unlock()

	for i := 30; i > 0; i-- {
		bn.Insert(ItemHandle{Key: i, Value: i})
	}

	scanAllSum := 0
	bn.ScanAll(func(ih ItemHandle) {
		scanAllSum += ih.Key.(int)
	})
	if scanAllSum != 465 {
		t.Fatalf("It should be 465, but instead we got %d", scanAllSum)
	}

	scanGteSum := 0
	bn.ScanGreaterThanEqual(31, func(ih ItemHandle) {
		scanGteSum += ih.Key.(int)
	})
	if scanGteSum != 0 {
		t.Fatalf("It should be 0, but instead we got %d", scanGteSum)
	}
	bn.ScanGreaterThanEqual(15, func(ih ItemHandle) {
		scanGteSum += ih.Key.(int)
	})
	if scanGteSum != 360 {
		t.Fatalf("It should be 360, but instead we got %d", scanGteSum)
	}
	scanGteSum = 0
	bn.ScanGreaterThanEqual(-1, func(ih ItemHandle) {
		scanGteSum += ih.Key.(int)
	})
	if scanGteSum != 465 {
		t.Fatalf("It should be 465, but instead we got %d", scanGteSum)
	}

	scanStrictLtSum := 0
	bn.ScanStrictlyLessThan(36, func(ih ItemHandle) {
		scanStrictLtSum += ih.Key.(int)
	})
	if scanStrictLtSum != 465 {
		t.Fatalf("It should be 465, but instead we got %d", scanStrictLtSum)
	}
	scanStrictLtSum = 0
	bn.ScanStrictlyLessThan(1, func(ih ItemHandle) {
		scanStrictLtSum += ih.Key.(int)
	})
	if scanStrictLtSum != 0 {
		t.Fatalf("It should be 0, but instead we got %d", scanStrictLtSum)
	}
	bn.ScanStrictlyLessThan(15, func(ih ItemHandle) {
		scanStrictLtSum += ih.Key.(int)
	})
	if scanStrictLtSum != 105 {
		t.Fatalf("It should be 105, but instead we got %d", scanStrictLtSum)
	}
}
