package bowl

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
	ok := bn.lock()
	if !ok {
		t.Fatal("It should be `true` but it is not")
	}
	defer bn.unlock()

	ok = bn.exist(1)
	if ok {
		t.Fatal("It should be false, cause still empty, but it is not")
	}

	_, err := bn.get(1)
	if err == nil || err != errNodeIsEmpty {
		t.Fatalf("err should be errNodeIsEmpty, but instead we got %v", err)
	}

	err = bn.insert(ItemHandle{key: 1, value: 1})
	if err != nil {
		t.Fatalf("It should be okay to insert, but instead we got %v", err)
	}
	for i := 32; i > 1; i-- {
		err = bn.insert(ItemHandle{key: i, value: i})
		if err != nil {
			t.Fatalf("It should be okay to insert, but instead we got %v", err)
		}
	}

	err = bn.insert(ItemHandle{})
	if err == nil || err != errNodeIsFull {
		t.Fatalf("err should be errNodeIsFull, but instead we got %v", err)
	}

	val, err := bn.get(16)
	if err != nil {
		t.Fatalf("err should be nil, caused 16 exists, but instead we got %v", err)
	}
	if val.(int) != 16 {
		t.Fatalf("Val should be 16, but instead we got %d", val.(int))
	}

	_, err = bn.get(33)
	if err == nil || err != errDataNotFound {
		t.Fatalf("err should be errDataNotFound, but instead we got %v", err)
	}
	ok = bn.exist(33)
	if ok {
		t.Fatalf("It shouldn't be found, cause `get` already return errDataNotFound, but instead it is")
	}

	ok, err = bn.checkKeyStrictlyLessThanMin(4)
	if err != nil {
		t.Fatalf("It shouldn't be an error, cause node has data, but instead we got %v", err)
	}
	if ok {
		t.Fatal("It should be false, cause current max is key `1`")
	}

	ok, err = bn.checkKeyStrictlyLessThanMin(0)
	if err != nil {
		t.Fatalf("It shouldn't be an error, cause node has data, but instead we got %v", err)
	}
	if !ok {
		t.Fatal("It should be true, cause current max is key `1`")
	}

	ok, err = bn.checkKeyStrictlyLessThanMax(35)
	if err != nil {
		t.Fatalf("It shouldn't be an error, cause node has data, but instead we got %v", err)
	}
	if ok {
		t.Fatal("It should be false, cause current max is key `32`")
	}

	err = bn.update(ItemHandle{key: 16, value: 50})
	if err != nil {
		t.Fatalf("It should be okay to update value of key 16, cause it exists, but instead we got %v", err)
	}
	val, _ = bn.get(16)
	if val.(int) != 50 {
		t.Fatalf("It should be 50 after we updated it, but instead we got %d", val.(int))
	}

	err = bn.update(ItemHandle{key: 34, value: 34})
	if err == nil || err != errDataNotFound {
		t.Fatalf("updating 34 should fail with errDataNotFound, but instead we got %v", err)
	}

	err = bn.delete(41)
	if err == nil || err != errDataNotFound {
		t.Fatalf("deleting 41 should fail with errDataNotFound, but instead we got %v", err)
	}

	err = bn.delete(17)
	if err != nil {
		t.Fatalf("deleting 17 should be ok cause it exists, but instead we got %v", err)
	}
	ok = bn.exist(17)
	if ok {
		t.Fatal("It should be false, cause 17 already got deleted, but it is not")
	}

	err = bn.insert(ItemHandle{key: 38, value: 38})
	if err != nil {
		t.Fatalf("It should be inserted cause we have slot, but instead we got %v", err)
	}

	ok, err = bn.checkKeyStrictlyLessThanMax(35)
	if err != nil {
		t.Fatalf("It shouldn't be an error, cause node has data, but instead we got %v", err)
	}
	if !ok {
		t.Fatal("It should be true, cause current max is key `38`")
	}
}

func TestBOWLNewOrderedNode(t *testing.T) {
	ihs := make([]ItemHandle, 2)
	ihs[0] = ItemHandle{key: 1, value: 1}
	ihs[1] = ItemHandle{key: 2, value: 2}
	bn := NewNodeWithOrderedSlice(5, ihs, cmpTest)

	if len(bn.data) != 32 {
		t.Fatalf("It should still have len 32, but instead we got %d", len(bn.data))
	}

	bn.lock()
	defer bn.unlock()

	if bn.dataCount != 2 {
		t.Fatalf("It should only have 2 data, but instead we got %d", bn.dataCount)
	}

	if !bn.exist(1) || !bn.exist(2) {
		t.Fatal("Both should be exist but instead it is not")
	}
}

func TestBOWLNodeMarkRemoval(t *testing.T) {
	bn := NewEmptyNode(5, cmpTest)

	ok := bn.lock()
	if !ok {
		t.Fatal("It should be `true` but it is not")
	}
	bn.unlock()

	bn.markRemoval()
	ok = bn.lock()
	if ok {
		t.Fatal("It should be `false` but it is not")
	}
}

func TestBOWLNodeConnect(t *testing.T) {
	bn := NewEmptyNode(5, cmpTest)
	bn2 := NewEmptyNode(4, cmpTest)
	bn.lock()
	defer bn.unlock()

	err := bn.connectNode(3, bn2)
	if err != nil {
		t.Fatalf("err should be nil, but instead we got %v", err)
	}
	for i := 0; i < 5; i++ {
		if i == 2 {
			if bn.nextNodes[i] != bn2 {
				t.Fatalf("It should be bn2, but instead we got %v", bn.nextNodes[2])
			}
		} else if bn.nextNodes[i] != nil {
			t.Fatalf("It should be nil, cause none connected, but instead we got %v", bn.nextNodes[i])
		}
	}

	err = bn.connectNode(6, bn2)
	if err == nil || err != errHeightOutsideRange {
		t.Fatalf("err should be errHeightOutsideRange, but instead we got %v", err)
	}

	err = bn.disconnectNode(3)
	if err != nil {
		t.Fatalf("err should be nil, but instead we got %v", err)
	}
	for i := 0; i < 5; i++ {
		if bn.nextNodes[i] != nil {
			t.Fatalf("It should be nil, cause none connected, but instead we got %v", bn.nextNodes[i])
		}
	}
}
