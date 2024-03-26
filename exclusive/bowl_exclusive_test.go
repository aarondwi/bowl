package exclusive

import (
	"math/rand"
	"testing"
	"time"

	"github.com/aarondwi/bowl/node"
)

func cmpTest(a, b interface{}) int {
	if a.(int) < b.(int) {
		return -1
	}
	if a.(int) == b.(int) {
		return 0
	}
	return 1
}

func TestBowlExclusive(t *testing.T) {
	b := NewBOWL(cmpTest)

	// test insert overlapping
	insert1 := make([]node.ItemHandle, 0, 100)
	for i := 0; i < 100; i++ {
		j := 1 + (i * 5)
		insert1 = append(insert1, node.ItemHandle{Key: j, Value: j})
	}
	insert2 := make([]node.ItemHandle, 0, 81)
	insert2 = append(insert2, node.ItemHandle{Key: 11, Value: 11})
	for i := 0; i < 80; i++ {
		j := 2 + (i * 5)
		insert2 = append(insert2, node.ItemHandle{Key: j, Value: j})
	}
	errs := b.Insert(insert1)
	for _, err := range errs {
		if err != nil {
			t.Fatalf("Shouldn't return error when inserting `from1`, but instead we got %v", err)
		}
	}
	errs = b.Insert(insert2)
	if errs[0] == nil || errs[0] != node.ErrKeyAlreadyExist {
		t.Fatalf("errs[0] should be ErrKeyAlreadyExist, cause it is, but instead we got %v", errs[0])
	}
	for _, err := range errs[1:] {
		if err != nil {
			t.Fatalf("Shouldn't return error when inserting `from2`, but instead we got %v", err)
		}
	}

	// ensure all got properly ordered
	prevVal := 0
	b.ScanAll(func(ih node.ItemHandle) {
		if ih.Key.(int) <= prevVal {
			t.Fatalf("Should be bigger, but instead we got prevVal: %d and ih.Key: %d", prevVal, ih.Key.(int))
		}
		prevVal = ih.Key.(int)
	})

	// test update/delete + recheck existence
	update1 := make([]node.ItemHandle, 0, 4)
	update1 = append(update1, node.ItemHandle{Key: 6, Value: 1000})
	update1 = append(update1, node.ItemHandle{Key: 14, Value: 1000})
	update1 = append(update1, node.ItemHandle{Key: 17, Value: 1000})
	update1 = append(update1, node.ItemHandle{Key: 98, Value: 2000})

	errs = b.Update(update1)
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("Pos 0 and 2 shouldn't be non-nil, cause both exist, but instead we got `%v` and `%v`",
			errs[0], errs[2])
	}
	if errs[1] == nil || errs[3] == nil ||
		errs[1] != node.ErrDataNotFound || errs[3] != node.ErrDataNotFound {
		t.Fatalf("Pos 1 and 3 shouldn't be nil, and be ErrDataNotFound, but instead we got `%v` and `%v`",
			errs[1], errs[3])
	}

	delete1 := make([]interface{}, 0, 6)
	delete1 = append(delete1, 10)
	delete1 = append(delete1, 22)
	delete1 = append(delete1, 73)
	delete1 = append(delete1, 76)
	delete1 = append(delete1, 230)
	delete1 = append(delete1, 1000)

	errs = b.Delete(delete1)
	if errs[1] != nil || errs[3] != nil {
		t.Fatalf("Pos 1 and 3 should be nil, cause both exist and can successfully be deleted, but instead we got %v and %v",
			errs[1], errs[3])
	}
	if errs[0] == nil || errs[2] == nil ||
		errs[4] == nil || errs[5] == nil ||
		errs[0] != node.ErrDataNotFound || errs[2] != node.ErrDataNotFound ||
		errs[4] != node.ErrDataNotFound || errs[5] != node.ErrDataNotFound {
		t.Fatalf("Pos 0, 2, 4, and 5 shouldn't be nil and be ErrDataNotFound, but instead we got %v, %v, and %v",
			errs[0], errs[2], errs[4])
	}

	keysToGet := make([]interface{}, 0, 10)
	keysToGet = append(keysToGet, 3)
	keysToGet = append(keysToGet, 10)
	keysToGet = append(keysToGet, 17)
	keysToGet = append(keysToGet, 21)
	keysToGet = append(keysToGet, 31)
	keysToGet = append(keysToGet, 44)
	keysToGet = append(keysToGet, 47)
	keysToGet = append(keysToGet, 59)
	keysToGet = append(keysToGet, 71)
	keysToGet = append(keysToGet, 76)

	valuesGot := make([]interface{}, 0, 10)
	valuesGot = append(valuesGot, nil)
	valuesGot = append(valuesGot, nil)
	valuesGot = append(valuesGot, 1000)
	valuesGot = append(valuesGot, 21)
	valuesGot = append(valuesGot, 31)
	valuesGot = append(valuesGot, nil)
	valuesGot = append(valuesGot, 47)
	valuesGot = append(valuesGot, nil)
	valuesGot = append(valuesGot, 71)
	valuesGot = append(valuesGot, nil)

	res := b.Get(keysToGet)
	for i, r := range res {
		if ri, ok := r.(int); ok {
			if ri != valuesGot[i] {
				t.Fatalf("It should be the same, but instead at iter %d we got %d when it should be %d", i, ri, valuesGot[i])
			}
		} else {
			if r != nil {
				t.Fatalf("It should be nil at iter %d, but instead we got %v", i, r)
			}
		}
	}

	// test range
	rangeSum := 0
	toDeleteLater := make([]interface{}, 0, 40)
	b.ScanRange(301, 400, func(ih node.ItemHandle) {
		rangeSum += ih.Key.(int)
		toDeleteLater = append(toDeleteLater, ih.Key)
	})
	if rangeSum != 13960 {
		t.Fatalf("All values in range 301-400 should total 13960, but instead we got %d", rangeSum)
	}

	rangeSum = 0
	// testing reconnection
	errs = b.Delete(toDeleteLater)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Should be nil cause all keys to delete exist,, but instead at iter %d we got %v", i, err)
		}
	}
	b.ScanRange(301, 400, func(ih node.ItemHandle) {
		rangeSum += ih.Key.(int)
	})
	if rangeSum != 0 {
		t.Fatalf("All values in range 301-400 should total 0, cause all already got deleted, but instead we got %d", rangeSum)
	}

	keysToGet = make([]interface{}, 0, 5)
	keysToGet = append(keysToGet, 157)
	keysToGet = append(keysToGet, 211)
	keysToGet = append(keysToGet, 282)
	keysToGet = append(keysToGet, 309)
	keysToGet = append(keysToGet, 417)

	valuesGot = make([]interface{}, 0, 5)
	valuesGot = append(valuesGot, 157)
	valuesGot = append(valuesGot, 211)
	valuesGot = append(valuesGot, 282)
	valuesGot = append(valuesGot, nil)
	valuesGot = append(valuesGot, 417)

	res = b.Get(keysToGet)
	for i, r := range res {
		if ri, ok := r.(int); ok {
			if ri != valuesGot[i] {
				t.Fatalf("It should be the same, but instead at iter %d we got %d when it should be %d", i, ri, valuesGot[i])
			}
		} else {
			if r != nil {
				t.Fatalf("It should be nil at iter %d, but instead we got %v", i, r)
			}
		}
	}

	gteSum1 := 0
	b.ScanGreaterThanEqual(401, func(ih node.ItemHandle) {
		gteSum1 += ih.Value.(int)
	})
	gteSum2 := 0
	b.ScanGreaterThanEqual(400, func(ih node.ItemHandle) {
		gteSum2 += ih.Value.(int)
	})
	if (gteSum1 != gteSum2) || gteSum1 != 8970 {
		t.Fatalf("Both should be the same, and is 8970, but instead we got %d and %d", gteSum1, gteSum2)
	}
	ltSum1 := 0
	b.ScanStrictlyLessThan(100, func(ih node.ItemHandle) {
		ltSum1 += ih.Value.(int)
	})
	ltSum2 := 0
	b.ScanStrictlyLessThan(99, func(ih node.ItemHandle) {
		ltSum2 += ih.Value.(int)
	})
	if (ltSum1 != ltSum2) || ltSum1 != 3839 {
		t.Fatalf("Both should be the same, and is 3839, but instead we got %d and %d", ltSum1, ltSum2)
	}
}

func BenchmarkBowlExclusiveWrite(b *testing.B) {
	// we only test insert
	// as it is already representative about update and delete
	//
	// update and delete both also search, but will not result in reconnection scheme
	b.Logf("Starting to prepare the payloads.....")
	b.StopTimer()
	bowl := NewBOWL(cmpTest)
	ch := make(chan []node.ItemHandle, 1024)
	rnd := rand.New(rand.NewSource(rand.Int63()))
	for i := 0; i < 1024; i++ {
		data := make([]node.ItemHandle, 0, 1024)
		prev := 0
		for j := 0; j < 1024; j++ {
			val := prev + rnd.Intn(65536) + 1
			data = append(data, node.ItemHandle{
				Key: val, Value: val})
			prev = val
		}
		ch <- data
	}

	b.Logf("Start benchmarking......")
	b.N = 1024
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		data := <-ch
		errs := bowl.Insert(data)
		for _, err := range errs {
			if err != nil && err != node.ErrKeyAlreadyExist {
				b.Fatal(err)
			}
		}
	}

	b.StopTimer()

	// validate all ordered just fine
	b.Logf("Start validating......")
	prevVal := 0
	bowl.ScanAll(func(ih node.ItemHandle) {
		if ih.Key.(int) <= prevVal {
			b.Fatalf("Should be bigger, but instead we got prevVal: %d and ih.Key: %d", prevVal, ih.Key.(int))
		}
		prevVal = ih.Key.(int)
	})
	b.Logf("Finished")
}

func BenchmarkBowlExclusiveRead(b *testing.B) {
	b.StopTimer()
	bowl := NewBOWL(cmpTest)
	chInsert := make(chan []node.ItemHandle, 4096)
	chRead := make(chan []interface{}, 4096)

	go func() { // for insertion
		rnd := rand.New(rand.NewSource(rand.Int63()))
		for i := 0; i < 1024; i++ {
			data := make([]node.ItemHandle, 0, 1024)
			prev := 0
			for j := 0; j < 1024; j++ {
				val := prev + rnd.Intn(65536) + 1
				data = append(data, node.ItemHandle{Key: val, Value: val})
				prev = val
			}
			chInsert <- data
		}
	}()
	go func() { // for read benchmark
		rnd := rand.New(rand.NewSource(rand.Int63()))
		for {
			data := make([]interface{}, 0, 1024)
			prev := 0
			for j := 0; j < 1024; j++ {
				val := prev + rnd.Intn(65536) + 1
				data = append(data, val)
				prev = val
			}
			chRead <- data
		}
	}()
	time.Sleep(10 * time.Second)
	for i := 0; i < 1024; i++ {
		data := <-chInsert
		bowl.Insert(data)
	}

	b.N = 1024
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		data := <-chRead
		bowl.Get(data)
	}
}
