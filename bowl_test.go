package bowl

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func cmpTest(a int, b int) int {
	if a < b {
		return -1
	}
	if a == b {
		return 0
	}
	return 1
}

func TestBowlExclusive(t *testing.T) {
	b := NewBOWL[int, int](cmpTest)

	// test insert overlapping
	insert1 := make([]Item[int, int], 0, 100)
	for i := 0; i < 100; i++ {
		j := 1 + (i * 5)
		insert1 = append(insert1, Item[int, int]{Key: j, Value: j})
	}
	insert2 := make([]Item[int, int], 0, 81)
	insert2 = append(insert2, Item[int, int]{Key: 11, Value: 11})
	for i := 0; i < 80; i++ {
		j := 2 + (i * 5)
		insert2 = append(insert2, Item[int, int]{Key: j, Value: j})
	}
	errs := b.Insert(insert1)
	for _, err := range errs {
		if err != nil {
			t.Fatalf("Shouldn't return error when inserting `from1`, but instead we got %v", err)
		}
	}
	errs = b.Insert(insert2)
	if errs[0] == nil || errs[0] != ErrKeyAlreadyExist {
		t.Fatalf("errs[0] should be ErrKeyAlreadyExist, cause it is, but instead we got %v", errs[0])
	}
	for _, err := range errs[1:] {
		if err != nil {
			t.Fatalf("Shouldn't return error when inserting `from2`, but instead we got %v", err)
		}
	}

	// ensure all got properly ordered
	prevVal := 0
	b.ScanAll(func(ih Item[int, int]) {
		if ih.Key <= prevVal {
			t.Fatalf("Should be bigger, but instead we got prevVal: %d and ih.Key: %d", prevVal, ih.Key)
		}
		prevVal = ih.Key
	})

	// test update/delete + recheck existence
	update1 := make([]Item[int, int], 0, 4)
	update1 = append(update1, Item[int, int]{Key: 6, Value: 1000})
	update1 = append(update1, Item[int, int]{Key: 14, Value: 1000})
	update1 = append(update1, Item[int, int]{Key: 17, Value: 1000})
	update1 = append(update1, Item[int, int]{Key: 98, Value: 2000})

	errs = b.Update(update1)
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("Pos 0 and 2 shouldn't be non-nil, cause both exist, but instead we got `%v` and `%v`",
			errs[0], errs[2])
	}
	if errs[1] == nil || errs[3] == nil ||
		errs[1] != ErrDataNotFound || errs[3] != ErrDataNotFound {
		t.Fatalf("Pos 1 and 3 shouldn't be nil, and be ErrDataNotFound, but instead we got `%v` and `%v`",
			errs[1], errs[3])
	}

	delete1 := make([]int, 0, 6)
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
		errs[0] != ErrDataNotFound || errs[2] != ErrDataNotFound ||
		errs[4] != ErrDataNotFound || errs[5] != ErrDataNotFound {
		t.Fatalf("Pos 0, 2, 4, and 5 shouldn't be nil and be ErrDataNotFound, but instead we got %v, %v, and %v",
			errs[0], errs[2], errs[4])
	}

	keysToGet := make([]int, 0, 10)
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

	valuesGot := make([]int, 0, 10)
	valuesGot = append(valuesGot, math.MinInt)
	valuesGot = append(valuesGot, math.MinInt)
	valuesGot = append(valuesGot, 1000)
	valuesGot = append(valuesGot, 21)
	valuesGot = append(valuesGot, 31)
	valuesGot = append(valuesGot, math.MinInt)
	valuesGot = append(valuesGot, 47)
	valuesGot = append(valuesGot, math.MinInt)
	valuesGot = append(valuesGot, 71)
	valuesGot = append(valuesGot, math.MinInt)

	res := b.Get(keysToGet, math.MinInt)
	for i, r := range res {
		if r != valuesGot[i] {
			t.Fatalf("It should be the same, but instead at iter %d we got %d when it should be %d", i, r, valuesGot[i])
		}
	}

	// test range
	rangeSum := 0
	toDeleteLater := make([]int, 0, 40)
	b.ScanRange(301, 400, func(ih Item[int, int]) {
		rangeSum += ih.Key
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
	b.ScanRange(301, 400, func(ih Item[int, int]) {
		rangeSum += ih.Key
	})
	if rangeSum != 0 {
		t.Fatalf("All values in range 301-400 should total 0, cause all already got deleted, but instead we got %d", rangeSum)
	}

	keysToGet = make([]int, 0, 5)
	keysToGet = append(keysToGet, 157)
	keysToGet = append(keysToGet, 211)
	keysToGet = append(keysToGet, 282)
	keysToGet = append(keysToGet, 309)
	keysToGet = append(keysToGet, 417)

	valuesGot = make([]int, 0, 5)
	valuesGot = append(valuesGot, 157)
	valuesGot = append(valuesGot, 211)
	valuesGot = append(valuesGot, 282)
	valuesGot = append(valuesGot, math.MinInt)
	valuesGot = append(valuesGot, math.MinInt)

	res = b.Get(keysToGet, math.MinInt)
	for i, r := range res {
		if r != valuesGot[i] {
			t.Fatalf("It should be the same, but instead at iter %d we got %d when it should be %d", i, r, valuesGot[i])
		}
	}

	gteSum1 := 0
	b.ScanGreaterThanEqual(401, func(ih Item[int, int]) {
		gteSum1 += ih.Value
	})
	gteSum2 := 0
	b.ScanGreaterThanEqual(400, func(ih Item[int, int]) {
		gteSum2 += ih.Value
	})
	if (gteSum1 != gteSum2) || gteSum1 != 8970 {
		t.Fatalf("Both should be the same, and is 8970, but instead we got %d and %d", gteSum1, gteSum2)
	}
	ltSum1 := 0
	b.ScanStrictlyLessThan(100, func(ih Item[int, int]) {
		ltSum1 += ih.Value
	})
	ltSum2 := 0
	b.ScanStrictlyLessThan(99, func(ih Item[int, int]) {
		ltSum2 += ih.Value
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
	bowl := NewBOWL[int, int](cmpTest)

	rnd := rand.New(rand.NewSource(rand.Int63()))
	data := make([]Item[int, int], 0, 1024)

	b.Log("set counter to 0")
	counter := 0
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		prev := 0
		for j := 0; j < 1024; j++ {
			val := prev + rnd.Intn(65536) + 1
			data = append(data, Item[int, int]{
				Key: val, Value: val})
			prev = val
		}
		b.StartTimer()
		errs := bowl.Insert(data)
		for _, err := range errs {
			if err != nil && err != ErrKeyAlreadyExist {
				b.Fatal(err)
			}
		}
		data = data[:0]
		counter += 1
	}

	b.StopTimer()

	// validate all ordered just fine
	b.Logf("Start validating %d runs......", counter)
	prevVal := 0
	bowl.ScanAll(func(ih Item[int, int]) {
		if ih.Key <= prevVal {
			b.Fatalf("Should be bigger, but instead we got prevVal: %d and ih.Key: %d", prevVal, ih.Key)
		}
		prevVal = ih.Key
	})
	b.Logf("Finished")
}

func BenchmarkBowlExclusiveRead(b *testing.B) {
	b.StopTimer()
	bowl := NewBOWL[int, int](cmpTest)
	chInsert := make(chan []Item[int, int], 4096)
	chRead := make(chan []int, 4096)

	go func() { // for insertion
		rnd := rand.New(rand.NewSource(rand.Int63()))
		for i := 0; i < 2048; i++ {
			data := make([]Item[int, int], 0, 1024)
			prev := 0
			for j := 0; j < 1024; j++ {
				val := prev + rnd.Intn(65536) + 1
				data = append(data, Item[int, int]{Key: val, Value: val})
				prev = val
			}
			chInsert <- data
		}
	}()
	go func() { // for read benchmark
		rnd := rand.New(rand.NewSource(rand.Int63()))
		for {
			data := make([]int, 0, 1024)
			prev := 0
			for j := 0; j < 1024; j++ {
				val := prev + rnd.Intn(65536) + 1
				data = append(data, val)
				prev = val
			}
			chRead <- data
		}
	}()
	go func() {
		for i := 0; i < 2048; i++ {
			data := <-chInsert
			bowl.Insert(data)
		}
	}()
	time.Sleep(2 * time.Second)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		data := <-chRead
		bowl.Get(data, math.MinInt)
	}
}
