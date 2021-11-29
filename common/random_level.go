package common

import (
	"math/rand"
	"sync"
)

// taken from https://github.com/AceDarkknight/ConcurrentSkipList/blob/master/skipList.go,
// which in turn use redis' implementations

var randLevelMutex sync.Mutex

func RandomLevelGenerator(maxHeight int) <-chan int {
	randLevelMutex.Lock()
	defer randLevelMutex.Unlock()

	// put a buffer, so sudden rise on throughput does not bottleneck here
	ch := make(chan int, 4)
	rnd := rand.New(rand.NewSource(rand.Int63()))
	go randomLevelGenerate(rnd, maxHeight, ch)
	return ch
}

func randomLevelGenerate(rnd *rand.Rand, maxHeight int, ch chan int) {
	for {
		level := 1
		for rnd.Float64() < 0.5 && level < maxHeight {
			level++
		}
		ch <- level
	}
}
