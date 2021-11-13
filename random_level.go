package bowl

import "sync/atomic"

// taken from https://github.com/xphoenix/skiplist/blob/master/skiplist.go
func randomLevel(seed int32, height int) int {
	mask := 0
	for mask == 0 {
		src := atomic.LoadInt32(&seed)
		x := src
		x ^= x << 13
		x ^= int32(uint32(x) >> 17)
		x ^= x << 5
		if atomic.CompareAndSwapInt32(&seed, src, x) {
			mask = int(x)
		}
	}

	if mask&0x80000001 == 0x80000001 {
		return 0
	}

	level := 1
	for ; mask&1 == 1 && level < height; mask = mask >> 1 {
		level++
	}
	return level
}
