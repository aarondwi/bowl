package bowl

// Should return:
//
// 1. -1 if a < b
//
// 2. 0 if a == b
//
// 3. 1 if a > b
//
// and NOT anything else
type Comparator[c comparable] func(a, b c) int
