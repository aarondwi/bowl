package common

import "testing"

func TestRandomLevelGenerator(t *testing.T) {
	ch := RandomLevelGenerator(32)
	for i := 0; i < 64; i++ {
		val := <-ch
		if val < 0 || val >= 32 {
			t.Fatalf("It should only return [0, 32), but instead we got %d", val)
		}
	}
}
