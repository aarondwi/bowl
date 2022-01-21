# bowl

An experimental skip list prototype, which is a skip-zip-list, optimized for batched workloads (affect how we search the next item)
There are 2 versions `exclusive` and `concurrent`.
Downsides are :

* no backward check/scan, only one way (like most skiplist implementations)
* kinda slow compared to concurrent b-tree, and much slower than ART or Masstree, also like typical skiplist

With simple bench tests, current implementations is super slow (batch size ~1000 only get at most 200K write/s, most time spent on `connectUntil`, via pprof). And this is still with exclusive version. Unless I find good way to optimize, the concurrent ver prototype won't get tried

## todo

0. Actually finish concurrent version
1. reduce number of pointers -> reduce memory usage
2. add `hint` API, so can skip head
3. Add fuzzy test
4. Adaptive max NODE_SIZE
