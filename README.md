# bowl

An experimental skip list variant, which is a skip-zip-list, optimized for batched workloads (affect how we search the next item)
There are 2 versions `exclusive` and `concurrent`.
Downsides are no backward check/scan, only one way (like most skiplist implementations).

## todo

0. Actually finish concurrent version
1. reduce number of pointers -> reduce memory usage
2. add `hint` API, so can skip head
3. Add fuzzy test
