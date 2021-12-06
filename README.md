# bowl

An experimental skip list variant, which is a concurrent-skip-zip-list, optimized for batched workloads (affect how we search the next item)
Downsides are no backward check/scan, only one way.

## todo

1. reduce number of pointers -> reduce memory usage
2. add `hint` API, so can skip head
