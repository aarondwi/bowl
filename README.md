# bowl

An experimental skip list variant, which is a skip-zip-list, optimized for batched workloads (affect how we search the next item)
There are 3 versions (`exclusive`, `singlewriter`, `concurrent`).
Downsides are no backward check/scan, only one way (like any other skiplists).

## todo

1. reduce number of pointers -> reduce memory usage
2. add `hint` API, so can skip head
