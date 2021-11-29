# bowl

An experimental skip list variant, which is a concurrent-skip-zip-list, optimized for batched workloads (affect how we search the next item)
Downsides are no backward check/scan, only one way.

## todo

1. how to nicely delete -> `Delete by mark removal and have concurrent search helps` works, but alreayd heavy on connecting
2. how to connect on insert -> can cause a deadlock. But rare enough
3. reduce number of pointers -> reduce memory usage
4. Decide how to unlock or not. Currently, not assuming taking locks can fail (affect delete semantic)
5. add `hint` API, so can skip head
