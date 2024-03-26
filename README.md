# bowl

An experimental single-threaded skip list prototype, which is a unrolled-skip-list, optimized for ordered batched operations (affect how we search the next item). 

Won't do any other low level optimizations, will just see how good this optimization is

# benchmark
With MAX_HEIGHT = 64 + NODE_SIZE = 256, reaching a rate of 1.76M/s for ordered inserts

## todo

1. Add fuzzy test
