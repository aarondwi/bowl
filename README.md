# bowl

An experimental skip list prototype, which is a unrolled-skip-list, optimized for batched workloads (affect how we search the next item). 

Won't do any other low level optimizations. Just to see how good this optimization is

# benchmark
With interface (not moved to generics yet), ordered writes reaching 600K/s.

## todo

1. move to generics
2. Add fuzzy test
