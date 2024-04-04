# bowl

An experimental single-threaded skip list prototype, which is a unrolled-skip-list, optimized for ordered batched operations (affect how we search the next item). 

Won't do any other low level optimizations, will just see how good this optimization is

# benchmark

Given
1. MAX_HEIGHT = 64
2. NODE_SIZE = 256
3. 1024 key/value per inserts, ordered per batch

It is reaching a rate of 1.6-1.8M/s
