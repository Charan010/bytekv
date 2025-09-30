# ByteKV😛:

#### A Java-Based Multithreaded Key-Value store focused on speed, reliability and persistence of data,It has LSM trees for storage, write ahead logging for replayability when instance crashes, TTL  support for automatic data eviction and LRU caching for fetching faster.

## Key Features:

#### *LSM-Trees* - most useful for writes where sorting and storing is done in background    when compared to on average O(logn) time for B or B+ trees.

#### *Write ahead logging* - to guarantee data persistence even if instance crashes.

#### *Protobuf logging* - to reduce file size, using binary protobuf which could be faster to write and parse.

#### *Asynchronous writes* - All writes are handled by a asynchronous thread which can handle flush overflow when log file compaction is going on.

#### *TTL Expiration* - Keys that can become stale are evicted lazily. ByteKV only removes expired keys once about 25% of a random sample of keys have expired, reducing unnecessary scans and keeping performance smooth.

#### *LRU caching* - To reduce reads latency which is the major bottleneck in LSM-Based databases, LRU stores hot data which can be fetched faster.

#### *SST merging* - Using levelled compaction to decrease the number of SST and overwrite stale data.

#### *Bloom filters* - To overcome read latency, using bloom filters which can give false positives but never false negatives. Instead of Storing all indexes to each SST , we can sparsely store and use bloom filters when SST size is too large and memory heavy.

```markdown
### Benchmarks

- PUTs: 100,000 ops — avg latency 227µs, max 738ms
- GETs: 50,000 ops — avg latency 11µs, max 1.18ms
- TTL expired keys seen: 30,330
```





