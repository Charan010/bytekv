# ByteKV

ByteKV is a light-weight, multithreaded key-value store built in Java, inspired from Redis. I implemented this to understand nosql databases better and their internal working.

---

# Features✨

###  Log-Structured Merge (LSM) Trees:
Sure, you can just store in a HashMap but, for storing millions of keys and values, it's pretty much impossible. So, that's why i went with LSM trees where we flush data to disk in sorted order and i'm using .index files to jump to specific keys.

###  Write-Ahead Logging (WAL):
Every single operation is logged into a .log file using protobuf binary so that storing and parsing is much faster. This log file could be used to replay all values when database instance crashes.

### Log Compaction:
A background thread which would start compressing by removing overwritten key and values when number of entries exceed 1000.

### Multi-Threaded access:
Instead of spinning up new thread for each request which would lead to context overhead or even worse using only Main thread for serving requests. I'm using threadpool for efficient serving of requests without creating and deleting new threads each time.

when threadpool is exhausted and blocking queue is also full. it sends a RejectedExecutionException.

### Background tasks:
TTL-Expiry - Instead of searching through whether pair TTL is expired or not which would waste resources.Instead i have implemented lazy cleaning inspired by Redis :P .
full TTL scan would only trigger when atleast 25% of TTL pairs are expired.So TTl deletes are not immediate.

###  Bloom Filters:
Bloom filter is a probabilistic data structure which can help avoid unnecessary disk reads by using hashing,It can give false positives but never false negatives.

###  SSTable Compaction Strategy:
  To maintain optimized disk usage,when number of SST tables reach more than 5,merging would be triggered and 2 oldest SST tables would be merged into one.

### CLI:
  added CLI to use keyvalue store from terminal like redis.

### Future implementations:
    disk operations could be written in c++ for better performance.
    snapshotting to save log space by deleting old log shit.
