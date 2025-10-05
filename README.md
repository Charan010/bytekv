## ByteKV 🤠:

#### Built in Java with multithreading at its core, reliability. It is kind of borrowed from facebook's RocksDB. I wanted to understand how to write stuff at scale and design.

-----------------------------------

###  What Makes It Not Suck🔥 ?

#### **LSM Trees** : Write-optimized implementation compared to O(log n) B-trees. Sorting and storage happen in the background while you can serve other requests.

#### **Write-Ahead Logging** : Data can be replayed from WAL file if there is any crash.

#### **Protobuf Serialization** : Compact binary format that's smaller and faster than stupid JSON data. Less disk thrashing, more throughput.

#### **Async Everything** : Writes are handled by asynchronous writer thread where writes are done on background to make it non blocking and increase throughput.

#### **Smart TTL Expiration** :  Lazy eviction that only kicks in when ~25% of sampled keys are expired. No pointless scans burning CPU for nothing.

#### **LRU Caching** : Hot or recent data is cached to reduce read latency.

#### **Leveled Compaction** :  Merging of SST to reduce storage required . Stale data gets yeeted automatically.

#### **Bloom Filters** :  Probabilistic magic that tells you when a key *definitely* doesn't exist. False positives? Maybe. False negatives? Never. Shines the best when we need to iterate through a lot of SST.


#### 🛠️ Build and Run:

``` bash
git clone github.com/Charan010/bytekv.git
cd bytekv/

# build or compile files
./gradlew clean build

# run (currently its a benchmark test)
./gradlew run

```

### BenchMarks:

```markdown
Mixed workload Benchmark (70% read / 30% write):
    Total ops: 200000
    Duration: 0.92 s
    Throughput: 217589.83 ops/sec 
    Avg latency: 30.28 µs | Median: 19.26 µs | 95th: 52.74 µs | 99th: 371.97 µs | Max: 29.72 ms
```





