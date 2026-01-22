# ByteKV

A write-optimized key-value store inspired by RocksDB.
Built to learn storage engines, LSM trees, and high-throughput systems.

## Features

- LSM-tree storage engine
- Write-Ahead Log (crash recovery)
- Asynchronous writes
- Leveled compaction
- Bloom filters per SSTable
- TTL support (lazy expiration)
- LRU read cache
- Protobuf-based serialization

## Benchmarks

**Mixed workload (70% read / 30% write)**  
- Throughput: ~217K ops/sec  
- Avg latency: ~30 µs  
- P99 latency: ~372 µs  

(See benchmark code for details.)

## Build & Run

```bash
git clone https://github.com/Charan010/bytekv.git
cd bytekv
./gradlew clean build
./gradlew run
