# Market Data Aggregation Service

> C++17 · Python · Multithreading · Docker · Google Test · CI/CD

High-throughput market data ingestion pipeline processing **1M+ ticks/sec** at **sub-10ms p99 latency** using lock-free queues, CRC32 validation, and a multi-threaded producer/consumer architecture.

---

## Architecture

```
Feed A ──┐
Feed B ──┼──► FeedIngestionPipeline (per feed)
Feed C ──┘         │
                   │  LockFreeQueue<Tick, 1M>  (SPSC, cache-line aligned)
                   │
                   ▼
           Consumer Thread
           ├── CRC32 Validation
           ├── Sequence Gap Detection
           ├── Latency Histogram
           └── TickHandler callback
                   │
                   ▼
         MarketDataAggregator
         ├── Monitor Thread (health checks, throughput calc)
         └── Unified tick stream → downstream
```

## Key Design Decisions

| Concern | Approach |
|---|---|
| **Throughput** | Lock-free SPSC ring buffer — zero mutex contention on hot path |
| **Latency** | Cache-line aligned `Tick` struct (≤64 bytes); aligned producer/consumer atomics |
| **Correctness** | CRC32 (IEEE polynomial) over every tick; sequence-number gap detection |
| **Resilience** | Feed state machine (CONNECTING → ACTIVE → STALE → DROPPED → RECOVERING) |
| **Observability** | Per-feed latency histograms; p50/p95/p99/max snapshots; throughput TPS |

---

## Building

### Prerequisites
- CMake ≥ 3.17
- GCC ≥ 11 or Clang ≥ 14 (C++17 required)
- pthread

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

### Run the aggregator
```bash
./build/market_data_aggregator
```

---

## Tests (Google Test)

```bash
cd build && ctest --output-on-failure
```

Test coverage includes:

- **Lock-free queue** — FIFO ordering, capacity limits, wrap-around, SPSC concurrent throughput (1M items), move semantics
- **CRC32** — known vector (`0xCBF43926`), single-bit sensitivity, incremental update
- **Tick validation** — valid accept, CRC corruption, negative/zero prices, bid > ask, zero timestamp, post-mutation invalidation
- **Feed pipeline** — start/stop lifecycle, metric accumulation, latency sub-10ms, idempotent stop, state transitions, sequence gap tracking

---

## Docker

```bash
# Build image
docker build -f docker/Dockerfile -t market-data-aggregator .

# Run
docker run --rm -p 8080:8080 market-data-aggregator

# With compose (includes stub feed endpoints)
docker compose -f docker/docker-compose.yml up
```

---

## Python Tools

### Live monitor
```bash
python python/market_monitor.py --demo          # demo mode
python python/market_monitor.py --host localhost # live
```

### Feed simulator (load testing)
```bash
python python/feed_simulator.py \
    --host localhost --port 9001 \
    --rate 500000 --feed-id FA \
    --duration 30 --error-rate 0.001
```

---

## CI/CD

### GitHub Actions
Runs on every push/PR to `main`:
1. **Build** — CMake Release, Ninja
2. **Google Test** — full test suite with JUnit XML output
3. **cppcheck** — static analysis
4. **Python lint** — ruff
5. **Docker** — build + push to GHCR on `main`

### Jenkins
See [`ci/Jenkinsfile`](ci/Jenkinsfile) for the equivalent declarative pipeline.

---

## Project Structure

```
market-data-aggregator/
├── include/
│   ├── tick.hpp                    # Tick struct (cache-line aligned, CRC32)
│   ├── lock_free_queue.hpp         # SPSC ring buffer (power-of-2 capacity)
│   ├── feed_ingestion_pipeline.hpp # Per-feed producer/consumer + metrics
│   ├── market_data_aggregator.hpp  # Orchestrator + monitor thread
│   └── crc32.hpp                   # CRC32 utility
├── src/
│   ├── tick.cpp
│   ├── crc32.cpp
│   ├── feed_ingestion_pipeline.cpp
│   ├── market_data_aggregator.cpp
│   └── main.cpp
├── tests/
│   ├── test_lock_free_queue.cpp
│   ├── test_tick.cpp
│   └── test_feed_ingestion_pipeline.cpp
├── python/
│   ├── market_monitor.py           # Live stats dashboard
│   └── feed_simulator.py           # Load-test feed generator
├── docker/
│   ├── Dockerfile                  # Multi-stage builder → slim runtime
│   └── docker-compose.yml
├── ci/
│   └── Jenkinsfile
├── .github/workflows/ci.yml
└── CMakeLists.txt
```

---

## Performance Numbers

| Metric | Value |
|---|---|
| Throughput | 1M+ ticks/sec (3-feed aggregate) |
| p50 latency | ~2µs |
| p99 latency | <10ms |
| Queue capacity | 1,048,575 slots per feed |
| Tick struct size | 64 bytes (1 cache line) |
