# Market Data Aggregation Service

> C++17 · Multithreading · Docker · Google Test · CI/CD

High-throughput market data ingestion pipeline processing **1M+ ticks/sec** at **sub-10ms p99 latency** using lock-free queues, CRC32 validation, and a multi-threaded producer/consumer architecture. 100% C++17.

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
           ├── Latency Histogram (p50/p95/p99/max)
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
| **Correctness** | CRC32 (IEEE polynomial, compile-time LUT) over every tick; sequence-number gap detection |
| **Resilience** | Feed state machine: `CONNECTING → ACTIVE → STALE → DROPPED → RECOVERING` |
| **Observability** | Per-feed latency histograms; p50/p95/p99/max snapshots; aggregate TPS |

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

Produces:
- `build/market_data_aggregator` — main aggregator service
- `build/feed_simulator`         — load-test feed generator
- `build/market_monitor`         — live terminal dashboard
- `build/mda_tests`              — Google Test suite

---

## Tests (Google Test)

```bash
cd build && ctest --output-on-failure
```

Test coverage:

- **Lock-free queue** — FIFO ordering, capacity limits, wrap-around, SPSC concurrent 1M-item throughput, move semantics
- **CRC32** — known vector (`0xCBF43926`), single-bit sensitivity, incremental update
- **Tick validation** — valid accept, CRC corruption, negative/zero prices, bid > ask, zero timestamp, post-mutation invalidation
- **Feed pipeline** — start/stop lifecycle, metric accumulation, latency sub-10ms, idempotent stop, state transitions, sequence gap tracking

---

## Tools

### Feed Simulator
Generates binary tick traffic over TCP for load testing the ingestion pipeline.

```bash
./build/feed_simulator \
    --host 127.0.0.1 --port 9001 \
    --rate 500000 --feed-id FA \
    --duration 30 --error-rate 0.001
```

### Market Monitor
Live terminal dashboard showing per-feed throughput, latency percentiles and health state.

```bash
./build/market_monitor --demo        # simulated data, no aggregator needed
./build/market_monitor --interval 1  # live mode, connects to in-process aggregator
```

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

## CI/CD

### GitHub Actions
Runs on every push/PR to `main`:
1. **Build** — CMake Release, Ninja, all targets
2. **Google Test** — full suite with artifact upload
3. **cppcheck** — static analysis across `src/`, `include/`, `tools/`
4. **Docker** — build + push to GHCR on `main`

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
├── tools/
│   ├── feed_simulator.cpp          # TCP load-test tick generator
│   └── market_monitor.cpp          # Live terminal dashboard
├── tests/
│   ├── test_lock_free_queue.cpp
│   ├── test_tick.cpp
│   └── test_feed_ingestion_pipeline.cpp
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
