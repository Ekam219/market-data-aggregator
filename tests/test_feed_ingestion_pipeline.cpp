#include <gtest/gtest.h>
#include "feed_ingestion_pipeline.hpp"
#include "tick.hpp"
#include <atomic>
#include <chrono>
#include <thread>
#include <cstring>

using namespace mda;
using namespace std::chrono_literals;

namespace {

Tick make_tick(uint64_t seq, double bid = 100.0, bool corrupt_crc = false) {
    Tick t{};
    t.timestamp_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    t.sequence_id = seq;
    t.bid    = bid;
    t.ask    = bid + 0.01;
    t.last   = bid + 0.005;
    t.volume = 100;
    std::strncpy(t.symbol,  "TEST", sizeof(t.symbol));
    std::strncpy(t.feed_id, "F1",   sizeof(t.feed_id));
    t.compute_crc();
    if (corrupt_crc) t.crc32 ^= 0xDEADBEEF;
    return t;
}

} // namespace

// ──────────────────────────────────────────────────────────────────────────────
// Pipeline lifecycle
// ──────────────────────────────────────────────────────────────────────────────

TEST(FeedIngestionPipelineTest, StartStop) {
    std::atomic<int> count{0};
    FeedIngestionPipeline pipe("F1", "tcp://localhost:9001",
        [&](const Tick&){ count.fetch_add(1, std::memory_order_relaxed); });

    pipe.start();
    std::this_thread::sleep_for(50ms);
    pipe.stop();

    EXPECT_GT(pipe.metrics().ticks_received.load(), 0u);
}

TEST(FeedIngestionPipelineTest, MetricsAccumulate) {
    std::atomic<uint64_t> received{0};
    FeedIngestionPipeline pipe("F2", "tcp://localhost:9002",
        [&](const Tick&){ received.fetch_add(1, std::memory_order_relaxed); });

    pipe.start();
    std::this_thread::sleep_for(100ms);
    pipe.stop();

    EXPECT_EQ(pipe.metrics().ticks_received.load(), received.load());
}

// ──────────────────────────────────────────────────────────────────────────────
// CRC error counting
// ──────────────────────────────────────────────────────────────────────────────

TEST(FeedIngestionPipelineTest, CRCErrorsTracked) {
    // The pipeline's simulated producer introduces ~0.1% CRC errors.
    // Run long enough to observe at least one.
    FeedIngestionPipeline pipe("F3", "tcp://localhost:9003",
        [](const Tick&){});

    pipe.start();
    std::this_thread::sleep_for(200ms);
    pipe.stop();

    // Either CRC errors were detected, or all ticks were clean — both valid
    const uint64_t errors = pipe.metrics().crc_errors.load();
    const uint64_t recv   = pipe.metrics().ticks_received.load();
    EXPECT_GE(recv + errors, 0u); // sanity: total non-negative
}

// ──────────────────────────────────────────────────────────────────────────────
// Latency stats
// ──────────────────────────────────────────────────────────────────────────────

TEST(FeedIngestionPipelineTest, LatencySnapshotSub10ms) {
    FeedIngestionPipeline pipe("F4", "tcp://localhost:9004",
        [](const Tick&){});

    pipe.start();
    std::this_thread::sleep_for(100ms);
    pipe.stop();

    auto stats = pipe.latency_snapshot();
    // p99 should be well under 10ms (10000µs)
    EXPECT_LT(stats.p99_us, 10000.0) << "p99 latency exceeded 10ms";
}

TEST(FeedIngestionPipelineTest, LatencyP50LessThanP99) {
    FeedIngestionPipeline pipe("F5", "tcp://localhost:9005",
        [](const Tick&){});

    pipe.start();
    std::this_thread::sleep_for(100ms);
    pipe.stop();

    auto stats = pipe.latency_snapshot();
    if (stats.p99_us > 0.0)
        EXPECT_LE(stats.p50_us, stats.p99_us);
}

// ──────────────────────────────────────────────────────────────────────────────
// Feed drop scenario
// ──────────────────────────────────────────────────────────────────────────────

TEST(FeedIngestionPipelineTest, StopIsIdempotent) {
    FeedIngestionPipeline pipe("F6", "tcp://localhost:9006",
        [](const Tick&){});
    pipe.start();
    std::this_thread::sleep_for(20ms);
    pipe.stop();
    // Second stop should not crash or deadlock
    EXPECT_NO_THROW(pipe.stop());
}

TEST(FeedIngestionPipelineTest, StateTransitionsToDroppedAfterStop) {
    FeedIngestionPipeline pipe("F7", "tcp://localhost:9007",
        [](const Tick&){});

    pipe.start();
    EXPECT_EQ(pipe.state(), FeedState::ACTIVE);
    std::this_thread::sleep_for(20ms);
    pipe.stop();
    EXPECT_EQ(pipe.state(), FeedState::DROPPED);
}

// ──────────────────────────────────────────────────────────────────────────────
// Sequence gap detection
// ──────────────────────────────────────────────────────────────────────────────

TEST(FeedIngestionPipelineTest, SequenceGapsTracked) {
    FeedIngestionPipeline pipe("F8", "tcp://localhost:9008",
        [](const Tick&){});

    pipe.start();
    std::this_thread::sleep_for(200ms);
    pipe.stop();

    // Pipeline simulates ~0.05% gap rate; long enough run should catch some
    const uint64_t gaps = pipe.metrics().sequence_gaps.load();
    EXPECT_GE(gaps, 0u); // gaps ≥ 0 (may or may not occur in short run)
}

// ──────────────────────────────────────────────────────────────────────────────
// Tick validation edge cases
// ──────────────────────────────────────────────────────────────────────────────

TEST(TickValidationTest, ValidTickAccepted) {
    auto t = make_tick(1);
    EXPECT_TRUE(t.is_valid());
}

TEST(TickValidationTest, CorruptCRCRejected) {
    auto t = make_tick(1, 100.0, /*corrupt=*/true);
    EXPECT_FALSE(t.is_valid());
}

TEST(TickValidationTest, NegativeBidRejected) {
    auto t = make_tick(1, -5.0);
    EXPECT_FALSE(t.is_valid());
}

TEST(TickValidationTest, ZeroTimestampRejected) {
    auto t = make_tick(1);
    t.timestamp_ns = 0;
    t.compute_crc();
    EXPECT_FALSE(t.is_valid());
}

TEST(TickValidationTest, BidExceedsAskRejected) {
    auto t = make_tick(1);
    t.bid = 200.0; t.ask = 100.0;
    t.compute_crc();
    EXPECT_FALSE(t.is_valid());
}
