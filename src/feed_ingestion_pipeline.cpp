#include "feed_ingestion_pipeline.hpp"
#include "crc32.hpp"
#include <chrono>
#include <cstring>
#include <algorithm>
#include <iostream>
#include <random>
#include <thread>

namespace mda {

using namespace std::chrono;

FeedIngestionPipeline::FeedIngestionPipeline(std::string feed_id,
                                              std::string endpoint,
                                              TickHandler handler)
    : feed_id_(std::move(feed_id))
    , endpoint_(std::move(endpoint))
    , handler_(std::move(handler))
{
    for (auto& b : latency_hist_) b.store(0, std::memory_order_relaxed);
}

FeedIngestionPipeline::~FeedIngestionPipeline() {
    stop();
}

void FeedIngestionPipeline::start() {
    if (running_.exchange(true)) return;
    metrics_.state.store(FeedState::CONNECTING);
    producer_thread_ = std::thread([this]{ producer_loop(); });
    consumer_thread_ = std::thread([this]{ consumer_loop(); });
}

void FeedIngestionPipeline::stop() {
    running_.store(false);
    if (producer_thread_.joinable()) producer_thread_.join();
    if (consumer_thread_.joinable()) consumer_thread_.join();
}

// ---------- producer_loop --------------------------------------------------
// In production this would read from a network socket / multicast feed.
// Here we simulate a realistic 1M tick/sec stream with occasional anomalies.
void FeedIngestionPipeline::producer_loop() {
    std::mt19937_64 rng(std::random_device{}());
    std::uniform_real_distribution<double> price_dist(100.0, 200.0);
    std::uniform_int_distribution<int>     err_dist(0, 9999);

    uint64_t seq = 0;
    metrics_.state.store(FeedState::ACTIVE);

    while (running_.load(std::memory_order_relaxed)) {
        Tick tick{};
        tick.timestamp_ns = static_cast<uint64_t>(
            duration_cast<nanoseconds>(
                system_clock::now().time_since_epoch()).count());
        tick.sequence_id = ++seq;
        tick.bid  = price_dist(rng);
        tick.ask  = tick.bid + 0.01;
        tick.last = tick.bid + 0.005;
        tick.volume = rng() % 10000 + 1;
        std::strncpy(tick.symbol, "AAPL", sizeof(tick.symbol));
        std::strncpy(tick.feed_id, feed_id_.c_str(), sizeof(tick.feed_id));
        tick.compute_crc();

        // Simulate ~0.1% CRC corruption
        if (err_dist(rng) == 0) tick.crc32 ^= 0xDEADBEEF;

        // Simulate ~0.05% sequence gap
        if (err_dist(rng) < 5) seq += rng() % 10 + 1;

        if (!queue_.push(tick)) {
            metrics_.ticks_dropped.fetch_add(1, std::memory_order_relaxed);
        }

        // ~1M ticks/sec: sleep 1µs every tick (no-op spin in prod)
        // In real code this would be driven by socket readiness
    }
    metrics_.state.store(FeedState::DROPPED);
}

// ---------- consumer_loop --------------------------------------------------
void FeedIngestionPipeline::consumer_loop() {
    while (running_.load(std::memory_order_relaxed) || !queue_.empty()) {
        auto opt = queue_.pop();
        if (!opt) {
            std::this_thread::yield();
            continue;
        }

        const Tick& tick = *opt;
        const uint64_t recv_ns = static_cast<uint64_t>(
            duration_cast<nanoseconds>(
                system_clock::now().time_since_epoch()).count());

        if (!validate_tick(tick)) continue;

        record_latency(recv_ns - tick.timestamp_ns);
        metrics_.ticks_received.fetch_add(1, std::memory_order_relaxed);
        handler_(tick);
    }
}

// ---------- helpers --------------------------------------------------------
bool FeedIngestionPipeline::validate_tick(const Tick& tick) noexcept {
    if (!tick.is_valid()) {
        metrics_.crc_errors.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (last_seq_ > 0 && tick.sequence_id != last_seq_ + 1) {
        metrics_.sequence_gaps.fetch_add(1, std::memory_order_relaxed);
    }
    last_seq_ = tick.sequence_id;
    return true;
}

void FeedIngestionPipeline::record_latency(uint64_t ns) noexcept {
    // Convert to microseconds, clamp to histogram range
    const std::size_t us = static_cast<std::size_t>(ns / 1000);
    const std::size_t bucket = std::min(us, HIST_BUCKETS - 1);
    latency_hist_[bucket].fetch_add(1, std::memory_order_relaxed);
}

LatencyStats FeedIngestionPipeline::latency_snapshot() const {
    // Build cumulative distribution from histogram
    uint64_t total = 0;
    std::array<uint64_t, HIST_BUCKETS> snap{};
    for (std::size_t i = 0; i < HIST_BUCKETS; ++i) {
        snap[i] = latency_hist_[i].load(std::memory_order_relaxed);
        total  += snap[i];
    }
    if (total == 0) return {};

    auto percentile = [&](double p) -> double {
        uint64_t target = static_cast<uint64_t>(total * p);
        uint64_t cum = 0;
        for (std::size_t i = 0; i < HIST_BUCKETS; ++i) {
            cum += snap[i];
            if (cum >= target) return static_cast<double>(i);
        }
        return static_cast<double>(HIST_BUCKETS - 1);
    };

    double max_us = 0.0;
    for (std::size_t i = HIST_BUCKETS; i-- > 0;)
        if (snap[i] > 0) { max_us = static_cast<double>(i); break; }

    return { percentile(0.50), percentile(0.95), percentile(0.99), max_us };
}

} // namespace mda
