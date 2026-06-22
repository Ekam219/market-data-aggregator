#pragma once
#include "tick.hpp"
#include "lock_free_queue.hpp"
#include <atomic>
#include <thread>
#include <functional>
#include <chrono>
#include <string>
#include <vector>

namespace mda {

// Latency stats snapshot
struct LatencyStats {
    double p50_us;
    double p95_us;
    double p99_us;
    double max_us;
};

// Feed health state
enum class FeedState {
    CONNECTING,
    ACTIVE,
    STALE,
    DROPPED,
    RECOVERING
};

struct FeedMetrics {
    std::atomic<uint64_t> ticks_received{0};
    std::atomic<uint64_t> ticks_dropped{0};
    std::atomic<uint64_t> crc_errors{0};
    std::atomic<uint64_t> sequence_gaps{0};
    std::atomic<FeedState> state{FeedState::CONNECTING};
};

/**
 * Ingestion pipeline for a single market data feed.
 * Runs a dedicated producer thread pushing into a lock-free queue.
 * A consumer thread drains and dispatches to registered handlers.
 */
class FeedIngestionPipeline {
public:
    static constexpr std::size_t QUEUE_CAPACITY = 1 << 20; // 1M slots

    using TickHandler = std::function<void(const Tick&)>;

    explicit FeedIngestionPipeline(std::string feed_id,
                                    std::string endpoint,
                                    TickHandler handler);
    ~FeedIngestionPipeline();

    // Non-copyable, non-movable
    FeedIngestionPipeline(const FeedIngestionPipeline&)            = delete;
    FeedIngestionPipeline& operator=(const FeedIngestionPipeline&) = delete;

    void start();
    void stop();

    const FeedMetrics& metrics() const { return metrics_; }
    LatencyStats       latency_snapshot() const;
    FeedState          state() const { return metrics_.state.load(); }

private:
    void producer_loop();
    void consumer_loop();

    bool validate_tick(const Tick& tick) noexcept;
    void record_latency(uint64_t ns) noexcept;

    std::string  feed_id_;
    std::string  endpoint_;
    TickHandler  handler_;

    LockFreeQueue<Tick, QUEUE_CAPACITY> queue_;

    std::thread  producer_thread_;
    std::thread  consumer_thread_;
    std::atomic<bool> running_{false};

    FeedMetrics  metrics_;
    uint64_t     last_seq_{0};

    // Rolling latency histogram (microseconds, 1µs buckets up to 10ms)
    static constexpr std::size_t HIST_BUCKETS = 10000;
    std::array<std::atomic<uint64_t>, HIST_BUCKETS> latency_hist_{};
};

} // namespace mda
