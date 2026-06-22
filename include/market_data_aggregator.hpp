#pragma once
#include "feed_ingestion_pipeline.hpp"
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <string>
#include <functional>

namespace mda {

struct AggregatorConfig {
    std::vector<std::pair<std::string, std::string>> feeds; // {feed_id, endpoint}
    uint32_t heartbeat_interval_ms = 1000;
    uint32_t stale_feed_timeout_ms = 5000;
    uint32_t recovery_retry_ms     = 2000;
};

/**
 * Top-level aggregator: manages multiple FeedIngestionPipelines,
 * performs health monitoring, and provides a unified tick stream.
 */
class MarketDataAggregator {
public:
    using TickCallback = std::function<void(const std::string& feed, const Tick&)>;

    explicit MarketDataAggregator(AggregatorConfig config, TickCallback cb);
    ~MarketDataAggregator();

    void start();
    void stop();

    // Per-feed metrics snapshot
    std::unordered_map<std::string, FeedMetrics*> all_metrics();

    // Aggregate throughput (ticks/sec, averaged over last second)
    double throughput_tps() const;

private:
    void monitor_loop();

    AggregatorConfig config_;
    TickCallback     callback_;

    std::vector<std::unique_ptr<FeedIngestionPipeline>> pipelines_;
    mutable std::shared_mutex pipelines_mutex_;

    std::thread       monitor_thread_;
    std::atomic<bool> running_{false};

    std::atomic<uint64_t> total_ticks_{0};
    mutable std::atomic<double> throughput_{0.0};
};

} // namespace mda
