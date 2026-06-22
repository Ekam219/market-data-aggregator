#include "market_data_aggregator.hpp"
#include <chrono>
#include <thread>
#include <iostream>

namespace mda {

using namespace std::chrono;

MarketDataAggregator::MarketDataAggregator(AggregatorConfig config,
                                            TickCallback cb)
    : config_(std::move(config)), callback_(std::move(cb))
{}

MarketDataAggregator::~MarketDataAggregator() {
    stop();
}

void MarketDataAggregator::start() {
    if (running_.exchange(true)) return;

    {
        std::unique_lock lock(pipelines_mutex_);
        for (auto& [id, ep] : config_.feeds) {
            auto handler = [this, fid = id](const Tick& t) {
                total_ticks_.fetch_add(1, std::memory_order_relaxed);
                callback_(fid, t);
            };
            auto p = std::make_unique<FeedIngestionPipeline>(id, ep, handler);
            p->start();
            pipelines_.push_back(std::move(p));
        }
    }

    monitor_thread_ = std::thread([this]{ monitor_loop(); });
}

void MarketDataAggregator::stop() {
    if (!running_.exchange(false)) return;
    if (monitor_thread_.joinable()) monitor_thread_.join();

    std::unique_lock lock(pipelines_mutex_);
    for (auto& p : pipelines_) p->stop();
    pipelines_.clear();
}

void MarketDataAggregator::monitor_loop() {
    uint64_t prev_ticks = 0;
    auto prev_time = steady_clock::now();

    while (running_.load()) {
        std::this_thread::sleep_for(milliseconds(config_.heartbeat_interval_ms));

        auto now = steady_clock::now();
        uint64_t cur_ticks = total_ticks_.load(std::memory_order_relaxed);
        double elapsed_s = duration<double>(now - prev_time).count();
        throughput_.store((cur_ticks - prev_ticks) / elapsed_s,
                          std::memory_order_relaxed);
        prev_ticks = cur_ticks;
        prev_time  = now;

        // Health check each pipeline
        std::shared_lock lock(pipelines_mutex_);
        for (auto& p : pipelines_) {
            if (p->state() == FeedState::DROPPED ||
                p->state() == FeedState::STALE) {
                std::cerr << "[Monitor] Feed in bad state, flagging recovery\n";
            }
        }
    }
}

double MarketDataAggregator::throughput_tps() const {
    return throughput_.load(std::memory_order_relaxed);
}

std::unordered_map<std::string, FeedMetrics*>
MarketDataAggregator::all_metrics() {
    std::unordered_map<std::string, FeedMetrics*> out;
    std::shared_lock lock(pipelines_mutex_);
    for (auto& p : pipelines_)
        out[p->metrics().ticks_received.load() > 0 ? "active" : "idle"]
            = const_cast<FeedMetrics*>(&p->metrics());
    return out;
}

} // namespace mda
