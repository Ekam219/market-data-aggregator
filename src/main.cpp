#include "market_data_aggregator.hpp"
#include <iostream>
#include <iomanip>
#include <csignal>
#include <thread>
#include <chrono>
#include <atomic>

static std::atomic<bool> g_shutdown{false};

void signal_handler(int) { g_shutdown.store(true); }

int main() {
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    mda::AggregatorConfig cfg;
    cfg.feeds = {
        {"FEED_A", "tcp://market-feed-a:9001"},
        {"FEED_B", "tcp://market-feed-b:9002"},
        {"FEED_C", "tcp://market-feed-c:9003"},
    };
    cfg.heartbeat_interval_ms = 1000;
    cfg.stale_feed_timeout_ms = 5000;

    uint64_t processed = 0;

    mda::MarketDataAggregator agg(cfg,
        [&processed](const std::string& feed, const mda::Tick& tick) {
            ++processed;
            // In production: normalise, store, forward downstream
            (void)feed; (void)tick;
        });

    agg.start();
    std::cout << "[MDA] Aggregator started on " << cfg.feeds.size()
              << " feeds. Press Ctrl-C to stop.\n";

    while (!g_shutdown.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cout << std::fixed << std::setprecision(0)
                  << "[MDA] Throughput: " << agg.throughput_tps()
                  << " ticks/sec  |  Total processed: " << processed << "\n";
    }

    std::cout << "[MDA] Shutting down…\n";
    agg.stop();
    std::cout << "[MDA] Clean exit. Total ticks processed: " << processed << "\n";
    return 0;
}
