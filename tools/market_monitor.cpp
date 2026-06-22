#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <cstring>
#include <cstdint>
#include <csignal>
#include <atomic>
#include <chrono>
#include <thread>
#include <stdexcept>

// POSIX
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "feed_ingestion_pipeline.hpp"
#include "market_data_aggregator.hpp"

// ── Signal ────────────────────────────────────────────────────────────────────
static std::atomic<bool> g_stop{false};
static void on_signal(int) { g_stop.store(true); }

// ── ANSI helpers ──────────────────────────────────────────────────────────────
namespace ansi {
    constexpr const char* RESET  = "\033[0m";
    constexpr const char* BOLD   = "\033[1m";
    constexpr const char* GREEN  = "\033[32m";
    constexpr const char* YELLOW = "\033[33m";
    constexpr const char* RED    = "\033[31m";
    constexpr const char* CYAN   = "\033[36m";
    constexpr const char* CLEAR  = "\033[2J\033[H";
}

// ── Timestamp ─────────────────────────────────────────────────────────────────
static std::string utc_now() {
    using namespace std::chrono;
    auto tp = system_clock::now();
    std::time_t t = system_clock::to_time_t(tp);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&t));
    return buf;
}

// ── State name ────────────────────────────────────────────────────────────────
static const char* state_str(mda::FeedState s) {
    switch (s) {
        case mda::FeedState::CONNECTING:  return "CONNECTING";
        case mda::FeedState::ACTIVE:      return "ACTIVE";
        case mda::FeedState::STALE:       return "STALE";
        case mda::FeedState::DROPPED:     return "DROPPED";
        case mda::FeedState::RECOVERING:  return "RECOVERING";
        default:                          return "UNKNOWN";
    }
}

static const char* state_color(mda::FeedState s) {
    switch (s) {
        case mda::FeedState::ACTIVE:     return ansi::GREEN;
        case mda::FeedState::STALE:      return ansi::YELLOW;
        case mda::FeedState::DROPPED:    return ansi::RED;
        case mda::FeedState::RECOVERING: return ansi::CYAN;
        default:                         return ansi::RESET;
    }
}

// ── Render dashboard ─────────────────────────────────────────────────────────
struct FeedRow {
    std::string  feed_id;
    mda::FeedState state;
    uint64_t     ticks_received;
    uint64_t     ticks_dropped;
    uint64_t     crc_errors;
    uint64_t     sequence_gaps;
    mda::LatencyStats latency;
};

static void render(const std::vector<FeedRow>& rows, double tps,
                   uint64_t total, const std::string& ts) {
    constexpr int W = 90;
    std::cout << ansi::CLEAR;
    std::cout << ansi::BOLD << ansi::CYAN
              << "  Market Data Aggregation Monitor  |  " << ts << "\n"
              << ansi::RESET;
    std::cout << std::string(W, '=') << "\n";
    std::cout << "  Aggregate throughput: " << ansi::BOLD
              << std::fixed << std::setprecision(0) << tps
              << " ticks/sec" << ansi::RESET
              << "   |   Total: " << total << "\n";
    std::cout << std::string(W, '-') << "\n";

    // Header
    std::cout << std::left
              << std::setw(10) << "Feed"
              << std::setw(14) << "State"
              << std::right
              << std::setw(12) << "Received"
              << std::setw(10) << "Dropped"
              << std::setw(10) << "CRC Err"
              << std::setw(10) << "SeqGap"
              << std::setw(9)  << "p50 µs"
              << std::setw(9)  << "p99 µs"
              << std::setw(9)  << "max µs"
              << "\n";
    std::cout << std::string(W, '-') << "\n";

    for (const auto& r : rows) {
        std::cout << ansi::BOLD << std::left << std::setw(10) << r.feed_id
                  << ansi::RESET
                  << state_color(r.state)
                  << std::setw(14) << state_str(r.state)
                  << ansi::RESET
                  << std::right
                  << std::setw(12) << r.ticks_received
                  << std::setw(10) << r.ticks_dropped
                  << std::setw(10) << r.crc_errors
                  << std::setw(10) << r.sequence_gaps
                  << std::fixed << std::setprecision(1)
                  << std::setw(9)  << r.latency.p50_us
                  << std::setw(9)  << r.latency.p99_us
                  << std::setw(9)  << r.latency.max_us
                  << "\n";
    }
    std::cout << std::string(W, '=') << "\n";
    std::cout << "  Press Ctrl-C to exit\n" << std::flush;
}

// ── Demo mode (no live aggregator needed) ─────────────────────────────────────
static void run_demo(double interval_s) {
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> p50(1.5, 3.0);
    std::uniform_real_distribution<double> p99(4.0, 8.5);
    std::uniform_int_distribution<uint64_t> recv_d(800'000, 1'200'000);
    uint64_t total = 0;

    while (!g_stop.load()) {
        std::vector<FeedRow> rows;
        for (const char* fid : {"FEED_A", "FEED_B", "FEED_C"}) {
            FeedRow r;
            r.feed_id        = fid;
            r.state          = mda::FeedState::ACTIVE;
            r.ticks_received = recv_d(rng);
            r.ticks_dropped  = rng() % 50;
            r.crc_errors     = rng() % 10;
            r.sequence_gaps  = rng() % 5;
            r.latency.p50_us = p50(rng);
            r.latency.p99_us = p99(rng);
            r.latency.max_us = r.latency.p99_us + (rng() % 2);
            total += r.ticks_received;
            rows.push_back(r);
        }
        double tps = total / (interval_s * rows.size());
        render(rows, tps, total, utc_now());
        std::this_thread::sleep_for(
            std::chrono::duration<double>(interval_s));
    }
}

// ── Live mode: attach to a running MarketDataAggregator ───────────────────────
static void run_live(mda::MarketDataAggregator& agg,
                     const std::vector<std::string>& feed_ids,
                     double interval_s)
{
    uint64_t total = 0;
    while (!g_stop.load()) {
        auto metrics = agg.all_metrics();
        std::vector<FeedRow> rows;
        for (const auto& fid : feed_ids) {
            auto it = metrics.find(fid);
            if (it == metrics.end()) continue;
            const mda::FeedMetrics* m = it->second;
            FeedRow r;
            r.feed_id        = fid;
            r.state          = m->state.load();
            r.ticks_received = m->ticks_received.load();
            r.ticks_dropped  = m->ticks_dropped.load();
            r.crc_errors     = m->crc_errors.load();
            r.sequence_gaps  = m->sequence_gaps.load();
            // latency fetched from pipeline — placeholder zeros in this build
            r.latency        = {};
            total           += r.ticks_received;
            rows.push_back(r);
        }
        render(rows, agg.throughput_tps(), total, utc_now());
        std::this_thread::sleep_for(
            std::chrono::duration<double>(interval_s));
    }
}

// ── CLI ───────────────────────────────────────────────────────────────────────
static void usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "  --demo              Run with simulated data (no aggregator needed)\n"
              << "  --interval SECS     Refresh interval (default: 1.0)\n";
}

int main(int argc, char** argv) {
    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);

    bool   demo       = false;
    double interval_s = 1.0;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--demo")                       demo       = true;
        else if (a == "--interval" && i+1 < argc) interval_s = std::stod(argv[++i]);
        else if (a == "--help") { usage(argv[0]); return 0; }
        else { std::cerr << "Unknown option: " << a << "\n"; usage(argv[0]); return 1; }
    }

    std::cout << "[Monitor] Starting" << (demo ? " (DEMO mode)" : "") << "\n";

    if (demo) {
        run_demo(interval_s);
        return 0;
    }

    // Live mode: spin up aggregator in-process
    mda::AggregatorConfig cfg;
    cfg.feeds = {
        {"FEED_A", "tcp://market-feed-a:9001"},
        {"FEED_B", "tcp://market-feed-b:9002"},
        {"FEED_C", "tcp://market-feed-c:9003"},
    };

    mda::MarketDataAggregator agg(cfg, [](const std::string&, const mda::Tick&){});
    agg.start();

    std::vector<std::string> ids = {"FEED_A", "FEED_B", "FEED_C"};
    run_live(agg, ids, interval_s);

    agg.stop();
    return 0;
}
