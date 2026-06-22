#include <iostream>
#include <iomanip>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <random>
#include <chrono>
#include <thread>
#include <atomic>
#include <csignal>
#include <string>
#include <stdexcept>

// POSIX networking
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "tick.hpp"
#include "crc32.hpp"

// ── Signal handling ───────────────────────────────────────────────────────────
static std::atomic<bool> g_stop{false};
static void on_signal(int) { g_stop.store(true); }

// ── Helpers ───────────────────────────────────────────────────────────────────
static uint64_t now_ns() {
    using namespace std::chrono;
    return static_cast<uint64_t>(
        duration_cast<nanoseconds>(
            system_clock::now().time_since_epoch()).count());
}

struct SimConfig {
    std::string host       = "127.0.0.1";
    int         port       = 9001;
    uint64_t    rate       = 100'000;   // ticks/sec
    std::string feed_id    = "FA";
    double      duration_s = 10.0;
    double      error_rate = 0.001;     // fraction with bad CRC
    std::string symbol     = "AAPL";
};

// ── TCP connect ───────────────────────────────────────────────────────────────
static int tcp_connect(const std::string& host, int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket() failed");

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1)
        throw std::runtime_error("Invalid host: " + host);

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("connect() failed to " + host + ":" + std::to_string(port));
    }

    int flag = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    return fd;
}

// ── Build a Tick ──────────────────────────────────────────────────────────────
static mda::Tick build_tick(uint64_t seq, const SimConfig& cfg,
                             std::mt19937_64& rng, bool corrupt) {
    std::uniform_real_distribution<double> price(100.0, 200.0);
    std::uniform_int_distribution<uint64_t> vol(1, 10000);

    mda::Tick t{};
    t.timestamp_ns = now_ns();
    t.sequence_id  = seq;
    t.bid          = price(rng);
    t.ask          = t.bid + 0.01;
    t.last         = t.bid + 0.005;
    t.volume       = vol(rng);
    std::strncpy(t.symbol,  cfg.symbol.c_str(),  sizeof(t.symbol)  - 1);
    std::strncpy(t.feed_id, cfg.feed_id.c_str(), sizeof(t.feed_id) - 1);
    t.compute_crc();
    if (corrupt) t.crc32 ^= 0xDEADBEEFu;
    return t;
}

// ── Simulate ──────────────────────────────────────────────────────────────────
static void simulate(const SimConfig& cfg) {
    std::cout << "[FeedSim] Connecting to " << cfg.host << ":" << cfg.port
              << "  rate=" << cfg.rate << " ticks/sec  duration="
              << cfg.duration_s << "s\n";

    int fd = tcp_connect(cfg.host, cfg.port);
    std::cout << "[FeedSim] Connected. Streaming feed=" << cfg.feed_id << "\n";

    std::mt19937_64 rng(std::random_device{}());
    std::bernoulli_distribution err_dist(cfg.error_rate);

    const double interval_ns = (cfg.rate > 0)
        ? 1e9 / static_cast<double>(cfg.rate) : 0.0;

    uint64_t seq     = 0;
    uint64_t sent    = 0;
    uint64_t errors  = 0;

    auto t_start  = std::chrono::steady_clock::now();
    auto t_end    = t_start + std::chrono::duration<double>(cfg.duration_s);
    auto next_tick = t_start;

    while (!g_stop.load() && std::chrono::steady_clock::now() < t_end) {
        ++seq;
        bool corrupt = err_dist(rng);
        mda::Tick tick = build_tick(seq, cfg, rng, corrupt);

        ssize_t n = ::send(fd, &tick, sizeof(tick), MSG_NOSIGNAL);
        if (n <= 0) {
            std::cerr << "[FeedSim] Connection lost at seq=" << seq << "\n";
            break;
        }
        ++sent;
        if (corrupt) ++errors;

        if (interval_ns > 0.0) {
            next_tick += std::chrono::nanoseconds(
                static_cast<long long>(interval_ns));
            auto now = std::chrono::steady_clock::now();
            if (next_tick > now)
                std::this_thread::sleep_until(next_tick);
        }
    }

    ::close(fd);

    double elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t_start).count();

    std::cout << "\n[FeedSim] Done.\n"
              << "  Sent:        " << sent << " ticks\n"
              << "  CRC errors:  " << errors
              << " (" << std::fixed << std::setprecision(3)
              << (100.0 * errors / std::max<uint64_t>(sent, 1)) << "%)\n"
              << "  Elapsed:     " << std::setprecision(2) << elapsed << "s\n"
              << "  Actual rate: " << std::setprecision(0)
              << (sent / elapsed) << " ticks/sec\n";
}

// ── CLI ───────────────────────────────────────────────────────────────────────
static void usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "  --host       HOST   (default: 127.0.0.1)\n"
              << "  --port       PORT   (default: 9001)\n"
              << "  --rate       N      ticks/sec (default: 100000)\n"
              << "  --feed-id    ID     (default: FA)\n"
              << "  --duration   SECS   (default: 10.0)\n"
              << "  --error-rate FRAC   fraction of corrupt ticks (default: 0.001)\n"
              << "  --symbol     SYM    (default: AAPL)\n";
}

int main(int argc, char** argv) {
    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);

    SimConfig cfg;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if ((a == "--host")       && i+1 < argc) cfg.host       = argv[++i];
        else if ((a == "--port")  && i+1 < argc) cfg.port       = std::stoi(argv[++i]);
        else if ((a == "--rate")  && i+1 < argc) cfg.rate       = std::stoull(argv[++i]);
        else if ((a == "--feed-id") && i+1 < argc) cfg.feed_id  = argv[++i];
        else if ((a == "--duration") && i+1 < argc) cfg.duration_s = std::stod(argv[++i]);
        else if ((a == "--error-rate") && i+1 < argc) cfg.error_rate = std::stod(argv[++i]);
        else if ((a == "--symbol") && i+1 < argc) cfg.symbol    = argv[++i];
        else if (a == "--help") { usage(argv[0]); return 0; }
        else { std::cerr << "Unknown option: " << a << "\n"; usage(argv[0]); return 1; }
    }

    try {
        simulate(cfg);
    } catch (const std::exception& e) {
        std::cerr << "[FeedSim] ERROR: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
