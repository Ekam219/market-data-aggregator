#include <gtest/gtest.h>
#include "tick.hpp"
#include "crc32.hpp"
#include <cstring>

using namespace mda;

namespace {

Tick make_valid_tick() {
    Tick t{};
    t.timestamp_ns = 1'700'000'000'000'000'000ULL;
    t.sequence_id  = 1;
    t.bid          = 150.00;
    t.ask          = 150.01;
    t.last         = 150.005;
    t.volume       = 1000;
    std::strncpy(t.symbol,  "AAPL", sizeof(t.symbol));
    std::strncpy(t.feed_id, "FA",   sizeof(t.feed_id));
    t.compute_crc();
    return t;
}

} // anonymous namespace

// ──────────────────────────────────────────────────────────────────────────────
// CRC32 unit tests
// ──────────────────────────────────────────────────────────────────────────────

TEST(CRC32Test, KnownVector) {
    // CRC32 of "123456789" is 0xCBF43926
    const char* data = "123456789";
    EXPECT_EQ(crc::compute(data, 9), 0xCBF43926u);
}

TEST(CRC32Test, EmptyInput) {
    EXPECT_NE(crc::compute(nullptr, 0), 0u); // implementation-defined but consistent
}

TEST(CRC32Test, SingleByteDifference) {
    std::array<uint8_t, 16> a{}, b{};
    a.fill(0xAB); b.fill(0xAB);
    b[7] ^= 0x01;
    EXPECT_NE(crc::compute(a.data(), a.size()), crc::compute(b.data(), b.size()));
}

// ──────────────────────────────────────────────────────────────────────────────
// Tick validation tests
// ──────────────────────────────────────────────────────────────────────────────

TEST(TickTest, ValidTickPassesValidation) {
    auto tick = make_valid_tick();
    EXPECT_TRUE(tick.is_valid());
}

TEST(TickTest, CorruptedCRCFailsValidation) {
    auto tick = make_valid_tick();
    tick.crc32 ^= 0xDEADBEEF;
    EXPECT_FALSE(tick.is_valid());
}

TEST(TickTest, ZeroBidFailsValidation) {
    auto tick = make_valid_tick();
    tick.bid = 0.0;
    tick.compute_crc();
    EXPECT_FALSE(tick.is_valid());
}

TEST(TickTest, NegativePriceFailsValidation) {
    auto tick = make_valid_tick();
    tick.bid = -1.0;
    tick.compute_crc();
    EXPECT_FALSE(tick.is_valid());
}

TEST(TickTest, BidGreaterThanAskFailsValidation) {
    auto tick = make_valid_tick();
    tick.bid = 150.02;
    tick.ask = 150.01;
    tick.compute_crc();
    EXPECT_FALSE(tick.is_valid());
}

TEST(TickTest, ZeroTimestampFailsValidation) {
    auto tick = make_valid_tick();
    tick.timestamp_ns = 0;
    tick.compute_crc();
    EXPECT_FALSE(tick.is_valid());
}

TEST(TickTest, CRCIsDeterministic) {
    auto t1 = make_valid_tick();
    auto t2 = make_valid_tick();
    EXPECT_EQ(t1.crc32, t2.crc32);
}

TEST(TickTest, ModifyingFieldAfterCRCInvalidates) {
    auto tick = make_valid_tick();
    ASSERT_TRUE(tick.is_valid());
    tick.volume += 1; // tamper without recomputing CRC
    EXPECT_FALSE(tick.is_valid());
}

TEST(TickTest, TickFitsInCacheLine) {
    EXPECT_LE(sizeof(Tick), 64u);
}
