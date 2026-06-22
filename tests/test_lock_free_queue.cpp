#include <gtest/gtest.h>
#include "lock_free_queue.hpp"
#include <thread>
#include <vector>
#include <numeric>
#include <atomic>

using namespace mda;

// ──────────────────────────────────────────────────────────────────────────────
// Basic correctness
// ──────────────────────────────────────────────────────────────────────────────

TEST(LockFreeQueueTest, InitiallyEmpty) {
    LockFreeQueue<int, 16> q;
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
}

TEST(LockFreeQueueTest, PushAndPop) {
    LockFreeQueue<int, 16> q;
    ASSERT_TRUE(q.push(42));
    EXPECT_FALSE(q.empty());
    auto val = q.pop();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, 42);
    EXPECT_TRUE(q.empty());
}

TEST(LockFreeQueueTest, FIFOOrdering) {
    LockFreeQueue<int, 32> q;
    for (int i = 0; i < 10; ++i) ASSERT_TRUE(q.push(i));
    for (int i = 0; i < 10; ++i) {
        auto v = q.pop();
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(*v, i);
    }
}

TEST(LockFreeQueueTest, PopOnEmptyReturnsNullopt) {
    LockFreeQueue<int, 8> q;
    EXPECT_FALSE(q.pop().has_value());
}

TEST(LockFreeQueueTest, FillToCapacity) {
    LockFreeQueue<int, 8> q; // capacity = 7 usable slots
    int pushed = 0;
    while (q.push(pushed)) ++pushed;
    EXPECT_EQ(pushed, static_cast<int>(q.capacity()));
    EXPECT_EQ(q.size(), q.capacity());
}

TEST(LockFreeQueueTest, WrapAround) {
    LockFreeQueue<int, 8> q;
    // Fill halfway, drain, fill again — exercises wrap-around
    for (int i = 0; i < 3; ++i) ASSERT_TRUE(q.push(i));
    for (int i = 0; i < 3; ++i) q.pop();
    for (int i = 100; i < 106; ++i) {
        if (!q.push(i)) break;
    }
    int expected = 100;
    while (!q.empty()) {
        auto v = q.pop();
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(*v, expected++);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Concurrent SPSC throughput test
// ──────────────────────────────────────────────────────────────────────────────

TEST(LockFreeQueueTest, SPSCConcurrentThroughput) {
    constexpr int N = 1'000'000;
    LockFreeQueue<int, 1 << 20> q;

    std::atomic<bool> start{false};
    std::atomic<int>  consumed{0};

    std::thread producer([&]{
        while (!start.load(std::memory_order_acquire));
        for (int i = 0; i < N; ++i) {
            while (!q.push(i)) std::this_thread::yield();
        }
    });

    std::thread consumer([&]{
        while (!start.load(std::memory_order_acquire));
        int expected = 0;
        while (expected < N) {
            auto v = q.pop();
            if (v) {
                EXPECT_EQ(*v, expected++);
                consumed.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    start.store(true, std::memory_order_release);
    producer.join();
    consumer.join();

    EXPECT_EQ(consumed.load(), N);
    EXPECT_TRUE(q.empty());
}

// ──────────────────────────────────────────────────────────────────────────────
// Edge cases
// ──────────────────────────────────────────────────────────────────────────────

TEST(LockFreeQueueTest, MoveSemantics) {
    LockFreeQueue<std::string, 8> q;
    std::string s = "hello";
    ASSERT_TRUE(q.push(std::move(s)));
    EXPECT_TRUE(s.empty()); // moved-from
    auto v = q.pop();
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, "hello");
}

TEST(LockFreeQueueTest, SizeAccuracy) {
    LockFreeQueue<int, 16> q;
    for (int i = 1; i <= 5; ++i) {
        q.push(i);
        EXPECT_EQ(q.size(), static_cast<std::size_t>(i));
    }
    for (int i = 4; i >= 0; --i) {
        q.pop();
        EXPECT_EQ(q.size(), static_cast<std::size_t>(i));
    }
}
