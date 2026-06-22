#pragma once
#include <atomic>
#include <array>
#include <optional>
#include <cstddef>
#include <cassert>

namespace mda {

/**
 * Lock-free Single-Producer Single-Consumer ring buffer.
 * Cache-line aligned to eliminate false sharing.
 * Capacity must be a power of 2.
 */
template <typename T, std::size_t Capacity>
class LockFreeQueue {
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of 2");

public:
    LockFreeQueue() : head_(0), tail_(0) {}

    // Producer side — returns false if queue is full
    bool push(const T& item) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next = (tail + 1) & mask_;
        if (next == head_.load(std::memory_order_acquire))
            return false; // full
        buffer_[tail] = item;
        tail_.store(next, std::memory_order_release);
        return true;
    }

    bool push(T&& item) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next = (tail + 1) & mask_;
        if (next == head_.load(std::memory_order_acquire))
            return false;
        buffer_[tail] = std::move(item);
        tail_.store(next, std::memory_order_release);
        return true;
    }

    // Consumer side — returns empty optional if queue is empty
    std::optional<T> pop() noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire))
            return std::nullopt; // empty
        T item = std::move(buffer_[head]);
        head_.store((head + 1) & mask_, std::memory_order_release);
        return item;
    }

    bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

    std::size_t size() const noexcept {
        const std::size_t h = head_.load(std::memory_order_acquire);
        const std::size_t t = tail_.load(std::memory_order_acquire);
        return (t - h + Capacity) & mask_;
    }

    static constexpr std::size_t capacity() { return Capacity - 1; }

private:
    static constexpr std::size_t mask_ = Capacity - 1;

    alignas(64) std::atomic<std::size_t> head_;
    alignas(64) std::atomic<std::size_t> tail_;
    std::array<T, Capacity> buffer_;
};

} // namespace mda
