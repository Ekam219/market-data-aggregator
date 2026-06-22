#pragma once
#include <cstdint>
#include <string>
#include <array>

namespace mda {

// Market tick structure — packed for cache efficiency
struct alignas(64) Tick {
    uint64_t timestamp_ns;     // nanoseconds since epoch
    uint64_t sequence_id;      // monotonic sequence number
    double   bid;
    double   ask;
    double   last;
    uint64_t volume;
    uint32_t crc32;
    char     symbol[12];
    char     feed_id[4];

    bool is_valid() const noexcept;
    void compute_crc() noexcept;
};

static_assert(sizeof(Tick) <= 64, "Tick must fit in a single cache line");

} // namespace mda
