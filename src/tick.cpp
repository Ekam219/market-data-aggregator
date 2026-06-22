#include "tick.hpp"
#include "crc32.hpp"
#include <cstring>

namespace mda {

void Tick::compute_crc() noexcept {
    // CRC over all fields except the crc32 field itself
    const std::size_t offset = offsetof(Tick, crc32);
    const std::size_t tail   = sizeof(Tick) - offset - sizeof(uint32_t);

    uint32_t c = crc::compute(this, offset);
    const uint8_t* after = reinterpret_cast<const uint8_t*>(this) + offset + sizeof(uint32_t);
    c = crc::update(c ^ 0xFFFFFFFFu, after, tail) ^ 0xFFFFFFFFu;
    crc32 = c;
}

bool Tick::is_valid() const noexcept {
    if (bid <= 0.0 || ask <= 0.0 || bid > ask) return false;
    if (timestamp_ns == 0)                       return false;

    // Recompute CRC and compare
    Tick copy = *this;
    copy.compute_crc();
    return copy.crc32 == crc32;
}

} // namespace mda
