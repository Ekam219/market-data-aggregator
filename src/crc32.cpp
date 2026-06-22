#include "crc32.hpp"
#include <array>

namespace mda {
namespace crc {

namespace {

// Pre-computed CRC32 lookup table (IEEE polynomial)
constexpr uint32_t POLY = 0xEDB88320u;

constexpr std::array<uint32_t, 256> make_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int j = 0; j < 8; ++j)
            c = (c & 1) ? (POLY ^ (c >> 1)) : (c >> 1);
        table[i] = c;
    }
    return table;
}

constexpr auto TABLE = make_table();

} // anonymous namespace

uint32_t compute(const void* data, std::size_t length) noexcept {
    return update(0xFFFFFFFFu, data, length) ^ 0xFFFFFFFFu;
}

uint32_t update(uint32_t crc, const void* data, std::size_t length) noexcept {
    const auto* bytes = static_cast<const uint8_t*>(data);
    for (std::size_t i = 0; i < length; ++i)
        crc = TABLE[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    return crc;
}

} // namespace crc
} // namespace mda
