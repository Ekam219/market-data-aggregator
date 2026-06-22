#pragma once
#include <cstdint>
#include <cstddef>

namespace mda {
namespace crc {

// CRC32 using the standard IEEE polynomial (0xEDB88320)
uint32_t compute(const void* data, std::size_t length) noexcept;

// Incremental update
uint32_t update(uint32_t crc, const void* data, std::size_t length) noexcept;

} // namespace crc
} // namespace mda
