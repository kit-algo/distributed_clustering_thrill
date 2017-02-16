#pragma once

#include <assert.h>
#include <cstdint>
#include <functional>

namespace Util {

template <typename T>
inline std::size_t combined_hash(const T& v) { return std::hash<T>{}(v); }

template <typename T, typename... Rest>
inline std::size_t combined_hash(const T& v, Rest... rest) {
  std::hash<T> hasher;
  std::size_t seed = combined_hash(rest...);
  return seed ^ (hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2));
}

inline uint64_t combine_u32ints(const uint32_t int1, const uint32_t int2) { return (uint64_t) int1 << 32 | int2; }

}
