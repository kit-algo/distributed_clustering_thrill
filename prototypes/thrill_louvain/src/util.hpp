#pragma once

#include <cstdint>

#include <thrill/common/hash.hpp>

namespace Util {

template <typename T>
inline uint32_t combined_hash(const T& v) { return thrill::common::hash<T>{}(v); }

template <typename T, typename... Rest>
inline uint32_t combined_hash(const T& v, Rest... rest) {
  thrill::common::hash<T> hasher;
  uint32_t seed = combined_hash(rest...);
  uint32_t value_hash = hasher(v);
  return seed ^ (value_hash + 0x9e3779b9 + (seed<<6) + (seed>>2));
}

inline uint64_t combine_u32ints(const uint32_t int1, const uint32_t int2) { return (uint64_t) int1 << 32 | int2; }

}
