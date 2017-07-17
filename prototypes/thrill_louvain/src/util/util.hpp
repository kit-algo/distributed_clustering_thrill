#pragma once

#include <cstdint>
#include <algorithm>

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


template<class ValueType, class CompareFunc, class MergeFunc>
void merge(const std::vector<ValueType>& in1, const std::vector<ValueType>& in2, std::vector<ValueType>& out, const CompareFunc& compare, const MergeFunc& merge) {
  auto current1 = in1.begin();
  auto current2 = in2.begin();

  while (current1 != in1.end() || current2 != in2.end()) {
    while (current1 != in1.end() && (current2 == in2.end() || compare(*current1, *current2) >= 0)) {
      if (!out.empty() && compare(*current1, out.back()) == 0) {
        out.back() = merge(out.back(), *current1);
      } else {
        out.push_back(*current1);
      }
      current1++;
    }

    while (current2 != in2.end() && (current1 == in1.end() || compare(*current2, *current1) >= 0)) {
      if (!out.empty() && compare(*current2, out.back()) == 0) {
        out.back() = merge(out.back(), *current2);
      } else {
        out.push_back(*current2);
      }
      current2++;
    }
  }
}

}
