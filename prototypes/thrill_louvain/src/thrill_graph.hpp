#pragma once

#include <thrill/api/dia.hpp>

#include <cstdint>
#include <iostream>

using NodeId = uint32_t;
using Weight = uint64_t;
using ClusterId = uint32_t;

struct Edge {
  NodeId tail, head;

  Weight getWeight() const {
    return 1;
  }
};

struct WeightedEdge {
  NodeId tail, head;
  Weight weight;

  Weight getWeight() const {
    return weight;
  }
};

std::ostream& operator << (std::ostream& os, Edge& e) {
  return os << e.tail << " -> " << e.head;
}

std::ostream& operator << (std::ostream& os, WeightedEdge& e) {
  return os << e.tail << " -> " << e.head << " (" << e.weight << ")";
}

template<typename EdgeType>
struct DiaGraph {
  thrill::DIA<EdgeType> edge_list;
  size_t node_count, total_weight;
};
