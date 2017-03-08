#pragma once

#include <thrill/api/dia.hpp>

#include <cstdint>
#include <iostream>
#include <algorithm>

using NodeId = uint32_t;
using Weight = uint64_t;
using ClusterId = uint32_t;

struct Edge;
struct WeightedEdge;
struct NodeWithLinks;
struct NodeWithWeightedLinks;

struct EdgeTarget {
  using EdgeType = Edge;

  NodeId target;

  inline NodeId getTarget() const { return target; }
  inline Weight getWeight() const { return 1; }
};

struct WeightedEdgeTarget {
  using EdgeType = WeightedEdge;

  NodeId target;
  Weight weight;

  inline NodeId getTarget() const { return target; }
  inline Weight getWeight() const { return weight; }
};

struct Edge {
  using NodeType = NodeWithLinks;

  NodeId tail, head;

  Weight getWeight() const {
    return 1;
  }

  static Edge fromLink(NodeId node, const EdgeTarget& link) {
    return Edge { node, link.target };
  }
};
std::ostream& operator << (std::ostream& os, Edge& e) {
  return os << e.tail << " -> " << e.head;
}

struct WeightedEdge {
  using NodeType = NodeWithWeightedLinks;

  NodeId tail, head;
  Weight weight;

  Weight getWeight() const {
    return weight;
  }

  static WeightedEdge fromLink(NodeId node, const WeightedEdgeTarget& link) {
    return WeightedEdge { node, link.target, link.weight };
  }
};
std::ostream& operator << (std::ostream& os, WeightedEdge& e) {
  return os << e.tail << " -> " << e.head << " (" << e.weight << ")";
}


struct NodeWithLinks {
  using LinkType = EdgeTarget;

  NodeId id;
  std::vector<EdgeTarget> links;

  Weight weightedDegree() const { return links.size(); }

  static NodeWithLinks fromEdge(const Edge& edge) {
    return NodeWithLinks { edge.tail, { EdgeTarget { edge.head } } };
  }

  NodeWithLinks operator+(const NodeWithLinks& other) const {
    assert(id == other.id);
    std::vector<EdgeTarget> merged(links.size() + other.links.size());
    std::merge(links.begin(), links.end(), other.links.begin(), other.links.end(), merged.begin(), [](const EdgeTarget& e1, const EdgeTarget& e2) { return e1.target < e2.target; });
    return NodeWithLinks { id, merged };
  }
};


struct NodeWithWeightedLinks {
  using LinkType = WeightedEdgeTarget;

  NodeId id;
  std::vector<WeightedEdgeTarget> links;
  Weight weighted_degree_cache;

  Weight weightedDegree() const { return weighted_degree_cache; }

  static NodeWithWeightedLinks fromEdge(const WeightedEdge& edge) {
    return NodeWithWeightedLinks { edge.tail, { WeightedEdgeTarget { edge.head, edge.weight } }, edge.weight };
  }

  NodeWithWeightedLinks operator+(const NodeWithWeightedLinks& other) const {
    assert(id == other.id);
    std::vector<WeightedEdgeTarget> merged(links.size() + other.links.size());
    std::merge(links.begin(), links.end(), other.links.begin(), other.links.end(), merged.begin(), [](const WeightedEdgeTarget& e1, const WeightedEdgeTarget& e2) { return e1.target < e2.target; });
    return NodeWithWeightedLinks { id, merged, weighted_degree_cache + other.weighted_degree_cache };
  }
};

namespace thrill {
namespace data {
template <typename Archive>
struct Serialization<Archive, NodeWithLinks>{
  static void Serialize(const NodeWithLinks& node, Archive& ar) {
    Serialization<Archive, NodeId>::Serialize(node.id, ar);
    Serialization<Archive, std::vector<EdgeTarget>>::Serialize(node.links, ar);
  }
  static NodeWithLinks Deserialize(Archive& ar) {
    return NodeWithLinks { Serialization<Archive, NodeId>::Deserialize(ar), Serialization<Archive, std::vector<EdgeTarget>>::Deserialize(ar) };
  }
  static constexpr bool is_fixed_size = false;
  static constexpr size_t fixed_size = 0;
};

template <typename Archive>
struct Serialization<Archive, NodeWithWeightedLinks>{
  static void Serialize(const NodeWithWeightedLinks& node, Archive& ar) {
    Serialization<Archive, NodeId>::Serialize(node.id, ar);
    Serialization<Archive, std::vector<WeightedEdgeTarget>>::Serialize(node.links, ar);
    Serialization<Archive, Weight>::Serialize(node.weighted_degree_cache, ar);
  }
  static NodeWithWeightedLinks Deserialize(Archive& ar) {
    return NodeWithWeightedLinks { Serialization<Archive, NodeId>::Deserialize(ar), Serialization<Archive, std::vector<WeightedEdgeTarget>>::Deserialize(ar), Serialization<Archive, Weight>::Deserialize(ar) };
  }
  static constexpr bool is_fixed_size = false;
  static constexpr size_t fixed_size = 0;
};
} // data
} // thrill


template<typename EdgeType>
struct DiaEdgeGraph {
  using Edge = EdgeType;

  thrill::DIA<EdgeType> edges;
  size_t node_count, total_weight;
};

template<typename NodeType>
struct DiaNodeGraph {
  using Node = NodeType;

  thrill::DIA<NodeType> nodes;
  size_t node_count, total_weight;
};

template<typename NodeType, typename EdgeType>
struct DiaGraph {
  using Node = NodeType;
  using Edge = EdgeType;

  thrill::DIA<NodeType> nodes;
  thrill::DIA<EdgeType> edges;
  size_t node_count, total_weight;
};

template<typename EdgeType>
inline thrill::DIA<typename EdgeType::NodeType> edgesToNodes(thrill::DIA<EdgeType>& edges, uint32_t node_count) {
  using NodeType = typename EdgeType::NodeType;

  return edges
    .Map([](const EdgeType& edge) { return NodeType::fromEdge(edge); })
    .ReduceToIndex(
      [](const NodeType& node) -> size_t { return node.id; },
      [](const NodeType& node1, const NodeType& node2) { return node1 + node2; },
      node_count);
}

template<typename NodeType>
inline thrill::DIA<typename NodeType::EdgeType> nodesToEdges(thrill::DIA<NodeType>& nodes) {
  using LinkType = typename NodeType::LinkType;
  using EdgeType = typename LinkType::EdgeType;

  return nodes
    .template FlatMap<EdgeType>(
      [](const NodeType& node, auto emit) {
        for (const LinkType link : node.links) {
          emit(EdgeType::fromLink(node.id, link));
        }
      });
}
