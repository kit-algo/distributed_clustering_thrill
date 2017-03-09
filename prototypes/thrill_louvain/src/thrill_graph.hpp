#pragma once

#include <thrill/api/dia.hpp>
#include <thrill/api/group_to_index.hpp>

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

  static EdgeTarget fromEdge(const Edge& edge);
};

struct WeightedEdgeTarget {
  using EdgeType = WeightedEdge;

  NodeId target;
  Weight weight;

  inline NodeId getTarget() const { return target; }
  inline Weight getWeight() const { return weight; }

  static WeightedEdgeTarget fromEdge(const WeightedEdge& edge);
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
EdgeTarget EdgeTarget::fromEdge(const Edge& edge) {
  return EdgeTarget { edge.head };
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
WeightedEdgeTarget WeightedEdgeTarget::fromEdge(const WeightedEdge& edge) {
  return WeightedEdgeTarget { edge.head, edge.weight };
}


struct NodeWithLinks {
  using LinkType = EdgeTarget;

  NodeId id;
  std::vector<EdgeTarget> links;

  Weight weightedDegree() const { return links.size(); }

  void push_back(const EdgeTarget& link) {
    links.push_back(link);
  }
};


struct NodeWithWeightedLinks {
  using LinkType = WeightedEdgeTarget;

  NodeId id;
  std::vector<WeightedEdgeTarget> links;
  Weight weighted_degree_cache;

  Weight weightedDegree() const { return weighted_degree_cache; }

  void push_back(const WeightedEdgeTarget& link) {
    weighted_degree_cache += link.weight;
    links.push_back(link);
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

template<typename EdgeType, typename Stack>
auto edgesToNodes(const thrill::DIA<EdgeType, Stack>& edges, uint32_t node_count) {
  using NodeType = typename EdgeType::NodeType;
  using LinkType = typename NodeType::LinkType;

  return edges
    .template GroupToIndex<NodeType>(
      [](const EdgeType& edge) -> size_t { return edge.tail; },
      [](auto& iterator, const NodeId node) {
        NodeType first { node, {} };
        while (iterator.HasNext()) {
          first.push_back(LinkType::fromEdge(iterator.Next()));
        }
        return first;
      },
      node_count);
}

template<typename NodeType, typename Stack>
auto nodesToEdges(const thrill::DIA<NodeType, Stack>& nodes) {
  using LinkType = typename NodeType::LinkType;
  using EdgeType = typename LinkType::EdgeType;

  return nodes
    .template FlatMap<EdgeType>(
      [](const NodeType& node, auto emit) {
        for (const LinkType& link : node.links) {
          emit(EdgeType::fromLink(node.id, link));
        }
      });
}
