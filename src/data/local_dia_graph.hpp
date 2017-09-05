#pragma once

#include "data/graph.hpp"

using NodeId = typename Graph::NodeId;
using EdgeId = typename Graph::EdgeId;
using Weight = typename Graph::Weight;

template<typename NodeType>
class LocalDiaGraph {
private:

  const std::vector<NodeType>& nodes;
  Weight total_weight;

public:
  LocalDiaGraph(const std::vector<NodeType>& nodes, Weight total_weight) :
    nodes(nodes), total_weight(total_weight) {}

  NodeId getNodeCount() const { return nodes.size(); }
  NodeId getNodeCountIncludingGhost() const { return nodes.size(); }
  Weight getTotalWeight() const { return total_weight; }
  bool isWeighted() const { return std::is_same<NodeType, NodeWithWeightedLinks>::value; }

  Weight nodeDegree(const NodeId node_id) const {
    return nodes[node_id].weightedDegree();
  }

  template<class F>
  void forEachAdjacentNode(NodeId node, F f) const {
    for (const auto& link : nodes[node].links) {
      f(link.target, link.getWeight());
    }
  }

  template<class F>
  void forEachEdge(F f) const {
    for (NodeId node = 0; node < getNodeCount(); node++) {
      forEachAdjacentNode(node, [&node, &f](NodeId neighbor, Weight weight) {
        f(node, neighbor, weight);
      });
    }
  }
};
