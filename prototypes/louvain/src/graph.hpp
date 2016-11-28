#pragma once

#include <algorithm>
#include <numeric>
#include <tuple>
#include <vector>
#include <iostream>
#include <assert.h>
#include <map>
#include <cstdint>

class Graph {
public:

  typedef uint32_t NodeId;
  typedef uint32_t EdgeId; // TODO 64 Bit
  typedef uint32_t Weight;

private:

  NodeId node_count;
  EdgeId edge_count;
  std::vector<EdgeId> edge_indexes;
  std::vector<Weight> degrees;
  std::vector<NodeId> neighbors;
  std::vector<Weight> weights;
  Weight total_weight;

  void initializeAccumulatedWeights() {
    assert(std::accumulate(weights.begin(), weights.end(), 0) % 2 == 0);
    for (NodeId node = 0; node < node_count; node++) {
      degrees[node] = std::accumulate(weights.begin() + edge_indexes[node], weights.begin() + edge_indexes[node + 1], 0);
    }
    total_weight = std::accumulate(degrees.begin(), degrees.end(), 0) / 2;
  }

public:
  Graph(const NodeId node_count, const EdgeId edge_count) : node_count(node_count), edge_count(edge_count),
    edge_indexes(node_count + 1, 2 * edge_count), degrees(node_count, 0),
    neighbors(2 * edge_count), weights(2 * edge_count) {}

  NodeId getNodeCount() const { return node_count; }
  EdgeId getEdgeCount() const { return edge_count; }
  Weight getTotalWeight() const { return total_weight; }

  Weight weightedNodeDegree(const NodeId node_id) const {
    return degrees[node_id];
  }

  template<class F>
  void forEachAdjacentNode(NodeId node, F f) const {
    for (EdgeId edge_index = edge_indexes[node]; edge_index < edge_indexes[node + 1]; edge_index++) {
      f(neighbors[edge_index], weights[edge_index]);
    }
  }

  void setEdgesWithMissingBackwardArcs(std::vector<std::tuple<NodeId, NodeId, Weight>> &edges) {
    std::vector<std::tuple<NodeId, NodeId, Weight>> foo(2 * edge_count);
    for (EdgeId i = 0; i < edge_count; i++) {
      foo[i] = edges[i];
    }
    for (EdgeId i = 0; i < edge_count; i++) {
      foo[edge_count + i] = std::make_tuple(std::get<1>(edges[i]), std::get<0>(edges[i]), std::get<2>(edges[i]));
    }

    std::sort(foo.begin(), foo.end(), [](const auto& edge1, const auto& edge2) {
      return std::get<0>(edge1) < std::get<0>(edge2);
    });

    NodeId current_node = 0;
    for (EdgeId i = 0; i < 2 * edge_count; i++) {
      NodeId tail = std::get<0>(foo[i]);
      while (current_node <= tail) {
        edge_indexes[current_node++] = i;
      }
      neighbors[i] = std::get<1>(foo[i]);
      weights[i] = std::get<2>(foo[i]);
      assert(std::get<2>(foo[i]) > 0);
    }

    initializeAccumulatedWeights();
  }

  void setEdgesByAdjacencyMatrix(const std::vector<std::map<NodeId, Weight>> &adjacency_weights) {
    EdgeId current_edge_index = 0;
    NodeId current_node = 0;
    for (NodeId tail = 0; tail < node_count; tail++) {
      for (const auto &edge : adjacency_weights[tail]) {
        while (current_node <= tail) {
          edge_indexes[current_node++] = current_edge_index;
        }
        neighbors[current_edge_index] = edge.first;
        weights[current_edge_index++] = edge.second;
        assert(edge.second > 0);

        if (tail == edge.first) {
          neighbors[current_edge_index] = edge.first;
          weights[current_edge_index - 1] /= 2;
          weights[current_edge_index++] = edge.second / 2;
        }
      }
    }

    edge_indexes[node_count] = current_edge_index;
    neighbors.resize(current_edge_index);
    weights.resize(current_edge_index);
    initializeAccumulatedWeights();
  }

  void setEdgesByAdjacencyLists(std::vector<std::vector<NodeId>> &neighbors) {
    EdgeId current_edge_index = 0;
    for (NodeId node = 0; node < node_count; node++) {
      edge_indexes[node] = current_edge_index;
      for (NodeId& neighbor : neighbors[node]) {
        this->neighbors[current_edge_index] = neighbor;
        weights[current_edge_index] = 1;
        current_edge_index++;
      }
    }

    initializeAccumulatedWeights();
  }
};
