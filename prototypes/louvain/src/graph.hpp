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
  bool weighted;
  std::vector<EdgeId> first_out;
  std::vector<Weight> degrees;
  std::vector<NodeId> neighbors;
  std::vector<Weight> weights;
  Weight total_weight;

public:
  Graph(const NodeId node_count, const EdgeId edge_count, bool weighted = false) :
    node_count(node_count), edge_count(edge_count), weighted(weighted),
    first_out(node_count + 1, 2 * edge_count), degrees(weighted ? node_count : 0, 0),
    neighbors(2 * edge_count), weights(weighted ? 2 * edge_count : 0) {}

  NodeId getNodeCount() const { return node_count; }
  EdgeId getEdgeCount() const { return edge_count; }
  Weight getTotalWeight() const { return total_weight; }

  void overrideTotalWeight(const Weight weight) { total_weight = weight; }

  Weight nodeDegree(const NodeId node_id) const {
    if (weighted) {
      return degrees[node_id];
    } else {
      return first_out[node_id + 1] - first_out[node_id];
    }
  }

  template<class F>
  void forEachAdjacentNode(NodeId node, F f) const {
    if (weighted) {
      for (EdgeId edge_index = first_out[node]; edge_index < first_out[node + 1]; edge_index++) {
        f(neighbors[edge_index], weights[edge_index]);
      }
    } else {
      for (EdgeId edge_index = first_out[node]; edge_index < first_out[node + 1]; edge_index++) {
        f(neighbors[edge_index], 1);
      }
    }
  }

  void setEdgesByAdjacencyMatrix(const std::vector<std::map<NodeId, Weight>> &adjacency_weights) {
    EdgeId current_edge_index = 0;
    NodeId current_node = 0;
    for (NodeId tail = 0; tail < node_count; tail++) {
      while (current_node <= tail) {
        first_out[current_node++] = current_edge_index;
      }

      for (const auto &edge : adjacency_weights[tail]) {
        neighbors[current_edge_index] = edge.first;
        if (weighted) {
          weights[current_edge_index] = edge.second;
        }
        assert(edge.second > 0);
        current_edge_index++;

        if (tail == edge.first) {
          neighbors[current_edge_index] = edge.first;
          if (weighted) {
            weights[current_edge_index - 1] /= 2;
            weights[current_edge_index] = edge.second / 2;
          }
          current_edge_index++;
        }
      }
    }

    first_out[node_count] = current_edge_index;
    neighbors.resize(current_edge_index);
    weights.resize(current_edge_index);
    // TODO Update Edge Count?
    initializeAccumulatedWeights();
  }

  void setEdgesByAdjacencyLists(std::vector<std::vector<NodeId>> &neighbors) {
    assert(!weighted);
    EdgeId current_edge_index = 0;
    for (NodeId node = 0; node < node_count; node++) {
      first_out[node] = current_edge_index;
      for (NodeId& neighbor : neighbors[node]) {
        this->neighbors[current_edge_index] = neighbor;
        current_edge_index++;
      }
    }

    initializeAccumulatedWeights();
  }

  void applyNodePermutation(const std::vector<NodeId> &permutation) {
    assert(permutation.size() == node_count);
    std::vector<NodeId> reversed_permutation(node_count);
    for (NodeId node = 0; node < node_count; node++) {
      reversed_permutation[permutation[node]] = node;
    }

    std::vector<EdgeId> new_first_out(first_out.size(), 2 * edge_count);
    std::vector<NodeId> new_neighbors(neighbors.size());
    std::vector<Weight> new_weights(weighted ? weights.size() : 0);

    EdgeId current_first_out_index = 0;
    for (NodeId node = 0; node < node_count; node++) {
      NodeId old_id = reversed_permutation[node];
      new_first_out[node] = current_first_out_index;

      for (EdgeId old_edge_index = first_out[old_id]; old_edge_index < first_out[old_id + 1]; old_edge_index++) {
        new_neighbors[current_first_out_index] = permutation[neighbors[old_edge_index]];
        if (weighted) {
          new_weights[current_first_out_index] = weights[neighbors[old_edge_index]];
        }
        current_first_out_index++;
      }
      // TODO Sorting?
    }

    first_out.assign(new_first_out.begin(), new_first_out.end());
    neighbors.assign(new_neighbors.begin(), new_neighbors.end());
    weights.assign(new_weights.begin(), new_weights.end());

    initializeAccumulatedWeights();
  }

  void fixIdOrder() {
    std::vector<NodeId> permutation(node_count, node_count);
    NodeId current_new_id = 0;
    for (NodeId neighbor : neighbors) {
      if (permutation[neighbor] == node_count) {
        permutation[neighbor] = current_new_id++;
      }
    }

    for (NodeId node = 0; node < node_count; node++) {
      if (permutation[node] == node_count) {
        permutation[node] = current_new_id++;
      }
    }

    assert(current_new_id == node_count);
    applyNodePermutation(permutation);
  }

private:
  void initializeAccumulatedWeights() {
    assert(std::accumulate(weights.begin(), weights.end(), 0) % 2 == 0);
    if (weighted) {
      for (NodeId node = 0; node < node_count; node++) {
        degrees[node] = std::accumulate(weights.begin() + first_out[node], weights.begin() + first_out[node + 1], 0);
      }
      total_weight = std::accumulate(degrees.begin(), degrees.end(), 0) / 2;
    } else {
      total_weight = edge_count;
    }
  }
};
