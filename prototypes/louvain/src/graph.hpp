#pragma once

#include <algorithm>
#include <numeric>
#include <tuple>
#include <vector>
#include <iostream>
#include <assert.h>

class Graph {
private:

  int node_count;
  int edge_count;
  std::vector<int> edge_indexes;
  std::vector<int> degrees;
  std::vector<int> neighbors;
  std::vector<int> weights;
  int total_weight;

  void initializeAccumulatedWeights() {
    assert(std::accumulate(weights.begin(), weights.end(), 0) % 2 == 0);
    for (int node = 0; node < node_count; node++) {
      degrees[node] = std::accumulate(weights.begin() + edge_indexes[node], weights.begin() + edge_indexes[node + 1], 0);
    }
    total_weight = std::accumulate(degrees.begin(), degrees.end(), 0) / 2;
  }

public:
  Graph(const int node_count, const int edge_count) : node_count(node_count), edge_count(edge_count),
    edge_indexes(node_count + 1, 2 * edge_count), degrees(node_count, 0),
    neighbors(2 * edge_count), weights(2 * edge_count) {}

  int getNodeCount() const { return node_count; }
  int getEdgeCount() const { return edge_count; }
  int getTotalWeight() const { return total_weight; }

  int weightedNodeDegree(const int node_id) const {
    return degrees[node_id];
  }

  template<class F>
  void forEachAdjacentNode(int node, F f) const {
    for (int edge_index = edge_indexes[node]; edge_index < edge_indexes[node + 1]; edge_index++) {
      f(neighbors[edge_index], weights[edge_index]);
    }
  }

  void setEdgesWithMissingBackwardArcs(std::vector<std::tuple<int, int, int>> &edges) {
    std::vector<std::tuple<int, int, int>> foo(2 * edge_count);
    for (int i = 0; i < edge_count; i++) {
      foo[i] = edges[i];
    }
    for (int i = 0; i < edge_count; i++) {
      foo[edge_count + i] = std::make_tuple(std::get<1>(edges[i]), std::get<0>(edges[i]), std::get<2>(edges[i]));
    }

    std::sort(foo.begin(), foo.end(), [](const auto& edge1, const auto& edge2) {
      return std::get<0>(edge1) < std::get<0>(edge2);
    });

    int current_node = -1;
    for (int i = 0; i < 2 * edge_count; i++) {
      int tail = std::get<0>(foo[i]);
      while (current_node < tail) {
        edge_indexes[++current_node] = i;
      }
      neighbors[i] = std::get<1>(foo[i]);
      weights[i] = std::get<2>(foo[i]);
      assert(std::get<2>(foo[i]) > 0);
    }

    initializeAccumulatedWeights();
  }

  void setEdgesByAdjacencyLists(std::vector<std::vector<int>> &neighbors) {
    int current_edge_index = 0;
    for (int node = 0; node < node_count; node++) {
      edge_indexes[node] = current_edge_index;
      for (int& neighbor : neighbors[node]) {
        this->neighbors[current_edge_index] = neighbor;
        weights[current_edge_index] = 1;
        current_edge_index++;
      }
    }

    initializeAccumulatedWeights();
  }

  double modularity(std::vector<int> const &clusters) const {
    std::vector<int> inner_weights(clusters.size(), 0);
    std::vector<long> incident_weights(clusters.size(), 0);

    for (int node = 0; node < node_count; node++) {
      incident_weights[clusters[node]] += weightedNodeDegree(node);
      forEachAdjacentNode(node, [&](int neighbor, int weight) {
        if (clusters[neighbor] == clusters[node]) {
          inner_weights[clusters[node]] += weight;
        }
      });
    }

    int inner_sum = std::accumulate(inner_weights.begin(), inner_weights.end(), 0);
    long incident_sum = std::accumulate(incident_weights.begin(), incident_weights.end(), 0l, [](const long& agg, const long& elem) {
      assert(elem >= 0);
      assert(agg >= 0);
      return agg + (elem * elem);
    });
    // std::cout << "inner " << inner_sum << " incident " << incident_sum << "\n";

    return (inner_sum / (2.*total_weight)) - (incident_sum / (4.*total_weight*total_weight));
  }
};
