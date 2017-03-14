#pragma once

#include <algorithm>
#include <numeric>
#include <tuple>
#include <vector>
#include <iostream>
#include <assert.h>
#include <map>
#include <unordered_map>
#include <cstdint>

#include "../thrill_graph.hpp"

template<bool weighted = false>
class GhostGraph {

private:

  std::vector<size_t> first_out;
  std::vector<Weight> degrees;
  std::vector<NodeId> neighbors;
  std::vector<Weight> weights;
  Weight total_weight;

public:
  using EdgeId = size_t;

  GhostGraph(const NodeId node_count_approx, const Weight total_weight) :
    total_weight(total_weight) {
    first_out.reserve(node_count_approx + 1);
    first_out.push_back(0);
    if (weighted) {
      degrees.reserve(node_count_approx);
    }
  }

  template<class F>
  std::vector<NodeId> initialize(const F& f) {
    std::unordered_map<NodeId, NodeId> id_mapping;
    std::map<NodeId, Weight> ghost_node_degrees;
    NodeId id_counter = 0;
    size_t edge_counter = 0;

    std::vector<NodeId> reverse_mapping;
    reverse_mapping.reserve(first_out.capacity());

    f([this, &id_mapping, &id_counter, &ghost_node_degrees, &reverse_mapping, &edge_counter](const auto& node) {
      reverse_mapping.push_back(node.id);
      id_mapping[node.id] = id_counter++;

      first_out.push_back(first_out.back() + node.links.size());
      if (weighted) {
        degrees.push_back(node.weightedDegree());
      }

      neighbors.reserve(edge_counter * first_out.capacity() / first_out.size());
      if (weighted) {
        weights.reserve(edge_counter * first_out.capacity() / first_out.size());
      }
      for (const auto& link : node.links) {
        neighbors.push_back(link.target);
        if (weighted) {
          weights.push_back(link.getWeight());
        }
        edge_counter++;

        if (id_mapping.find(link.target) != id_mapping.end()) {
          ghost_node_degrees[link.target] = link.target_degree;
        }
      }
    });

    for (NodeId& neighbor : neighbors) {
      if (id_mapping.find(neighbor) != id_mapping.end()) {
        neighbor = id_mapping[neighbor];
      } else {
        id_mapping[neighbor] = id_counter;
        neighbor = id_counter++;
      }
    }

    degrees.resize(weighted ? id_counter : id_counter - getNodeCount());

    for (const auto& pair : ghost_node_degrees) {
      if (id_mapping[pair.first] >= getNodeCount()) {
        degrees[id_mapping[pair.first] - (weighted ? 0 : getNodeCount())] = pair.second;
      }
    }

    return reverse_mapping;
  }

  NodeId getNodeCount() const { return first_out.size() - 1; }
  NodeId getNodeCountIncludingGhost() const {
    if (weighted) {
      return degrees.size();
    } else {
      return getNodeCount() + degrees.size();
    }
  }
  size_t getEdgeCount() const { return neighbors.size() / 2; }
  Weight getTotalWeight() const { return total_weight; }

  void overrideTotalWeight(const Weight weight) { total_weight = weight; }

  Weight nodeDegree(const NodeId node_id) const {
    if (weighted) {
      return degrees[node_id];
    } else {
      if (node_id < getNodeCount()) {
        return first_out[node_id + 1] - first_out[node_id];
      } else {
        return degrees[node_id - getNodeCount()];
      }
    }
  }

  template<class F>
  void forEachAdjacentNode(NodeId node, F f) const {
    if (weighted) {
      for (size_t edge_index = first_out[node]; edge_index < first_out[node + 1]; edge_index++) {
        f(neighbors[edge_index], weights[edge_index]);
      }
    } else {
      for (size_t edge_index = first_out[node]; edge_index < first_out[node + 1]; edge_index++) {
        f(neighbors[edge_index], 1);
      }
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

private:
};
