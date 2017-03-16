#pragma once

#include <thrill/api/inner_join.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/all_reduce.hpp>

#include <cstdint>

#include "thrill_graph.hpp"

namespace Modularity {

using NodeCluster = std::pair<NodeId, ClusterId>;
using int128_t = __int128_t;

template<typename Graph>
double modularity(const Graph& graph, const thrill::DIA<NodeCluster>& clusters) {
  auto cluster_degrees_and_inside_weights = graph.edges
    .InnerJoin(clusters,
      [](const typename Graph::Edge& edge) { return edge.tail; },
      [](const NodeCluster& node_cluster) { return node_cluster.first; },
      [](typename Graph::Edge edge, const NodeCluster& node_cluster) {
        edge.tail = node_cluster.second;
        return edge;
      })
    .InnerJoin(clusters,
      [](const typename Graph::Edge& edge) { return edge.head; },
      [](const NodeCluster& node_cluster) { return node_cluster.first; },
      [](typename Graph::Edge edge, const NodeCluster& node_cluster) {
        return std::make_pair(edge.tail, std::pair<uint64_t, uint64_t>(edge.getWeight(), edge.tail == node_cluster.second ? edge.getWeight() : 0));
      })
    .ReducePair(
      [](const std::pair<uint64_t, uint64_t>& weights1, const std::pair<uint64_t, uint64_t>& weights2) {
        return std::make_pair(weights1.first + weights2.first, weights1.second + weights2.second);
      })
    .Map(
      [](const std::pair<ClusterId, std::pair<uint64_t, uint64_t>>& weights) {
        return std::make_pair(int128_t(weights.second.first) * int128_t(weights.second.first), weights.second.second);
      })
    .AllReduce(
      [](const std::pair<int128_t, uint64_t>& weights1, const std::pair<int128_t, uint64_t>& weights2) {
        return std::make_pair(weights1.first + weights2.first, weights1.second + weights2.second);
      });

  return (cluster_degrees_and_inside_weights.second / (2.* graph.total_weight)) - (cluster_degrees_and_inside_weights.first / (4.*graph.total_weight*graph.total_weight));
}

} // Modularity
