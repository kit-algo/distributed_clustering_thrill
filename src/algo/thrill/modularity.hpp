#pragma once

#include <thrill/api/inner_join.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/all_reduce.hpp>
#include <thrill/api/collapse.hpp>

#include <cstdint>

#include "data/thrill/graph.hpp"

namespace Modularity {

using NodeCluster = std::pair<NodeId, ClusterId>;
using int128_t = __int128_t;

template<typename NodeType, typename ClusterDIA>
double Modularity(const DiaNodeGraph<NodeType>& graph, const ClusterDIA& clusters) {
  auto inside_and_total_weights = graph.nodes
    .Zip(clusters,
      [](const NodeType& node, const NodeCluster& node_cluster) {
        assert(node_cluster.first == node.id);
        return std::make_pair(node, node_cluster.second);
      })
    .template FoldByKey<std::vector<NodeType>>(thrill::NoDuplicateDetectionTag,
      [](const std::pair<NodeType, ClusterId>& node_cluster) { return node_cluster.second; },
      [](std::vector<NodeType>&& acc, const std::pair<NodeType, ClusterId>& node_cluster) {
        acc.push_back(node_cluster.first);
        return std::move(acc);
      })
    .Map(
      [](const std::pair<ClusterId, std::vector<NodeType>>& cluster_nodes) {
        spp::sparse_hash_set<NodeId> cluster_node_ids;
        cluster_node_ids.reserve(cluster_nodes.second.size());
        Weight total_weight = 0;
        Weight inner_weight = 0;
        for (const NodeType& node : cluster_nodes.second) {
          total_weight += node.weightedDegree();
          cluster_node_ids.insert(node.id);
        }
        for (const NodeType& node : cluster_nodes.second) {
          for (const typename NodeType::LinkType& link : node.links) {
            if (cluster_node_ids.find(link.target) != cluster_node_ids.end()) {
              inner_weight += link.getWeight();
            }
          }
        }

        return std::make_pair(inner_weight, int128_t(total_weight) * int128_t(total_weight));
      })
    .AllReduce(
      [](const std::pair<uint64_t, int128_t>& weights1, const std::pair<uint64_t, int128_t>& weights2) {
        return std::make_pair(weights1.first + weights2.first, weights1.second + weights2.second);
      });

  return (inside_and_total_weights.first / (2.* graph.total_weight)) - (inside_and_total_weights.second / (4.*graph.total_weight*graph.total_weight));
}

} // Modularity
