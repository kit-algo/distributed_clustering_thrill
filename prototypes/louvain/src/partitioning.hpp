#pragma once

#include "graph.hpp"
#include "modularity.hpp"
#include "logging.hpp"

#include <assert.h>
#include <cstdint>
#include <limits>
#include <algorithm>

namespace Partitioning {

using NodeId = typename Graph::NodeId;
using Weight = typename Graph::Weight;
using ClusterId = typename ClusterStore::ClusterId;
using PartitionElementId = uint32_t;

NodeId partitionElementTargetSize(const NodeId node_count, const uint32_t partition_size) {
  return (node_count + partition_size - 1) / partition_size;
}

NodeId partitionElementTargetSize(const Graph& graph, const uint32_t partition_size) {
  return partitionElementTargetSize(graph.getNodeCount(), partition_size);
}

Logging::Id deterministicGreedyWithLinearPenalty(const Graph& graph, const uint32_t partition_size, std::vector<PartitionElementId>& node_partition_elements, bool shuffled = false) {
  assert(graph.getNodeCount() == node_partition_elements.size());
  NodeId partition_target_size = partitionElementTargetSize(graph, partition_size);

  std::vector<NodeId> partition_sizes(partition_size, 0);
  std::vector<NodeId> neighbor_partition_intersection_sizes(partition_size, 0);
  std::vector<PartitionElementId> partition_elements(partition_size);
  std::iota(partition_elements.begin(), partition_elements.end(), 0);

  std::vector<NodeId> node_ids(graph.getNodeCount());
  std::iota(node_ids.begin(), node_ids.end(), 0);
  if (shuffled) {
    std::shuffle(node_ids.begin(), node_ids.end(), Modularity::rng);
  }

  for (NodeId node : node_ids) {
    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight) {
      if (neighbor < node) {
        neighbor_partition_intersection_sizes[node_partition_elements[neighbor]]++;
      }
    });

    std::shuffle(partition_elements.begin(), partition_elements.end(), Modularity::rng);
    double best_partition_value = -std::numeric_limits<double>::max();
    PartitionElementId best_partition = partition_size;
    for (PartitionElementId partition : partition_elements) {
      double value = (1. - (double(partition_sizes[partition]) / partition_target_size)) * neighbor_partition_intersection_sizes[partition];

      if (value > best_partition_value) {
        best_partition_value = value;
        best_partition = partition;
      }

      neighbor_partition_intersection_sizes[partition] = 0;
    }

    node_partition_elements[node] = best_partition;
    partition_sizes[best_partition]++;
  }

  Logging::Id partition_logging_id = Logging::getUnusedId();
  Logging::report("partition", partition_logging_id, "algorithm", shuffled ? "random_order_deterministic_greedy_with_linear_penalty" : "deterministic_greedy_with_linear_penalty");
  return partition_logging_id;
}

Logging::Id chunk(const Graph& graph, const uint32_t partition_size, std::vector<PartitionElementId>& node_partition_elements) {
  assert(graph.getNodeCount() == node_partition_elements.size());
  NodeId partition_target_size = partitionElementTargetSize(graph, partition_size);

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    node_partition_elements[node] = node / partition_target_size;
  }

  Logging::Id partition_logging_id = Logging::getUnusedId();
  Logging::report("partition", partition_logging_id, "algorithm", "chunk");
  return partition_logging_id;
}

void chunkIdsInOrder(const Graph& graph, const uint32_t partition_size, std::vector<PartitionElementId>& node_partition_elements, std::vector<NodeId>& ordered_node_ids) {
  assert(graph.getNodeCount() == node_partition_elements.size());
  assert(graph.getNodeCount() == ordered_node_ids.size());

  NodeId partition_target_size = partitionElementTargetSize(graph, partition_size);

  for (NodeId i = 0; i < graph.getNodeCount(); i++) {
    node_partition_elements[ordered_node_ids[i]] = i / partition_target_size;
  }
}

Logging::Id random(const Graph& graph, const uint32_t partition_size, std::vector<PartitionElementId>& node_partition_elements) {
  std::vector<NodeId> node_ids(graph.getNodeCount());
  std::iota(node_ids.begin(), node_ids.end(), 0);
  std::shuffle(node_ids.begin(), node_ids.end(), Modularity::rng);
  chunkIdsInOrder(graph, partition_size, node_partition_elements, node_ids);

  Logging::Id partition_logging_id = Logging::getUnusedId();
  Logging::report("partition", partition_logging_id, "algorithm", "random");
  return partition_logging_id;
}

void distributeClusters(const NodeId node_count, const std::vector<uint32_t>& cluster_sizes, const uint32_t partition_size, std::vector<uint32_t>& cluster_partition_element) {
  std::vector<NodeId> partition_element_sizes(partition_size, 0);

  std::vector<std::pair<ClusterId, uint32_t>> sorted_cluster_sizes(cluster_sizes.size());
  for (uint32_t i = 0; i < cluster_sizes.size(); i++) {
    sorted_cluster_sizes[i] = std::make_pair(i, cluster_sizes[i]);
  }
  std::sort(sorted_cluster_sizes.begin(), sorted_cluster_sizes.end(), [](const std::pair<ClusterId, uint32_t>& a, const std::pair<ClusterId, uint32_t>& b) {
    return a.second > b.second;
  });

  NodeId partition_target_size = partitionElementTargetSize(node_count, partition_size);

  for (const auto& cluster_size : sorted_cluster_sizes) {
    uint32_t best_partition = partition_size;
    if (cluster_size.second != 0) {
      for (uint32_t partition = 0; partition < partition_size; partition++) {
        if (best_partition != partition_size) {
          if ((partition_element_sizes[best_partition] + cluster_size.second > partition_target_size && partition_element_sizes[partition] < partition_element_sizes[best_partition])
              || (partition_element_sizes[partition] + cluster_size.second <= partition_target_size && partition_element_sizes[partition] > partition_element_sizes[best_partition])) {
            best_partition = partition;
          }
        } else {
          best_partition = partition;
        }
      }

      cluster_partition_element[cluster_size.first] = best_partition;
      partition_element_sizes[best_partition] += cluster_size.second;
    }
  }
}

Logging::Id clusteringBased(const Graph& graph, const uint32_t partition_size, std::vector<uint32_t>& node_partition_elements, const ClusterStore& clusters) {
  std::vector<uint32_t> cluster_sizes(clusters.idRangeUpperBound(), 0);
  clusters.clusterSizes(cluster_sizes);
  std::vector<uint32_t> cluster_partition_element(clusters.idRangeUpperBound(), partition_size);

  distributeClusters(graph.getNodeCount(), cluster_sizes, partition_size, cluster_partition_element);

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    node_partition_elements[node] = cluster_partition_element[clusters[node]];
  }

  Logging::Id partition_logging_id = Logging::getUnusedId();
  Logging::report("partition", partition_logging_id, "algorithm", "cluster_based");
  return partition_logging_id;
}

template<class LoggingId>
std::vector<Logging::Id> analyse(const Graph& graph, const std::vector<uint32_t>& node_partition_elements, const LoggingId partition_logging_id) {
  const uint32_t partition_size = *std::max_element(node_partition_elements.begin(), node_partition_elements.end()) + 1;
  std::vector<Logging::Id> partition_element_logging_ids(partition_size);
  // CUT SIZE
  Weight cut_weight = 0;
  graph.forEachEdge([&](NodeId from, NodeId to, Weight weight) {
    if (node_partition_elements[from] != node_partition_elements[to]) {
      cut_weight += weight;
    }
  });
  cut_weight /= 2;
  Logging::report("partition", partition_logging_id, "cut_weight", cut_weight);
  Logging::report("partition", partition_logging_id, "size", partition_size);

  // CONNECTED COMPONENT
  std::vector<std::vector<uint32_t>> connect_component_sizes(partition_size);
  std::vector<bool> reached_nodes(graph.getNodeCount(), false);

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    if (!reached_nodes[node]) {
      reached_nodes[node] = true;
      uint32_t component_size = 1;

      std::vector<NodeId> queue;
      queue.push_back(node);
      while (!queue.empty()) {
        NodeId current_node = queue.back();
        queue.pop_back();
        graph.forEachAdjacentNode(current_node, [&](const NodeId neighbor, Weight) {
          if (node_partition_elements[neighbor] == node_partition_elements[node] && !reached_nodes[neighbor]) {
            component_size += 1; // TODO bug?
            reached_nodes[neighbor] = true;
            queue.push_back(neighbor);
          }
        });
      }

      connect_component_sizes[node_partition_elements[node]].push_back(component_size);
    }
  }

  // GHOST COUNT
  std::vector<uint32_t> ghost_vertex_counts(partition_size, 0);
  std::vector<bool> reachable_from_partition(partition_size, false);

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight) {
      if (node_partition_elements[neighbor] != node_partition_elements[node] && !reachable_from_partition[node_partition_elements[neighbor]]) {
        reachable_from_partition[node_partition_elements[neighbor]] = true;
        ghost_vertex_counts[node_partition_elements[neighbor]] += 1;
      }
    });
    reachable_from_partition.assign(reachable_from_partition.size(), false);
  }

  // COUNT
  std::vector<uint32_t> node_counts(partition_size, 0);
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    node_counts[node_partition_elements[node]]++;
  }

  // LOG
  for (PartitionElementId partition_element = 0; partition_element < partition_size; partition_element++) {
    Logging::Id element_logging_id = Logging::getUnusedId();
    partition_element_logging_ids[partition_element] = element_logging_id;
    Logging::report("partition_element", element_logging_id, "partition_id", partition_logging_id);
    Logging::report("partition_element", element_logging_id, "node_count", node_counts[partition_element]);
    Logging::report("partition_element", element_logging_id, "ghost_count", ghost_vertex_counts[partition_element]);
    Logging::report("partition_element", element_logging_id, "connected_components", connect_component_sizes[partition_element]);
  }

  return partition_element_logging_ids;
}

}
