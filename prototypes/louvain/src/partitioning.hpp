#pragma once

#include "graph.hpp"
#include "modularity.hpp"
#include "logging.hpp"

#include <assert.h>
#include <cstdint>
#include <limits>

namespace Partitioning {

using NodeId = typename Graph::NodeId;
using Weight = typename Graph::Weight;
using PartitionElementId = uint32_t;

NodeId partitionElementTargetSize(const Graph& graph, const uint32_t partition_element_count) {
  return (graph.getNodeCount() + partition_element_count - 1) / partition_element_count;
}

Logging::Id deterministicGreedyWithLinearPenalty(const Graph& graph, const uint32_t partition_element_count, std::vector<PartitionElementId>& node_partition_elements, bool shuffled = false) {
  assert(graph.getNodeCount() == node_partition_elements.size());
  NodeId partition_target_size = partitionElementTargetSize(graph, partition_element_count);

  std::vector<NodeId> partition_sizes(partition_element_count, 0);
  std::vector<NodeId> neighbor_partition_intersection_sizes(partition_element_count, 0);

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

    double best_partition_value = -std::numeric_limits<double>::max();
    PartitionElementId best_partition = partition_element_count;
    for (PartitionElementId partition = 0; partition < partition_element_count; partition++) {
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
  Logging::report("partition", partition_logging_id, "element_count", partition_element_count);
  return partition_logging_id;
}

Logging::Id chunk(const Graph& graph, const uint32_t partition_element_count, std::vector<PartitionElementId>& node_partition_elements) {
  assert(graph.getNodeCount() == node_partition_elements.size());
  NodeId partition_target_size = partitionElementTargetSize(graph, partition_element_count);

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    node_partition_elements[node] = node / partition_target_size;
  }

  Logging::Id partition_logging_id = Logging::getUnusedId();
  Logging::report("partition", partition_logging_id, "algorithm", "chunk");
  Logging::report("partition", partition_logging_id, "element_count", partition_element_count);
  return partition_logging_id;
}

void chunkIdsInOrder(const Graph& graph, const uint32_t partition_element_count, std::vector<PartitionElementId>& node_partition_elements, std::vector<NodeId>& ordered_node_ids) {
  assert(graph.getNodeCount() == node_partition_elements.size());
  assert(graph.getNodeCount() == ordered_node_ids.size());

  NodeId partition_target_size = partitionElementTargetSize(graph, partition_element_count);

  for (NodeId node : ordered_node_ids) {
    node_partition_elements[node] = node / partition_target_size;
  }
}

Logging::Id random(const Graph& graph, const uint32_t partition_element_count, std::vector<PartitionElementId>& node_partition_elements) {
  std::vector<NodeId> node_ids(graph.getNodeCount());
  std::iota(node_ids.begin(), node_ids.end(), 0);
  std::shuffle(node_ids.begin(), node_ids.end(), Modularity::rng);
  chunkIdsInOrder(graph, partition_element_count, node_partition_elements, node_ids);

  Logging::Id partition_logging_id = Logging::getUnusedId();
  Logging::report("partition", partition_logging_id, "algorithm", "random");
  Logging::report("partition", partition_logging_id, "element_count", partition_element_count);
  return partition_logging_id;
}

Logging::Id clusteringBased(const Graph& graph, const uint32_t partition_element_count, std::vector<uint32_t>& node_partition_elements, const ClusterStore& clusters) {
  assert(graph.getNodeCount() == node_partition_elements.size());
  std::vector<NodeId> node_ids(graph.getNodeCount());
  std::iota(node_ids.begin(), node_ids.end(), 0);

  std::sort(node_ids.begin(), node_ids.end(), [&clusters](const NodeId node1, const NodeId node2) {
    return (clusters[node1] == clusters[node2] && node1 < node2) || clusters[node1] < clusters[node2];
  });

  chunkIdsInOrder(graph, partition_element_count, node_partition_elements, node_ids);

  Logging::Id partition_logging_id = Logging::getUnusedId();
  Logging::report("partition", partition_logging_id, "algorithm", "cluster_based");
  Logging::report("partition", partition_logging_id, "element_count", partition_element_count);
  return partition_logging_id;
}

}
