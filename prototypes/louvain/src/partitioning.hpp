#pragma once

#include "graph.hpp"

#include <assert.h>
#include <cstdint>
#include <limits>

namespace Partitioning {

using NodeId = typename Graph::NodeId;
using Weight = typename Graph::Weight;

void deterministic_greedy_with_linear_penalty(const Graph& graph, uint32_t partition_count, std::vector<uint32_t>& partitions) {
  assert(graph.getNodeCount() == partitions.size());
  NodeId partition_target_size = (graph.getNodeCount() + partition_count - 1) / partition_count;

  std::vector<NodeId> partition_sizes(partition_count, 0);
  std::vector<NodeId> neighbor_partition_intersection_sizes(partition_count, 0);

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight) {
      if (neighbor < node) {
        neighbor_partition_intersection_sizes[partitions[neighbor]]++;
      }
    });

    double best_partition_value = -std::numeric_limits<double>::max();
    uint32_t best_partition = partition_count;
    for (uint32_t partition = 0; partition < partition_count; partition++) {
      double value = (1. - (double(partition_sizes[partition]) / partition_target_size)) * neighbor_partition_intersection_sizes[partition];

      if (value > best_partition_value) {
        best_partition_value = value;
        best_partition = partition;
      }

      neighbor_partition_intersection_sizes[partition] = 0;
    }

    partitions[node] = best_partition;
    partition_sizes[best_partition]++;
  }
}

void chunk(const Graph& graph, uint32_t partition_count, std::vector<uint32_t>& partitions) {
  assert(graph.getNodeCount() == partitions.size());
  NodeId partition_target_size = (graph.getNodeCount() + partition_count - 1) / partition_count;

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    partitions[node] = node / partition_target_size;
  }
}

}
