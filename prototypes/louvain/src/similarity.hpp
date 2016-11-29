#pragma once

#include "cluster_store.hpp"
#include "graph.hpp"

#include <vector>
#include <unordered_map>
#include <assert.h>
#include <cstdint>

namespace Similarity {

using NodeId = typename Graph::NodeId;
using ClusterId = typename ClusterStore::ClusterId;

double adjustedRandIndex(const ClusterStore &c, const ClusterStore &d) {
  NodeId node_count = c.size();
  ClusterStore intersection(0, node_count);
  c.intersection(d, intersection);

  // TODO reduce sizes to actual number of clusters
  std::vector<uint32_t> c_cluster_sizes(node_count, 0);
  std::vector<uint32_t> d_cluster_sizes(node_count, 0);
  std::vector<uint32_t> intersection_cluster_sizes(node_count, 0);

  // precompute sizes for each cluster
  for (NodeId node = 0; node < node_count; node++) {
    c_cluster_sizes[c[node]]++;
    d_cluster_sizes[d[node]]++;
    intersection_cluster_sizes[intersection[node]]++;
  }

  uint32_t rand_index = 0;
  for (uint32_t s : intersection_cluster_sizes) {
    rand_index += s * (s - 1) / 2;
  }

  uint32_t sum_c = 0;
  for (uint32_t s : c_cluster_sizes) {
    sum_c += s * (s - 1) / 2;
  }

  uint32_t sum_d = 0;
  for (uint32_t s : d_cluster_sizes) {
    sum_d += s * (s - 1) / 2;
  }

  double maxIndex = 0.5 * (sum_c + sum_d);

  double expectedIndex = sum_c * sum_d / (node_count * (node_count-1) / 2);

  if (maxIndex == 0) { // both clusterings are singleton clusterings
    return 1.0;
  } else if (maxIndex == expectedIndex) { // both partitions contain one cluster the whole graph
    return 1.0;
  } else {
    return (rand_index - expectedIndex) / (maxIndex - expectedIndex);
  }
}

};
