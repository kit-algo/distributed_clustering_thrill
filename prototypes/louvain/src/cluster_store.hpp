#pragma once

#include "graph.hpp"

#include <vector>
#include <unordered_map>
#include <map>
#include <assert.h>

using NodeId = typename Graph::NodeId;

class ClusterStore {

public:

  typedef NodeId ClusterId;

private:

  std::vector<ClusterId> node_clusters;
  ClusterId id_range_lower_bound;
  ClusterId id_range_upper_bound;

public:

  ClusterStore(const NodeId node_count, const ClusterId initial_cluster_id = 0) :
    node_clusters(node_count, initial_cluster_id),
    id_range_lower_bound(initial_cluster_id),
    id_range_upper_bound(node_count == 0 ? 1 : node_count) {
    assert(initial_cluster_id >= id_range_lower_bound);
    assert(initial_cluster_id < id_range_upper_bound);
  }

  inline void set(const NodeId node, const ClusterId cluster_id) {
    assert(cluster_id >= id_range_lower_bound);
    assert(cluster_id < id_range_upper_bound);
    node_clusters[node] = cluster_id;
  }

  inline ClusterId operator[](const NodeId node) const {
    return node_clusters[node];
  }

  ClusterId size() const {
    return node_clusters.size();
  }

  ClusterId idRangeUpperBound() const { return id_range_upper_bound; }

  void assignSingletonClusterIds() {
    std::iota(node_clusters.begin(), node_clusters.end(), 0);
    resetBounds();
  }

  void resetBounds() {
    id_range_lower_bound = 0;
    id_range_upper_bound = size();
  }

  ClusterId rewriteClusterIds(std::vector<NodeId>& nodes, ClusterId id_counter = 0) {
    std::unordered_map<NodeId, ClusterId> old_to_new;

    for (NodeId node : nodes) {
      ClusterId& cluster_id = node_clusters[node];
      if (old_to_new.find(cluster_id) == old_to_new.end()) {
        old_to_new[cluster_id] = id_counter++;
      }
      ClusterId new_id = old_to_new[cluster_id];
      cluster_id = new_id;
    }

    return id_counter;
  }

  ClusterId rewriteClusterIds(ClusterId id_counter = 0) {
    id_range_lower_bound = id_counter;
    std::unordered_map<NodeId, ClusterId> old_to_new;

    for (ClusterId& cluster_id : node_clusters) {
      if (old_to_new.find(cluster_id) == old_to_new.end()) {
        old_to_new[cluster_id] = id_counter++;
      }
      ClusterId new_id = old_to_new[cluster_id];
      cluster_id = new_id;
    }

    id_range_upper_bound = id_counter;
    return id_counter;
  }

  void intersection(const ClusterStore &other, ClusterStore &intersection) const {
    assert(size() == other.size() && size() == intersection.size());
    intersection.id_range_upper_bound = id_range_upper_bound * other.id_range_upper_bound;

    for (NodeId node = 0; node < size(); node++) {
      // TODO danger of overflows if bounds are high
      intersection.node_clusters[node] = node_clusters[node] * other.id_range_upper_bound + other.node_clusters[node];
    }

    intersection.rewriteClusterIds();
  }

  void clusterSizes(std::vector<uint32_t>& cluster_sizes) const {
    assert(cluster_sizes.size() >= id_range_upper_bound);
    for (auto& cluster_id : node_clusters) {
      cluster_sizes[cluster_id]++;
    }
  }

  void clusterSizeDistribution(std::map<uint32_t, uint32_t> & distribution) const {
    std::vector<uint32_t> cluster_sizes(id_range_upper_bound, 0);
    clusterSizes(cluster_sizes);

    for (auto& cluster_size : cluster_sizes) {
      if (cluster_size != 0) {
        distribution[cluster_size]++;
      }
    }
  }
};
