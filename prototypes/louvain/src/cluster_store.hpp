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
    id_range_upper_bound(node_count) {
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

  void assignSingletonClusterIds() {
    for (NodeId id = 0; id < size(); id++) {
      node_clusters[id] = id;
    }
    resetBounds();
  }

  void resetBounds() {
    id_range_lower_bound = 0;
    id_range_upper_bound = size();
  }

  ClusterId rewriteClusterIds(std::vector<NodeId>& nodes, ClusterId id_counter = 0) {
    std::map<NodeId, ClusterId> old_to_new;

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
    std::map<NodeId, ClusterId> old_to_new;

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
      intersection.node_clusters[node] = node_clusters[node] * other.id_range_upper_bound + other.node_clusters[node];
    }

    intersection.rewriteClusterIds();
  }

  void generateConsecutiveClusterNodeIdPermutation(std::vector<NodeId> & permutation) {
    assert(permutation.size() == size());
    std::vector<std::pair<ClusterId, NodeId>> cluster_and_node_list(size());
    for (NodeId node = 0; node < size(); node++) {
      cluster_and_node_list[node].first = node_clusters[node];
      cluster_and_node_list[node].second = node;
    }

    std::sort(cluster_and_node_list.begin(), cluster_and_node_list.end(), [](const std::pair<ClusterId, NodeId> & cn1, const std::pair<ClusterId, NodeId> & cn2) {
      return (cn1.first == cn2.first && cn1.second < cn2.second) || cn1.first < cn2.first;
    });

    for (NodeId new_id = 0; new_id < size(); new_id++) {
      permutation[cluster_and_node_list[new_id].second] = new_id;
    }
  }

  void clusterSizeDistribution(std::map<uint32_t, uint32_t> & distribution) {
    std::vector<uint32_t> cluster_sizes(size(), 0);
    for (auto& cluster_id : node_clusters) {
      cluster_sizes[cluster_id]++;
    }

    for (auto& cluster_size : cluster_sizes) {
      if (cluster_size != 0) {
        distribution[cluster_size]++;
      }
    }
  }
};
