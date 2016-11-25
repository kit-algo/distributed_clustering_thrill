#pragma once

#include <vector>
#include <unordered_map>
#include <assert.h>

class ClusterStore {
private:

  std::size_t offset;
  std::vector<int> node_clusters;

public:

  ClusterStore(const std::size_t node_range_lower_bound, const std::size_t node_range_upper_bound, const int initial_cluster_id = 0) :
    offset(node_range_lower_bound), node_clusters(node_range_upper_bound - node_range_lower_bound, initial_cluster_id) {}

  inline void set(const std::size_t node, const int cluster_id) {
    node_clusters[node - offset] = cluster_id;
  }

  inline int operator[](const std::size_t node) const {
    if (node >= offset && node < offset + node_clusters.size()) {
      return node_clusters[node - offset];
    }
    return node;
  }

  std::size_t size() const {
    return node_clusters.size();
  }

  void assignSingletonClusterIds() {
    for (std::size_t id = offset; id < offset + node_clusters.size(); id++) {
      node_clusters[id - offset] = id;
    }
  }

  int rewriteClusterIds() {
    std::size_t id_counter = offset;
    std::unordered_map<std::size_t, std::size_t> old_to_new;

    for (auto& cluster_id : node_clusters) {
      if (old_to_new.find(cluster_id) == old_to_new.end()) {
        old_to_new[cluster_id] = id_counter++;
      }
      cluster_id = old_to_new[cluster_id];
    }

    return id_counter;
  }
};
