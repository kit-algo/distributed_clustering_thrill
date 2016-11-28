#pragma once

#include "graph.hpp"

#include <vector>
#include <unordered_map>
#include <assert.h>

using NodeId = typename Graph::NodeId;

class ClusterStore {

public:

  typedef NodeId ClusterId;

private:

  Graph::NodeId offset;
  std::vector<ClusterId> node_clusters;

public:

  ClusterStore(const NodeId node_range_lower_bound, const NodeId node_range_upper_bound, const ClusterId initial_cluster_id = 0) :
    offset(node_range_lower_bound), node_clusters(node_range_upper_bound - node_range_lower_bound, initial_cluster_id) {}

  inline void set(const NodeId node, const ClusterId cluster_id) {
    node_clusters[node - offset] = cluster_id;
  }

  inline ClusterId operator[](const NodeId node) const {
    if (node >= offset && node < offset + node_clusters.size()) {
      return node_clusters[node - offset];
    }
    return node;
  }

  ClusterId size() const {
    return node_clusters.size();
  }

  void assignSingletonClusterIds() {
    for (NodeId id = offset; id < offset + node_clusters.size(); id++) {
      node_clusters[id - offset] = id;
    }
  }

  ClusterId rewriteClusterIds() {
    ClusterId id_counter = offset;
    std::unordered_map<ClusterId, ClusterId> old_to_new;

    for (auto& cluster_id : node_clusters) {
      if (old_to_new.find(cluster_id) == old_to_new.end()) {
        old_to_new[cluster_id] = id_counter++;
      }
      cluster_id = old_to_new[cluster_id];
    }

    return id_counter;
  }
};
