#pragma once

#include "graph.hpp"

#include <vector>
#include <routingkit/bit_vector.h>
#include <routingkit/id_mapper.h>
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

    if (cluster_id >= id_range_upper_bound) {
      id_range_upper_bound = cluster_id + 1;
    }

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
    RoutingKit::BitVector vector(idRangeUpperBound());

    for (NodeId node : nodes) {
      vector.set(node_clusters[node]);
    }
    RoutingKit::LocalIDMapper id_mapper(vector);
    for (NodeId node : nodes) {
      node_clusters[node] = id_counter + id_mapper.to_local(node_clusters[node]);
    }

    return id_counter + id_mapper.local_id_count();
  }

  ClusterId rewriteClusterIds(ClusterId id_counter = 0) {
    RoutingKit::BitVector vector(idRangeUpperBound());

    for (ClusterId cluster_id : node_clusters) {
      vector.set(cluster_id);
    }
    RoutingKit::LocalIDMapper id_mapper(vector);
    for (ClusterId& cluster_id : node_clusters) {
      cluster_id = id_counter + id_mapper.to_local(cluster_id);
    }

    id_range_upper_bound = id_counter + id_mapper.local_id_count();
    return id_counter + id_mapper.local_id_count();
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
