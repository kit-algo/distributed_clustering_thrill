#pragma once

#include "graph.hpp"
#include "cluster_store.hpp"

#include <map>
#include <algorithm>

#include <iostream>
#include <assert.h>
#include <cmath>
#include <cstdint>

namespace Modularity {

using NodeId = typename Graph::NodeId;
using EdgeId = typename Graph::EdgeId;
using Weight = typename Graph::Weight;
using ClusterId = typename ClusterStore::ClusterId;

double modularity(Graph const &graph, ClusterStore const &clusters) {
  std::vector<Weight> inner_weights(clusters.size(), 0);
  std::vector<Weight> incident_weights(clusters.size(), 0); // TODO assert size

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    incident_weights[clusters[node]] += graph.weightedNodeDegree(node);
    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight weight) {
      if (clusters[neighbor] == clusters[node]) {
        inner_weights[clusters[node]] += weight;
      }
    });
  }

  Weight inner_sum = std::accumulate(inner_weights.begin(), inner_weights.end(), 0);
  uint64_t incident_sum = std::accumulate(incident_weights.begin(), incident_weights.end(), 0l, [](const uint64_t& agg, const uint64_t& elem) {
    assert(elem >= 0);
    assert(agg >= 0);
    return agg + (elem * elem);
  });

  uint64_t total_weight = graph.getTotalWeight();
  return (inner_sum / (2.*total_weight)) - (incident_sum / (4.*total_weight*total_weight));

  static_assert(sizeof(decltype(total_weight)) >= 2 * sizeof(Graph::EdgeId), "Modularity has to be able to captuare a value of maximum number of edges squared");
  static_assert(sizeof(decltype(incident_sum)) >= 2 * sizeof(Graph::EdgeId), "Modularity has to be able to captuare a value of maximum number of edges squared");
}


int64_t deltaModularity(const Graph &graph,
                        const NodeId node,
                        const ClusterId current_cluster,
                        const ClusterId target_cluster,
                        const int64_t weight_between_node_and_current_cluster,
                        const int64_t weight_between_node_and_target_cluster,
                        const std::vector<Weight> &cluster_weights) {

  int64_t target_cluster_incident_edges_weight = cluster_weights[target_cluster];
  if (target_cluster == current_cluster) {
    target_cluster_incident_edges_weight -= graph.weightedNodeDegree(node);
  }

  int64_t current_cluster_incident_edges_weight = cluster_weights[current_cluster] - graph.weightedNodeDegree(node);

  int64_t e = (graph.getTotalWeight() * 2 * (weight_between_node_and_target_cluster - weight_between_node_and_current_cluster));
  int64_t a = (target_cluster_incident_edges_weight - current_cluster_incident_edges_weight) * graph.weightedNodeDegree(node);
  return e - a;

  static_assert(sizeof(decltype(e)) >= 2 * sizeof(Graph::EdgeId), "Delta Modularity has to be able to captuare a value of maximum number of edges squared");
  static_assert(sizeof(decltype(a)) >= 2 * sizeof(Graph::EdgeId), "Delta Modularity has to be able to captuare a value of maximum number of edges squared");
  static_assert(sizeof(deltaModularity(std::declval<Graph>(), std::declval<NodeId>(), std::declval<ClusterId>(), std::declval<ClusterId>(), std::declval<Weight>(), std::declval<Weight>(), std::declval<std::vector<Weight>>())) >= 2 * sizeof(Graph::EdgeId), "Delta Modularity has to be able to captuare a value of maximum number of edges squared");
}

bool localMoving(const Graph& graph, ClusterStore &clusters);
bool localMoving(const Graph& graph, ClusterStore &clusters, NodeId node_range_lower_bound, NodeId node_range_upper_bound);

bool localMoving(const Graph& graph, ClusterStore &clusters) {
  return localMoving(graph, clusters, 0, graph.getNodeCount());
}

bool localMoving(const Graph& graph, ClusterStore &clusters, NodeId node_range_lower_bound, NodeId node_range_upper_bound) {
  assert(node_range_upper_bound - node_range_lower_bound == clusters.size());
  bool changed = false;

  clusters.assignSingletonClusterIds();
  std::map<ClusterId, Weight> node_to_cluster_weights;
  std::vector<Weight> cluster_weights(graph.getNodeCount());
  for (NodeId i = 0; i < graph.getNodeCount(); i++) {
    cluster_weights[i] = graph.weightedNodeDegree(i);
  }

  NodeId current_node = node_range_lower_bound;
  NodeId unchanged_count = 0;
  while(unchanged_count < node_range_upper_bound - node_range_lower_bound) {
    // std::cout << "local moving: " << current_node << "\n";
    ClusterId current_node_cluster = clusters[current_node];
    Weight weight_between_node_and_current_cluster = 0;
    ClusterId best_cluster = current_node_cluster;
    int64_t best_delta_modularity = 0;

    // double current_modularity = 0;
    // current_modularity = graph.modularity(clusters);
    // assert(deltaModularity(current_node, graph, clusters[current_node], clusters, cluster_weights) == 0);

    graph.forEachAdjacentNode(current_node, [&](NodeId neighbor, Weight weight) {
      ClusterId neighbor_cluster = clusters[neighbor];
      if (neighbor != current_node) {
        if (neighbor_cluster != current_node_cluster) {
          node_to_cluster_weights[neighbor_cluster] += weight;
        } else {
          weight_between_node_and_current_cluster += weight;
        }
      }
    });

    for (auto& incident_cluster : node_to_cluster_weights) {
      int64_t neighbor_cluster_delta = deltaModularity(graph, current_node, current_node_cluster, incident_cluster.first, weight_between_node_and_current_cluster, incident_cluster.second, cluster_weights);
      // std::cout << current_node << " -> " << neighbor << "(" << clusters[neighbor] << "): " << neighbor_cluster_delta << "\n";
      if (neighbor_cluster_delta > best_delta_modularity) {
        best_delta_modularity = neighbor_cluster_delta;
        best_cluster = incident_cluster.first;
      }
    }

    if (best_cluster != current_node_cluster) {
      cluster_weights[current_node_cluster] -= graph.weightedNodeDegree(current_node);
      clusters.set(current_node, best_cluster);
      cluster_weights[best_cluster] += graph.weightedNodeDegree(current_node);
      unchanged_count = 0;
      changed = true;
      // assert(deltaModularity(current_node, graph, current_node_cluster, clusters, cluster_weights) == -best_delta_modularity);
      // std::cout << current_modularity << "," << (best_delta_modularity / (2.*graph.getTotalWeight()*graph.getTotalWeight())) << "," << graph.modularity(clusters);
      // assert(abs(current_modularity + (best_delta_modularity / (2.*graph.getTotalWeight()*graph.getTotalWeight())) - graph.modularity(clusters)) < 0.0001);
    } else {
      unchanged_count++;
    }

    node_to_cluster_weights.clear();
    current_node++;
    if (current_node >= node_range_upper_bound) {
      current_node = node_range_lower_bound;
    }
  }

  return changed;
}

void contractAndReapply(const Graph &, ClusterStore &);

void louvain(const Graph& graph, ClusterStore &clusters) {
  std::cout << "louvain\n";
  assert(abs(graph.modularity(ClusterStore(0, graph.getNodeCount(), 0))) == 0);

  bool changed = localMoving(graph, clusters);

  if (changed) {
    contractAndReapply(graph, clusters);
  }
}

void partitionedLouvain(const Graph& graph, ClusterStore &clusters) {
  std::cout << "louvain\n";

  uint32_t partition_count = 4;
  NodeId partition_size = (graph.getNodeCount() + partition_count - 1) / partition_count;

  for (uint32_t partition = 0; partition < partition_count; partition++) {
    NodeId lower = partition * partition_size;
    NodeId upper = std::min((partition+1) * partition_size, graph.getNodeCount());
    ClusterStore partition_clustering(lower, upper);
    std::cout << partition << "move\n";
    localMoving(graph, partition_clustering, lower, upper);
    std::cout << partition << "rewrite clusters\n";
    partition_clustering.rewriteClusterIds();

    std::cout << partition << "combine\n";
    for (NodeId node = lower; node < upper; node++) {
      clusters.set(node, partition_clustering[node]);
    }
  }

  contractAndReapply(graph, clusters);
}

void contractAndReapply(const Graph& graph, ClusterStore &clusters) {
  ClusterId cluster_count = clusters.rewriteClusterIds();
  std::cout << "contracting " << cluster_count << " clusters\n";

  std::map<std::pair<NodeId, NodeId>, Weight> weight_matrix;
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight weight) {
      weight_matrix[std::pair<NodeId, NodeId>(clusters[node], clusters[neighbor])] += weight;
    });
  }

  Graph meta_graph(cluster_count, weight_matrix.size());
  meta_graph.setEdgesByAdjacencyMatrix(weight_matrix);

  assert(graph.getTotalWeight() == meta_graph.getTotalWeight());
  ClusterStore meta_singleton(0, cluster_count);
  meta_singleton.assignSingletonClusterIds();
  assert(graph.modularity(clusters) == meta_graph.modularity(meta_singleton));

  ClusterStore meta_clusters(0, meta_graph.getNodeCount());
  louvain(meta_graph, meta_clusters);

  // translate meta clusters
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    clusters.set(node, meta_clusters[clusters[node]]);
  }
}

}
