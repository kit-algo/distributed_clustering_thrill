#pragma once

#include "graph.hpp"
#include "cluster_store.hpp"

#include <map>
#include <algorithm>

#include <iostream>
#include <assert.h>
#include <cmath>

namespace Modularity {

template <typename T> int sgn(T val) {
  return (T(0) < val) - (val < T(0));
}

long deltaModularity(const int node, const Graph& graph, int target_cluster, const ClusterStore &clusters, const std::vector<int> &cluster_weights) {
  long weight_between_node_and_target_cluster = 0;
  graph.forEachAdjacentNode(node, [&](int neighbor, int weight) {
    if (clusters[neighbor] == target_cluster && neighbor != node) {
      weight_between_node_and_target_cluster += weight;
    }
  });
  assert(weight_between_node_and_target_cluster >= 0);

  long weight_between_node_and_current_cluster = 0;
  graph.forEachAdjacentNode(node, [&](int neighbor, int weight) {
    if (clusters[neighbor] == clusters[node] && neighbor != node) {
      weight_between_node_and_current_cluster += weight;
    }
  });
  assert(weight_between_node_and_current_cluster >= 0);

  long target_cluster_incident_edges_weight = cluster_weights[target_cluster];
  if (target_cluster == clusters[node]) {
    target_cluster_incident_edges_weight -= graph.weightedNodeDegree(node);
  }
  assert(target_cluster_incident_edges_weight >= 0);

  long current_cluster_incident_edges_weight = cluster_weights[clusters[node]] - graph.weightedNodeDegree(node);
  assert(current_cluster_incident_edges_weight >= 0);

  long e = (graph.getTotalWeight() * 2 * (weight_between_node_and_target_cluster - weight_between_node_and_current_cluster));
  assert(sgn(e) == sgn(weight_between_node_and_target_cluster - weight_between_node_and_current_cluster));
  long a = (target_cluster_incident_edges_weight - current_cluster_incident_edges_weight) * graph.weightedNodeDegree(node);
  assert(graph.weightedNodeDegree(node) > 0);
  assert(sgn(a) == sgn((target_cluster_incident_edges_weight - current_cluster_incident_edges_weight)));

  return e - a;
}

bool localMoving(const Graph& graph, ClusterStore &clusters);
bool localMoving(const Graph& graph, ClusterStore &clusters, unsigned int node_range_lower_bound, unsigned int node_range_upper_bound);

bool localMoving(const Graph& graph, ClusterStore &clusters) {
  return localMoving(graph, clusters, 0, graph.getNodeCount());
}

bool localMoving(const Graph& graph, ClusterStore &clusters, unsigned int node_range_lower_bound, unsigned int node_range_upper_bound) {
  assert(node_range_upper_bound - node_range_lower_bound == clusters.size());
  bool changed = false;

  clusters.assignSingletonClusterIds();
  std::vector<int> cluster_weights(graph.getNodeCount());
  for (int i = 0; i < graph.getNodeCount(); i++) {
    cluster_weights[i] = graph.weightedNodeDegree(i);
  }

  int current_node = node_range_lower_bound;
  int unchanged_count = 0;
  while(unchanged_count < node_range_upper_bound - node_range_lower_bound) {
    // std::cout << "local moving: " << current_node << "\n";
    int current_node_cluster = clusters[current_node];
    int best_cluster = current_node_cluster;
    long best_delta_modularity = 0;

    // double current_modularity = 0;
    // current_modularity = graph.modularity(clusters);
    // assert(deltaModularity(current_node, graph, clusters[current_node], clusters, cluster_weights) == 0);

    graph.forEachAdjacentNode(current_node, [&](int neighbor, int) {
      int neighbor_cluster = clusters[neighbor];
      if (neighbor_cluster != current_node_cluster) {
        long neighbor_cluster_delta = deltaModularity(current_node, graph, clusters[neighbor], clusters, cluster_weights);
        // std::cout << current_node << " -> " << neighbor << "(" << clusters[neighbor] << "): " << neighbor_cluster_delta << "\n";
        if (neighbor_cluster_delta > best_delta_modularity) {
          best_delta_modularity = neighbor_cluster_delta;
          best_cluster = neighbor_cluster;
        }
      }
    });

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

    current_node++;
    if (current_node >= node_range_upper_bound) {
      current_node = node_range_lower_bound;
    }
  }

  return changed;
}

void louvain(const Graph& graph, ClusterStore &clusters) {
  std::cout << "louvain\n";
  assert(abs(graph.modularity(ClusterStore(0, graph.getNodeCount(),0))) == 0);

  bool changed = localMoving(graph, clusters);

  if (changed) {
    long cluster_count = clusters.rewriteClusterIds();
    std::cout << "contracting " << cluster_count << " clusters\n";

    std::map<std::pair<int, int>, int> weight_matrix;
    for (int node = 0; node < graph.getNodeCount(); node++) {
      graph.forEachAdjacentNode(node, [&](int neighbor, int weight) {
        weight_matrix[std::pair<int, int>(clusters[node], clusters[neighbor])] += weight;
      });
    }

    std::cout << "new graph " << cluster_count << "\n";
    Graph meta_graph(cluster_count, weight_matrix.size());
    meta_graph.setEdgesByAdjacencyMatrix(weight_matrix);
    assert(graph.getTotalWeight() == meta_graph.getTotalWeight());
    ClusterStore meta_singleton(0, cluster_count);
    meta_singleton.assignSingletonClusterIds();
    assert(graph.modularity(clusters) == meta_graph.modularity(meta_singleton));
    ClusterStore meta_clusters(0, meta_graph.getNodeCount());
    louvain(meta_graph, meta_clusters);

    // translate meta clusters
    for (int node = 0; node < graph.getNodeCount(); node++) {
      clusters.set(node, meta_clusters[clusters[node]]);
    }
  }
}

void partitioned_louvain(const Graph& graph, ClusterStore &clusters) {
  std::cout << "louvain\n";

  int partition_count = 4;
  int partition_size = (graph.getNodeCount() + partition_count - 1) / partition_count;

  for (int partition = 0; partition < partition_count; partition++) {
    std::cout << partition << "\n";
    int lower = partition * partition_size;
    int upper = std::min((partition+1) * partition_size, graph.getNodeCount());
    ClusterStore partition_clustering(lower, upper);
    std::cout << partition << "move\n";
    localMoving(graph, partition_clustering, lower, upper);
    std::cout << partition << "rewrite clusters\n";
    partition_clustering.rewriteClusterIds();

    std::cout << partition << "combine\n";
    for (int node = lower; node < upper; node++) {
      clusters.set(node, partition_clustering[node]);
    }
  }

  long cluster_count = clusters.rewriteClusterIds();
  std::cout << "contracting " << cluster_count << " clusters\n";

  std::map<std::pair<int, int>, int> weight_matrix;
  for (int node = 0; node < graph.getNodeCount(); node++) {
    graph.forEachAdjacentNode(node, [&](int neighbor, int weight) {
      weight_matrix[std::pair<int, int>(clusters[node], clusters[neighbor])] += weight;
    });
  }

  std::cout << "new graph " << cluster_count << "\n";
  Graph meta_graph(cluster_count, weight_matrix.size());
  meta_graph.setEdgesByAdjacencyMatrix(weight_matrix);
  assert(graph.getTotalWeight() == meta_graph.getTotalWeight());
  ClusterStore meta_singleton(0, cluster_count);
  meta_singleton.assignSingletonClusterIds();
  assert(graph.modularity(clusters) == meta_graph.modularity(meta_singleton));
  ClusterStore meta_clusters(0, meta_graph.getNodeCount());
  louvain(meta_graph, meta_clusters);

  // translate meta clusters
  for (int node = 0; node < graph.getNodeCount(); node++) {
    clusters.set(node, meta_clusters[clusters[node]]);
  }
}

}
