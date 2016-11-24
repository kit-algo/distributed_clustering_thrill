#pragma once

#include "graph.hpp"

#include <map>
#include <unordered_map>

#include <iostream>
#include <assert.h>
#include <cmath>

namespace Modularity {

template <typename T> int sgn(T val) {
  return (T(0) < val) - (val < T(0));
}

long deltaModularity(const int node, const Graph& graph, int target_cluster, const std::vector<int> &clusters, const std::vector<int> &cluster_weights) {
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

int rewriteClusterIds(std::vector<int> &clusters, const int node_count) {
  int id_counter = 0;
  std::unordered_map<int, int> old_to_new;

  for (int i = 0; i < node_count; i++) {
    if (old_to_new.find(clusters[i]) == old_to_new.end()) {
      old_to_new[clusters[i]] = id_counter++;
    }
    clusters[i] = old_to_new[clusters[i]];
  }

  return id_counter;
}



void louvain(const Graph& graph, std::vector<int> &node_clusters, int level = 0) {
  std::cout << "louvain\n";
  assert(abs(graph.modularity(std::vector<int>(graph.getNodeCount(),0))) == 0);
  bool changed = false;
  std::vector<int> cluster_weights(graph.getNodeCount());

  for (int i = 0; i < graph.getNodeCount(); i++) {
    node_clusters[i] = i;
    cluster_weights[i] = graph.weightedNodeDegree(i);
  }

  int current_node = 0;
  int unchanged_count = 0;
  while(unchanged_count < graph.getNodeCount()) {
    // std::cout << "local moving: " << current_node << "\n";
    int current_node_cluster = node_clusters[current_node];
    int best_cluster = current_node_cluster;
    long best_delta_modularity = 0;

    // double current_modularity = 0;
    // current_modularity = graph.modularity(node_clusters);
    // assert(deltaModularity(current_node, graph, node_clusters[current_node], node_clusters, cluster_weights) == 0);

    graph.forEachAdjacentNode(current_node, [&](int neighbor, int) {
      if (node_clusters[neighbor] != current_node_cluster) {
        long neighbor_cluster_delta = deltaModularity(current_node, graph, node_clusters[neighbor], node_clusters, cluster_weights);
        // std::cout << current_node << " -> " << neighbor << "(" << node_clusters[neighbor] << "): " << neighbor_cluster_delta << "\n";
        if (neighbor_cluster_delta > best_delta_modularity) {
          best_delta_modularity = neighbor_cluster_delta;
          best_cluster = node_clusters[neighbor];
        }
      }
    });

    if (best_cluster != current_node_cluster) {
      assert(best_delta_modularity > 0);
      cluster_weights[current_node_cluster] -= graph.weightedNodeDegree(current_node);
      node_clusters[current_node] = best_cluster;
      cluster_weights[best_cluster] += graph.weightedNodeDegree(current_node);
      unchanged_count = 0;
      changed = true;
      // assert(deltaModularity(current_node, graph, current_node_cluster, node_clusters, cluster_weights) == -best_delta_modularity);
      // std::cout << current_modularity << "," << (best_delta_modularity / (2.*graph.getTotalWeight()*graph.getTotalWeight())) << "," << graph.modularity(node_clusters);
      // assert(abs(current_modularity + (best_delta_modularity / (2.*graph.getTotalWeight()*graph.getTotalWeight())) - graph.modularity(node_clusters)) < 0.0001);
    } else {
      unchanged_count++;
    }

    current_node = (current_node + 1) % graph.getNodeCount();
  }

  if (changed) {
    long cluster_count = rewriteClusterIds(node_clusters, graph.getNodeCount());
    std::cout << "contracting " << cluster_count << " clusters\n";

    std::map<std::pair<int, int>, int> weight_matrix;
    for (int node = 0; node < graph.getNodeCount(); node++) {
      graph.forEachAdjacentNode(node, [&](int neighbor, int weight) {
        weight_matrix[std::pair<int, int>(node_clusters[node], node_clusters[neighbor])] += weight;
      });
    }

    std::cout << "new graph " << cluster_count << "\n";
    Graph meta_graph(cluster_count, weight_matrix.size());
    meta_graph.setEdgesByAdjacencyMatrix(weight_matrix);
    assert(graph.getTotalWeight() == meta_graph.getTotalWeight());
    std::vector<int> meta_singleton(cluster_count);
    for (int i = 0; i < cluster_count; i++) {
      meta_singleton[i] = i;
    }
    assert(graph.modularity(node_clusters) == meta_graph.modularity(meta_singleton));
    std::vector<int> meta_clusters(meta_graph.getNodeCount());
    louvain(meta_graph, meta_clusters, level + 1);

    // translate meta clusters
    for (int node = 0; node < graph.getNodeCount(); node++) {
      node_clusters[node] = meta_clusters[node_clusters[node]];
    }
  }
}

}
