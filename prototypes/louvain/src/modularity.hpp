#pragma once

#include "graph.hpp"
#include "cluster_store.hpp"
#include "logging.hpp"

#include <map>
#include <algorithm>

#include <iostream>
#include <assert.h>
#include <cmath>
#include <cstdint>
#include <algorithm>

namespace Modularity {

std::default_random_engine rng;

using NodeId = typename Graph::NodeId;
using EdgeId = typename Graph::EdgeId;
using Weight = typename Graph::Weight;
using ClusterId = typename ClusterStore::ClusterId;

double modularity(Graph const &graph, ClusterStore const &clusters) {
  std::vector<Weight> inner_weights(clusters.size(), 0);
  std::vector<Weight> incident_weights(clusters.size(), 0);

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    incident_weights[clusters[node]] += graph.nodeDegree(node);
    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight weight) {
      if (clusters[neighbor] == clusters[node]) {
        inner_weights[clusters[node]] += weight;
      }
    });
  }

  Weight inner_sum = std::accumulate(inner_weights.begin(), inner_weights.end(), 0);
  uint64_t incident_sum = std::accumulate(incident_weights.begin(), incident_weights.end(), 0l, [](const uint64_t& agg, const uint64_t& elem) {
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
    target_cluster_incident_edges_weight -= graph.nodeDegree(node);
  }

  int64_t current_cluster_incident_edges_weight = cluster_weights[current_cluster] - graph.nodeDegree(node);

  int64_t e = (graph.getTotalWeight() * 2 * (weight_between_node_and_target_cluster - weight_between_node_and_current_cluster));
  int64_t a = (target_cluster_incident_edges_weight - current_cluster_incident_edges_weight) * graph.nodeDegree(node);
  return e - a;

  static_assert(sizeof(decltype(e)) >= 2 * sizeof(Graph::EdgeId), "Delta Modularity has to be able to captuare a value of maximum number of edges squared");
  static_assert(sizeof(decltype(a)) >= 2 * sizeof(Graph::EdgeId), "Delta Modularity has to be able to captuare a value of maximum number of edges squared");
  static_assert(sizeof(deltaModularity(std::declval<Graph>(), std::declval<NodeId>(), std::declval<ClusterId>(), std::declval<ClusterId>(), std::declval<Weight>(), std::declval<Weight>(), std::declval<std::vector<Weight>>())) >= 2 * sizeof(Graph::EdgeId), "Delta Modularity has to be able to captuare a value of maximum number of edges squared");
}

bool localMoving(const Graph& graph, ClusterStore &clusters);
bool localMoving(const Graph& graph, ClusterStore &clusters, std::vector<NodeId>& nodes_to_move);

bool localMoving(const Graph& graph, ClusterStore &clusters) {
  std::vector<NodeId> nodes_to_move(graph.getNodeCount());
  std::iota(nodes_to_move.begin(), nodes_to_move.end(), 0);
  return localMoving(graph, clusters, nodes_to_move);
}

bool localMoving(const Graph& graph, ClusterStore &clusters, std::vector<NodeId>& nodes_to_move) {
  bool changed = false;

  clusters.assignSingletonClusterIds();
  std::map<ClusterId, Weight> node_to_cluster_weights;
  std::vector<Weight> cluster_weights(graph.getNodeCount());

  for (NodeId i = 0; i < graph.getNodeCount(); i++) {
    cluster_weights[i] = graph.nodeDegree(i);
  }
  std::shuffle(nodes_to_move.begin(), nodes_to_move.end(), rng);

  NodeId current_node_index = 0;
  NodeId unchanged_count = 0;
  int current_iteration = 0;
  while(current_iteration < 32 && unchanged_count < nodes_to_move.size()) {
    NodeId current_node = nodes_to_move[current_node_index];
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
      cluster_weights[current_node_cluster] -= graph.nodeDegree(current_node);
      clusters.set(current_node, best_cluster);
      cluster_weights[best_cluster] += graph.nodeDegree(current_node);
      unchanged_count = 0;
      changed = true;
      // assert(deltaModularity(current_node, graph, current_node_cluster, clusters, cluster_weights) == -best_delta_modularity);
      // std::cout << current_modularity << "," << (best_delta_modularity / (2.*graph.getTotalWeight()*graph.getTotalWeight())) << "," << graph.modularity(clusters);
      // assert(abs(current_modularity + (best_delta_modularity / (2.*graph.getTotalWeight()*graph.getTotalWeight())) - graph.modularity(clusters)) < 0.0001);
    } else {
      unchanged_count++;
    }

    node_to_cluster_weights.clear();
    current_node_index++;
    if (current_node_index >= nodes_to_move.size()) {
      current_node_index = 0;
      current_iteration++;
      std::shuffle(nodes_to_move.begin(), nodes_to_move.end(), rng);
    }
  }

  return changed;
}

void contractAndReapply(const Graph &, ClusterStore &, uint64_t);

void louvain(const Graph& graph, ClusterStore &clusters, uint64_t algo_run_id) {
  assert(abs(modularity(graph, ClusterStore(0, graph.getNodeCount(), 0))) == 0);

  bool changed = localMoving(graph, clusters);

  if (changed) {
    contractAndReapply(graph, clusters, algo_run_id);
  }
}

void partitionedLouvain(const Graph& graph, ClusterStore &clusters, const std::vector<uint32_t>& partitions, uint64_t algo_run_id) {
  assert(partitions.size() == graph.getNodeCount());
  uint32_t partition_count = *std::max_element(partitions.begin(), partitions.end()) + 1;
  std::vector<std::vector<NodeId>> partition_nodes(partition_count);
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    partition_nodes[partitions[node]].push_back(node);
  }

  for (uint32_t partition = 0; partition < partition_nodes.size(); partition++) {
    ClusterStore partition_clustering(0, graph.getNodeCount());
    // std::cout << partition << "move\n";
    localMoving(graph, partition_clustering, partition_nodes[partition]);
    // std::cout << partition << "rewrite clusters\n";
    partition_clustering.rewriteClusterIds(partition * graph.getNodeCount());

    // std::cout << partition << "combine\n";
    for (NodeId node : partition_nodes[partition]) {
      clusters.set(node, partition_clustering[node]);
    }
  }

  contractAndReapply(graph, clusters, algo_run_id);
}

void contractAndReapply(const Graph& graph, ClusterStore &clusters, uint64_t algo_run_id) {
  ClusterId cluster_count = clusters.rewriteClusterIds();

  uint64_t level_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_level", level_logging_id, "algorithm_run_id", algo_run_id);
  Logging::report("algorithm_level", level_logging_id, "node_count", graph.getNodeCount());
  Logging::report("algorithm_level", level_logging_id, "cluster_count", cluster_count);

  uint64_t distribution_logging_id = Logging::getUnusedId();
  Logging::report("cluster_size_distribution", distribution_logging_id, "algorithm_level_id", level_logging_id);
  std::map<uint32_t, uint32_t> size_distribution;
  clusters.clusterSizeDistribution(size_distribution);
  for (auto & size_count : size_distribution) {
    Logging::report("cluster_size_distribution", distribution_logging_id, size_count.first, size_count.second);
  }

  std::vector<std::map<NodeId, Weight>> cluster_connection_weights(cluster_count);
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight weight) {
      cluster_connection_weights[clusters[node]][clusters[neighbor]] += weight;
    });
  }

  EdgeId edge_count = std::accumulate(cluster_connection_weights.begin(), cluster_connection_weights.end(), 0, [](const EdgeId agg, const std::map<NodeId, Weight>& elem) {
    return agg + elem.size() / 2 + 1;
  });
  Graph meta_graph(cluster_count, edge_count, true);
  meta_graph.setEdgesByAdjacencyMatrix(cluster_connection_weights);

  assert(graph.getTotalWeight() == meta_graph.getTotalWeight());
  ClusterStore meta_singleton(0, cluster_count);
  meta_singleton.assignSingletonClusterIds();
  assert(modularity(graph, clusters) == modularity(meta_graph, meta_singleton));

  ClusterStore meta_clusters(0, meta_graph.getNodeCount());
  louvain(meta_graph, meta_clusters, algo_run_id);

  // translate meta clusters
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    clusters.set(node, meta_clusters[clusters[node]]);
  }
}

}
