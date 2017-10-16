#pragma once

#include "data/graph.hpp"
#include "data/cluster_store.hpp"

#include <algorithm>
#include <iostream>
#include <assert.h>
#include <cmath>
#include <cstdint>
#include <algorithm>
#include <random>

namespace Modularity {

using int128_t = __int128_t;

std::default_random_engine rng;

using NodeId = typename Graph::NodeId;
using EdgeId = typename Graph::EdgeId;
using Weight = typename Graph::Weight;
using ClusterId = typename ClusterStore::ClusterId;

template<class GraphType, class ClusterStoreType>
double modularity(GraphType const &graph, ClusterStoreType const &clusters) {
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
  int128_t incident_sum = std::accumulate(incident_weights.begin(), incident_weights.end(), 0l, [](const int128_t& agg, const Weight& elem) {
    return agg + ((int128_t) elem * (int128_t) elem);
  });

  int128_t total_weight = graph.getTotalWeight();
  return (inner_sum / (2.*total_weight)) - (incident_sum / (4.*total_weight*total_weight));

  static_assert(sizeof(decltype(total_weight)) >= 2 * sizeof(Graph::EdgeId), "Modularity has to be able to captuare a value of maximum number of edges squared");
  static_assert(sizeof(decltype(incident_sum)) >= 2 * sizeof(Graph::EdgeId), "Modularity has to be able to captuare a value of maximum number of edges squared");
}


template<class GraphType>
int128_t deltaModularity(const GraphType &graph,
                        const NodeId node,
                        const ClusterId current_cluster,
                        const ClusterId target_cluster,
                        const int128_t weight_between_node_and_current_cluster,
                        const int128_t weight_between_node_and_target_cluster,
                        const std::vector<Weight> &cluster_weights) {

  int128_t target_cluster_incident_edges_weight = cluster_weights[target_cluster];
  if (target_cluster == current_cluster) {
    target_cluster_incident_edges_weight -= graph.nodeDegree(node);
  }

  int128_t current_cluster_incident_edges_weight = cluster_weights[current_cluster] - graph.nodeDegree(node);

  int128_t e = (graph.getTotalWeight() * 2 * (weight_between_node_and_target_cluster - weight_between_node_and_current_cluster));
  int128_t a = (target_cluster_incident_edges_weight - current_cluster_incident_edges_weight) * graph.nodeDegree(node);
  return e - a;

  static_assert(sizeof(decltype(e)) >= 2 * sizeof(typename Graph::EdgeId), "Delta Modularity has to be able to captuare a value of maximum number of edges squared");
  static_assert(sizeof(decltype(a)) >= 2 * sizeof(typename Graph::EdgeId), "Delta Modularity has to be able to captuare a value of maximum number of edges squared");
  static_assert(sizeof(deltaModularity(std::declval<GraphType>(), std::declval<NodeId>(), std::declval<ClusterId>(), std::declval<ClusterId>(), std::declval<Weight>(), std::declval<Weight>(), std::declval<std::vector<Weight>>())) >= 2 * sizeof(typename Graph::EdgeId), "Delta Modularity has to be able to captuare a value of maximum number of edges squared");
}

template<class GraphType, class ClusterStoreType>
bool localMoving(const GraphType& graph, ClusterStoreType &clusters);
template<class GraphType, class ClusterStoreType, bool move_to_ghosts = true>
bool localMoving(const GraphType& graph, ClusterStoreType &clusters, std::vector<NodeId>& nodes_to_move);

template<class GraphType, class ClusterStoreType>
bool localMoving(const GraphType& graph, ClusterStoreType &clusters) {
  std::vector<NodeId> nodes_to_move(graph.getNodeCount());
  std::iota(nodes_to_move.begin(), nodes_to_move.end(), 0);
  return localMoving(graph, clusters, nodes_to_move);
}

template<class GraphType, class ClusterStoreType, bool move_to_ghosts>
bool localMoving(const GraphType& graph, ClusterStoreType &clusters, std::vector<NodeId>& nodes_to_move) {
  std::vector<bool> included_nodes(move_to_ghosts ? 0 : graph.getNodeCount(), false);
  if (!move_to_ghosts) {
    for (NodeId node : nodes_to_move) {
      included_nodes[node] = true;
    }
  }

  bool changed = false;

  clusters.assignSingletonClusterIds();
  std::vector<Weight> node_to_cluster_weights(graph.getNodeCountIncludingGhost(), 0);
  std::vector<ClusterId> incident_clusters;
  std::vector<Weight> cluster_weights(graph.getNodeCountIncludingGhost());

  for (NodeId i = 0; i < graph.getNodeCountIncludingGhost(); i++) {
    cluster_weights[i] = graph.nodeDegree(i);
  }
  std::shuffle(nodes_to_move.begin(), nodes_to_move.end(), rng);

  NodeId current_node_index = 0;
  NodeId unchanged_count = 0;
  int current_iteration = 0;
  while (current_iteration < 32 && unchanged_count < nodes_to_move.size()) {
    NodeId current_node = nodes_to_move[current_node_index];
    // std::cout << "local moving: " << current_node << "\n";
    ClusterId current_node_cluster = clusters[current_node];
    Weight weight_between_node_and_current_cluster = 0;
    ClusterId best_cluster = current_node_cluster;
    int128_t best_delta_modularity = 0;

    // double current_modularity = 0;
    // current_modularity = modularity(graph, clusters);
    // assert(deltaModularity(current_node, graph, clusters[current_node], clusters, cluster_weights) == 0);

    graph.forEachAdjacentNode(current_node, [&](NodeId neighbor, Weight weight) {
      ClusterId neighbor_cluster = clusters[neighbor];
      if (neighbor != current_node && (move_to_ghosts || included_nodes[neighbor])) {
        if (neighbor_cluster != current_node_cluster) {
          if (node_to_cluster_weights[neighbor_cluster] == 0) {
            incident_clusters.push_back(neighbor_cluster);
          }
          node_to_cluster_weights[neighbor_cluster] += weight;
        } else {
          weight_between_node_and_current_cluster += weight;
        }
      }
    });

    for (ClusterId& incident_cluster : incident_clusters) {
      int128_t neighbor_cluster_delta = deltaModularity(graph, current_node, current_node_cluster, incident_cluster, weight_between_node_and_current_cluster, node_to_cluster_weights[incident_cluster], cluster_weights);
      // std::cout << current_node << " -> " << neighbor << "(" << clusters[neighbor] << "): " << neighbor_cluster_delta << "\n";
      if (neighbor_cluster_delta > best_delta_modularity) {
        best_delta_modularity = neighbor_cluster_delta;
        best_cluster = incident_cluster;
      }

      node_to_cluster_weights[incident_cluster] = 0;
    }

    incident_clusters.clear();

    if (best_cluster != current_node_cluster) {
      cluster_weights[current_node_cluster] -= graph.nodeDegree(current_node);
      clusters.set(current_node, best_cluster);
      cluster_weights[best_cluster] += graph.nodeDegree(current_node);
      unchanged_count = 0;
      changed = true;
      // assert(deltaModularity(current_node, graph, current_node_cluster, clusters, cluster_weights) == -best_delta_modularity);
      // std::cout << current_modularity << "," << (best_delta_modularity / (2.*graph.getTotalWeight()*graph.getTotalWeight())) << "," << graph.modularity(clusters);
      // assert(std::abs(current_modularity + (best_delta_modularity / (2.*graph.getTotalWeight()*graph.getTotalWeight())) - modularity(graph, clusters)) < 0.0001);
    } else {
      unchanged_count++;
    }

    current_node_index++;
    if (current_node_index >= nodes_to_move.size()) {
      current_node_index = 0;
      current_iteration++;
      std::shuffle(nodes_to_move.begin(), nodes_to_move.end(), rng);
    }
  }

  return changed;
}

}
