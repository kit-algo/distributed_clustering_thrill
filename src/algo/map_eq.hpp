#pragma once

#include "data/graph.hpp"
#include "data/cluster_store.hpp"
#include "algo/contraction.hpp"
#include "util/logging.hpp"

#include <algorithm>
#include <iostream>
#include <assert.h>
#include <cmath>
#include <cstdint>
#include <algorithm>
#include <random>

namespace MapEq {

template<class GraphType, class ClusterStoreType>
bool localMoving(const GraphType& graph, ClusterStoreType &clusters, std::vector<NodeId>& nodes_to_move) {
  bool changed = false;

  clusters.assignSingletonClusterIds();
  std::vector<Weight> cluster_volumes(graph.getNodeCount());
  std::vector<Weight> cluster_cuts(graph.getNodeCount());
  Weight total_vol = graph.getTotalWeight() * 2;
  Weight total_inter_vol = 0;

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    cluster_volumes[node] = graph.nodeDegree(node);

    graph.forEachAdjacentNode(current_node, [&](NodeId neighbor, Weight weight) {
      if (node != current_node) {
        cluster_cut[node] += weight;
        total_inter_vol += weight;
      }
    });
  }
  std::shuffle(nodes_to_move.begin(), nodes_to_move.end(), rng);

  const auto plogp_rel = [total_vol](Weight w) -> double {
    if (w > 0) {
      double p = static_cast<double>(w) / total_vol;
      return p * log(p);
    }

    return 0;
  };

  const auto update_cost = [&](const NodeId, const Weight deg_u, const Weight loop_u, const ClusterId clus_u, const ClusterId target_clus, const Weight weight_to_target, const Weight weight_to_orig) -> double {
    int64_t cut_diff_old = 2 * weight_to_orig - deg_u + loop_u;
    double values[5];
    if (clus_u != target_clus) {
      int64_t cut_diff_new = deg_u - 2 * weight_to_target - loop_u;

      values[0] = static_cast<double>(total_inter_vol + cut_diff_old + cut_diff_new);
      values[1] = static_cast<double>(cluster_cuts[target_clus] + cut_diff_new);
      values[2] = static_cast<double>(cluster_cuts[target_clus]);
      values[3] = static_cast<double>(cluster_cuts[target_clus] + cut_diff_new + cluster_volumes[target_clus] + deg_u);
      values[4] = static_cast<double>(cluster_cuts[target_clus] + cluster_volumes[target_clus]);
    } else {
      values[0] = static_cast<double>(total_inter_vol);
      values[1] = static_cast<double>(cluster_cuts[clus_u]);
      values[2] = static_cast<double>(cluster_cuts[clus_u] + cut_diff_old);
      values[3] = static_cast<double>(cluster_cuts[clus_u] + cluster_volumes[clus_u]);
      values[4] = static_cast<double>(cluster_cuts[clus_u] + cut_diff_old + cluster_volumes[clus_u] - deg_u);
    }

    double result[5];

    uint8_t i = 0;
#if MAX_VECTOR_SIZE >= 256
    double inverse_total_volume = 1. / total_volumne;
    Vec4d value_vec, result_vec;
    value_vec.load(values);
    value_vec *= inverse_total_volume;
    result_vec = select(value_vec > .0, value_vec * log(value_vec), Vec4d(0,0,0,0));
    result_vec.store(result);

    i = 4;
#else
#pragma omp simd
#endif
    for (; i < 5; ++i) {
      result[i] = 0;
      values[i] /= total_volumne;
      if (values[i] > .0) {
        result[i] = values[i] * log(values[i]);
      }
    }

    return result[0] + ((result[3] - result[4]) - (2 * (result[1] - result[2] )));
  };

  const auto move_node = [&](const NodeId u, const Weight deg_u, const Weight loop_u, const ClusterId clus_u, const ClusterId target_clus, const Weight weight_to_target, const Weight weight_to_orig) {
    int64_t cut_diff_old = 2 * weight_to_orig - deg_u + loop_u;
    int64_t cut_diff_new = deg_u - 2 * weight_to_target - loop_u;

    total_inter_vol += cut_diff_old + cut_diff_new;
    assert(static_cast<int64_t>(cluster_cuts[clus_u]) >= cut_diff_old * -1);
    cluster_cuts[clus_u] += cut_diff_old;
    assert(static_cast<int64_t>(cluster_cuts[target_clus]) >= cut_diff_new * -1);
    cluster_cuts[target_clus] += cut_diff_new;

    cluster_volumes[target_clus] += deg_u;
    cluster_volumes[clus_u] -= deg_u;

    clusters.set(u, target_clus);
  };

  std::vector<Weight> node_to_cluster_weights(graph.getNodeCountIncludingGhost(), 0);
  std::vector<ClusterId> incident_clusters;
  NodeId current_node_index = 0;
  NodeId unchanged_count = 0;
  int current_iteration = 0;

  while (current_iteration < 32 && unchanged_count < nodes_to_move.size()) {
    NodeId current_node = nodes_to_move[current_node_index];
    ClusterId current_node_cluster = clusters[current_node];
    Weight weight_between_node_and_current_cluster = 0;
    Weight current_loop_weight = 0;
    ClusterId best_cluster = current_node_cluster;

    graph.forEachAdjacentNode(current_node, [&](NodeId neighbor, Weight weight) {
      ClusterId neighbor_cluster = clusters[neighbor];
      if (neighbor != current_node) {
        if (neighbor_cluster != current_node_cluster) {
          if (node_to_cluster_weights[neighbor_cluster] == 0) {
            incident_clusters.push_back(neighbor_cluster);
          }
          node_to_cluster_weights[neighbor_cluster] += weight;
        } else {
          weight_between_node_and_current_cluster += weight;
        }
      } else {
        current_loop_weight += weight;
      }
    });

    Weight weight_between_node_and_best_cluster = weight_between_node_and_current_cluster;
    double max_gain = update_cost(current_node, graph.nodeDegree(current_node), current_loop_weight, current_node_cluster, current_node_cluster, weight_between_node_and_current_cluster, weight_between_node_and_current_cluster);

    for (ClusterId& incident_cluster : incident_clusters) {
      double gain = update_cost(current_node, graph.nodeDegree(current_node), current_loop_weight, current_node_cluster, incident_cluster, node_to_cluster_weights[neighbor_cluster], weight_between_node_and_current_cluster);

      if (gain < max_gain || (gain == max_gain && incident_cluster < best_cluster)) {
        best_delta_modularity = neighbor_cluster_delta;
        best_cluster = incident_cluster;
      }

      node_to_cluster_weights[incident_cluster] = 0;
    }

    incident_clusters.clear();

    if (best_cluster != current_node_cluster) {
      unchanged_count = 0;
      changed = true;
      move_node(current_node, graph.nodeDegree(current_node), current_loop_weight, current_node_cluster, best_cluster, weight_between_node_and_best_cluster, weight_between_node_and_current_cluster);
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

};
