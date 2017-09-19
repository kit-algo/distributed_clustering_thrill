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

std::default_random_engine rng;

using NodeId = typename Graph::NodeId;
using EdgeId = typename Graph::EdgeId;
using Weight = typename Graph::Weight;
using ClusterId = typename ClusterStore::ClusterId;

template<class GraphType, class ClusterStoreType>
bool localMoving(const GraphType& graph, ClusterStoreType &clusters) {
  std::vector<NodeId> nodes_to_move(graph.getNodeCount());
  std::iota(nodes_to_move.begin(), nodes_to_move.end(), 0);
  bool changed = false;

  clusters.assignSingletonClusterIds();
  std::vector<Weight> cluster_volumes(graph.getNodeCount());
  std::vector<Weight> cluster_cuts(graph.getNodeCount());
  Weight total_vol = graph.getTotalWeight() * 2;
  Weight total_inter_vol = 0;

  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    cluster_volumes[node] = graph.nodeDegree(node);

    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight weight) {
      if (neighbor != node) {
        cluster_cuts[node] += weight;
        total_inter_vol += weight;
      }
    });
  }
  std::shuffle(nodes_to_move.begin(), nodes_to_move.end(), rng);

#ifndef NDEBUG
  const auto plogp_rel = [total_vol](Weight w) -> double {
    if (w > 0) {
      double p = static_cast<double>(w) / total_vol;
      return p * log(p);
    }

    return 0;
  };

  long double sum_p_log_p_cluster_cut = 0;
  long double sum_p_log_p_cluster_cut_plus_vol = 0;

  auto update_p_log_p_sums = [&]() {
    sum_p_log_p_cluster_cut = std::accumulate<decltype(cluster_cuts.begin()), long double>(cluster_cuts.begin(), cluster_cuts.end(), .0, [&](long double sum, Weight cut) {
      return sum + plogp_rel(cut);
    });

    sum_p_log_p_cluster_cut_plus_vol = 0;
    for (size_t i = 0; i < cluster_cuts.size(); ++i) {
      sum_p_log_p_cluster_cut_plus_vol += plogp_rel(cluster_cuts[i] + cluster_volumes[i]);
    }
  };

  update_p_log_p_sums();

  const long double sum_p_log_p_w_alpha = std::accumulate<decltype(cluster_cuts.begin()), long double>(cluster_volumes.begin(), cluster_volumes.end(), .0, [&](long double sum, Weight vol) {
    return sum + plogp_rel(vol);
  });

  const auto map_equation = [&]() -> long double {
    return plogp_rel(total_inter_vol) - 2 * sum_p_log_p_cluster_cut + sum_p_log_p_cluster_cut_plus_vol - sum_p_log_p_w_alpha;
  };

  std::vector<Weight> debug_cluster_cut(cluster_cuts.size(), 0);
#endif

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

#if MAX_VECTOR_SIZE >= 256
    double inverse_total_volume = 1. / total_vol;
    Vec4d value_vec, result_vec;
    value_vec.load(values);
    value_vec *= inverse_total_volume;
    result_vec = select(value_vec > .0, value_vec * log(value_vec), Vec4d(0,0,0,0));
    result_vec.store(result);

    for (uint8_t i = 4; i < 5; ++i) {
#else
#pragma omp simd
    for (uint8_t i = 0; i < 5; ++i) {
#endif
      result[i] = 0;
      values[i] /= total_vol;
      if (values[i] > .0) {
        result[i] = values[i] * log(values[i]);
      }
    }

    return result[0] + ((result[3] - result[4]) - (2 * (result[1] - result[2])));
  };

  const auto move_node = [&](const NodeId u, const Weight deg_u, const Weight loop_u, const ClusterId clus_u, const ClusterId target_clus, const Weight weight_to_target, const Weight weight_to_orig) {
  #ifndef NDEBUG
    long double old_val = map_equation();
    assert(old_val > 0);
    double diff = update_cost(u, deg_u, loop_u, clus_u, target_clus, weight_to_target, weight_to_orig) - update_cost(u, deg_u, loop_u, clus_u, clus_u, weight_to_orig, weight_to_orig);
    assert(diff < 0);

    sum_p_log_p_cluster_cut -= plogp_rel(cluster_cuts[clus_u]);
    sum_p_log_p_cluster_cut_plus_vol -= plogp_rel(cluster_cuts[clus_u] + cluster_volumes[clus_u]);
    sum_p_log_p_cluster_cut -= plogp_rel(cluster_cuts[target_clus]);
    sum_p_log_p_cluster_cut_plus_vol -= plogp_rel(cluster_cuts[target_clus] + cluster_volumes[target_clus]);
  #endif

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

  #ifndef NDEBUG
    sum_p_log_p_cluster_cut += plogp_rel(cluster_cuts[clus_u]);
    sum_p_log_p_cluster_cut_plus_vol += plogp_rel(cluster_cuts[clus_u] + cluster_volumes[clus_u]);
    sum_p_log_p_cluster_cut += plogp_rel(cluster_cuts[target_clus]);
    sum_p_log_p_cluster_cut_plus_vol += plogp_rel(cluster_cuts[target_clus] + cluster_volumes[target_clus]);

    // if (u % 1000 == 0) {
    if (false) {
      debug_cluster_cut.assign(cluster_cuts.size(), 0);
      for (NodeId u = 0; u < graph.getNodeCount(); ++u) {
        ClusterId part_u = clusters[u];
        graph.forEachAdjacentNode(u, [&](NodeId v, Weight w) {
          if (part_u != clusters[v]) {
            debug_cluster_cut[part_u] += w;
          }
        });
      }

      for (size_t i = 0; i < debug_cluster_cut.size(); ++i) {
        assert(cluster_cuts[i] == debug_cluster_cut[i]);
      }

      assert(std::accumulate(debug_cluster_cut.begin(), debug_cluster_cut.end(), 0ul) == total_inter_vol);
    }

    long double new_val = map_equation();
    assert(new_val > 0);
    if (std::abs(old_val + diff - new_val) > 0.0001) {
      std::cout << "Old: " << old_val << ", diff: " << diff << " new: " << new_val << std::endl;
      update_p_log_p_sums();
      std::cout << "After update: " << map_equation() << std::endl;
      assert(false);
    }
    // assert(loop_u == 0);
    assert(std::accumulate(cluster_cuts.begin(), cluster_cuts.end(), 0ull) == total_inter_vol);
#endif
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
      double gain = update_cost(current_node, graph.nodeDegree(current_node), current_loop_weight, current_node_cluster, incident_cluster, node_to_cluster_weights[incident_cluster], weight_between_node_and_current_cluster);

      if (gain < max_gain || (gain == max_gain && incident_cluster < best_cluster)) {
        max_gain = gain;
        best_cluster = incident_cluster;
        weight_between_node_and_best_cluster = node_to_cluster_weights[incident_cluster];
      }

      node_to_cluster_weights[incident_cluster] = 0;
    }

    incident_clusters.clear();

    if (best_cluster != current_node_cluster) {
      unchanged_count = 0;
      changed = true;
      move_node(current_node, graph.nodeDegree(current_node), current_loop_weight, current_node_cluster, best_cluster, weight_between_node_and_best_cluster, weight_between_node_and_current_cluster);
    } else {
      unchanged_count++;
    }

    current_node_index++;
    if (current_node_index >= nodes_to_move.size()) {
#ifndef NDEBUG
      update_p_log_p_sums();
      std::cout << "Map equation after iteration " << current_iteration << " is " << map_equation() << std::endl;
#endif
      current_node_index = 0;
      current_iteration++;
      std::shuffle(nodes_to_move.begin(), nodes_to_move.end(), rng);
    }
  }

  return changed;
}

};
