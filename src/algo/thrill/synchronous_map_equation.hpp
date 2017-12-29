#pragma once

#include <thrill/api/cache.hpp>
#include <thrill/api/collect_local.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/fold_by_key.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/reduce_to_index_without_precombine.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/zip.hpp>

#include <vector>
#include <algorithm>
#include <sparsepp/spp.h>
#include "vectorclass.h"
#include "vectormath_exp.h"

#include "algo/louvain.hpp"
#include "util/util.hpp"
#include "util/logging.hpp"
#include "data/thrill/graph.hpp"
#include "data/local_dia_graph.hpp"

namespace LocalMoving {

using int128_t = __int128_t;

struct IncidentClusterInfo {
  NodeId node_id;
  ClusterId cluster;
  Weight inbetween_weight;
  Weight total_weight;
  Weight cut;
};

double deltaMapEq(const Weight node_degree,
                  const Weight loop_weight,
                  const IncidentClusterInfo& current_cluster,
                  const IncidentClusterInfo& neighbored_cluster,
                  const Weight total_inter_vol,
                  const Weight total_weight) {
  const Weight total_volumne = 2 * total_weight;
  int64_t cut_diff_old = 2 * current_cluster.inbetween_weight - node_degree + loop_weight;
  double values[5];
  if (current_cluster.cluster != neighbored_cluster.cluster) {
    int64_t cut_diff_new = node_degree - 2 * neighbored_cluster.inbetween_weight - loop_weight;

    values[0] = static_cast<double>(total_inter_vol + cut_diff_old + cut_diff_new);
    values[1] = static_cast<double>(neighbored_cluster.cut + cut_diff_new);
    values[2] = static_cast<double>(neighbored_cluster.cut);
    values[3] = static_cast<double>(neighbored_cluster.cut + cut_diff_new + neighbored_cluster.total_weight + node_degree);
    values[4] = static_cast<double>(neighbored_cluster.cut + neighbored_cluster.total_weight);
  } else {
    values[0] = static_cast<double>(total_inter_vol);
    values[1] = static_cast<double>(current_cluster.cut);
    values[2] = static_cast<double>(current_cluster.cut + cut_diff_old);
    values[3] = static_cast<double>(current_cluster.cut + current_cluster.total_weight);
    values[4] = static_cast<double>(current_cluster.cut + cut_diff_old + current_cluster.total_weight - node_degree);
  }

  double result[5];

#if MAX_VECTOR_SIZE >= 256
  double inverse_total_volume = 1. / total_volumne;
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
    values[i] /= total_volumne;
    if (values[i] > .0) {
      result[i] = values[i] * log(values[i]);
    }
  }

  return result[0] + ((result[3] - result[4]) - (2 * (result[1] - result[2])));
};

bool nodeIncluded(const NodeId node, const uint32_t iteration, const uint32_t rate, const uint32_t seed) {
  #if defined(FIXED_RATIO)
    uint32_t hash = Util::combined_hash(node, iteration / FIXED_RATIO, seed);
    return hash % FIXED_RATIO == iteration % FIXED_RATIO;
  #else
    uint32_t hash = Util::combined_hash(node, iteration, seed);
    return hash % 1000 < rate;
  #endif
}

static_assert(sizeof(EdgeTargetWithDegree) == 8, "Too big");

template<class NodeType>
auto distributedLocalMoving(const DiaNodeGraph<NodeType>& graph, uint32_t num_iterations, const uint32_t seed, Logging::Id level_logging_id) {
  thrill::common::Range id_range = thrill::common::CalculateLocalRange(graph.node_count, graph.nodes.context().num_workers(), graph.nodes.context().my_rank());

  std::vector<std::pair<Weight, Weight>> node_degrees;
  node_degrees.reserve(id_range.size());
  auto node_degree = [&id_range, &node_degrees](NodeId id) { return node_degrees[id - id_range.begin].first; };
  auto loop_weight = [&id_range, &node_degrees](NodeId id) { return node_degrees[id - id_range.begin].second; };

  std::vector<ClusterId> clusters;
  clusters.reserve(id_range.size());
  auto cluster = [&id_range, &clusters](NodeId id) { return clusters[id - id_range.begin]; };

  auto degree_data = graph.nodes.Keep().Map([](const NodeType& node) { return std::make_pair(node.weightedDegree(), node.loopWeight()); });
  Weight total_cut = degree_data.Keep().Map([](const std::pair<Weight, Weight>& degree_data) { return degree_data.first - degree_data.second; }).Sum();
  degree_data.CollectLocal(&node_degrees);
  assert(node_degrees.size() == id_range.size());

  auto node_clusters = graph.nodes
    .Map([](const NodeType& node) { return std::make_pair(std::make_pair(node, node.id), false); })
    .Collapse();

  auto first_iteration = [](const auto& included, const auto& node_clusters) {
    return node_clusters
      .template FlatMap<IncidentClusterInfo>(
        [&included](const std::pair<std::pair<NodeType, ClusterId>, bool>& node_cluster_moved, auto emit) {
          const auto& node_cluster = node_cluster_moved.first;
          Weight cut = node_cluster.first.weightedDegree() - node_cluster.first.loopWeight();

          if (included(node_cluster.first.id)) {
            emit(IncidentClusterInfo {
              node_cluster.first.id,
              node_cluster.second,
              0,
              node_cluster.first.weightedDegree(),
              cut
            });
          }

          for (const typename NodeType::LinkType& link : node_cluster.first.links) {
            if (link.target != node_cluster.first.id && included(link.target)) {
             emit(IncidentClusterInfo {
               link.target,
               node_cluster.second,
               link.getWeight(),
               node_cluster.first.weightedDegree(),
               cut
             });
            }
          }
        });
  };

  auto other_iterations = [&total_cut](const auto& included, const auto& node_clusters) {
    auto cluster_data = node_clusters
      .template FoldByKey<std::vector<NodeType>>(thrill::NoDuplicateDetectionTag,
        [](const std::pair<std::pair<NodeType, ClusterId>, bool>& node_cluster) { return node_cluster.first.second; },
        [](std::vector<NodeType>&& acc, const std::pair<std::pair<NodeType, ClusterId>, bool>& node_cluster) {
          acc.push_back(node_cluster.first.first);
          return std::move(acc);
        })
      .Map(
        [](const std::pair<ClusterId, std::vector<NodeType>>& cluster_nodes) {
          spp::sparse_hash_set<NodeId> cluster_node_ids;
          cluster_node_ids.reserve(cluster_nodes.second.size());
          Weight cut = 0;
          for (const NodeType& node : cluster_nodes.second) {
            cluster_node_ids.insert(node.id);
          }
          for (const NodeType& node : cluster_nodes.second) {
            for (const typename NodeType::LinkType& link : node.links) {
              if (node.id != link.target && cluster_node_ids.find(link.target) == cluster_node_ids.end()) {
                cut += link.getWeight();
              }
            }
          }
          return std::make_pair(cluster_nodes, cut);
        });

    total_cut = cluster_data.Keep().Map([](const std::pair<std::pair<ClusterId, std::vector<NodeType>>, Weight>& cluster_info) { return cluster_info.second; }).Sum();

    return cluster_data
      .template FlatMap<IncidentClusterInfo>(
        [&included](const std::pair<std::pair<ClusterId, std::vector<NodeType>>, Weight>& cluster_info, auto emit) {
          const auto& cluster_nodes = cluster_info.first;
          const Weight cut = cluster_info.second;
          spp::sparse_hash_map<NodeId, Weight> node_inbetween_weight;
          Weight total_weight = 0;

          for (const NodeType& node : cluster_nodes.second) {
            total_weight += node.weightedDegree();
          }

          for (const NodeType& node : cluster_nodes.second) {
            for (const typename NodeType::LinkType& link : node.links) {
              if (node.id != link.target && included(link.target)) {
                node_inbetween_weight[link.target] += link.getWeight();
              }
            }
            if (included(node.id)) {
               node_inbetween_weight[node.id] += 0;
            }
          }

          for (const auto& node_cluster_link : node_inbetween_weight) {
            emit(IncidentClusterInfo {
              node_cluster_link.first,
              cluster_nodes.first,
              node_cluster_link.second,
              total_weight,
              cut
            });
          }
        });
  };

  auto reduceToBestCluster = [&graph, &total_cut, &id_range, &node_degree, &loop_weight, &cluster](const auto& incoming) {
    return incoming
      // Reduce to best cluster
      .template GroupToIndex<IncidentClusterInfo>(
        [](const IncidentClusterInfo& lme) -> size_t { return lme.node_id; },
        [total_weight = graph.total_weight, total_cut, &id_range, &node_degree, &loop_weight, &cluster](auto& iterator, NodeId node) {
          IncidentClusterInfo current_cluster_info;
          std::vector<IncidentClusterInfo> incoming;
          while (iterator.HasNext()) {
            const IncidentClusterInfo& cluster_info = iterator.Next();
            if (cluster_info.cluster == cluster(node)) {
              current_cluster_info = cluster_info;
            } else {
              incoming.push_back(cluster_info);
            }
          }

          IncidentClusterInfo best_cluster = current_cluster_info;
          double best_delta = deltaMapEq(node_degree(node), loop_weight(node), current_cluster_info, current_cluster_info, total_cut, total_weight);

          for (const IncidentClusterInfo& cluster_info : incoming) {
            double delta = deltaMapEq(node_degree(node), loop_weight(node), current_cluster_info, cluster_info, total_cut, total_weight);
            if (delta < best_delta) {
              best_delta = delta;
              best_cluster = cluster_info;
            }
          }

          return best_cluster;
        },
        graph.node_count);
  };


  #if !defined(STOP_MOVECOUNT)
    size_t cluster_count = graph.node_count;
  #endif

  #if defined(FIXED_RATIO)
    uint32_t rate = 1000 / FIXED_RATIO;
  #else
    uint32_t rate = 200;
  #endif
  uint32_t rate_sum = 0;

  uint32_t level_seed = Util::combined_hash(seed, level_logging_id);

  uint32_t iteration;
  for (iteration = 0; iteration < num_iterations; iteration++) {
    auto included = [iteration, rate, level_seed](const NodeId id) { return nodeIncluded(id, iteration, rate, level_seed); };

    clusters.clear();
    node_clusters.Keep().Map([](const std::pair<std::pair<NodeType, ClusterId>, bool>& node_cluster_moved) { return node_cluster_moved.first.second; }).CollectLocal(&clusters);

    size_t considered_nodes_estimate = graph.node_count * rate / 1000;

    if (considered_nodes_estimate > 0) {
      node_clusters = (iteration == 0 ?
        reduceToBestCluster(first_iteration(included, node_clusters)) :
        reduceToBestCluster(other_iterations(included, node_clusters.Keep())))
        .Zip(thrill::NoRebalanceTag, node_clusters,
          [&included](const IncidentClusterInfo& lme, const std::pair<std::pair<NodeType, ClusterId>, bool>& old_node_cluster) {
            if (included(old_node_cluster.first.first.id)) {
              assert(lme.node_id == old_node_cluster.first.first.id);
              return std::make_pair(std::make_pair(old_node_cluster.first.first, lme.cluster), lme.cluster != old_node_cluster.first.second);
            } else {
              return std::make_pair(old_node_cluster.first, false);
            }
          })
        .Cache();

      rate_sum += rate;
      #if !defined(FIXED_RATIO) || defined(STOP_MOVECOUNT)
      size_t moved = node_clusters.Keep().Filter([&included](const std::pair<std::pair<NodeType, ClusterId>, bool>& pair) { return pair.second && included(pair.first.first.id); }).Size();
      #endif
      #if !defined(FIXED_RATIO)
        rate = std::max(1000 - (moved * 1000 / considered_nodes_estimate), 200ul);
      #endif

      if (rate_sum >= 1000) {
        #if defined(STOP_MOVECOUNT)
          if (moved <= graph.node_count / 50) {
            break;
          }
        #else
          size_t round_cluster_count = node_clusters.Keep().Map([](const std::pair<std::pair<NodeType, ClusterId>, bool>& node_cluster) { return node_cluster.first.second; }).Uniq().Size();
          assert(graph.node_count == node_clusters.Size());

          if (cluster_count - round_cluster_count <= graph.node_count / 100) {
            break;
          }

          cluster_count = round_cluster_count;
        #endif
        rate_sum -= 1000;
      }
    } else {
      #if !defined(FIXED_RATIO)
        rate += 200;
        if (rate > 1000) { rate = 1000; }
      #endif
    }
  }
  if (node_clusters.context().my_rank() == 0) {
    Logging::report("algorithm_level", level_logging_id, "iterations", iteration);
  }

  return std::make_pair(node_clusters
    .Map(
      [](const std::pair<std::pair<NodeType, ClusterId>, bool>& node_cluster) {
        return node_cluster.first;
      }).Collapse(),
    false);
}
} // LocalMoving
