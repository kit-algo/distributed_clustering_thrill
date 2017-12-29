#pragma once

#include <thrill/api/cache.hpp>
#include <thrill/api/collect_local.hpp>
#include <thrill/api/fold_by_key.hpp>
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

#include "util/util.hpp"
#include "data/thrill/graph.hpp"
#include "data/local_dia_graph.hpp"
#include "algo/thrill/partitioning.hpp"

#include "data/ghost_graph.hpp"
#include "data/ghost_cluster_store.hpp"
#include "algo/louvain.hpp"
#include "algo/partitioning.hpp"

namespace LocalMoving {

using int128_t = __int128_t;

struct NodeClusterLink {
  Weight inbetween_weight = 0;
  Weight total_weight = 0;
};

struct IncidentClusterInfo {
  NodeId node_id;
  ClusterId cluster;
  Weight inbetween_weight;
  Weight total_weight;
};

int128_t deltaModularity(const Weight node_degree, const IncidentClusterInfo& neighbored_cluster, Weight total_weight) {
  int128_t e = int128_t(neighbored_cluster.inbetween_weight) * total_weight * 2;
  int128_t a = int128_t(neighbored_cluster.total_weight) * node_degree;
  return e - a;
}

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
  #if defined(SWITCH_TO_SEQ)
    if (graph.node_count < 1000000) {
      auto local_nodes = graph.nodes.Gather();
      std::vector<ClusterId> local_result(local_nodes.size());

      if (graph.nodes.context().my_rank() == 0) {
        Logging::Id seq_algo_logging_id = Logging::getUnusedId();
        Logging::report("algorithm_run", seq_algo_logging_id, "distributed_algorithm_run_id", level_logging_id);
        Logging::report("algorithm_run", seq_algo_logging_id, "algorithm", "sequential louvain");
        LocalDiaGraph<NodeType> local_graph(local_nodes, graph.total_weight);
        ClusterStore clusters(graph.node_count);
        Louvain::louvainModularity(local_graph, clusters, seq_algo_logging_id);

        for (NodeId node = 0; node < graph.node_count; node++) {
          local_result[node] = clusters[node];
        }
      }

      auto nodes = thrill::api::Distribute(graph.nodes.context(), local_nodes);
      auto clusters = thrill::api::Distribute(graph.nodes.context(), local_result);

      return std::make_pair(nodes.Zip(clusters, [](const NodeType& node, const ClusterId& cluster) { return std::make_pair(node, cluster); }).Collapse(), true);
    }
  #endif

  thrill::common::Range id_range = thrill::common::CalculateLocalRange(graph.node_count, graph.nodes.context().num_workers(), graph.nodes.context().my_rank());
  std::vector<Weight> node_degrees;
  node_degrees.reserve(id_range.size());
  graph.nodes.Keep().Map([](const NodeType& node) { return node.weightedDegree(); }).CollectLocal(&node_degrees);
  assert(node_degrees.size() == id_range.size());

  auto reduceToBestCluster = [&graph, &id_range, &node_degrees](const auto& incoming) {
    return incoming
      // Reduce to best cluster
      .ReduceToIndexWithoutPrecombine(
        [](const IncidentClusterInfo& lme) -> size_t { return lme.node_id; },
        [total_weight = graph.total_weight, &id_range, &node_degrees](const IncidentClusterInfo& lme1, const IncidentClusterInfo& lme2) {
          assert(lme1.node_id >= id_range.begin && lme1.node_id < id_range.end);
          assert(lme2.node_id >= id_range.begin && lme2.node_id < id_range.end);

          int128_t d1 = deltaModularity(node_degrees[lme2.node_id - id_range.begin], lme1, total_weight);
          int128_t d2 = deltaModularity(node_degrees[lme2.node_id - id_range.begin], lme2, total_weight);

          // TODO Tie breaking
          if (d1 > d2) {
            return lme1;
          } else {
            return lme2;
          }
        },
        graph.node_count);
  };


  auto node_clusters = graph.nodes
    .Map([](const NodeType& node) { return std::make_pair(std::make_pair(node, node.id), false); })
    .Collapse();

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

    size_t considered_nodes_estimate = graph.node_count * rate / 1000;

    if (considered_nodes_estimate > 0) {
      node_clusters = (iteration == 0 ?
        reduceToBestCluster(node_clusters
          .template FlatMap<IncidentClusterInfo>(
            [&included](const std::pair<std::pair<NodeType, ClusterId>, bool>& node_cluster_moved, auto emit) {
              const auto& node_cluster = node_cluster_moved.first;

              if (included(node_cluster.first.id)) {
                emit(IncidentClusterInfo {
                  node_cluster.first.id,
                  node_cluster.second,
                  0,
                  node_cluster.first.weightedDegree()
                });
              }

              for (const typename NodeType::LinkType& link : node_cluster.first.links) {
                if (included(link.target)) {
                 emit(IncidentClusterInfo {
                   link.target,
                   node_cluster.second,
                   link.getWeight(),
                   node_cluster.first.weightedDegree()
                 });
                }
              }
            })) :
        reduceToBestCluster(node_clusters
          .template FoldByKey<std::vector<NodeType>>(thrill::NoDuplicateDetectionTag,
            [](const std::pair<std::pair<NodeType, ClusterId>, bool>& node_cluster) { return node_cluster.first.second; },
            [](std::vector<NodeType>&& acc, const std::pair<std::pair<NodeType, ClusterId>, bool>& node_cluster) {
              acc.push_back(node_cluster.first.first);
              return std::move(acc);
            })
          .template FlatMap<IncidentClusterInfo>(
            [&included, node_count = graph.node_count](const std::pair<ClusterId, std::vector<NodeType>>& cluster_nodes, auto emit) {
              Weight total_weight = 0;
              for (const NodeType& node : cluster_nodes.second) {
                total_weight += node.weightedDegree();
              }

              if (cluster_nodes.second.size() == 1) {
                auto& node = cluster_nodes.second[0];
                for (const typename NodeType::LinkType& link : node.links) {
                  if (node.id != link.target && included(link.target)) {
                    emit(IncidentClusterInfo {
                      link.target,
                      cluster_nodes.first,
                      link.getWeight(),
                      total_weight
                    });
                  }
                }

                if (included(node.id)) {
                  emit(IncidentClusterInfo {
                    node.id,
                    cluster_nodes.first,
                    0,
                    0
                  });
                }
              } else {
                spp::sparse_hash_map<NodeId, Weight> node_cluster_links;
                for (const NodeType& node : cluster_nodes.second) {
                  for (const typename NodeType::LinkType& link : node.links) {
                    if (node.id != link.target && included(link.target)) {
                      node_cluster_links[link.target] += link.getWeight();
                    }
                  }
                }

                for (const NodeType& node : cluster_nodes.second) {
                  if (included(node.id)) {
                    Weight node_cluster_link_weight = 0;
                    auto it = node_cluster_links.find(node.id);
                    if (it != node_cluster_links.end()) {
                      node_cluster_link_weight = it->second;
                      node_cluster_links.erase(it);
                    }
                    emit(IncidentClusterInfo {
                      node.id,
                      cluster_nodes.first,
                      node_cluster_link_weight,
                      total_weight - node.weightedDegree()
                    });
                  }
                }

                for (const auto& node_cluster_link : node_cluster_links) {
                  emit(IncidentClusterInfo {
                    node_cluster_link.first,
                    cluster_nodes.first,
                    node_cluster_link.second,
                    total_weight
                  });
                }
              }
            }))
          )
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
    #if defined(NO_CONTRACTION)
      true);
    #else
      false);
    #endif
}

template<class Graph>
auto partitionedLocalMoving(const Graph& graph, Logging::Id loggin_id) {
  constexpr bool weighted = std::is_same<typename Graph::Node, NodeWithWeightedLinks>::value;
  using Node = typename std::conditional<weighted, NodeWithWeightedLinksAndTargetDegree, NodeWithLinksAndTargetDegree>::type;

  uint32_t partition_size = graph.nodes.context().num_workers();
  uint32_t partition_element_size = Partitioning::partitionElementTargetSize(graph.node_count, partition_size);
  if (partition_element_size < 100000 && graph.node_count < 1000000) {
    partition_size = 1;
    partition_element_size = graph.node_count;
  }
  auto node_partitions = partition(graph, partition_size);

  using EdgeWithTargetDegreeType = typename std::conditional<weighted, std::tuple<NodeId, typename Graph::Node::LinkType, Weight>, std::tuple<NodeId, typename Graph::Node::LinkType, uint32_t>>::type;
  auto nodes = graph.nodes
    .template FlatMap<EdgeWithTargetDegreeType>(
      [](const typename Graph::Node& node, auto emit) {
        for (typename Graph::Node::LinkType link : node.links) {
          NodeId old_target = link.target;
          link.target = node.id;
          emit(EdgeWithTargetDegreeType(old_target, link, node.weightedDegree()));
        }
      })
    .template GroupToIndex<Node>(
      [](const EdgeWithTargetDegreeType& edge_with_target_degree) { return std::get<0>(edge_with_target_degree); },
      [](auto& iterator, const NodeId node_id) {
        Node node { node_id, {} };
        while (iterator.HasNext()) {
          const EdgeWithTargetDegreeType& edge_with_target_degree = iterator.Next();
          node.push_back(Node::LinkType::fromLink(std::get<1>(edge_with_target_degree), std::get<2>(edge_with_target_degree)));
        }
        return node;
      },
      graph.node_count);

  return std::make_pair(nodes
    .Zip(thrill::NoRebalanceTag, node_partitions,
      [](const Node& node, const NodePartition& node_partition) {
        assert(node.id == node_partition.node_id);
        return std::make_pair(node, node_partition.partition);
      })
    // Local Moving
    .template GroupToIndex<std::vector<std::pair<typename Graph::Node, ClusterId>>>(
      [](const std::pair<Node, uint32_t>& node_partition) -> size_t { return node_partition.second; },
      [total_weight = graph.total_weight, partition_element_size, partition_size, loggin_id](auto& iterator, const uint32_t) {
        // TODO deterministic random
        GhostGraph<weighted> graph(partition_element_size, total_weight);
        const std::vector<typename Graph::Node> reverse_mapping = graph.template initialize<typename Graph::Node>(
          [&iterator](const auto& emit) {
            while (iterator.HasNext()) {
              emit(iterator.Next().first);
            }
          });

        std::vector<std::pair<typename Graph::Node, ClusterId>> mapping;
        mapping.reserve(graph.getNodeCount());
        if (partition_size > 1) {
          GhostClusterStore clusters(graph.getNodeCount());

          Modularity::localMoving(graph, clusters);

          clusters.rewriteClusterIds(reverse_mapping);
          for (NodeId node = 0; node < graph.getNodeCount(); node++) {
            mapping.emplace_back(reverse_mapping[node], clusters[node]);
          }
        } else {
          ClusterStore clusters(graph.getNodeCount());

          Logging::Id seq_algo_logging_id = Logging::getUnusedId();
          Logging::report("algorithm_run", seq_algo_logging_id, "distributed_algorithm_run_id", loggin_id);
          Logging::report("algorithm_run", seq_algo_logging_id, "algorithm", "sequential louvain");
          Louvain::louvainModularity(graph, clusters, seq_algo_logging_id);

          clusters.rewriteClusterIds();

          for (NodeId node = 0; node < graph.getNodeCount(); node++) {
            mapping.emplace_back(reverse_mapping[node], clusters[node]);
          }
        }
        return mapping;
      }, partition_size)
    .template FlatMap<std::pair<typename Graph::Node, ClusterId>>(
      [](const std::vector<std::pair<typename Graph::Node, ClusterId>>& mapping, auto emit) {
        for (const auto& pair : mapping) {
          emit(pair);
        }
      })
    .ReduceToIndex(
      [](const std::pair<typename Graph::Node, ClusterId>& node_cluster) -> size_t { return node_cluster.first.id; },
      [](const std::pair<typename Graph::Node, ClusterId>& node_cluster, const std::pair<typename Graph::Node, ClusterId>&) { assert(false); return node_cluster; },
      graph.node_count),
    partition_size == 1);
}

} // LocalMoving
