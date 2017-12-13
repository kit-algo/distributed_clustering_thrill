#pragma once

#include "data/graph.hpp"
#include "data/cluster_store.hpp"
#include "algo/contraction.hpp"
#include "util/logging.hpp"
#include "algo/modularity.hpp"
#include "algo/map_eq.hpp"

#include <algorithm>
#include <iostream>
#include <assert.h>
#include <cmath>
#include <cstdint>
#include <algorithm>
#include <random>

namespace Louvain {

using int128_t = __int128_t;

std::default_random_engine rng;

using NodeId = typename Graph::NodeId;
using EdgeId = typename Graph::EdgeId;
using Weight = typename Graph::Weight;
using ClusterId = typename ClusterStore::ClusterId;

template<class GraphType, class ClusterStoreType, typename F>
void contractAndReapply(const GraphType &, ClusterStoreType &, uint64_t, uint32_t, const F& f);

template<class GraphType, class ClusterStoreType>
void louvainModularity(const GraphType& graph, ClusterStoreType &clusters, uint64_t algo_run_id, uint32_t level = 0) {
  assert(std::abs(Modularity::modularity(graph, ClusterStore(graph.getNodeCount(), 0))) == 0);

  bool changed = Modularity::localMoving(graph, clusters);

  if (changed) {
    contractAndReapply(graph, clusters, algo_run_id, level, [algo_run_id](const auto& meta_graph, auto& meta_clusters, uint32_t level) {
      return louvainModularity(meta_graph, meta_clusters, algo_run_id, level);
    });
  }
}

template<class GraphType, class ClusterStoreType>
void louvainMapEq(const GraphType& graph, ClusterStoreType &clusters, uint64_t algo_run_id, uint32_t level = 0) {
  bool changed = MapEq::localMoving(graph, clusters);

  if (changed) {
    contractAndReapply(graph, clusters, algo_run_id, level, [algo_run_id](const auto& meta_graph, auto& meta_clusters, uint32_t level) {
      return louvainMapEq(meta_graph, meta_clusters, algo_run_id, level);
    });
  }
}

template<bool move_to_ghosts = true>
void partitionedLouvain(const Graph& graph, ClusterStore &clusters, const std::vector<uint32_t>& partitions, uint64_t algo_run_id, const std::vector<Logging::Id>& partition_element_logging_ids) {
  assert(partitions.size() == graph.getNodeCount());
  clusters.resetBounds();
  uint32_t partition_count = *std::max_element(partitions.begin(), partitions.end()) + 1;
  assert(partition_count == partition_element_logging_ids.size());

  std::vector<std::vector<NodeId>> partition_nodes(partition_count);
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    partition_nodes[partitions[node]].push_back(node);
  }

  ClusterId minimum_partition_cluster_id = 0;
  ClusterStore partition_clustering(graph.getNodeCount());
  for (uint32_t partition = 0; partition < partition_nodes.size(); partition++) {
    partition_clustering.resetBounds();
    Modularity::localMoving<Graph, ClusterStore, move_to_ghosts>(graph, partition_clustering, partition_nodes[partition]);

    if (move_to_ghosts) {
      uint32_t nodes_in_ghost_clusters = 0;
      for (const NodeId node : partition_nodes[partition]) {
        if (partitions[partition_clustering[node]] != partition) {
          nodes_in_ghost_clusters++;
        }
      }
      Logging::report("partition_element", partition_element_logging_ids[partition], "nodes_in_ghost_clusters", nodes_in_ghost_clusters);
    }

    minimum_partition_cluster_id = partition_clustering.rewriteClusterIds(partition_nodes[partition], minimum_partition_cluster_id);

    for (NodeId node : partition_nodes[partition]) {
      clusters.set(node, partition_clustering[node]);
    }
  }

  contractAndReapply(graph, clusters, algo_run_id, 0, [algo_run_id](const auto& meta_graph, auto& meta_clusters, uint32_t level) {
    return louvainModularity(meta_graph, meta_clusters, algo_run_id, level);
  });
}

template<class GraphType, class ClusterStoreType, typename F>
void contractAndReapply(const GraphType& graph, ClusterStoreType &clusters, uint64_t algo_run_id, uint32_t level, const F& f) {
  Graph meta_graph = Contraction::contract(graph, clusters);

  uint64_t level_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_level", level_logging_id, "algorithm_run_id", algo_run_id);
  Logging::report("algorithm_level", level_logging_id, "node_count", graph.getNodeCount());
  Logging::report("algorithm_level", level_logging_id, "cluster_count", meta_graph.getNodeCount());
  Logging::report("algorithm_level", level_logging_id, "level", level);

  // uint64_t distribution_logging_id = Logging::getUnusedId();
  // Logging::report("cluster_size_distribution", distribution_logging_id, "algorithm_level_id", level_logging_id);
  // std::map<uint32_t, uint32_t> size_distribution;
  // clusters.clusterSizeDistribution(size_distribution);
  // for (auto & size_count : size_distribution) {
  //   Logging::report("cluster_size_distribution", distribution_logging_id, size_count.first, size_count.second);
  // }

  assert(graph.getTotalWeight() == meta_graph.getTotalWeight());
  ClusterStore meta_singleton(meta_graph.getNodeCount());
  meta_singleton.assignSingletonClusterIds();
  assert(Modularity::modularity(graph, clusters) == Modularity::modularity(meta_graph, meta_singleton));

  ClusterStore meta_clusters(meta_graph.getNodeCount());
  f(meta_graph, meta_clusters, level + 1);

  // translate meta clusters
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    clusters.set(node, meta_clusters[clusters[node]]);
  }
}

Logging::Id log_clustering(const Graph& graph, const ClusterStore& clusters) {
  Logging::Id logging_id = Logging::getUnusedId();
  Logging::report("clustering", logging_id, "cluster_count", clusters.clusterCount());
  Logging::report("clustering", logging_id, "modularity", Modularity::modularity(graph, clusters));
  Logging::report("clustering", logging_id, "map_equation", MapEq::mapEquation(graph, clusters));
  return logging_id;
}

};
