#pragma once

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/zip_with_index.hpp>

#include <vector>

#include "seq_louvain/graph.hpp"
#include "seq_louvain/cluster_store.hpp"
#include "modularity.hpp"

#include "input.hpp"
#include "util.hpp"
#include "thrill_modularity.hpp"

namespace Louvain {

using NodeCluster = std::pair<NodeId, ClusterId>;

template<class NodeType, class EdgeType, class F>
thrill::DIA<NodeCluster> louvain(const DiaGraph<NodeType, EdgeType>& graph, const F& local_moving) {
  auto node_clusters = local_moving(graph);
  auto distinct_cluster_ids = node_clusters.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq();
  size_t cluster_count = distinct_cluster_ids.Keep().Size();

  if (graph.node_count == cluster_count) {
    // TODO consume graph.edges
    distinct_cluster_ids.Size(); // consume it
    return node_clusters.Collapse();
  }

  auto cleaned_node_clusters = distinct_cluster_ids
    .ZipWithIndex([](const ClusterId cluster_id, const size_t index) { return std::make_pair(cluster_id, index); })
    .InnerJoin(node_clusters,
      [](const std::pair<ClusterId, size_t>& pair){ return pair.first; },
      [](const NodeCluster& node_cluster) { return node_cluster.second; },
      [](const std::pair<ClusterId, size_t>& pair, const NodeCluster& node_cluster) {
        return NodeCluster(node_cluster.first, pair.second);
      })
    .Cache()
    .Keep(); // consumed twice -> keep once

  // Build Meta Graph
  auto meta_graph_edges = graph.edges
    .InnerJoin(cleaned_node_clusters,
      [](const EdgeType& edge) { return edge.tail; },
      [](const NodeCluster& node_cluster) { return node_cluster.first; },
      [](EdgeType edge, const NodeCluster& node_cluster) {
          edge.tail = node_cluster.second;
          return edge;
      })
    .InnerJoin(cleaned_node_clusters,
      [](const EdgeType& edge) { return edge.head; },
      [](const NodeCluster& node_cluster) { return node_cluster.first; },
      [](EdgeType edge, const NodeCluster& node_cluster) {
          edge.head = node_cluster.second;
          return edge;
      })
    .Map([](EdgeType edge) { return WeightedEdge { edge.tail, edge.head, edge.getWeight() }; })
    .ReduceByKey(
      [](WeightedEdge edge) { return Util::combine_u32ints(edge.tail, edge.head); },
      [](WeightedEdge edge1, WeightedEdge edge2) { return WeightedEdge { edge1.tail, edge1.head, edge1.weight + edge2.weight }; })
    // turn loops into forward and backward arc
    .template FlatMap<WeightedEdge>(
      [](const WeightedEdge & edge, auto emit) {
        if (edge.tail == edge.head) {
          assert(edge.weight % 2 == 0);
          emit(WeightedEdge { edge.tail, edge.head, edge.weight / 2 });
          emit(WeightedEdge { edge.tail, edge.head, edge.weight / 2 });
        } else {
          emit(edge);
        }
      })
    .Cache()
    .Keep();

  auto nodes = edgesToNodes(meta_graph_edges, cluster_count).Collapse();
  assert(meta_graph_edges.Map([](const WeightedEdge& edge) { return edge.getWeight(); }).Sum() / 2 == graph.total_weight);

  return louvain(DiaGraph<NodeWithWeightedLinks, WeightedEdge> { nodes, meta_graph_edges, cluster_count, graph.total_weight }, local_moving)
    .InnerJoin(cleaned_node_clusters,
      [](const NodeCluster& meta_node_cluster) { return meta_node_cluster.first; },
      [](const NodeCluster& node_cluster) { return node_cluster.second; },
      [](const NodeCluster& meta_node_cluster, const NodeCluster& node_cluster) {
          return NodeCluster(node_cluster.first, meta_node_cluster.second);
      });
}

template<class F>
auto performAndEvaluate(int argc, char const *argv[], const std::string& algo, const F& run) {
  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    auto graph = Input::readGraph(argv[1], context);

    Logging::Id program_run_logging_id;
    if (context.my_rank() == 0) {
      program_run_logging_id = Logging::getUnusedId();
      Logging::report("program_run", program_run_logging_id, "binary", argv[0]);
      Logging::report("program_run", program_run_logging_id, "graph", argv[1]);
      Logging::report("program_run", program_run_logging_id, "node_count", graph.node_count);
      Logging::report("program_run", program_run_logging_id, "edge_count", graph.total_weight);
    }

    graph.edges.Keep();
    auto node_clusters = run(graph);

    graph.edges.Keep();
    double modularity = Modularity::modularity(graph, node_clusters.Keep());
    size_t cluster_count = node_clusters.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq().Size();

    auto local_node_clusters = node_clusters.Gather();

    ClusterStore clusters(context.my_rank() == 0 ? graph.node_count : 0);
    Logging::Id clusters_logging_id;
    if (context.my_rank() == 0) {
      Logging::Id algorithm_run_id = Logging::getUnusedId();
      Logging::report("algorithm_run", algorithm_run_id, "program_run_id", program_run_logging_id);
      Logging::report("algorithm_run", algorithm_run_id, "algorithm", algo);
      clusters_logging_id = Logging::getUnusedId();
      Logging::report("clustering", clusters_logging_id, "source", "computation");
      Logging::report("clustering", clusters_logging_id, "algorithm_run_id", algorithm_run_id);
      Logging::report("clustering", clusters_logging_id, "modularity", modularity);
      Logging::report("clustering", clusters_logging_id, "cluster_count", cluster_count);

      for (const auto& node_cluster : local_node_clusters) {
        clusters.set(node_cluster.first, node_cluster.second);
      }
    }
    if (argc > 2) {
      auto ground_proof = thrill::ReadBinary<NodeCluster>(context, argv[2]);

      modularity = Modularity::modularity(graph, ground_proof.Keep());
      cluster_count = ground_proof.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq().Size();

      auto local_ground_proof = ground_proof.Gather();

      if (context.my_rank() == 0) {
        Logging::report("program_run", program_run_logging_id, "ground_proof", argv[2]);

        ClusterStore ground_proof_clusters(graph.node_count);
        for (const auto& node_cluster : local_ground_proof) {
          ground_proof_clusters.set(node_cluster.first, node_cluster.second);
        }

        Logging::Id ground_proof_logging_id = Logging::getUnusedId();
        Logging::report("clustering", ground_proof_logging_id, "source", "ground_proof");
        Logging::report("clustering", ground_proof_logging_id, "program_run_id", program_run_logging_id);
        Logging::report("clustering", ground_proof_logging_id, "modularity", modularity);
        Logging::report("clustering", ground_proof_logging_id, "cluster_count", cluster_count);
        Logging::log_comparison_results(ground_proof_logging_id, ground_proof_clusters, clusters_logging_id, clusters);
      }
    }
  });
}

} // Louvain
