#include <thrill/api/gather.hpp>
#include <thrill/api/zip.hpp>

#include "algo/thrill/modularity.hpp"
#include "algo/thrill/louvain.hpp"
#include "util/thrill/input.hpp"

#include "data/ghost_cluster_store.hpp"
#include "util/logging.hpp"

using int128_t = __int128_t;

int main(int, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    std::pair<std::string, std::string> algo_clustering_input = Logging::parse_input_with_logging_id(argv[2]);
    auto node_clusters = Input::readClustering(algo_clustering_input.first, context)
      .Sort([](const NodeCluster& nc1, const NodeCluster& nc2) { return nc1.first < nc2.first; });

    auto graph = Input::readToNodeGraph(argv[1], context);

    Logging::Id program_run_logging_id;
    if (context.my_rank() == 0) {
      program_run_logging_id = Logging::getUnusedId();
      Logging::report("program_run", program_run_logging_id, "binary", argv[0]);
      Logging::report("program_run", program_run_logging_id, "hosts", context.num_hosts());
      Logging::report("program_run", program_run_logging_id, "total_workers", context.num_workers());
      Logging::report("program_run", program_run_logging_id, "workers_per_host", context.workers_per_host());
      Logging::report("program_run", program_run_logging_id, "graph", argv[1]);
      Logging::report("program_run", program_run_logging_id, "node_count", graph.node_count);
      Logging::report("program_run", program_run_logging_id, "edge_count", graph.total_weight);
      if (getenv("MOAB_JOBID")) {
        Logging::report("program_run", program_run_logging_id, "job_id", getenv("MOAB_JOBID"));
      }
    }

    size_t cluster_count = node_clusters.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq().Size();

    auto edges = graph.nodes
      .Zip(node_clusters,
        [](const NodeWithLinks& node, const NodeCluster& node_cluster) {
          assert(node.id == node_cluster.first);
          return std::make_pair(node, node_cluster.second);
        })
      .template FlatMap<Edge>(
        [](const std::pair<NodeWithLinks, ClusterId>& node_cluster, auto emit) {
          for (const auto& link : node_cluster.first.links) {
            emit(Edge { link.target, node_cluster.second });
          }
        });

    auto cluster_degrees_and_inside_weights = edgesToNodes(edges, graph.node_count)
      .Zip(node_clusters,
        [](const NodeWithLinks& node, const NodeCluster& node_cluster) {
          assert(node.id == node_cluster.first);
          return std::make_pair(node, node_cluster.second);
        })
      .template FlatMap<Edge>(
        [](const std::pair<NodeWithLinks, ClusterId>& node_cluster, auto emit) {
          for (const auto& link : node_cluster.first.links) {
            emit(Edge { node_cluster.second, link.target });
          }
        })
      .Map([](const Edge& edge) { return WeightedEdge { edge.tail, edge.head, edge.getWeight() }; })
      .ReduceByKey(
        [](const WeightedEdge& edge) { return Util::combine_u32ints(edge.tail, edge.head); },
        [](const WeightedEdge& edge1, const WeightedEdge& edge2) { return WeightedEdge { edge1.tail, edge1.head, edge1.weight + edge2.weight }; })
      .Map([](const WeightedEdge& edge) { return std::make_pair(edge.tail, std::pair<uint64_t, uint64_t>(edge.getWeight(), edge.tail == edge.head ? edge.getWeight() : 0)); })
      .ReducePair(
        [](const std::pair<uint64_t, uint64_t>& weights1, const std::pair<uint64_t, uint64_t>& weights2) {
          return std::make_pair(weights1.first + weights2.first, weights1.second + weights2.second);
        })
      .Map(
        [](const std::pair<ClusterId, std::pair<uint64_t, uint64_t>>& weights) {
          return std::make_pair(int128_t(weights.second.first) * int128_t(weights.second.first), weights.second.second);
        })
      .AllReduce(
        [](const std::pair<int128_t, uint64_t>& weights1, const std::pair<int128_t, uint64_t>& weights2) {
          return std::make_pair(weights1.first + weights2.first, weights1.second + weights2.second);
        });

    double modularity = (cluster_degrees_and_inside_weights.second / (2.* graph.total_weight)) - (cluster_degrees_and_inside_weights.first / (4.*graph.total_weight*graph.total_weight));

    if (context.my_rank() == 0) {
      Logging::report("clustering", algo_clustering_input.second, "path", algo_clustering_input.first);
      Logging::report("clustering", algo_clustering_input.second, "source", "ground_truth");
      Logging::report("clustering", algo_clustering_input.second, "modularity", modularity);
      Logging::report("clustering", algo_clustering_input.second, "cluster_count", cluster_count);
    }
  });
}

