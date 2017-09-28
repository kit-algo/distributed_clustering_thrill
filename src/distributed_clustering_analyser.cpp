#include <thrill/api/gather.hpp>
#include <thrill/api/zip.hpp>

#include "algo/thrill/clustering_quality.hpp"
#include "algo/thrill/louvain.hpp"
#include "util/thrill/input.hpp"

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

    graph.nodes.Keep();
    double modularity = ClusteringQuality::modularity(graph, node_clusters.Keep());
    double map_eq = ClusteringQuality::mapEquation(graph, node_clusters);

    if (context.my_rank() == 0) {
      Logging::report("clustering", algo_clustering_input.second, "path", algo_clustering_input.first);
      Logging::report("clustering", algo_clustering_input.second, "source", "ground_truth");
      Logging::report("clustering", algo_clustering_input.second, "modularity", modularity);
      Logging::report("clustering", algo_clustering_input.second, "map_equation", map_eq);
      Logging::report("clustering", algo_clustering_input.second, "cluster_count", cluster_count);
    }
  });
}

