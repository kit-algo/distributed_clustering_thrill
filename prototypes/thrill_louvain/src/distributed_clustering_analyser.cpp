#include <thrill/api/gather.hpp>
#include <thrill/api/zip.hpp>

#include "thrill_modularity.hpp"
#include "thrill_louvain.hpp"
#include "input.hpp"

#include "seq_louvain/cluster_store.hpp"
#include "logging.hpp"

int main(int, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    std::pair<std::string, std::string> algo_clustering_input = Logging::parse_input_with_logging_id(argv[2]);
    auto node_clusters = Input::readClustering(algo_clustering_input.first, context);

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

    Logging::Id algorithm_run_id = 0;
    if (context.my_rank() == 0) {
      algorithm_run_id = Logging::getUnusedId();
    }
    auto result = Louvain::louvain(graph, algorithm_run_id, [node_count = graph.node_count, &node_clusters](const auto& inner_graph, Logging::Id) {
      using NodeType = typename decltype(inner_graph.nodes)::ValueType;

      if (std::is_same<NodeType, NodeWithLinks>::value) {
        return node_clusters
          .Sort([](const NodeCluster& nc1, const NodeCluster& nc2) { return nc1.first < nc2.first; })
          .Zip(inner_graph.nodes,
            [](const NodeCluster& node_cluster, const NodeType& node) {
              assert(node.id == node_cluster.first);
              return std::make_pair(node, node_cluster.second);
            });
      } else {
        return inner_graph.nodes
          .Map([](const NodeType& node) { return std::make_pair(node, node.id); })
          .Collapse();
      }
    });

    size_t cluster_count = result.second.node_count;
    double modularity = Modularity::modularity(result.second);

    if (context.my_rank() == 0) {
      Logging::report("clustering", algo_clustering_input.second, "path", algo_clustering_input.first);
      Logging::report("clustering", algo_clustering_input.second, "source", "ground_truth");
      Logging::report("clustering", algo_clustering_input.second, "modularity", modularity);
      Logging::report("clustering", algo_clustering_input.second, "cluster_count", cluster_count);
    }
  });
}

