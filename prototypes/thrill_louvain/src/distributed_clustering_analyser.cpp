#include <thrill/api/gather.hpp>

#include "thrill_modularity.hpp"
#include "input.hpp"

#include "seq_louvain/cluster_store.hpp"
#include "logging.hpp"

int main(int argc, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    auto graph = Input::readToEdgeGraph(argv[1], context);

    std::pair<std::string, std::string> algo_clustering_input = Logging::parse_input_with_logging_id(argv[2]);
    auto node_clusters = Input::readClustering(algo_clustering_input.first, context);

    if (argc > 3) { graph.edges.Keep(); }
    double modularity = Modularity::modularity(graph, node_clusters.Keep());
    size_t cluster_count = node_clusters.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq().Size();

    auto local_node_clusters = node_clusters.Gather();

    ClusterStore clusters(context.my_rank() == 0 ? graph.node_count : 0);
    if (context.my_rank() == 0) {
      Logging::report("clustering", algo_clustering_input.second, "path", algo_clustering_input.first);
      Logging::report("clustering", algo_clustering_input.second, "modularity", modularity);
      Logging::report("clustering", algo_clustering_input.second, "cluster_count", cluster_count);

      for (const auto& node_cluster : local_node_clusters) {
        clusters.set(node_cluster.first, node_cluster.second);
      }
    }
    if (argc > 3) {
      std::pair<std::string, std::string> ground_truth_input = Logging::parse_input_with_logging_id(argv[2]);
      auto ground_truth = Input::readClustering(argv[3], context);

      modularity = Modularity::modularity(graph, ground_truth.Keep());
      cluster_count = ground_truth.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq().Size();

      auto local_ground_truth = ground_truth.Gather();

      if (context.my_rank() == 0) {
        ClusterStore ground_truth_clusters(graph.node_count);
        for (const auto& node_cluster : local_ground_truth) {
          ground_truth_clusters.set(node_cluster.first, node_cluster.second);
        }

        Logging::report("clustering", ground_truth_input.second, "path", ground_truth_input.first);
        Logging::report("clustering", ground_truth_input.second, "source", "ground_truth");
        Logging::report("clustering", ground_truth_input.second, "modularity", modularity);
        Logging::report("clustering", ground_truth_input.second, "cluster_count", cluster_count);
        Logging::log_comparison_results(ground_truth_input.second, ground_truth_clusters, algo_clustering_input.second, clusters);
      }
    }
  });
}

