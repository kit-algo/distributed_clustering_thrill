#include "graph.hpp"
#include "modularity.hpp"
#include "cluster_store.hpp"
#include "logging.hpp"
#include "partitioning.hpp"
#include "input.hpp"

#include <iostream>
#include <string>
#include <numeric>
#include <assert.h>
#include <random>
#include <chrono>

int main(int argc, char const *argv[]) {
  Logging::Id run_id = Logging::getUnusedId();
  Input input(argc, argv, run_id);
  input.initialize();

  if (!input.shouldRun()) {
    return input.getExitCode();
  }

  Modularity::rng = std::default_random_engine(input.getSeed());
  const Graph& graph = input.getGraph();

  ClusterStore clusters(graph.getNodeCount());

  input.forEachPartition([&](const std::vector<uint32_t>& node_partition_elements, const std::string& logging_id) {
    std::vector<Logging::Id> partition_element_logging_ids = Partitioning::analyse(graph, node_partition_elements, logging_id);

    uint64_t algo_run_logging_id = Logging::getUnusedId();
    Logging::report("algorithm_run", algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", algo_run_logging_id, "order", "original");
    Logging::report("algorithm_run", algo_run_logging_id, "partition_id", logging_id);

    Modularity::partitionedLouvain(graph, clusters, node_partition_elements, algo_run_logging_id, partition_element_logging_ids);
  });
}

