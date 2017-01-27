#include "graph.hpp"
#include "modularity.hpp"
#include "cluster_store.hpp"
#include "logging.hpp"
#include "io.hpp"

#include <iostream>
#include <string>
#include <numeric>
#include <assert.h>
#include <random>
#include <chrono>

int main(int argc, char const *argv[]) {
  uint64_t run_id = Logging::getUnusedId();

  std::vector<std::vector<Graph::NodeId>> neighbors;
  Graph::EdgeId edge_count = IO::read_graph(argv[1], neighbors);
  Graph graph(neighbors.size(), edge_count);
  graph.setEdgesByAdjacencyLists(neighbors);

  // unsigned seed = 3920764003;
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  Modularity::rng = std::default_random_engine(seed);

  Logging::report("program_run", run_id, "binary", argv[0]);
  Logging::report("program_run", run_id, "graph", argv[1]);
  Logging::report("program_run", run_id, "node_count", graph.getNodeCount());
  Logging::report("program_run", run_id, "edge_count", graph.getEdgeCount());
  Logging::report("program_run", run_id, "seed", seed);

  ClusterStore clusters(neighbors.size());

  for (int i = 2; i < argc; i += 2) {
    std::vector<uint32_t> node_partition_elements(graph.getNodeCount());
    IO::read_partition(argv[i], node_partition_elements);

    uint64_t algo_run_logging_id = Logging::getUnusedId();
    Logging::report("algorithm_run", algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", algo_run_logging_id, "order", "original");
    Logging::report("algorithm_run", algo_run_logging_id, "partition_id", argv[i+1]);

    Modularity::partitionedLouvain(graph, clusters, node_partition_elements, algo_run_logging_id);
  }
}

