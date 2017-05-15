#include "graph.hpp"
#include "modularity.hpp"
#include "cluster_store.hpp"
#include "similarity.hpp"
#include "partitioning.hpp"
#include "logging.hpp"
#include "input.hpp"

#include <iostream>
#include <string>
#include <numeric>
#include <assert.h>
#include <random>

#include <thrill/common/stats_timer.hpp>

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

  Logging::Id algo_run_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_run", algo_run_logging_id, "program_run_id", run_id);
  Logging::report("algorithm_run", algo_run_logging_id, "algorithm", "sequential louvain");

  thrill::common::StatsTimerBase<true> timer(/* autostart */ true);
  Modularity::louvain(graph, clusters, algo_run_logging_id);
  Logging::report("algorithm_run", algo_run_logging_id, "runtime", timer.Microseconds() / 1000000.0);

  Logging::Id cluster_logging_id = Modularity::log_clustering(graph, clusters);
  Logging::report("clustering", cluster_logging_id, "source", "computation");
  Logging::report("clustering", cluster_logging_id, "algorithm_run_id", algo_run_logging_id);

  if (input.shouldWriteOutput()) {
    Logging::report("clustering", cluster_logging_id, "path", input.outputFile());
    IO::write_clustering(input.outputFile(), clusters);
  }
}
