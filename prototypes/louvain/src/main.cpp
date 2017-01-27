#include "graph.hpp"
#include "modularity.hpp"
#include "cluster_store.hpp"
#include "similarity.hpp"
#include "partitioning.hpp"
#include "logging.hpp"
#include "io.hpp"

#include <iostream>
#include <string>
#include <numeric>
#include <assert.h>
#include <random>
#include <chrono>

Logging::Id log_clustering(const Graph & graph, const ClusterStore & clusters) {
  Logging::Id logging_id = Logging::getUnusedId();
  Logging::report("clustering", logging_id, "modularity", Modularity::modularity(graph, clusters));
  return logging_id;
}

void log_comparison_results(Logging::Id base_clustering_id, const ClusterStore & base_clusters, Logging::Id compare_clustering_id, const ClusterStore & compare_clusters) {
  Logging::Id comparison_id = Logging::getUnusedId();
  Logging::report("clustering_comparison", comparison_id, "base_clustering_id", base_clustering_id);
  Logging::report("clustering_comparison", comparison_id, "compare_clustering_id", compare_clustering_id);
  Logging::report("clustering_comparison", comparison_id, "NMI", Similarity::normalizedMutualInformation(base_clusters, compare_clusters));
  Logging::report("clustering_comparison", comparison_id, "ARI", Similarity::adjustedRandIndex(base_clusters, compare_clusters));
  std::pair<double, double> precision_recall = Similarity::weightedPrecisionRecall(base_clusters, compare_clusters);
  Logging::report("clustering_comparison", comparison_id, "Precision", precision_recall.first);
  Logging::report("clustering_comparison", comparison_id, "Recall", precision_recall.second);
}

int main(int argc, char const *argv[]) {
  Logging::Id run_id = Logging::getUnusedId();

  std::vector<std::vector<Graph::NodeId>> neighbors;
  Graph::EdgeId edge_count = IO::read_graph(argv[1], neighbors);
  Graph graph(neighbors.size(), edge_count);
  graph.setEdgesByAdjacencyLists(neighbors);

  std::vector<int> partition_sizes { 4, 32, 128, 1024 };
  // unsigned seed = 3920764003;
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  Modularity::rng = std::default_random_engine(seed);

  Logging::report("program_run", run_id, "binary", argv[0]);
  Logging::report("program_run", run_id, "graph", argv[1]);
  Logging::report("program_run", run_id, "node_count", graph.getNodeCount());
  Logging::report("program_run", run_id, "edge_count", graph.getEdgeCount());
  Logging::report("program_run", run_id, "seed", seed);

  ClusterStore ground_proof(neighbors.size());
  ClusterStore base_clusters(neighbors.size());
  ClusterStore compare_clusters(neighbors.size());

  bool ground_proof_available = argc >= 3;
  Logging::Id ground_proof_logging_id = 0;
  if (ground_proof_available) {
    IO::read_clustering(argv[2], ground_proof);
    ground_proof_logging_id = log_clustering(graph, ground_proof);
    Logging::report("clustering", ground_proof_logging_id, "source", "ground_proof");
    Logging::report("clustering", ground_proof_logging_id, "program_run_id", run_id);
    Logging::report("program_run", run_id, "ground_proof", argv[2]);
  }

  Logging::Id base_algo_run_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_run", base_algo_run_logging_id, "program_run_id", run_id);
  Logging::report("algorithm_run", base_algo_run_logging_id, "algorithm", "sequential louvain");

  Modularity::louvain(graph, base_clusters, base_algo_run_logging_id);
  Logging::Id base_cluster_logging_id = log_clustering(graph, base_clusters);
  Logging::report("clustering", base_cluster_logging_id, "source", "computation");
  Logging::report("clustering", base_cluster_logging_id, "algorithm_run_id", base_algo_run_logging_id);

  if (ground_proof_available) {
    log_comparison_results(ground_proof_logging_id, ground_proof, base_cluster_logging_id, base_clusters);
  }

  Logging::Id compare_algo_run_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
  Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "sequential louvain");

  Modularity::louvain(graph, compare_clusters, compare_algo_run_logging_id);
  Logging::Id compare_cluster_logging_id = log_clustering(graph, compare_clusters);
  Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
  Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

  log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
  if (ground_proof_available) {
    log_comparison_results(ground_proof_logging_id, ground_proof, compare_cluster_logging_id, compare_clusters);
  }

  for (int i : partition_sizes) {
    std::vector<uint32_t> partitions(graph.getNodeCount());

    // CHUNK

    compare_algo_run_logging_id = Logging::getUnusedId();
    Logging::Id partition_logging_id = Partitioning::chunk(graph, i, partitions);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_id", partition_logging_id);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
    if (ground_proof_available) {
      log_comparison_results(ground_proof_logging_id, ground_proof, compare_cluster_logging_id, compare_clusters);
    }

    // DET GREEDY

    compare_algo_run_logging_id = Logging::getUnusedId();
    partition_logging_id = Partitioning::deterministicGreedyWithLinearPenalty(graph, i, partitions);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_id", partition_logging_id);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
    if (ground_proof_available) {
      log_comparison_results(ground_proof_logging_id, ground_proof, compare_cluster_logging_id, compare_clusters);
    }

    // DET GREEDY RANDOM

    compare_algo_run_logging_id = Logging::getUnusedId();
    partition_logging_id = Partitioning::deterministicGreedyWithLinearPenalty(graph, i, partitions, true);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_id", partition_logging_id);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
    if (ground_proof_available) {
      log_comparison_results(ground_proof_logging_id, ground_proof, compare_cluster_logging_id, compare_clusters);
    }

    // CLUSTERING BASED

    compare_algo_run_logging_id = Logging::getUnusedId();
    partition_logging_id = Partitioning::clusteringBased(graph, i, partitions, ground_proof_available ? ground_proof : base_clusters); // TODO log cluster id?
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_id", partition_logging_id);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
    if (ground_proof_available) {
      log_comparison_results(ground_proof_logging_id, ground_proof, compare_cluster_logging_id, compare_clusters);
    }

    // CLUSTERING BASED

    compare_algo_run_logging_id = Logging::getUnusedId();
    partition_logging_id = Partitioning::random(graph, i, partitions);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_id", partition_logging_id);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
    if (ground_proof_available) {
      log_comparison_results(ground_proof_logging_id, ground_proof, compare_cluster_logging_id, compare_clusters);
    }
  }
}

