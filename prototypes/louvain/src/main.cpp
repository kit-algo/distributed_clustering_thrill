#include "graph.hpp"
#include "modularity.hpp"
#include "cluster_store.hpp"
#include "similarity.hpp"
#include "partitioning.hpp"
#include "logging.hpp"

#include <sstream>
#include <fstream>
#include <iostream>
#include <string>
#include <numeric>
#include <assert.h>
#include <random>
#include <chrono>

template<class F>
void open_file(const std::string& filename, F callback, std::ios_base::openmode mode = std::ios::in) {
    // std::cout << "Opening file " << filename << "\n";
    std::ifstream f(filename, mode);
    if(!f.is_open()) {
        throw std::runtime_error("Could not open file " + filename);
    }

    callback(f);

    // std::cout << "done reading " << filename << "\n";
}

Graph::EdgeId read_graph(const std::string& filename, std::vector<std::vector<Graph::NodeId>> &neighbors) {
  Graph::EdgeId edge_count;
  open_file(filename, [&](auto& file) {
    std::string line;
    Graph::NodeId node_count;
    std::getline(file, line);
    std::istringstream header_stream(line);
    header_stream >> node_count >> edge_count;
    neighbors.resize(node_count);

    Graph::NodeId i = 0;
    while (std::getline(file, line)) {
      std::istringstream line_stream(line);
      Graph::NodeId neighbor;
      while (line_stream >> neighbor) {
        neighbors[i].push_back(neighbor - 1);
      }
      i++;
    }
  });
  return edge_count;
}

void read_clustering(const std::string& filename, ClusterStore& clusters) {
  open_file(filename, [&](auto& file) {
    std::string line;
    Graph::NodeId node = 0;

    while (std::getline(file, line)) {
      std::istringstream line_stream(line);
      ClusterStore::ClusterId id;
      if (line_stream >> id) {
        clusters.set(node++, id);
      }
    }
  });
}

uint64_t log_clustering(const Graph & graph, const ClusterStore & clusters) {
  uint64_t logging_id = Logging::getUnusedId();
  Logging::report("clustering", logging_id, "modularity", Modularity::modularity(graph, clusters));
  return logging_id;
}

void log_comparison_results(uint64_t base_clustering_id, const ClusterStore & base_clusters, uint64_t compare_clustering_id, const ClusterStore & compare_clusters) {
  uint64_t comparison_id = Logging::getUnusedId();
  Logging::report("clustering_comparison", comparison_id, "base_clustering_id", base_clustering_id);
  Logging::report("clustering_comparison", comparison_id, "compare_clustering_id", compare_clustering_id);
  Logging::report("clustering_comparison", comparison_id, "NMI", Similarity::normalizedMutualInformation(base_clusters, compare_clusters));
  Logging::report("clustering_comparison", comparison_id, "ARI", Similarity::adjustedRandIndex(base_clusters, compare_clusters));
  std::pair<double, double> precision_recall = Similarity::weightedPrecisionRecall(base_clusters, compare_clusters);
  Logging::report("clustering_comparison", comparison_id, "Precision", precision_recall.first);
  Logging::report("clustering_comparison", comparison_id, "Recall", precision_recall.second);
}

int main(int argc, char const *argv[]) {
  uint64_t run_id = Logging::getUnusedId();

  std::vector<std::vector<Graph::NodeId>> neighbors;
  Graph::EdgeId edge_count = read_graph(argv[1], neighbors);
  Graph graph(neighbors.size(), edge_count);
  graph.setEdgesByAdjacencyLists(neighbors);

  std::vector<int> partition_sizes { 4, 32, 128, 1024 };
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  Modularity::rng = std::default_random_engine(seed);

  Logging::report("program_run", run_id, "graph", argv[1]);
  Logging::report("program_run", run_id, "node_count", graph.getNodeCount());
  Logging::report("program_run", run_id, "edge_count", graph.getEdgeCount());
  Logging::report("program_run", run_id, "seed", seed);

  ClusterStore ground_proof(0, neighbors.size());
  ClusterStore base_clusters(0, neighbors.size());
  ClusterStore compare_clusters(0, neighbors.size());

  bool ground_proof_available = argc >= 3;
  uint64_t ground_proof_logging_id = 0;
  if (ground_proof_available) {
    read_clustering(argv[2], ground_proof);
    ground_proof_logging_id = log_clustering(graph, ground_proof);
    Logging::report("clustering", ground_proof_logging_id, "source", "ground_proof");
  }

  // STANDARD ORDER

  uint64_t base_algo_run_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_run", base_algo_run_logging_id, "program_run_id", run_id);
  Logging::report("algorithm_run", base_algo_run_logging_id, "algorithm", "sequential louvain");
  Logging::report("algorithm_run", base_algo_run_logging_id, "order", "original");

  Modularity::louvain(graph, base_clusters, base_algo_run_logging_id);
  uint64_t base_cluster_logging_id = log_clustering(graph, base_clusters);
  Logging::report("clustering", base_cluster_logging_id, "source", "computation");
  Logging::report("clustering", base_cluster_logging_id, "algorithm_run_id", base_algo_run_logging_id);

  if (ground_proof_available) {
    log_comparison_results(ground_proof_logging_id, ground_proof, base_cluster_logging_id, base_clusters);
  }

  uint64_t compare_algo_run_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
  Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "sequential louvain");
  Logging::report("algorithm_run", compare_algo_run_logging_id, "order", "original");

  Modularity::louvain(graph, compare_clusters, compare_algo_run_logging_id);
  uint64_t compare_cluster_logging_id = log_clustering(graph, compare_clusters);
  Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
  Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

  log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
  if (ground_proof_available) {
    log_comparison_results(ground_proof_logging_id, ground_proof, compare_cluster_logging_id, compare_clusters);
  }

  for (int i : partition_sizes) {
    std::vector<uint32_t> partitions(graph.getNodeCount());

    compare_algo_run_logging_id = Logging::getUnusedId();
    Partitioning::chunk(graph, i, partitions);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "order", "original");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_algorithm", "chunk");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_count", i);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
    if (ground_proof_available) {
      log_comparison_results(ground_proof_logging_id, ground_proof, compare_cluster_logging_id, compare_clusters);
    }

    compare_algo_run_logging_id = Logging::getUnusedId();
    Partitioning::deterministic_greedy_with_linear_penalty(graph, i, partitions);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "order", "original");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_algorithm", "deterministic_greedy_with_linear_penalty");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_count", i);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
    if (ground_proof_available) {
      log_comparison_results(ground_proof_logging_id, ground_proof, compare_cluster_logging_id, compare_clusters);
    }
  }

  // CLUSTERING BASED ORDER

  std::vector<NodeId> permutation(graph.getNodeCount());
  if (ground_proof_available) {
    ground_proof.generateConsecutiveClusterNodeIdPermutation(permutation);
  } else {
    base_clusters.generateConsecutiveClusterNodeIdPermutation(permutation);
  }
  graph.applyNodePermutation(permutation);

  base_algo_run_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_run", base_algo_run_logging_id, "program_run_id", run_id);
  Logging::report("algorithm_run", base_algo_run_logging_id, "algorithm", "sequential louvain");
  Logging::report("algorithm_run", base_algo_run_logging_id, "order", "clustering_based");

  Modularity::louvain(graph, base_clusters, base_algo_run_logging_id);
  base_cluster_logging_id = log_clustering(graph, base_clusters);
  Logging::report("clustering", base_cluster_logging_id, "source", "computation");
  Logging::report("clustering", base_cluster_logging_id, "algorithm_run_id", base_algo_run_logging_id);


  compare_algo_run_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
  Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "sequential louvain");
  Logging::report("algorithm_run", compare_algo_run_logging_id, "order", "clustering_based");

  Modularity::louvain(graph, compare_clusters, compare_algo_run_logging_id);
  compare_cluster_logging_id = log_clustering(graph, compare_clusters);
  Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
  Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

  log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);

  for (int i : partition_sizes) {
    std::vector<uint32_t> partitions(graph.getNodeCount());

    compare_algo_run_logging_id = Logging::getUnusedId();
    Partitioning::chunk(graph, i, partitions);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "order", "clustering_based");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_algorithm", "chunk");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_count", i);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);

    compare_algo_run_logging_id = Logging::getUnusedId();
    Partitioning::deterministic_greedy_with_linear_penalty(graph, i, partitions);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "order", "clustering_based");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_algorithm", "deterministic_greedy_with_linear_penalty");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_count", i);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
  }

  // SHUFFLED ORDER

  std::iota(permutation.begin(), permutation.end(), 0);
  std::shuffle(permutation.begin(), permutation.end(), Modularity::rng);
  graph.applyNodePermutation(permutation);

  base_algo_run_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_run", base_algo_run_logging_id, "program_run_id", run_id);
  Logging::report("algorithm_run", base_algo_run_logging_id, "algorithm", "sequential louvain");
  Logging::report("algorithm_run", base_algo_run_logging_id, "order", "shuffled");

  Modularity::louvain(graph, base_clusters, base_algo_run_logging_id);
  base_cluster_logging_id = log_clustering(graph, base_clusters);
  Logging::report("clustering", base_cluster_logging_id, "source", "computation");
  Logging::report("clustering", base_cluster_logging_id, "algorithm_run_id", base_algo_run_logging_id);


  compare_algo_run_logging_id = Logging::getUnusedId();
  Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
  Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "sequential louvain");
  Logging::report("algorithm_run", compare_algo_run_logging_id, "order", "shuffled");

  Modularity::louvain(graph, compare_clusters, compare_algo_run_logging_id);
  compare_cluster_logging_id = log_clustering(graph, compare_clusters);
  Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
  Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

  log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);

  for (int i : partition_sizes) {
    std::vector<uint32_t> partitions(graph.getNodeCount());

    compare_algo_run_logging_id = Logging::getUnusedId();
    Partitioning::chunk(graph, i, partitions);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "order", "shuffled");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_algorithm", "chunk");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_count", i);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);

    compare_algo_run_logging_id = Logging::getUnusedId();
    Partitioning::deterministic_greedy_with_linear_penalty(graph, i, partitions);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", compare_algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "order", "shuffled");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_algorithm", "deterministic_greedy_with_linear_penalty");
    Logging::report("algorithm_run", compare_algo_run_logging_id, "partition_count", i);

    Modularity::partitionedLouvain(graph, compare_clusters, partitions, compare_algo_run_logging_id);
    compare_cluster_logging_id = log_clustering(graph, compare_clusters);
    Logging::report("clustering", compare_cluster_logging_id, "source", "computation");
    Logging::report("clustering", compare_cluster_logging_id, "algorithm_run_id", compare_algo_run_logging_id);

    log_comparison_results(base_cluster_logging_id, base_clusters, compare_cluster_logging_id, compare_clusters);
  }
}

