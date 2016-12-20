#include "graph.hpp"
#include "modularity.hpp"
#include "cluster_store.hpp"
#include "similarity.hpp"
#include "partitioning.hpp"

#include <sstream>
#include <fstream>
#include <iostream>
#include <string>
#include <numeric>
#include <assert.h>
#include <random>
#include <chrono>

template<class F>
void open_file(std::string filename, F callback, std::ios_base::openmode mode = std::ios::in) {
    // std::cout << "Opening file " << filename << "\n";
    std::ifstream f(filename, mode);
    if(!f.is_open()) {
        throw std::runtime_error("Could not open file " + filename);
    }

    callback(f);

    // std::cout << "done reading " << filename << "\n";
}

Graph::EdgeId read_graph(const std::string filename, std::vector<std::vector<Graph::NodeId>> &neighbors) {
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

void log_results(const Graph & graph, const ClusterStore & base_clusters, const ClusterStore & compare_clusters, std::vector<ClusterStore::ClusterId> & level_cluster_counts) {
  std::cout << Modularity::modularity(graph, compare_clusters) << ',';
  for (ClusterStore::ClusterId cluster_count : level_cluster_counts) {
    std::cout << cluster_count << " -> ";
  }
  std::cout << ',';
  std::cout << Similarity::normalizedMutualInformation(base_clusters, compare_clusters) << ',';
  std::cout << Similarity::adjustedRandIndex(base_clusters, compare_clusters) << ',';
  std::pair<double, double> precision_recall = Similarity::weightedPrecisionRecall(base_clusters, compare_clusters);
  std::cout << precision_recall.first << ',' << precision_recall.second << ',';
  level_cluster_counts.clear();
}

int main(int, char const *argv[]) {
  std::vector<std::vector<Graph::NodeId>> neighbors;
  Graph::EdgeId edge_count = read_graph(argv[1], neighbors);
  Graph graph(neighbors.size(), edge_count);
  graph.setEdgesByAdjacencyLists(neighbors);

  std::vector<int> partition_sizes { 4, 32, 128, 1024 };
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  Modularity::rng = std::default_random_engine(seed);
  std::cout << "Graph, N, M, Seed, Variant, Q Base, Levels Base, Q 1, Levels 1, NMI 1, ARI 1, precision 1, recall 1";
  for (int i : partition_sizes) {
    std::cout << "Q " << i << " chunk, Levels " << i << " chunk, NMI " << i << " chunk, ARI " << i << " chunk, precision " << i << " chunk, recall " << i << " chunk, Q " << i << " det_grd, Levels " << i << " det_grd, NMI " << i << " det_grd, ARI " << i << " det_grd, precision " << i << " det_grd, recall " << i << " det_grd";
  }
  std::cout << "\n";
  std::cout << argv[1] << ',' << graph.getNodeCount() << ',' << graph.getEdgeCount() << ',' << seed << ",original,";

  ClusterStore base_clusters(0, neighbors.size());
  ClusterStore compare_clusters(0, neighbors.size());
  std::vector<ClusterStore::ClusterId> level_cluster_counts;

  // STANDARD ORDER

  Modularity::louvain(graph, base_clusters, level_cluster_counts);
  std::cout << Modularity::modularity(graph, base_clusters) << ',';
  for (ClusterStore::ClusterId cluster_count : level_cluster_counts) {
    std::cout << cluster_count << " -> ";
  }
  std::cout << ',';
  level_cluster_counts.clear();

  Modularity::louvain(graph, compare_clusters, level_cluster_counts);
  log_results(graph, base_clusters, compare_clusters, level_cluster_counts);

  for (int i : partition_sizes) {
    std::vector<uint32_t> partitions(graph.getNodeCount());
    Partitioning::chunk(graph, i, partitions);
    Modularity::partitionedLouvain(graph, compare_clusters, partitions, level_cluster_counts);

    log_results(graph, base_clusters, compare_clusters, level_cluster_counts);

    Partitioning::deterministic_greedy_with_linear_penalty(graph, i, partitions);
    Modularity::partitionedLouvain(graph, compare_clusters, partitions, level_cluster_counts);

    log_results(graph, base_clusters, compare_clusters, level_cluster_counts);
  }

  std::cout << argv[1] << ',' << graph.getNodeCount() << ',' << graph.getEdgeCount() << ',' << seed << ",shuffled,";

  // SHUFFLED ORDER

  std::vector<NodeId> permutation(graph.getNodeCount());
  std::iota(permutation.begin(), permutation.end(), 0);
  std::shuffle(permutation.begin(), permutation.end(), Modularity::rng);
  graph.applyNodePermutation(permutation);

  Modularity::louvain(graph, base_clusters, level_cluster_counts);
  std::cout << Modularity::modularity(graph, base_clusters) << ',';
  for (ClusterStore::ClusterId cluster_count : level_cluster_counts) {
    std::cout << cluster_count << " -> ";
  }
  std::cout << ',';
  level_cluster_counts.clear();

  Modularity::louvain(graph, compare_clusters, level_cluster_counts);
  log_results(graph, base_clusters, compare_clusters, level_cluster_counts);

  for (int i : partition_sizes) {
    std::vector<uint32_t> partitions(graph.getNodeCount());
    Partitioning::chunk(graph, i, partitions);
    Modularity::partitionedLouvain(graph, compare_clusters, partitions, level_cluster_counts);

    log_results(graph, base_clusters, compare_clusters, level_cluster_counts);

    Partitioning::deterministic_greedy_with_linear_penalty(graph, i, partitions);
    Modularity::partitionedLouvain(graph, compare_clusters, partitions, level_cluster_counts);

    log_results(graph, base_clusters, compare_clusters, level_cluster_counts);
  }

  std::cout << "\n";
}

