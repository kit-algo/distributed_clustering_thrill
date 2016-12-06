#include "graph.hpp"
#include "modularity.hpp"
#include "cluster_store.hpp"
#include "similarity.hpp"

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

int main(int, char const *argv[]) {
  std::vector<std::vector<Graph::NodeId>> neighbors;
  Graph::EdgeId edge_count = read_graph(argv[1], neighbors);
  Graph graph(neighbors.size(), edge_count);
  graph.setEdgesByAdjacencyLists(neighbors);

  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  Modularity::rng = std::default_random_engine(seed);
  std::cout << argv[1] << ',' << graph.getNodeCount() << ',' << graph.getEdgeCount() << ',' << seed << ",original,";

  ClusterStore base_clusters(0, neighbors.size());
  ClusterStore compare_clusters(0, neighbors.size());

  // STANDARD ORDER

  Modularity::louvain(graph, base_clusters);
  Modularity::louvain(graph, compare_clusters);

  std::cout << Modularity::modularity(graph, base_clusters) << ',';

  std::cout << Modularity::modularity(graph, compare_clusters) << ',';
  std::cout << Similarity::normalizedMutualInformation(base_clusters, compare_clusters) << ',';
  std::cout << Similarity::adjustedRandIndex(base_clusters, compare_clusters) << ',';

  for (int i = 4; i <= 256; i *= 4) {
    Modularity::partitionedLouvain(graph, compare_clusters);
    std::cout << Modularity::modularity(graph, compare_clusters) << ',';
    std::cout << Similarity::normalizedMutualInformation(base_clusters, compare_clusters) << ',';
    std::cout << Similarity::adjustedRandIndex(base_clusters, compare_clusters) << ',';
  }

  std::cout << "\n,,,,shuffled,";

  // SHUFFLED ORDER

  std::vector<NodeId> permutation(graph.getNodeCount());
  std::iota(permutation.begin(), permutation.end(), 0);
  std::shuffle(permutation.begin(), permutation.end(), Modularity::rng);
  graph.applyNodePermutation(permutation);

  Modularity::louvain(graph, base_clusters);
  Modularity::louvain(graph, compare_clusters);

  std::cout << Modularity::modularity(graph, base_clusters) << ',';

  std::cout << Modularity::modularity(graph, compare_clusters) << ',';
  std::cout << Similarity::normalizedMutualInformation(base_clusters, compare_clusters) << ',';
  std::cout << Similarity::adjustedRandIndex(base_clusters, compare_clusters) << ',';

  for (int i = 4; i <= 256; i *= 4) {
    Modularity::partitionedLouvain(graph, compare_clusters);
    std::cout << Modularity::modularity(graph, compare_clusters) << ',';
    std::cout << Similarity::normalizedMutualInformation(base_clusters, compare_clusters) << ',';
    std::cout << Similarity::adjustedRandIndex(base_clusters, compare_clusters) << ',';
  }

  std::cout << "\n,,,,fixed,";

  // FIXED ORDER

  graph.fixIdOrder();

  Modularity::louvain(graph, base_clusters);
  Modularity::louvain(graph, compare_clusters);

  std::cout << Modularity::modularity(graph, base_clusters) << ',';

  std::cout << Modularity::modularity(graph, compare_clusters) << ',';
  std::cout << Similarity::normalizedMutualInformation(base_clusters, compare_clusters) << ',';
  std::cout << Similarity::adjustedRandIndex(base_clusters, compare_clusters) << ',';

  for (int i = 4; i <= 256; i *= 4) {
    Modularity::partitionedLouvain(graph, compare_clusters);
    std::cout << Modularity::modularity(graph, compare_clusters) << ',';
    std::cout << Similarity::normalizedMutualInformation(base_clusters, compare_clusters) << ',';
    std::cout << Similarity::adjustedRandIndex(base_clusters, compare_clusters) << ',';
  }

  std::cout << "\n";
}

