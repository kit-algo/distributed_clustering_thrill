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
    std::cout << "Opening file " << filename << "\n";
    std::ifstream f(filename, mode);
    if(!f.is_open()) {
        throw std::runtime_error("Could not open file " + filename);
    }

    callback(f);

    std::cout << "done reading " << filename << "\n";
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

  Modularity::seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::cout << "SEED: " << Modularity::seed << "\n";

  std::vector<NodeId> permutation(graph.getNodeCount());
  std::iota(permutation.begin(), permutation.end(), 0);
  std::shuffle(permutation.begin(), permutation.end(), std::default_random_engine(Modularity::seed));
  graph.applyNodePermutation(permutation);

  ClusterStore seq_clusters(0, neighbors.size());
  ClusterStore par_clusters(0, neighbors.size());
  Modularity::louvain(graph, seq_clusters);
  Modularity::partitionedLouvain(graph, par_clusters);

  std::cout << "Sequential Louvain Modularity: " << Modularity::modularity(graph, seq_clusters) << "\n";
  std::cout << "Partinioned Louvain Modularity: " << Modularity::modularity(graph, par_clusters) << "\n";
  std::cout << "ARI: " << Similarity::adjustedRandIndex(seq_clusters, par_clusters) << "\n";
  std::cout << "NMI: " << Similarity::normalizedMutualInformation(seq_clusters, par_clusters) << "\n";
}

