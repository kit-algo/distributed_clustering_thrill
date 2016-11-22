#include "graph.hpp"
#include <map>

#include <sstream>
#include <fstream>
#include <iostream>
#include <string>
#include <assert.h>
#include <cmath>

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

int read_graph(const std::string filename, std::vector<std::vector<int>> &neighbors) {
  int edge_count;
  open_file(filename, [&](auto& file) {
    std::string line;
    int node_count;
    std::getline(file, line);
    std::istringstream header_stream(line);
    header_stream >> node_count >> edge_count;
    neighbors.resize(node_count);

    int i = 0;
    while (std::getline(file, line)) {
      std::istringstream line_stream(line);
      int neighbor;
      while (line_stream >> neighbor) {
        neighbors[i].push_back(neighbor - 1);
      }
      i++;
    }
  });
  return edge_count;
}

int deltaModularity(const int node, const Graph& graph, int target_cluster, const std::vector<int> &clusters, const std::vector<int> &cluster_weights) {
  int weight_between_node_and_target_cluster = 0;
  graph.forEachAdjacentNode(node, [&](int neighbor, int weight) {
    if (clusters[neighbor] == target_cluster && neighbor != node) {
      weight_between_node_and_target_cluster += weight;
    }
  });
  // assert(weight_between_node_and_target_cluster != 0);
  // std::cout << weight_between_node_and_target_cluster << " ";

  int weight_between_node_and_current_cluster = 0;
  graph.forEachAdjacentNode(node, [&](int neighbor, int weight) {
    if (clusters[neighbor] == clusters[node] && neighbor != node) {
      weight_between_node_and_current_cluster += weight;
    }
  });
  // assert(weight_between_node_and_current_cluster != 0);
  // std::cout << weight_between_node_and_current_cluster << " ";

  // int test = 0;
  // for (int i = 0; i < graph.getNodeCount(); ++i) {
  //   if (clusters[i] == target_cluster && i != node) {
  //     test += graph.weightedNodeDegree(i);
  //   }
  // }
  int target_cluster_incident_edges_weight = cluster_weights[target_cluster];
  if (target_cluster == clusters[node]) {
    target_cluster_incident_edges_weight -= graph.weightedNodeDegree(node);
  }
  // assert(test == target_cluster_incident_edges_weight);
  // assert(target_cluster_incident_edges_weight != 0);
  // std::cout << target_cluster_incident_edges_weight << " ";

  // test = 0;
  // for (int i = 0; i < graph.getNodeCount(); ++i) {
  //   if (clusters[i] == clusters[node] && i != node) {
  //     test += graph.weightedNodeDegree(i);
  //   }
  // }
  int current_cluster_incident_edges_weight = cluster_weights[clusters[node]] - graph.weightedNodeDegree(node);
  // assert(test == current_cluster_incident_edges_weight);
  // assert(current_cluster_incident_edges_weight != 0);
  // std::cout << current_cluster_incident_edges_weight << "\n";

  return (graph.getTotalWeight() * 2 * (weight_between_node_and_target_cluster - weight_between_node_and_current_cluster))
    - ((target_cluster_incident_edges_weight - current_cluster_incident_edges_weight) * graph.weightedNodeDegree(node));
}

int rewriteClusterIds(std::vector<int> &clusters, const int node_count) {
  int id_counter = 0;
  std::map<int, int> old_to_new;

  for (int i = 0; i < node_count; i++) {
    if (old_to_new.find(clusters[i]) == old_to_new.end()) {
      old_to_new[clusters[i]] = id_counter++;
    }
    clusters[i] = old_to_new[clusters[i]];
  }

  return id_counter;
}

void louvain(const Graph& graph, std::vector<int> &node_clusters) {
  std::cout << "louvain\n";
  std::cout << graph.modularity(std::vector<int>(graph.getNodeCount(),0)) << "\n";
  assert(abs(graph.modularity(std::vector<int>(graph.getNodeCount(),0))) < 0.0001);
  bool changed = false;
  std::vector<int> cluster_weights(graph.getNodeCount());

  for (int i = 0; i < graph.getNodeCount(); i++) {
    node_clusters[i] = i;
    cluster_weights[i] = graph.weightedNodeDegree(i);
  }

  int current_node = 0;
  int unchanged_count = 0;
  while(unchanged_count < graph.getNodeCount()) {
    // std::cout << "local moving: " << current_node << "\n";
    int current_node_cluster = node_clusters[current_node];
    int best_cluster = current_node_cluster;
    int best_delta_modularity = 0;

    // double current_modularity = graph.modularity(node_clusters);

    // assert(deltaModularity(current_node, graph, node_clusters[current_node], node_clusters, cluster_weights) == 0);

    graph.forEachAdjacentNode(current_node, [&](int neighbor, int) {

      int neighbor_cluster_delta = deltaModularity(current_node, graph, node_clusters[neighbor], node_clusters, cluster_weights);
      // std::cout << current_node << " -> " << neighbor << "(" << node_clusters[neighbor] << "): " << neighbor_cluster_delta << "\n";
      if (neighbor_cluster_delta > best_delta_modularity) {
        best_delta_modularity = neighbor_cluster_delta;
        best_cluster = node_clusters[neighbor];
      }
    });

    if (best_cluster != current_node_cluster) {
      std::cout << "move " << current_node << " from " << current_node_cluster << " to " << best_cluster << " (" << best_delta_modularity << ")\n";
      cluster_weights[current_node_cluster] -= graph.weightedNodeDegree(current_node);
      node_clusters[current_node] = best_cluster;
      cluster_weights[best_cluster] += graph.weightedNodeDegree(current_node);
      unchanged_count = 0;
      changed = true;
      // assert(deltaModularity(current_node, graph, current_node_cluster, node_clusters, cluster_weights) == -best_delta_modularity);
      // std::cout << current_modularity << "," << (best_delta_modularity / (2.*graph.getTotalWeight()*graph.getTotalWeight())) << "," << graph.modularity(node_clusters);
      // assert(abs(current_modularity + (best_delta_modularity / (2.*graph.getTotalWeight()*graph.getTotalWeight())) - graph.modularity(node_clusters)) < 0.0001);
    } else {
      unchanged_count++;
    }

    current_node = (current_node + 1) % graph.getNodeCount();
  }

  if (changed) {
    long cluster_count = rewriteClusterIds(node_clusters, graph.getNodeCount());
    std::cout << "contracting " << cluster_count << " clusters\n";

    // contract to meta_graph
    std::cout << cluster_count * cluster_count << "\n";
    std::vector<int> weight_matrix(cluster_count * cluster_count, 0);
    for (int node = 0; node < graph.getNodeCount(); node++) {
      graph.forEachAdjacentNode(node, [&](int neighbor, int weight) {
        weight_matrix[cluster_count * node_clusters[node] + node_clusters[neighbor]] += weight;
      });
    }

    std::vector<std::tuple<int, int, int>> edges;
    for (int i = 0; i < cluster_count; i++) {
      for (int j = i; j < cluster_count; j++) {
        int weight = weight_matrix[cluster_count * i + j];

        if (i == j) {
          assert(weight % 2 == 0);
          weight /= 2;
        }

        if (!weight == 0) {
          edges.push_back(std::make_tuple(i, j, weight));
        }
      }
    }

    std::cout << "new graph " << cluster_count << "\n";
    Graph meta_graph(cluster_count, edges.size());
    meta_graph.setEdgesWithMissingBackwardArcs(edges);
    assert(graph.getTotalWeight() == meta_graph.getTotalWeight());
    std::vector<int> meta_clusters(meta_graph.getNodeCount());
    louvain(meta_graph, meta_clusters);

    // translate meta clusters
    for (int node = 0; node < graph.getNodeCount(); node++) {
      node_clusters[node] = meta_clusters[node_clusters[node]];
    }
  }
}

int main(int, char const *argv[]) {
  std::vector<std::vector<int>> neighbors;
  int edge_count = read_graph(argv[1], neighbors);
  Graph graph(neighbors.size(), edge_count);
  graph.setEdgesByAdjacencyLists(neighbors);
  std::vector<int> clusters(neighbors.size());
  louvain(graph, clusters);
  for (auto& cluster : clusters) {
    std::cout << cluster << " ";
  }
  std::cout << '\n';
}

