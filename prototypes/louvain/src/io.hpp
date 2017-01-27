#pragma once

#include "graph.hpp"
#include "cluster_store.hpp"

#include <assert.h>
#include <cstdint>
#include <sstream>
#include <fstream>
#include <iostream>

namespace IO {

using ClusterId = typename ClusterStore::ClusterId;
using NodeId = typename Graph::NodeId;
using Weight = typename Graph::Weight;

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

void read_partition(const std::string& filename, std::vector<uint32_t>& node_partition_elements) {
  open_file(filename, [&](auto& file) {
    std::string line;
    Graph::NodeId node = 0;

    while (std::getline(file, line)) {
      std::istringstream line_stream(line);
      ClusterStore::ClusterId id;
      if (line_stream >> id) {
        node_partition_elements[node] = id;
      }
      node++;
    }
  });
}

}
