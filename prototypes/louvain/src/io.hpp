#pragma once

#include "graph.hpp"
#include "cluster_store.hpp"

#include <assert.h>
#include <cstdint>
#include <sstream>
#include <fstream>
#include <iostream>

#include <thrill/vfs/file_io.hpp>

namespace IO {

using ClusterId = typename ClusterStore::ClusterId;
using NodeId = typename Graph::NodeId;
using Weight = typename Graph::Weight;

template<class F>
void open_file(const std::string& filename, F callback, std::ios_base::openmode mode = std::ios::in) {
  std::ifstream f(filename, mode);
  if(!f.is_open()) {
      throw std::runtime_error("Could not open file " + filename);
  }

  callback(f);
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

Graph::EdgeId read_graph_txt(const std::string& filename, std::vector<std::vector<Graph::NodeId>> &neighbors, std::unordered_map<Graph::NodeId, Graph::NodeId>& id_mapping) {
  Graph::EdgeId edge_count = 0;
  open_file(filename, [&](auto& file) {
    std::string line;

    while (std::getline(file, line)) {
      if (!line.empty() && line[0] != '#') {
        std::istringstream line_stream(line);
        Graph::NodeId tail, head;
        if (line_stream >> tail >> head) {
          neighbors[id_mapping[tail]].push_back(id_mapping[head]);
          neighbors[id_mapping[head]].push_back(id_mapping[tail]);
        }
        edge_count++;
      }
    }
  });
  return edge_count;
}

template <typename stream_t>
uint64_t GetVarint(stream_t &is) {
  auto get_byte = [&is]() -> uint8_t {
    uint8_t result;
    is.read(reinterpret_cast<char*>(&result), 1);
    return result;
  };
  uint64_t u, v = get_byte();
  if (!(v & 0x80)) return v;
  v &= 0x7F;
  u = get_byte(), v |= (u & 0x7F) << 7;
  if (!(u & 0x80)) return v;
  u = get_byte(), v |= (u & 0x7F) << 14;
  if (!(u & 0x80)) return v;
  u = get_byte(), v |= (u & 0x7F) << 21;
  if (!(u & 0x80)) return v;
  u = get_byte(), v |= (u & 0x7F) << 28;
  if (!(u & 0x80)) return v;
  u = get_byte(), v |= (u & 0x7F) << 35;
  if (!(u & 0x80)) return v;
  u = get_byte(), v |= (u & 0x7F) << 42;
  if (!(u & 0x80)) return v;
  u = get_byte(), v |= (u & 0x7F) << 49;
  if (!(u & 0x80)) return v;
  u = get_byte(), v |= (u & 0x7F) << 56;
  if (!(u & 0x80)) return v;
  u = get_byte();
  if (u & 0xFE)
    throw std::overflow_error("Overflow during varint64 decoding.");
  v |= (u & 0x7F) << 63;
  return v;
}

Graph::EdgeId read_graph_bin(const thrill::vfs::FileList& paths, std::vector<std::vector<Graph::NodeId>> &neighbors) {
  Graph::EdgeId edge_count = 0;

  assert(neighbors.empty());

  if (!paths.empty()) {
    std::ifstream is;
    size_t _file_index = 0;

    auto next_input = [&]() {
      is.close();
      if (_file_index < paths.size()) {
        is.open(paths[_file_index++].path);
      }
    };

    next_input();

    for (NodeId u = 0; is.good() && is.is_open(); ++u) {
      // Add node if it does not exist yet, only one may be missing
      if (u >= neighbors.size()) {
        neighbors.emplace_back();
      }

      for (size_t deg = GetVarint(is); deg > 0 && is.good(); --deg) {
        uint32_t v;
        if (!is.read(reinterpret_cast<char*>(&v), 4)) {
          throw std::runtime_error("I/O error while reading next neighbor");
        }

        assert(u != v);
        neighbors[u].push_back(v);
        if (v >= neighbors.size()) {
          neighbors.resize(v + 1);
        }
        neighbors[v].push_back(u);
        edge_count++;
      }

      if (is.is_open() && (is.peek() == std::char_traits<char>::eof() || !is.good())) {
        next_input();
      }
    }
  }

  return edge_count;
};

Graph::EdgeId read_graph_bin(const std::string& glob, std::vector<std::vector<Graph::NodeId>> &neighbors) {
  thrill::vfs::FileList files = thrill::vfs::Glob(std::vector<std::string>(1, glob), thrill::vfs::GlobType::File);
  return read_graph_bin(files, neighbors);
};

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

void write_clustering(const std::string& filename, ClusterStore& clusters) {
  std::ofstream f(filename, std::ios::out);
  if(!f.is_open()) {
      throw std::runtime_error("Could not open file " + filename);
  }

  for (NodeId node = 0; node < clusters.size(); node++) {
    f << clusters[node] << std::endl;
  }
}

void read_snap_clustering(const std::string& filename, ClusterStore &clusters, std::unordered_map<Graph::NodeId, Graph::NodeId>& id_mapping) {
  ClusterStore::ClusterId cluster_id = 0;
  open_file(filename, [&](auto& file) {
    std::string line;

    while (std::getline(file, line)) {
      if (!line.empty() && line[0] != '#') {
        std::istringstream line_stream(line);
        Graph::NodeId node;
        while (line_stream >> node) {
          clusters.set(id_mapping[node], cluster_id);
        }
        cluster_id++;
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
