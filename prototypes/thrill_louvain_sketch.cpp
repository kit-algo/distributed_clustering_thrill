#include <thrill/api/print.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/print.hpp>

#include <ostream>
#include <iostream>
#include <unordered_map>
#include <vector>

struct Edge {
  uint32_t tail, head;

  uint32_t getWeight() {
    return 1;
  }
};

struct Node {
  uint32_t id;
  std::vector<uint32_t> neighbors;
};

struct WeightedEdge {
  uint32_t tail, head, weight;

  uint32_t getWeight() {
    return weight;
  }
};

struct WeightedNodeEdge {
  uint32_t head, weight;
};

struct WeightedNode {
  uint32_t id;
  std::vector<WeightedNodeEdge> neighbors;
};

std::ostream& operator << (std::ostream& os, const Edge& e) {
  return os << e.tail << " -> " << e.head;
}

auto louvain(auto & nodes) {
  auto node_clusters = nodes
    .Keep()
    // Map to partition
    .Map([](const Node & node) { return std::make_pair(0, node) })
    // Local Moving
    // May need template param
    .GroupByKey(
      [](const std::pair<uint32_t, Node> & node_with_partition) { return node_with_partition.first; },
      [](auto & iterator, const uint32_t /*partition*/) {
        // std::vector<Node> nodes;
        std::vector<std::pair<uint32_t, uint32_t>> mapping;
        while (iterator.HasNext()) {
          // nodes.push_back(iterator.Next());
          const Node& node = iterator.Next();
          mapping.push_back(std::make_pair(node.id, node.id % 4)); // Dummy mapping
        }

        // build graph
        // perform local moving
        return mapping
      })
    .FlatMap(
      [](const std::vector<std::pair<uint32_t, uint32_t>>& mapping, auto emit) {
        for (auto& pair : mapping) {
          emit(std::make_pair(pair.first, pair.second));
        }
      })
    .Sort([](const std::pair<uint32_t, uint32_t> & node_cluster1, const std::pair<uint32_t, uint32_t> & node_cluster2) { return node_cluster1.first < node_cluster2.first; });
  // TODO cleanup cluster ids

  bool changed = node_clusters.AllReduce(
    [](bool acc, const std::pair<uint32_t, uint32_t> & node_cluster) {
      return acc && node_cluster.first == node_cluster.second;
    }, true);
  if (!changed) {
    return node_clusters;
  }

  auto clusters = node_clusters
    .Keep()
    .Map([](const std::pair<uint32_t, uint32_t> & node_cluster) { return node_cluster.second; });

  auto cluster_id_times_degree = nodes
    .Keep()
    .Sort([](const Node & node1, const Node & node2) { return node1.id < node2.id; })
    // No rebalance
    .Zip(clusters, [](const Node & node, uint32_t cluster_id) { return std::make_pair(node.neighbors.size(), cluster_id); })
    .FlatMap(
      [](const std::pair<size_t, uint32_t> degree_and_cluster, auto emit) {
        for (int i = 0; i < degree_and_cluster.first; i++) {
          emit(degree_and_cluster.second);
        }
      });

  // Build Meta Graph
  auto meta_graph_nodes = edge_list
    // To Edge List
    .FlatMap(
      [](const Node & node, auto emit) {
        for (uint32_t neighbor : node.neighbors) {
          emit(Edge { node, neighbor });
        }
      });
    // Translate Ids
    .Zip(cluster_id_times_degree.Keep(), [](const Edge & edge, const uint32_t & new_id) { return Edge { new_id, edge.head }; })
    .Map([](const Edge & edge) { return Edge { edge.head, edge.tail }; })
    .Sort(
      [](const Edge & e1, const Edge & e2) {
        return (e1.tail == e2.tail && e1.head < e2.head) || (e1.tail < e2.tail);
      })
    .Zip(cluster_id_times_degree, [](const Edge & edge, const uint32_t& new_id) { return Edge { new_id, edge.head }; });
    .Map([](const auto & edge) { return WeightedEdge { edge.tail, edge.head, edge.getWeight() } })
    .ReduceByKey(
      [](const WeightedEdge & edge) { return std::make_pair(edge.tail, edge.head) },
      [](const WeightedEdge & edge1, const WeightedEdge & edge2) { return WeightedEdge { edge1.tail, edge1.head, edge1.weight + edge2.weight }; })
    // Build Node Data Structure
    .Map([](const WeightedEdge & edge) { return WeightedNode { edge.tail, { WeightedNodeEdge { edge.head, edge.weight } } } })
    .ReduceByKey(
      [](const WeightedNode & node) { return node.id },
      [](const WeightedNode & node1, const WeightedNode & node2) {
        node1.neighbors.insert(node1.neighbors.end(), node2.neighbors.begin(), node2.neighbors.end());
        return node1;
      })

  // Recursion on meta graph
  auto meta_clustering = louvain(meta_graph_nodes);

  // translate meta clusters and return
  return node_clusters
    .Map([](auto & node_cluster) { return std::make_pair(node_cluster.second, std::vector<uint32_t>(1, node_cluster.first)) })
    .ReduceToIndex(
      [](auto & cluster_nodes) { return cluster_nodes.first; },
      [](auto & cluster_nodes1, auto & cluster_nodes2) {
        cluster_nodes1.second.insert(cluster_nodes1.second.end(), cluster_nodes2.second.begin(), cluster_nodes2.second.end());
        return cluster_nodes1;
      })
    .Zip(meta_clustering,
      [](auto & cluster_nodes, auto & meta_node_cluster) {
        cluster_nodes.first = meta_node_cluster.second;
      })
    .FlatMap(
      [](auto & cluster_nodes, auto emit) {
        for (uint32_t node : cluster_nodes.second) {
          emit(std::make_pair(node, cluster_nodes.first));
        }
      })
    .Sort([](const std::pair<uint32_t, uint32_t> & node_cluster1, const std::pair<uint32_t, uint32_t> & node_cluster2) { return node_cluster1.first < node_cluster2.first; });
}

int main(int, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    auto nodes = thrill::ReadLines(context, argv[1])
      .ZipWithIndex([](const std::string & line, const uint32_t& index) {
        Node node = { index, {} };

        std::istringstream line_stream(line);
        uint32_t neighbor;

        while (line >> neighbor) {
          node.neighbors.push_back(neighbor);
        }

        return node;
      });
    louvain(nodes);
  });
}

