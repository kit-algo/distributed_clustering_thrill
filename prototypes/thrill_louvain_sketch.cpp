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
};

struct WeightedEdge {
  uint32_t tail, head, weight;
};

struct Node {
  uint32_t id;
  std::vector<uint32_t> neighbors;
};

std::ostream& operator << (std::ostream& os, const Edge& e) {
  return os << e.tail << " -> " << e.head;
}

void convert(thrill::Context& context) {
  uint32_t partition_count = 4;
  auto nodes;

  auto clusters = nodes
    .Keep()
    // Map to partition
    .Map([](const Node & node) { return std::make_pair(node.id % 4, node) })
    // Local Moving
    // May need template param
    .GroupByKey(
      [](const std::pair<uint32_t, Node> & node_with_partition) { return node_with_partition.first; },
      [](auto & iterator, const uint32_t /*partition*/) {
        std::vector<Node> nodes;
        while (iterator.HasNext()) {
          nodes.push_back(iterator.Next());
        }

        // build graph
        // perform local moving
        // return some mapping
      })
    .FlatMap(
      [](const Mapping & mapping, auto emit) {
        mapping.forEach([](uint32_t node_id, uint32_t cluster_id) {
          emit(std::make_pair(node_id, cluster_id));
        });
      })
    .Sort([](const std::pair<uint32_t, uint32_t> & node_cluster1, const std::pair<uint32_t, uint32_t> & node_cluster2) { return node_cluster1.first < node_cluster2.first; })
    .Map([](const std::pair<uint32_t, uint32_t> & node_cluster) { return node_cluster.second; });

  auto cluster_id_times_degree = nodes
    // sorted???
    .Keep()
    // No rebalance
    .Zip(clusters, [](const Node & node, uint32_t cluster_id) { return std::make_pair(node.neighbors.size(), cluster_id); })
    .FlatMap(
      [](const std::pair<size_t, uint32_t> degree_and_cluster, auto emit) {
        for (int i = 0; i < degree_and_cluster.first; i++) {
          emit(degree_and_cluster.second);
        }
      });

  auto edge_list = nodes
    .FlatMap(
      [](const Node & node, auto emit) {
        for (uint32_t neighbor : node.neighbors) {
          emit(Edge { node, neighbor });
        }
      });

  auto meta_graph_edges = edge_list
    .Zip(cluster_id_times_degree, [](const Edge & edge, const uint32_t & new_id) { return Edge { new_id, edge.head }; })
    .Map([](const Edge & edge) { return Edge { edge.head, edge.tail }; })
    .Sort(
      [](const Edge & e1, const Edge & e2) {
        return (e1.tail == e2.tail && e1.head < e2.head) || (e1.tail < e2.tail);
      })
    .Zip(new_node_id_times_degree, [](const Edge & edge, const uint32_t& new_id) { return Edge { new_id, edge.head }; });
    .Map([](const Edge & edge) { return WeightedEdge { edge.tail, edge.head, 1 } })
    .ReduceByKey(
      [](const WeightedEdge & edge) { return std::make_pair(edge.tail, edge.head) },
      [](const WeightedEdge & edge1, const WeightedEdge & edge2) { return WeightedEdge { edge1.tail, edge1.head, edge1.weight + edge2.weight }; })

}

int main(int, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    convert(context, argv[1], std::string(argv[1]) + ".graph");
  });
}

