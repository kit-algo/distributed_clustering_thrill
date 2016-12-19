#include <thrill/api/print.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/all_reduce.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/reduce_to_index.hpp>

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
  uint32_t id, degree;
};

struct NodeInfo {
  uint32_t id, data;
};

struct WeightedEdge {
  uint32_t tail, head, weight;

  uint32_t getWeight() {
    return weight;
  }
};

std::ostream& operator << (std::ostream& os, Edge& e) {
  return os << e.tail << " -> " << e.head;
}

std::ostream& operator << (std::ostream& os, NodeInfo& node_info) {
  return os << node_info.id << ": " << node_info.data;
}

// template<class NodeType>
// typename NodeType::EdgeType constructEdge(uint32_t, typename NodeType::NeighborType&);

// template<> Edge constructEdge<Node>(uint32_t node, uint32_t& neighbor) {
//   return Edge { node, neighbor };
// }

// template<> WeightedEdge constructEdge<WeightedNode>(uint32_t node, WeightedNodeNeighbor& neighbor) {
//   return WeightedEdge { node, neighbor.head, neighbor.weight };
// }

template<class EdgeType>
thrill::DIA<NodeInfo> louvain(thrill::DIA<EdgeType>& edge_list) {
  std::cout << "louvain\n";

  auto nodes = edge_list
    .Keep()
    .Map([](const EdgeType & edge) { return Node { edge.tail, 1 }; })
    .ReduceByKey(
      [](const Node & node) { return node.id; },
      [](const Node & node1, const Node & node2) { return Node { node1.id, node1.degree + node2.degree }; })
    .Sort([](const Node & node1, const Node & node2) { return node1.id < node2.id; });

  const uint64_t node_count = nodes.Keep().Size();

  auto edge_partitions = nodes
    .Keep()
    // Map to degree times partition
    .template FlatMap<uint32_t>(
      [](const Node & node, auto emit) {
        for (uint32_t i = 0; i < node.degree; i++) {
          emit(0); // dummy partition
        }
      });

  // Local Moving
  auto node_clusters = edge_list
    .Keep()
    .Zip(thrill::api::NoRebalanceTag, edge_partitions, [](const EdgeType & edge, const uint32_t & partition) { return std::make_pair(partition, edge); })
    // .template GroupByKey<std::vector<std::pair<uint32_t, uint32_t>>>(
    //   [](const std::pair<uint32_t, EdgeType> & edge_with_partition) { return edge_with_partition.first; },
    //   [](auto &, const uint32_t &) {
    //     // std::vector<Node> nodes;
    //     // std::vector<NodeCluster> mapping;
    //     // while (iterator.HasNext()) {
    //     //   // nodes.push_back(iterator.Next());
    //     //   NodeType& node = iterator.Next();
    //     //   mapping.push_back(NodeCluster { node.id, node.id % 4 }); // Dummy mapping
    //     // }

    //     // build graph
    //     // perform local moving
    //     // return mapping;
    //     std::vector<std::pair<uint32_t, uint32_t>> foo;
    //     return foo;
    //   })
    // .template FlatMap<NodeInfo>(
    //   [](std::vector<std::pair<uint32_t, uint32_t>> mapping, auto emit) {
    //     for (const std::pair<uint32_t, uint32_t>& pair : mapping) {
    //       emit(NodeInfo { pair.first, pair.second });
    //     }
    //   })
    .Map([](const std::pair<uint32_t, EdgeType> & pair) { return pair.second.tail; })
    .ReduceByKey(
      [](const uint32_t node) { return node; },
      [](const uint32_t node, const uint32_t) { return node; })
    .Map([](const uint32_t node) { return NodeInfo { node, node % 4 }; })
    .Sort(
      [](const NodeInfo & node_cluster1, const NodeInfo & node_cluster2) {
        return node_cluster1.id < node_cluster2.id;
      });

  // TODO cleanup cluster ids

  bool unchanged = node_clusters
    .Keep()
    .Map([](const NodeInfo & node_cluster) { return node_cluster.id == node_cluster.data; })
    .AllReduce([](bool acc, const bool value) { return acc && value; }, true);
  if (unchanged) {
    return node_clusters;
  }

  auto clusters = node_clusters
    .Keep()
    .Map([](const NodeInfo & node_cluster) { return node_cluster.data; });

  auto cluster_id_times_degree = nodes
    .Keep()
    // No rebalance
    .Zip(thrill::api::NoRebalanceTag, clusters, [](const Node & node, const uint32_t cluster_id) { return std::make_pair(node.degree, cluster_id); })
    .template FlatMap<uint32_t>(
      [](std::pair<size_t, uint32_t> degree_and_cluster, auto emit) {
        for (uint32_t i = 0; i < degree_and_cluster.first; i++) {
          emit(degree_and_cluster.second);
        }
      });

  // Build Meta Graph
  auto meta_graph_edges = edge_list
    // Translate Ids
    .Zip(thrill::api::NoRebalanceTag, cluster_id_times_degree.Keep(),
      [](EdgeType edge, uint32_t new_id) {
        edge.tail = new_id;
        return edge;
      })
    .Map(
      [](EdgeType edge) {
        uint32_t tmp = edge.head;
        edge.head = edge.tail;
        edge.tail = tmp;
        return edge;
      })
    .Sort(
      [](const EdgeType & e1, const EdgeType & e2) {
        return (e1.tail == e2.tail && e1.head < e2.head) || (e1.tail < e2.tail);
      })
    .Zip(thrill::api::NoRebalanceTag, cluster_id_times_degree,
      [](EdgeType edge, uint32_t new_id) {
        edge.tail = new_id;
        return edge;
      })
    .Map([](EdgeType edge) { return WeightedEdge { edge.tail, edge.head, edge.getWeight() }; })
    .ReduceByKey(
      [&node_count](WeightedEdge edge) { return node_count * edge.tail + edge.head; },
      [](WeightedEdge edge1, WeightedEdge edge2) { return WeightedEdge { edge1.tail, edge1.head, edge1.weight + edge2.weight }; })
    .Collapse();

  // Recursion on meta graph
  auto meta_clustering = louvain(meta_graph_edges);

  // translate meta clusters and return
  auto cluster_sizes = node_clusters
    .Keep()
    .Map([](const NodeInfo & node_cluster) { return std::make_pair(node_cluster.data, 1u); })
    .ReduceByKey(
      [](const std::pair<uint32_t, uint32_t> & cluster_size) { return cluster_size.first; },
      [](const std::pair<uint32_t, uint32_t> & cluster_size1, const std::pair<uint32_t, uint32_t> & cluster_size2) {
        return std::make_pair(cluster_size1.first, cluster_size1.second + cluster_size2.second);
      })
    .Sort([](const std::pair<uint32_t, uint32_t> & cluster_sizes1, const std::pair<uint32_t, uint32_t> & cluster_sizes2) { return cluster_sizes1.first < cluster_sizes2.first; });

  auto new_cluster_ids_times_size = meta_clustering
    .Zip(thrill::api::NoRebalanceTag, cluster_sizes, [](const NodeInfo & meta_node_cluster, const std::pair<uint32_t, uint32_t> & cluster_size) { return std::make_pair(meta_node_cluster.data, cluster_size.second); }) // assert(meta_node_cluster.id == cluster_size.first)
    .template FlatMap<uint32_t>(
      [](const std::pair<uint32_t, uint32_t> & cluster_size, auto emit) {
        for (uint32_t i = 0; i < cluster_size.second; i++) {
          emit(cluster_size.first);
        }
      });

  return node_clusters
    .Sort(
      [](const NodeInfo & node_cluster1, const NodeInfo & node_cluster2) {
        return node_cluster1.data < node_cluster2.data;
      })
    .Zip(thrill::api::NoRebalanceTag, new_cluster_ids_times_size, [](const NodeInfo & node_cluster, uint32_t new_cluster_id) { return NodeInfo { node_cluster.id, new_cluster_id }; })
    .Sort(
      [](const NodeInfo & node_cluster1, const NodeInfo & node_cluster2) {
        return node_cluster1.id < node_cluster2.id;
      });
}

int main(int, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    auto edges = thrill::ReadLines(context, argv[1])
      .Filter([](const std::string& line) { return !line.empty() && line[0] != '#'; })
      .template FlatMap<Edge>(
        [](const std::string& line, auto emit) {
          std::istringstream line_stream(line);
          uint32_t tail, head;

          if (line_stream >> tail >> head) {
            tail--;
            head--;
            emit(Edge { tail, head });
            emit(Edge { head, tail });
          } else {
            die(std::string("malformatted edge: ") + line);
          }
        })
      .Sort([](const Edge & e1, const Edge & e2) {
        return (e1.tail == e2.tail && e1.head < e2.head) || (e1.tail < e2.tail);
      });

    louvain(edges).Print("Clusters");
  });
}

