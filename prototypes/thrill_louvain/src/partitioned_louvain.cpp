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
#include <thrill/api/sum.hpp>
#include <thrill/api/inner_join.hpp>

#include <ostream>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <set>
#include <map>

#include "seq_louvain/graph.hpp"
#include "seq_louvain/cluster_store.hpp"
#include "modularity.hpp"
#include "partitioning.hpp"

#include "thrill_graph.hpp"
#include "thrill_partitioning.hpp"
#include "input.hpp"

struct NodeInfo {
  NodeId id;
  ClusterId data;
};

std::ostream& operator << (std::ostream& os, NodeInfo& node_info) {
  return os << node_info.id << ": " << node_info.data;
}

template<class Graph>
thrill::DIA<NodeInfo> louvain(const Graph& graph) {
  constexpr bool weighted = std::is_same<typename Graph::Node, NodeWithWeightedLinks>::value;
  using Node = typename std::conditional<weighted, NodeWithWeightedLinksAndTargetDegree, NodeWithLinksAndTargetDegree>::type;
  using EdgeType = typename Graph::Edge;

  uint32_t partition_size = graph.nodes.context().num_workers();
  uint32_t partition_element_size = Partitioning::partitionElementTargetSize(graph.node_count, partition_size);
  if (partition_element_size < 1000 && graph.node_count < 10000) {
    partition_size = 1;
    partition_element_size = graph.node_count;
  }
  auto node_partitions = partition(graph, partition_size);

  auto nodes = graph.nodes
    .template FlatMap<std::pair<NodeId, std::pair<typename Graph::Node::LinkType, Weight>>>(
      [](const typename Graph::Node& node, auto emit) {
        for (typename Graph::Node::LinkType link : node.links) {
          NodeId old_target = link.target;
          link.target = node.id;
          emit(std::make_pair(old_target, std::make_pair(link, node.weightedDegree())));
        }
      })
    .template GroupToIndex<Node>(
      [](const std::pair<NodeId, std::pair<typename Graph::Node::LinkType, Weight>>& edge_with_target_degree) { return edge_with_target_degree.first; },
      [](auto& iterator, const NodeId node_id) {
        Node node { node_id, {} };
        while (iterator.HasNext()) {
          const std::pair<NodeId, std::pair<typename Graph::Node::LinkType, Weight>>& edge_with_target_degree = iterator.Next();
          node.push_back(Node::LinkType::fromLink(edge_with_target_degree.second.first, edge_with_target_degree.second.second));
        }
        return node;
      },
      graph.node_count);

  auto bloated_node_clusters = nodes
    .Zip(thrill::NoRebalanceTag, node_partitions,
      [](const Node& node, const NodePartition& node_partition) {
        assert(node.id == node_partition.node_id);
        return std::make_pair(node, node_partition.partition);
      })
    // Local Moving
    .template GroupToIndex<std::vector<std::pair<NodeId, ClusterId>>>(
      [](const std::pair<Node, uint32_t>& node_partition) -> size_t { return node_partition.second; },
      [total_weight = graph.total_weight, partition_element_size](auto& iterator, const uint32_t) {
        // TODO random
        GhostGraph<weighted> graph(partition_element_size, total_weight);
        const std::vector<NodeId> reverse_mapping = graph.initialize([&iterator](const auto& emit) {
          while (iterator.HasNext()) {
            emit(iterator.Next().first);
          }
        });

        GhostClusterStore clusters(graph.getNodeCount());
        Modularity::localMoving(graph, clusters);
        clusters.rewriteClusterIds(reverse_mapping);

        std::vector<std::pair<uint32_t, uint32_t>> mapping;
        mapping.reserve(graph.getNodeCount());
        for (NodeId node = 0; node < graph.getNodeCount(); node++) {
          mapping.push_back(std::make_pair(reverse_mapping[node], clusters[node]));
        }
        return mapping;
      }, partition_size)
    .template FlatMap<NodeInfo>(
      [](std::vector<std::pair<uint32_t, uint32_t>> mapping, auto emit) {
        for (const std::pair<uint32_t, uint32_t>& pair : mapping) {
          emit(NodeInfo { pair.first, pair.second });
        }
      })
    .Cache();

  auto clean_cluster_ids_mapping = bloated_node_clusters
    .Keep()
    .Map([](const NodeInfo& node_cluster) { return node_cluster.data; })
    .ReduceByKey(
      [](const uint32_t cluster) { return cluster; },
      [](const uint32_t cluster, const uint32_t) {
        return cluster;
      })
    .ZipWithIndex([](uint32_t old_cluster_id, uint32_t index) { return std::make_pair(old_cluster_id, index); });

  auto cluster_count = clean_cluster_ids_mapping.Keep().Size();

  auto node_clusters = bloated_node_clusters
    .Keep() // TODO find way to do only in debugging
    .InnerJoin(clean_cluster_ids_mapping,
      [](const NodeInfo& node_cluster) { return node_cluster.data; },
      [](const std::pair<uint32_t, uint32_t>& mapping) { return mapping.first; },
      [](const NodeInfo& node_cluster, const std::pair<uint32_t, uint32_t>& mapping) {
        return NodeInfo { node_cluster.id, mapping.second };
      })
    .Cache();

  assert(node_clusters.Keep().Size() == bloated_node_clusters.Size());

  if (graph.node_count == cluster_count) {
    return node_clusters;
  }

  // Build Meta Graph
  auto meta_graph_edges = graph.edges
    .InnerJoin(node_clusters,
      [](const EdgeType& edge) { return edge.tail; },
      [](const NodeInfo& node_cluster) { return node_cluster.id; },
      [](EdgeType edge, const NodeInfo& node_cluster) {
          edge.tail = node_cluster.data;
          return edge;
      })
    .InnerJoin(node_clusters,
      [](const EdgeType& edge) { return edge.head; },
      [](const NodeInfo& node_cluster) { return node_cluster.id; },
      [](EdgeType edge, const NodeInfo& node_cluster) {
          edge.head = node_cluster.data;
          return edge;
      })
    .Map([](EdgeType edge) { return WeightedEdge { edge.tail, edge.head, edge.getWeight() }; })
    .ReduceByKey(
      [](WeightedEdge edge) { return Util::combine_u32ints(edge.tail, edge.head); },
      [](WeightedEdge edge1, WeightedEdge edge2) { return WeightedEdge { edge1.tail, edge1.head, edge1.weight + edge2.weight }; })
    // turn loops into forward and backward arc
    .template FlatMap<WeightedEdge>(
      [](const WeightedEdge & edge, auto emit) {
        if (edge.tail == edge.head) {
          emit(WeightedEdge { edge.tail, edge.head, edge.weight / 2 });
          emit(WeightedEdge { edge.tail, edge.head, edge.weight / 2 });
        } else {
          emit(edge);
        }
      })
    .Cache();

  auto meta_nodes = edgesToNodes(meta_graph_edges, cluster_count).Collapse();

  assert(meta_graph_edges.Keep().Map([](const WeightedEdge& edge) { return edge.getWeight(); }).Sum() / 2 == graph.total_weight);

  // Recursion on meta graph
  auto meta_clustering = louvain(DiaGraph<NodeWithWeightedLinks, WeightedEdge> { meta_nodes, meta_graph_edges, cluster_count, graph.total_weight });

  return node_clusters
    .InnerJoin(meta_clustering,
      [](const NodeInfo& node_cluster) { return node_cluster.data; },
      [](const NodeInfo& meta_node_cluster) { return meta_node_cluster.id; },
      [](const NodeInfo& node_cluster, const NodeInfo& meta_node_cluster) {
          return NodeInfo { node_cluster.id, meta_node_cluster.data };
      });
}

int main(int, char const *argv[]) {
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  Modularity::rng = std::default_random_engine(seed);

  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    auto graph = Input::readGraph(argv[1], context);

    size_t cluster_count = louvain(graph)
      .Map([](const NodeInfo& node_cluster) { return node_cluster.data; })
      .ReduceByKey(
        [](const uint32_t cluster) { return cluster; },
        [](const uint32_t cluster, const uint32_t) {
          return cluster;
        })
      .Size();
    std::cout << "Result: " << cluster_count << std::endl;
  });
}

