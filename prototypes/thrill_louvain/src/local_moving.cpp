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
#include <thrill/api/size.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/all_gather.hpp>

#include <ostream>
#include <iostream>
#include <vector>
#include <map>

#include "util.hpp"
#include "thrill_graph.hpp"
#include "input.hpp"
#include "thrill_modularity.hpp"

#define SUBITERATIONS 4
#define SUPER_ITERATIONS 16

using ClusterId = NodeId;

using NodeCluster = std::pair<NodeId, ClusterId>;
using ClusterWeight = std::pair<ClusterId, Weight>;

struct Node {
  NodeId id;
  Weight degree;
};

struct IncidentClusterInfo {
  ClusterId cluster;
  Weight inbetween_weight;
  Weight total_weight;
};

struct LocalMovingNode {
  NodeId id;
  Weight degree;
  ClusterId cluster;
  Weight cluster_total_weight;
};

std::ostream& operator << (std::ostream& os, Node& node) {
  return os << node.id << " (" << node.degree << ")";
}

namespace std {
  std::ostream& operator << (std::ostream& os, NodeCluster& nc) {
    return os << nc.first << ": " << nc.second;
  }

  std::ostream& operator << (std::ostream& os, std::pair<NodeId, ClusterWeight>& node_cluster_weight) {
    return os << "n" << node_cluster_weight.first << " - c" << node_cluster_weight.second.first << " - w" << node_cluster_weight.second.second;
  }
} // std


int64_t deltaModularity(const Weight node_degree,
                        const ClusterId current_cluster,
                        const ClusterId target_cluster,
                        const int64_t weight_between_node_and_current_cluster,
                        const int64_t weight_between_node_and_target_cluster,
                        int64_t current_cluster_total_weight,
                        int64_t target_cluster_total_weight,
                        const Weight total_weight) {
  if (target_cluster == current_cluster) {
    target_cluster_total_weight -= node_degree;
  }

  current_cluster_total_weight -= node_degree;

  int64_t e = ((weight_between_node_and_target_cluster - weight_between_node_and_current_cluster) * total_weight * 2);
  int64_t a = (target_cluster_total_weight - current_cluster_total_weight) * node_degree;
  return e - a;
}

bool included(const NodeId node, const uint32_t iteration) {
  return node % SUBITERATIONS == iteration % SUBITERATIONS;
  // size_t hash = Util::combined_hash(node, iteration);
  // uint32_t sizes[] = { 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 4, 4, 4, 4, 3, 3, 3, 2, 2, 1, 1 };
  // return hash % sizes[iteration] == 0;
}

template<class EdgeType>
thrill::DIA<NodeCluster> local_moving(const DiaGraph<EdgeType>& graph, thrill::DIA<Node>& nodes, uint32_t num_iterations) {
  auto node_clusters = graph.edge_list
    .Keep()
    .Map([](const EdgeType& edge) { return edge.tail; })
    .Uniq()
    .Map([](const NodeId& id) { return NodeCluster(id, id); })
    .Collapse()
    .Cache();

  size_t cluster_count = graph.node_count;

  for (uint32_t iteration = 0; iteration < num_iterations * SUBITERATIONS; iteration++) {
    auto node_cluster_weights = graph.edge_list
      .Keep()
      .InnerJoin(node_clusters,
        [](const EdgeType& edge) { return edge.tail; },
        [](const NodeCluster& node_cluster) { return node_cluster.first; },
        [](const EdgeType& edge, const NodeCluster& node_cluster) { return ClusterWeight(node_cluster.second, edge.getWeight()); })
      .ReducePair([](const Weight weight1, const Weight weight2) { return weight1 + weight2; })
      .InnerJoin(node_clusters,
        [](const std::pair<ClusterId, Weight>& cluster_weight) { return cluster_weight.first; },
        [](const NodeCluster& node_cluster) { return node_cluster.second; },
        [](const ClusterWeight& cluster_weight, const NodeCluster& node_cluster) { return std::make_pair(node_cluster.first, cluster_weight); });

    auto other_node_clusters = node_clusters
      .Filter([iteration](const NodeCluster& node_cluster) { return !included(node_cluster.first, iteration); });

    node_clusters = graph.edge_list
      // filter out nodes not in subiteration
      .Filter([iteration](const EdgeType& edge) { return included(edge.tail, iteration); })
      // join cluster weight onto edge targets
      .InnerJoin(node_cluster_weights,
        [](const EdgeType& edge) { return edge.head; },
        [](const std::pair<NodeId, ClusterWeight>& node_cluster_weight) { return node_cluster_weight.first; },
        [](const EdgeType& edge, const std::pair<NodeId, ClusterWeight>& node_cluster_weight) {
          return std::make_pair(
            edge.tail,
            IncidentClusterInfo {
              node_cluster_weight.second.first,
              (edge.head == edge.tail) ? 0 : edge.getWeight(), // loops dont count towards the inbetween weight
              node_cluster_weight.second.second
            }
          );
        })
      // sum up edges with the same target cluster
      .ReduceByKey(
        [](const std::pair<NodeId, IncidentClusterInfo>& node_with_incident_cluster) -> uint64_t {
          return Util::combine_u32ints(node_with_incident_cluster.first, node_with_incident_cluster.second.cluster);
        },
        [](const std::pair<NodeId, IncidentClusterInfo>& node_with_incident_cluster1, const std::pair<NodeId, IncidentClusterInfo>& node_with_incident_cluster2) {
          return std::make_pair(node_with_incident_cluster1.first, IncidentClusterInfo { node_with_incident_cluster1.second.cluster, node_with_incident_cluster1.second.inbetween_weight + node_with_incident_cluster2.second.inbetween_weight , node_with_incident_cluster1.second.total_weight });
        })
      // join cluster weight of each node
      .InnerJoin(node_cluster_weights,
        [](const std::pair<NodeId, IncidentClusterInfo>& node_cluster) { return node_cluster.first; },
        [](const std::pair<NodeId, std::pair<ClusterId, Weight>>& node_cluster_weight) { return node_cluster_weight.first; },
        [](const std::pair<NodeId, IncidentClusterInfo>& node_cluster, const std::pair<NodeId, std::pair<ClusterId, Weight>>& node_cluster_weight) {
          return std::make_pair(node_cluster_weight, node_cluster.second);
        })
      // map to vector of neighbor clusters
      .Map(
        [](const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, IncidentClusterInfo>& local_moving_edge) {
          return std::make_pair(local_moving_edge.first, std::vector<IncidentClusterInfo>({ local_moving_edge.second }));
        })
      // reduce by each node so the vector is filled with all neighbors
      .ReduceByKey(
        [](const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, std::vector<IncidentClusterInfo>>& local_moving_node) { return local_moving_node.first.first; },
        [](const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, std::vector<IncidentClusterInfo>>& local_moving_node1, const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, std::vector<IncidentClusterInfo>>& local_moving_node2) {
          std::vector<IncidentClusterInfo> tmp(local_moving_node1.second.begin(), local_moving_node1.second.end());
          tmp.insert(tmp.end(), local_moving_node2.second.begin(), local_moving_node2.second.end());
          return std::make_pair(local_moving_node1.first, tmp);
        })
      // join node degree
      .InnerJoin(nodes.Keep(),
        [](const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, std::vector<IncidentClusterInfo>>& local_moving_node) {
          return local_moving_node.first.first;
        },
        [](const Node& node) { return node.id; },
        [](const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, std::vector<IncidentClusterInfo>>& local_moving_node, const Node& node) {
          return std::make_pair(LocalMovingNode { local_moving_node.first.first, node.degree, local_moving_node.first.second.first, local_moving_node.first.second.second }, local_moving_node.second);
        })
      // map to best neighbor cluster
      .Map(
        [&graph](const std::pair<LocalMovingNode, std::vector<IncidentClusterInfo>>& local_moving_node) {
          ClusterId best_cluster = local_moving_node.first.cluster;
          int64_t best_delta = 0;
          Weight degree = local_moving_node.first.degree;
          Weight weight_between_node_and_current_cluster = 0;
          for (const IncidentClusterInfo& incident_cluster : local_moving_node.second) {
            if (incident_cluster.cluster == local_moving_node.first.cluster) {
              weight_between_node_and_current_cluster = incident_cluster.inbetween_weight;
            }
          }

          for (const IncidentClusterInfo& incident_cluster : local_moving_node.second) {
            int64_t delta = deltaModularity(degree, local_moving_node.first.cluster, incident_cluster.cluster,
                                            weight_between_node_and_current_cluster, incident_cluster.inbetween_weight,
                                            local_moving_node.first.cluster_total_weight, incident_cluster.total_weight,
                                            graph.total_weight);
            if (delta > best_delta) {
              best_delta = delta;
              best_cluster = incident_cluster.cluster;
            }
          }

          return NodeCluster(local_moving_node.first.id, best_cluster);
          // return NodeCluster(local_moving_node.first.id, (iteration % 2 == 0 && best_cluster > local_moving_node.first.cluster) || (iteration % 2 != 0 && best_cluster < local_moving_node.first.cluster) ? best_cluster : local_moving_node.first.cluster);
        })
      // bring back other clusters
      .Union(other_node_clusters)
      .Collapse()
      .Cache();

    if (iteration % SUBITERATIONS == SUBITERATIONS - 1) {
      assert(graph.node_count == node_clusters.Keep().Size());

      size_t round_cluster_count = node_clusters.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq().Size();
      std::cout << "from " << cluster_count << " to " << round_cluster_count << std::endl;

      if (cluster_count - round_cluster_count <= graph.node_count / 100) {
        break;
      }

      cluster_count = round_cluster_count;
    }
  }

  return node_clusters;
}

template<class EdgeType>
thrill::DIA<NodeCluster> louvain(const DiaGraph<EdgeType>& graph) {
  std::cout << "louvain" << std::endl;

  auto nodes = graph.edge_list
    .Keep()
    .Map([](const EdgeType & edge) { return Node { edge.tail, edge.getWeight() }; })
    .ReduceByKey(
      [](const Node & node) { return node.id; },
      [](const Node & node1, const Node & node2) { return Node { node1.id, node1.degree + node2.degree }; });
    // .Cache(); ? is recalculated, but worth it?

  assert(nodes.Keep().Size() == graph.node_count);

  auto node_clusters = local_moving(graph, nodes, SUPER_ITERATIONS);
  auto distinct_cluster_ids = node_clusters.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq();
  size_t cluster_count = distinct_cluster_ids.Keep().Size();

  if (graph.node_count == cluster_count) {
    return node_clusters;
  }

  node_clusters = distinct_cluster_ids
    .ZipWithIndex([](const ClusterId cluster_id, const size_t index) { return std::make_pair(cluster_id, index); })
    .InnerJoin(node_clusters,
      [](const std::pair<ClusterId, size_t>& pair){ return pair.first; },
      [](const NodeCluster& node_cluster) { return node_cluster.second; },
      [](const std::pair<ClusterId, size_t>& pair, const NodeCluster& node_cluster) {
        return NodeCluster(node_cluster.first, pair.second);
      });

  // Build Meta Graph
  auto meta_graph_edges = graph.edge_list
    .InnerJoin(node_clusters.Keep(),
      [](const EdgeType& edge) { return edge.tail; },
      [](const NodeCluster& node_cluster) { return node_cluster.first; },
      [](EdgeType edge, const NodeCluster& node_cluster) {
          edge.tail = node_cluster.second;
          return edge;
      })
    .InnerJoin(node_clusters.Keep(),
      [](const EdgeType& edge) { return edge.head; },
      [](const NodeCluster& node_cluster) { return node_cluster.first; },
      [](EdgeType edge, const NodeCluster& node_cluster) {
          edge.head = node_cluster.second;
          return edge;
      })
    .Map([](EdgeType edge) { return WeightedEdge { edge.tail, edge.head, edge.getWeight() }; })
    .ReduceByKey(
      [&graph](WeightedEdge edge) { return graph.node_count * edge.tail + edge.head; },
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
    .Collapse();

  assert(meta_graph_edges.Keep().Map([](const WeightedEdge& edge) { return edge.getWeight(); }).Sum() / 2 == graph.total_weight);

  // Recursion on meta graph
  auto meta_clustering = louvain(DiaGraph<WeightedEdge> { meta_graph_edges, cluster_count, graph.total_weight });

  return node_clusters
    .Keep() // TODO why on earth is this necessary?
    .InnerJoin(meta_clustering,
      [](const NodeCluster& node_cluster) { return node_cluster.second; },
      [](const NodeCluster& meta_node_cluster) { return meta_node_cluster.first; },
      [](const NodeCluster& node_cluster, const NodeCluster& meta_node_cluster) {
          return NodeCluster(node_cluster.first, meta_node_cluster.second);
      });
}

int main(int, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    auto graph = Input::readGraph(argv[1], context);

    auto node_clusters = louvain(graph);

    double modularity = Modularity::modularity(graph, node_clusters);
    size_t cluster_count = node_clusters.Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq().Size();

    if (context.my_rank() == 0) {
      std::cout << "Modularity: " << modularity << " Result: " << cluster_count << std::endl;
    }
  });
}

