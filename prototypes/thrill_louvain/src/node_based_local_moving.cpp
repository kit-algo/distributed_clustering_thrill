#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_with_index.hpp>

#include <ostream>
#include <iostream>
#include <vector>

#include <cluster_store.hpp>
#include <logging.hpp>

#include "util.hpp"
#include "thrill_graph.hpp"
#include "input.hpp"
#include "thrill_modularity.hpp"

#define ITERATIONS 32

using int128_t = __int128_t;

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

  IncidentClusterInfo operator+(const IncidentClusterInfo& other) const {
    assert(cluster == other.cluster);
    return IncidentClusterInfo { cluster, inbetween_weight + other.inbetween_weight, std::min(total_weight, other.total_weight) };
  }
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


int128_t deltaModularity(const Node& node, const IncidentClusterInfo& neighbored_cluster, Weight total_weight) {
  int128_t e = neighbored_cluster.inbetween_weight * total_weight * 2;
  int128_t a = neighbored_cluster.total_weight * node.degree;
  return e - a;
}

bool nodeIncluded(const NodeId node, const uint32_t iteration, const uint32_t rate) {
  uint32_t hash = Util::combined_hash(node, iteration);
  return hash % 1000 < rate;
}

template<class NodeType, class EdgeType>
auto local_moving(const DiaGraph<NodeType, EdgeType>& graph, uint32_t num_iterations) {
  using NodeWithTargetDegreesType = typename std::conditional<std::is_same<NodeType, NodeWithLinks>::value, NodeWithLinksAndTargetDegree, NodeWithWeightedLinksAndTargetDegree>::type;

  auto nodes = graph.nodes
    .template FlatMap<std::pair<NodeId, std::pair<typename NodeType::LinkType, Weight>>>(
      [](const NodeType& node, auto emit) {
        for (typename NodeType::LinkType link : node.links) {
          NodeId old_target = link.target;
          link.target = node.id;
          emit(std::make_pair(old_target, std::make_pair(link, node.weightedDegree())));
        }
      })
    .template GroupToIndex<NodeWithTargetDegreesType>(
      [](const std::pair<NodeId, std::pair<typename NodeType::LinkType, Weight>>& edge_with_target_degree) { return edge_with_target_degree.first; },
      [](auto& iterator, const NodeId node_id) {
        NodeWithTargetDegreesType node { node_id, {} };
        while (iterator.HasNext()) {
          const std::pair<NodeId, std::pair<typename NodeType::LinkType, Weight>>& edge_with_target_degree = iterator.Next();
          node.push_back(NodeWithTargetDegreesType::LinkType::fromLink(edge_with_target_degree.second.first, edge_with_target_degree.second.second));
        }
        return node;
      },
      graph.node_count);

  auto node_clusters = nodes
    .Map([](const NodeWithTargetDegreesType& node) { return std::make_pair(std::make_pair(node, node.id), false); })
    .Cache();

  size_t cluster_count = graph.node_count;
  uint32_t rate = 200;
  uint32_t rate_sum = 0;

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    auto included = [iteration, rate](const NodeId id) { return nodeIncluded(id, iteration, rate); };

    size_t considered_nodes = node_clusters
      .Keep()
      .Filter(
        [&included](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) {
          return included(node_cluster.first.first.id);
        })
      .Size();

    if (considered_nodes > 0) {
      node_clusters = node_clusters
        .Keep()
        .Map([](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) { return std::make_pair(node_cluster.first.second, node_cluster.first.first.weightedDegree()); })
        .ReducePair([](const Weight weight1, const Weight weight2) { return weight1 + weight2; })
        .InnerJoin(node_clusters, // TODO PERFORMANCE aggregate with groub by may be cheaper
          [](const std::pair<ClusterId, Weight>& cluster_weight) { return cluster_weight.first; },
          [](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) { return node_cluster.first.second; },
          [](const ClusterWeight& cluster_weight, const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) { return std::make_pair(node_cluster.first, cluster_weight.second); })
        .template FlatMap<std::pair<Node, IncidentClusterInfo>>(
          [&included](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, Weight>& node_cluster_weight, auto emit) {
            for (const typename NodeWithTargetDegreesType::LinkType& link : node_cluster_weight.first.first.links) {
              if (included(link.target)) {
                emit(std::make_pair(
                  Node { link.target, link.target_degree },
                  IncidentClusterInfo {
                    node_cluster_weight.first.second,
                    node_cluster_weight.first.first.id != link.getTarget() ? link.getWeight() : 0,
                    node_cluster_weight.second
                  }
                ));
              }
            }
            if (included(node_cluster_weight.first.first.id)) {
              emit(std::make_pair(
                Node { node_cluster_weight.first.first.id, node_cluster_weight.first.first.weightedDegree() },
                IncidentClusterInfo {
                  node_cluster_weight.first.second,
                  0,
                  node_cluster_weight.second - node_cluster_weight.first.first.weightedDegree()
                }
              ));
            }
          })
        // Reduce equal clusters per node
        .ReduceByKey(
          [](const std::pair<Node, IncidentClusterInfo>& local_moving_edge) { return Util::combine_u32ints(local_moving_edge.first.id, local_moving_edge.second.cluster); },
          [](const std::pair<Node, IncidentClusterInfo>& lme1, const std::pair<Node, IncidentClusterInfo>& lme2) {
            return std::make_pair(lme1.first, lme1.second + lme2.second);
          })
        // Reduce to best cluster
        .ReduceToIndex(
          [](const std::pair<Node, IncidentClusterInfo>& lme) -> size_t { return lme.first.id; },
          [total_weight = graph.total_weight](const std::pair<Node, IncidentClusterInfo>& lme1, const std::pair<Node, IncidentClusterInfo> lme2) {
            // TODO Tie breaking
            if (deltaModularity(lme2.first, lme2.second, total_weight) > deltaModularity(lme1.first, lme1.second, total_weight)) {
              return lme2;
            } else {
              return lme1;
            }
          },
          graph.node_count)
        .Zip(thrill::NoRebalanceTag, node_clusters,
          [&included](const std::pair<Node, IncidentClusterInfo>& lme, const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& old_node_cluster) {
            assert(!included(old_node_cluster.first.first.id) || lme.first.id == old_node_cluster.first.first.id);
            if (included(old_node_cluster.first.first.id)) {
              return std::make_pair(std::make_pair(old_node_cluster.first.first, lme.second.cluster), lme.second.cluster != old_node_cluster.first.second);
            } else {
              return std::make_pair(old_node_cluster.first, false);
            }
          })
        .Cache();

      rate_sum += rate;
      rate = 1000 - (node_clusters.Keep().Filter([&included](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& pair) { return pair.second && included(pair.first.first.id); }).Size() * 1000 / considered_nodes);

      if (rate_sum >= 1000) {
        assert(graph.node_count == node_clusters.Keep().Size());

        size_t round_cluster_count = node_clusters.Keep().Map([](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) { return node_cluster.first.second; }).Uniq().Size();

        if (cluster_count - round_cluster_count <= graph.node_count / 100) {
          break;
        }

        cluster_count = round_cluster_count;
        rate_sum -= 1000;
      }
    } else {
      rate += 200;
      if (rate > 1000) { rate = 1000; }
    }
  }

  return node_clusters.Map([](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) { return NodeCluster(node_cluster.first.first.id, node_cluster.first.second); });
}


template<class NodeType, class EdgeType>
thrill::DIA<NodeCluster> louvain(const DiaGraph<NodeType, EdgeType>& graph) {
  auto node_clusters = local_moving(graph, ITERATIONS);
  auto distinct_cluster_ids = node_clusters.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq();
  size_t cluster_count = distinct_cluster_ids.Keep().Size();

  if (graph.node_count == cluster_count) {
    return node_clusters.Collapse();
  }

  auto cleaned_node_clusters = distinct_cluster_ids
    .ZipWithIndex([](const ClusterId cluster_id, const size_t index) { return std::make_pair(cluster_id, index); })
    .InnerJoin(node_clusters,
      [](const std::pair<ClusterId, size_t>& pair){ return pair.first; },
      [](const NodeCluster& node_cluster) { return node_cluster.second; },
      [](const std::pair<ClusterId, size_t>& pair, const NodeCluster& node_cluster) {
        return NodeCluster(node_cluster.first, pair.second);
      });

  // Build Meta Graph
  auto meta_graph_edges = graph.edges
    .Keep()
    .InnerJoin(cleaned_node_clusters.Keep(),
      [](const EdgeType& edge) { return edge.tail; },
      [](const NodeCluster& node_cluster) { return node_cluster.first; },
      [](EdgeType edge, const NodeCluster& node_cluster) {
          edge.tail = node_cluster.second;
          return edge;
      })
    .InnerJoin(cleaned_node_clusters.Keep(),
      [](const EdgeType& edge) { return edge.head; },
      [](const NodeCluster& node_cluster) { return node_cluster.first; },
      [](EdgeType edge, const NodeCluster& node_cluster) {
          edge.head = node_cluster.second;
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

  auto nodes = edgesToNodes(meta_graph_edges, cluster_count).Collapse();

  assert(meta_graph_edges.Keep().Map([](const WeightedEdge& edge) { return edge.getWeight(); }).Sum() / 2 == graph.total_weight);

  // Recursion on meta graph
  auto meta_clustering = louvain(DiaGraph<NodeWithWeightedLinks, WeightedEdge> { nodes, meta_graph_edges, cluster_count, graph.total_weight });

  return cleaned_node_clusters
    .InnerJoin(meta_clustering,
      [](const NodeCluster& node_cluster) { return node_cluster.second; },
      [](const NodeCluster& meta_node_cluster) { return meta_node_cluster.first; },
      [](const NodeCluster& node_cluster, const NodeCluster& meta_node_cluster) {
          return NodeCluster(node_cluster.first, meta_node_cluster.second);
      });
}

int main(int argc, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    auto graph = Input::readGraph(argv[1], context);

    Logging::Id program_run_logging_id;
    if (context.my_rank() == 0) {
      program_run_logging_id = Logging::getUnusedId();
      Logging::report("program_run", program_run_logging_id, "binary", argv[0]);
      Logging::report("program_run", program_run_logging_id, "graph", argv[1]);
      Logging::report("program_run", program_run_logging_id, "node_count", graph.node_count);
      Logging::report("program_run", program_run_logging_id, "edge_count", graph.total_weight);
    }

    auto node_clusters = louvain(graph);

    double modularity = Modularity::modularity(graph, node_clusters);
    size_t cluster_count = node_clusters.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq().Size();

    auto local_node_clusters = node_clusters.Gather();

    ClusterStore clusters(context.my_rank() == 0 ? graph.node_count : 0);
    Logging::Id clusters_logging_id;
    if (context.my_rank() == 0) {
      Logging::Id algorithm_run_id = Logging::getUnusedId();
      Logging::report("algorithm_run", algorithm_run_id, "program_run_id", program_run_logging_id);
      Logging::report("algorithm_run", algorithm_run_id, "algorithm", "thrill louvain fully distributed local moving");
      clusters_logging_id = Logging::getUnusedId();
      Logging::report("clustering", clusters_logging_id, "source", "computation");
      Logging::report("clustering", clusters_logging_id, "algorithm_run_id", algorithm_run_id);
      Logging::report("clustering", clusters_logging_id, "modularity", modularity);
      Logging::report("clustering", clusters_logging_id, "cluster_count", cluster_count);

      for (const auto& node_cluster : local_node_clusters) {
        clusters.set(node_cluster.first, node_cluster.second);
      }
    }
    if (argc > 2) {
      auto ground_proof = thrill::ReadBinary<NodeCluster>(context, argv[2]);

      modularity = Modularity::modularity(graph, ground_proof);
      cluster_count = ground_proof.Keep().Map([](const NodeCluster& node_cluster) { return node_cluster.second; }).Uniq().Size();

      auto local_ground_proof = ground_proof.Gather();

      if (context.my_rank() == 0) {
        Logging::report("program_run", program_run_logging_id, "ground_proof", argv[2]);

        ClusterStore ground_proof_clusters(graph.node_count);
        for (const auto& node_cluster : local_ground_proof) {
          ground_proof_clusters.set(node_cluster.first, node_cluster.second);
        }

        Logging::Id ground_proof_logging_id = Logging::getUnusedId();
        Logging::report("clustering", ground_proof_logging_id, "source", "ground_proof");
        Logging::report("clustering", ground_proof_logging_id, "program_run_id", program_run_logging_id);
        Logging::report("clustering", ground_proof_logging_id, "modularity", modularity);
        Logging::report("clustering", ground_proof_logging_id, "cluster_count", cluster_count);
        Logging::log_comparison_results(ground_proof_logging_id, ground_proof_clusters, clusters_logging_id, clusters);
      }
    }
  });
}

