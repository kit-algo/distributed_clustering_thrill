#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/gather.hpp>

#include <ostream>
#include <iostream>
#include <vector>
#include <map>

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

struct FullNodeCluster {
  NodeId id;
  Weight degree;
  ClusterId cluster;
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


int128_t deltaModularity(const Weight node_degree,
                        const ClusterId current_cluster,
                        const ClusterId target_cluster,
                        const int128_t weight_between_node_and_current_cluster,
                        const int128_t weight_between_node_and_target_cluster,
                        int128_t current_cluster_total_weight,
                        int128_t target_cluster_total_weight,
                        const Weight total_weight) {
  if (target_cluster == current_cluster) {
    target_cluster_total_weight -= node_degree;
  }

  current_cluster_total_weight -= node_degree;

  int128_t e = ((weight_between_node_and_target_cluster - weight_between_node_and_current_cluster) * total_weight * 2);
  int128_t a = (target_cluster_total_weight - current_cluster_total_weight) * node_degree;
  return e - a;
}

bool included(const NodeId node, const uint32_t iteration, const uint32_t rate) {
  // return node % SUBITERATIONS == iteration % SUBITERATIONS;
  uint32_t hash = Util::combined_hash(node, iteration);
  return hash % 1000 < rate;
  // uint32_t sizes[] = { 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 4, 4, 4, 4, 3, 3, 3, 2, 2, 1, 1 };
  // return hash % sizes[iteration] == 0;
}

template<class EdgeType>
thrill::DIA<NodeCluster> local_moving(const DiaEdgeGraph<EdgeType>& graph, thrill::DIA<Node>& nodes, uint32_t num_iterations) {
  auto node_clusters = nodes
    .Map([](const Node& node) { return FullNodeCluster { node.id, node.degree, node.id }; })
    .Collapse();

  size_t cluster_count = graph.node_count;
  uint32_t rate = 200;
  uint32_t rate_sum = 0;

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    // if (node_clusters.context().my_rank() == 0) {
    //   std::cout << "Iteration: " << iteration << " Rate: " << rate << std::endl;
    // }

    auto node_cluster_weights = node_clusters
      .Keep()
      .Map([](const FullNodeCluster& node_cluster) { return std::make_pair(node_cluster.cluster, node_cluster.degree); })
      .ReducePair([](const Weight weight1, const Weight weight2) { return weight1 + weight2; })
      .InnerJoin(node_clusters,
        [](const std::pair<ClusterId, Weight>& cluster_weight) { return cluster_weight.first; },
        [](const FullNodeCluster& node_cluster) { return node_cluster.cluster; },
        [](const ClusterWeight& cluster_weight, const FullNodeCluster& node_cluster) { return std::make_pair(node_cluster, cluster_weight.second); });

    assert(node_cluster_weights.Keep().Size() == graph.node_count);

    auto other_node_clusters = node_clusters
      .Filter([iteration, rate](const FullNodeCluster& node_cluster) { return !included(node_cluster.id, iteration, rate); });

    size_t considered_nodes = graph.node_count - other_node_clusters.Keep().Size();

    if (considered_nodes > 0) {
      auto new_node_clusters = graph.edges
        .Keep()
        // filter out nodes not in subiteration
        .Filter([iteration, rate](const EdgeType& edge) { return included(edge.tail, iteration, rate); })
        // join cluster weight onto edge targets
        .InnerJoin(node_cluster_weights,
          [](const EdgeType& edge) { return edge.head; },
          [](const std::pair<FullNodeCluster, Weight>& node_cluster_weight) { return node_cluster_weight.first.id; },
          [](const EdgeType& edge, const std::pair<FullNodeCluster, Weight>& node_cluster_weight) {
            return std::make_pair(
              edge.tail,
              IncidentClusterInfo {
                node_cluster_weight.first.cluster,
                (edge.head == edge.tail) ? 0u : edge.getWeight(), // loops dont count towards the inbetween weight
                node_cluster_weight.second
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
        // map to vector of neighbor clusters
        .Map(
          [](const std::pair<NodeId, IncidentClusterInfo>& local_moving_edge) {
            return std::make_pair(local_moving_edge.first, std::vector<IncidentClusterInfo>({ local_moving_edge.second }));
          })
        // reduce by each node so the vector is filled with all neighbors
        .ReduceByKey(
          [](const std::pair<NodeId, std::vector<IncidentClusterInfo>>& local_moving_node) { return local_moving_node.first; },
          [](const std::pair<NodeId, std::vector<IncidentClusterInfo>>& local_moving_node1, const std::pair<NodeId, std::vector<IncidentClusterInfo>>& local_moving_node2) {
            std::vector<IncidentClusterInfo> tmp(local_moving_node1.second.begin(), local_moving_node1.second.end());
            tmp.insert(tmp.end(), local_moving_node2.second.begin(), local_moving_node2.second.end());
            return std::make_pair(local_moving_node1.first, tmp);
          })
        // join cluster weight and node degree of each node
        .InnerJoin(node_cluster_weights,
          [](const std::pair<NodeId, std::vector<IncidentClusterInfo>>& node_cluster) { return node_cluster.first; },
          [](const std::pair<FullNodeCluster, Weight>& node_cluster_weight) { return node_cluster_weight.first.id; },
          [](const std::pair<NodeId, std::vector<IncidentClusterInfo>>& node_cluster, const std::pair<FullNodeCluster, Weight>& node_cluster_weight) {
            return std::make_pair(LocalMovingNode { node_cluster_weight.first.id, node_cluster_weight.first.degree, node_cluster_weight.first.cluster, node_cluster_weight.second }, node_cluster.second);
          })
        // map to best neighbor cluster
        .Map(
          [&graph](const std::pair<LocalMovingNode, std::vector<IncidentClusterInfo>>& local_moving_node) {
            ClusterId best_cluster = local_moving_node.first.cluster;
            int128_t best_delta = 0;
            bool move = false;
            Weight weight_between_node_and_current_cluster = 0;
            for (const IncidentClusterInfo& incident_cluster : local_moving_node.second) {
              if (incident_cluster.cluster == local_moving_node.first.cluster) {
                weight_between_node_and_current_cluster = incident_cluster.inbetween_weight;
              }
            }

            for (const IncidentClusterInfo& incident_cluster : local_moving_node.second) {
              int128_t delta = deltaModularity(local_moving_node.first.degree, local_moving_node.first.cluster, incident_cluster.cluster,
                                              weight_between_node_and_current_cluster, incident_cluster.inbetween_weight,
                                              local_moving_node.first.cluster_total_weight, incident_cluster.total_weight,
                                              graph.total_weight);
              if (delta > best_delta) {
                best_delta = delta;
                best_cluster = incident_cluster.cluster;
                move = true;
              }
            }

            return std::make_pair(FullNodeCluster { local_moving_node.first.id, local_moving_node.first.degree, best_cluster }, move);
            // return NodeCluster(local_moving_node.first.id, (iteration % 2 == 0 && best_cluster > local_moving_node.first.cluster) || (iteration % 2 != 0 && best_cluster < local_moving_node.first.cluster) ? best_cluster : local_moving_node.first.cluster);
          })
        .Cache();

      rate_sum += rate;
      rate = 1000 - (new_node_clusters.Keep().Filter([](const std::pair<FullNodeCluster, bool>& pair) { return pair.second; }).Size() * 1000 / considered_nodes);

      node_clusters = new_node_clusters
        .Map([](const std::pair<FullNodeCluster, bool>& pair) { return pair.first; })
        // bring back other clusters
        .Union(other_node_clusters)
        .Cache(); // breaks if removed. TODO why?

      if (rate_sum >= 1000) {
        assert(graph.node_count == node_clusters.Keep().Size());

        size_t round_cluster_count = node_clusters.Keep().Map([](const FullNodeCluster& node_cluster) { return node_cluster.cluster; }).Uniq().Size();
        // if (node_clusters.context().my_rank() == 0) {
        //   std::cout << "from " << cluster_count << " to " << round_cluster_count << std::endl;
        // }

        if (cluster_count - round_cluster_count <= graph.node_count / 100) {
          break;
        }

        cluster_count = round_cluster_count;
        rate_sum -= 1000;
      }
    } else {
      node_clusters = other_node_clusters.Collapse();
      rate += 200;
      if (rate > 1000) { rate = 1000; }
    }
  }

  return node_clusters.Map([](const FullNodeCluster& node_cluster) { return NodeCluster(node_cluster.id, node_cluster.cluster); }).Collapse();
}

template<class EdgeType>
thrill::DIA<NodeCluster> louvain(const DiaEdgeGraph<EdgeType>& graph) {
  auto nodes = graph.edges
    .Keep()
    .Map([](const EdgeType & edge) { return Node { edge.tail, edge.getWeight() }; })
    .ReduceByKey(
      [](const Node & node) -> size_t { return node.id; },
      [](const Node & node1, const Node & node2) { return Node { node1.id, node1.degree + node2.degree }; });
    // .Cache(); ? is recalculated, but worth it?

  assert(nodes.Keep().Size() == graph.node_count);

  auto node_clusters = local_moving(graph, nodes, ITERATIONS);
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
  auto meta_graph_edges = graph.edges
    .Keep()
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
      });

  assert(meta_graph_edges.Keep().Map([](const WeightedEdge& edge) { return edge.getWeight(); }).Sum() / 2 == graph.total_weight);

  // Recursion on meta graph
  auto meta_clustering = louvain(DiaEdgeGraph<WeightedEdge> { meta_graph_edges.Cache(), cluster_count, graph.total_weight });

  return node_clusters
    .Keep() // TODO why on earth is this necessary?
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

    auto graph = Input::readToEdgeGraph(argv[1], context);

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

