#pragma once

#include <thrill/api/cache.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/zip.hpp>

#include <vector>

#include "util.hpp"
#include "thrill_graph.hpp"
#include "thrill_partitioning.hpp"

#include "seq_louvain/graph.hpp"
#include "seq_louvain/cluster_store.hpp"
#include "modularity.hpp"
#include "partitioning.hpp"

namespace LocalMoving {

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
auto distributedLocalMoving(const DiaGraph<NodeType, EdgeType>& graph, uint32_t num_iterations) {
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

        size_t round_cluster_count = node_clusters.Map([](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) { return node_cluster.first.second; }).Uniq().Size();

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

template<class Graph>
auto partitionedLocalMoving(const Graph& graph) {
  constexpr bool weighted = std::is_same<typename Graph::Node, NodeWithWeightedLinks>::value;
  using Node = typename std::conditional<weighted, NodeWithWeightedLinksAndTargetDegree, NodeWithLinksAndTargetDegree>::type;

  uint32_t partition_size = graph.nodes.context().num_workers();
  uint32_t partition_element_size = Partitioning::partitionElementTargetSize(graph.node_count, partition_size);
  if (partition_element_size < 100000 && graph.node_count < 1000000) {
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

  return nodes
    .Zip(thrill::NoRebalanceTag, node_partitions,
      [](const Node& node, const NodePartition& node_partition) {
        assert(node.id == node_partition.node_id);
        return std::make_pair(node, node_partition.partition);
      })
    // Local Moving
    .template GroupToIndex<std::vector<NodeCluster>>(
      [](const std::pair<Node, uint32_t>& node_partition) -> size_t { return node_partition.second; },
      [total_weight = graph.total_weight, partition_element_size](auto& iterator, const uint32_t) {
        // TODO deterministic random
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
          mapping.push_back(NodeCluster(reverse_mapping[node], clusters[node]));
        }
        return mapping;
      }, partition_size)
    .template FlatMap<NodeCluster>(
      [](std::vector<NodeCluster> mapping, auto emit) {
        for (const NodeCluster& pair : mapping) {
          emit(pair);
        }
      })
    .Cache();
}

} // LocalMoving
