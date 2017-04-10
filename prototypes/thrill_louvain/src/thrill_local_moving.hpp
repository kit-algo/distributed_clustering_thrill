#pragma once

#include <thrill/api/cache.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/fold_by_key.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/zip.hpp>

#include <vector>
#include <algorithm>
#include <sparsepp/spp.h>

#include "util.hpp"
#include "thrill_graph.hpp"
#include "thrill_partitioning.hpp"

#include "seq_louvain/graph.hpp"
#include "seq_louvain/cluster_store.hpp"
#include "modularity.hpp"
#include "partitioning.hpp"

namespace LocalMoving {

using int128_t = __int128_t;

struct Node {
  NodeId id;
  Weight degree;
};

struct NodeClusterLink {
  Weight node_degree = 0;
  Weight inbetween_weight = 0;
  Weight total_weight = 0;
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

int128_t deltaModularity(const Node& node, const IncidentClusterInfo& neighbored_cluster, Weight total_weight) {
  int128_t e = neighbored_cluster.inbetween_weight * total_weight * 2;
  int128_t a = neighbored_cluster.total_weight * node.degree;
  return e - a;
}

bool nodeIncluded(const NodeId node, const uint32_t iteration, const uint32_t rate) {
  uint32_t hash = Util::combined_hash(node, iteration);
  return hash % 1000 < rate;
}

template<class NodeType>
auto distributedLocalMoving(const DiaNodeGraph<NodeType>& graph, uint32_t num_iterations) {
  using NodeWithTargetDegreesType = typename std::conditional<std::is_same<NodeType, NodeWithLinks>::value, NodeWithLinksAndTargetDegree, NodeWithWeightedLinksAndTargetDegree>::type;

  auto reduceToBestCluster = [&graph](const auto& incoming) {
    return incoming
      // Reduce to best cluster
      .ReduceToIndex(
        [](const std::pair<Node, IncidentClusterInfo>& lme) -> size_t { return lme.first.id; },
        [total_weight = graph.total_weight](const std::pair<Node, IncidentClusterInfo>& lme1, const std::pair<Node, IncidentClusterInfo>& lme2) {
          // TODO Tie breaking
          if (deltaModularity(lme2.first, lme2.second, total_weight) > deltaModularity(lme1.first, lme1.second, total_weight)) {
            return lme2;
          } else {
            return lme1;
          }
        },
        graph.node_count);
  };

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
    .Collapse();

  size_t cluster_count = graph.node_count;
  uint32_t rate = 200;
  uint32_t rate_sum = 0;

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    auto included = [iteration, rate](const NodeId id) { return nodeIncluded(id, iteration, rate); };

    size_t considered_nodes_estimate = graph.node_count * rate / 1000;

    if (considered_nodes_estimate > 0) {
      node_clusters = (iteration == 0 ?
        reduceToBestCluster(node_clusters
          .template FlatMap<std::pair<Node, IncidentClusterInfo>>(
            [&included](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster_moved, auto emit) {
              const auto& node_cluster = node_cluster_moved.first;

              if (included(node_cluster.first.id)) {
                emit(std::make_pair(
                  Node { node_cluster.first.id, node_cluster.first.weightedDegree() },
                  IncidentClusterInfo {
                    node_cluster.second,
                    0,
                    node_cluster.first.weightedDegree()
                  }
                ));
              }

              for (const typename NodeWithTargetDegreesType::LinkType& link : node_cluster.first.links) {
                if (included(link.target)) {
                 emit(std::make_pair(
                   Node { link.target, link.target_degree },
                   IncidentClusterInfo {
                     node_cluster.second,
                     link.getWeight(),
                     node_cluster.first.weightedDegree()
                   }
                 ));
                }
              }
            })) :
        reduceToBestCluster(node_clusters
          .template FoldByKey<std::vector<NodeWithTargetDegreesType>>(thrill::NoDuplicateDetectionTag,
            [](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) { return node_cluster.first.second; },
            [](std::vector<NodeWithTargetDegreesType>&& acc, const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) {
              acc.push_back(node_cluster.first.first);
              return std::move(acc);
            })
          .template FlatMap<std::pair<Node, IncidentClusterInfo>>(
            [&included](const std::pair<ClusterId, std::vector<NodeWithTargetDegreesType>>& cluster_nodes, auto emit) {
              spp::sparse_hash_map<NodeId, NodeClusterLink> node_cluster_links;
              Weight total_weight = 0;
              for (const NodeWithTargetDegreesType& node : cluster_nodes.second) {
                total_weight += node.weightedDegree();
              }
              for (const NodeWithTargetDegreesType& node : cluster_nodes.second) {
                for (const typename NodeWithTargetDegreesType::LinkType& link : node.links) {
                  if (node.id != link.target && included(link.target)) {
                    auto it = node_cluster_links.find(link.target);
                    if (it != node_cluster_links.end()) {
                      it->second.inbetween_weight += link.getWeight();
                    } else {
                      node_cluster_links[link.target] = NodeClusterLink {
                        link.target_degree,
                        link.getWeight(),
                        total_weight
                      };
                    }
                  }
                }
                if (included(node.id)) {
                  NodeClusterLink& cluster_link = node_cluster_links[node.id];
                  cluster_link.node_degree = node.weightedDegree();
                  cluster_link.total_weight = total_weight - node.weightedDegree();
                }
              }

              for (const auto& node_cluster_link : node_cluster_links) {
                emit(std::make_pair(
                  Node { node_cluster_link.first, node_cluster_link.second.node_degree },
                  IncidentClusterInfo {
                    cluster_nodes.first,
                    node_cluster_link.second.inbetween_weight,
                    node_cluster_link.second.total_weight
                  }
                ));
              }
            }))
          )
        .Zip(thrill::NoRebalanceTag, node_clusters,
          [&included](const std::pair<Node, IncidentClusterInfo>& lme, const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& old_node_cluster) {
            if (included(old_node_cluster.first.first.id)) {
              assert(lme.first.id == old_node_cluster.first.first.id);
              return std::make_pair(std::make_pair(old_node_cluster.first.first, lme.second.cluster), lme.second.cluster != old_node_cluster.first.second);
            } else {
              return std::make_pair(old_node_cluster.first, false);
            }
          })
        .Cache();

      rate_sum += rate;
      rate = std::max(1000 - (node_clusters.Keep().Filter([&included](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& pair) { return pair.second && included(pair.first.first.id); }).Size() * 1000 / considered_nodes_estimate), 200ul);

      if (rate_sum >= 1000) {
        size_t round_cluster_count = node_clusters.Keep().Map([](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) { return node_cluster.first.second; }).Uniq().Size();
        assert(graph.node_count == node_clusters.Size());

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

  return node_clusters
    .Map(
      [](const std::pair<std::pair<NodeWithTargetDegreesType, ClusterId>, bool>& node_cluster) {
        return std::make_pair(node_cluster.first.first.toNodeWithoutTargetDegrees(), node_cluster.first.second);
      });
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
    .template GroupToIndex<std::vector<std::pair<typename Graph::Node, ClusterId>>>(
      [](const std::pair<Node, uint32_t>& node_partition) -> size_t { return node_partition.second; },
      [total_weight = graph.total_weight, partition_element_size](auto& iterator, const uint32_t) {
        // TODO deterministic random
        GhostGraph<weighted> graph(partition_element_size, total_weight);
        const std::vector<typename Graph::Node> reverse_mapping = graph.template initialize<typename Graph::Node>(
          [&iterator](const auto& emit) {
            while (iterator.HasNext()) {
              emit(iterator.Next().first);
            }
          });

        GhostClusterStore clusters(graph.getNodeCount());
        Modularity::localMoving(graph, clusters);
        clusters.rewriteClusterIds(reverse_mapping);

        std::vector<std::pair<typename Graph::Node, ClusterId>> mapping;
        mapping.reserve(graph.getNodeCount());
        for (NodeId node = 0; node < graph.getNodeCount(); node++) {
          mapping.emplace_back(reverse_mapping[node], clusters[node]);
        }
        return mapping;
      }, partition_size)
    .template FlatMap<std::pair<typename Graph::Node, ClusterId>>(
      [](const std::vector<std::pair<typename Graph::Node, ClusterId>>& mapping, auto emit) {
        for (const auto& pair : mapping) {
          emit(pair);
        }
      })
    .ReduceToIndex(
      [](const std::pair<typename Graph::Node, ClusterId>& node_cluster) -> size_t { return node_cluster.first.id; },
      [](const std::pair<typename Graph::Node, ClusterId>& node_cluster, const std::pair<typename Graph::Node, ClusterId>&) { assert(false); return node_cluster; },
      graph.node_count);
}

} // LocalMoving
