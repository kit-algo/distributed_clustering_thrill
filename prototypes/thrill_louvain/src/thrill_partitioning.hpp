#pragma once

#include <thrill/api/collapse.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_with_index.hpp>

#include <vector>

#include "util.hpp"

#include "partitioning.hpp"

using Label = NodeId;

struct NodeIdLabel {
  NodeId node;
  Label label;
};

struct NodePartition {
  NodeId node_id;
  uint32_t partition;
};

template<class Graph>
auto label_propagation(Graph& graph, uint32_t max_num_iterations, uint32_t target_partition_element_size) {
  using Node = typename Graph::Node;
  using Link = typename Node::LinkType;
  using NodeLabel = std::pair<Node, Label>;

  auto node_labels = graph.nodes.Map([](const Node& node) { return node.id; }).Collapse();

  for (uint32_t iteration = 0; iteration < max_num_iterations; iteration++) {
    auto new_node_labels = node_labels
      .Zip(graph.nodes.Keep(), [](const Label label, const Node& node) { return NodeLabel(node, label); })
      .template GroupByKey<std::pair<std::vector<Node>, Label>>(
        [](const NodeLabel& node_label) { return node_label.second; },
        [](auto& iterator, const Label label) {
          std::vector<Node> nodes;
          while (iterator.HasNext()) {
            nodes.push_back(iterator.Next().first);
          }
          return std::make_pair(nodes, label);
        })
      .template FlatMap<NodeIdLabel>(
        [target_partition_element_size](const std::pair<std::vector<Node>, Label>& nodes_label, auto emit) {
          for (const Node& node : nodes_label.first) {
            for (const Link& neighbor : node.links) {
              if (nodes_label.first.size() < target_partition_element_size) {
                emit(NodeIdLabel { neighbor.target, nodes_label.second });
              }
            }
            emit(NodeIdLabel { node.id, nodes_label.second });
          }
        })
      .Map([](const NodeIdLabel& label) { return std::make_tuple(label, 1u, 1u); })
      .ReduceByKey(
        [](const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info) { return Util::combine_u32ints(std::get<0>(label_info).node, std::get<0>(label_info).label); },
        [](const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info1, const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info2) {
          return std::make_tuple(std::get<0>(label_info1), std::get<1>(label_info1) + std::get<1>(label_info2), 1u);
        })
      .ReduceToIndex(
        [](const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info) -> size_t { return std::get<0>(label_info).node; },
        [iteration](const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info1, const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info2) {
          if (std::get<1>(label_info1) > std::get<1>(label_info2)) { // different label - one has more occurences
            return std::make_tuple(std::get<0>(label_info1), std::get<1>(label_info1), 1u);
          } else if (std::get<1>(label_info1) < std::get<1>(label_info2)) {
            return std::make_tuple(std::get<0>(label_info2), std::get<1>(label_info2), 1u);
          } else { // different labels, some occurence count, choose random
            uint32_t random_challenge_counter_sum = std::get<2>(label_info1) + std::get<2>(label_info2);
            if (Util::combined_hash(std::get<0>(label_info1).label, std::get<0>(label_info2).label, iteration) % random_challenge_counter_sum < std::get<2>(label_info1)) {
              return std::make_tuple(std::get<0>(label_info1), std::get<1>(label_info1), random_challenge_counter_sum);
            } else {
              return std::make_tuple(std::get<0>(label_info2), std::get<1>(label_info2), random_challenge_counter_sum);
            }
          }
        },
        graph.node_count)
      .Map([](const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info) { return std::get<0>(label_info).label; })
      .Collapse();

    size_t num_changed = new_node_labels
      .Keep()
      .Zip(node_labels, [](const Label& l1, const Label& l2) { return std::make_pair(l1, l2); })
      .Filter([](const std::pair<Label, Label>& labels) { return labels.first != labels.second; })
      .Size();

    node_labels = new_node_labels;

    if (num_changed == 0) {
      break;
    }
  }

  return node_labels.Zip(graph.nodes, [](const Label label, const Node& node) { return NodeIdLabel { node.id, label }; });
}

template<class Graph>
auto partition(const Graph& graph, const uint32_t partition_size) {
  if (partition_size == 1) {
    return graph.nodes.Map([](const typename Graph::Node& node) { return NodePartition { node.id, 0 }; }).Collapse();
  }

  auto& context = graph.nodes.context();
  NodeId partition_element_target_size = Partitioning::partitionElementTargetSize(graph.node_count, partition_size);

  auto labels_with_nodes = label_propagation(graph, 16, partition_element_target_size)
    .template GroupByKey<std::pair<Label, std::vector<NodeId>>>(
      [](const NodeIdLabel& node_label) { return node_label.label; },
      [](auto& iterator, const Label label) {
        std::vector<NodeId> nodes;
        while (iterator.HasNext()) {
          nodes.push_back(iterator.Next().node);
        }
        return std::make_pair(label, nodes);
      })
    .ZipWithIndex([](const std::pair<Label, std::vector<NodeId>> label_nodes, const Label new_id) { return std::make_pair(new_id, label_nodes.second); });

  auto label_counts = labels_with_nodes
    .Keep()
    .Map([](const std::pair<Label, std::vector<NodeId>>& label_nodes) -> uint32_t { return label_nodes.second.size(); })
    .Gather();

  std::vector<uint32_t> label_partition_element(label_counts.size());
  if (context.my_rank() == 0) {
    Partitioning::distributeClusters(graph.node_count, label_counts, partition_size, label_partition_element);
  }

  return thrill::Distribute(context, label_partition_element)
    .Zip(labels_with_nodes, [](const uint32_t partition, const std::pair<Label, std::vector<NodeId>>& label_nodes) { return std::make_pair(partition, label_nodes.second); })
    .template FlatMap<NodePartition>(
      [](const std::pair<Label, std::vector<NodeId>>& label_nodes, auto emit) {
        for (const NodeId& node : label_nodes.second) {
          emit(NodePartition { node, label_nodes.first });
        }
      })
    .ReduceToIndex(
      [](const NodePartition& label) -> size_t { return label.node_id; },
      [](const NodePartition& label, const NodePartition&) { assert(false); return label; },
      graph.node_count);
}

