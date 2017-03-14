#pragma once

#include <thrill/api/collapse.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/gather.hpp>
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

  auto node_labels = graph.nodes.Keep().Map([](const Node& node) { return node.id; }).Collapse();

  for (uint32_t iteration = 0; iteration < max_num_iterations; iteration++) {
    auto label_counts = node_labels
      .Map([](const Label label) { return std::make_pair(label, 1u); })
      .ReducePair([](const uint32_t count1, const uint32_t count2) { return count1 + count2; });

    auto new_node_labels = node_labels
      .Keep()
      .Zip(graph.nodes.Keep(), [](const Label label, const Node& node) { return NodeLabel(node, label); })
      .InnerJoin(label_counts,
        [](const NodeLabel& node_label) { return node_label.second; },
        [](const std::pair<Label, uint32_t>& label_count) { return label_count.first; },
        [](const NodeLabel& node_label, const std::pair<Label, uint32_t>& label_count) {
          return std::make_pair(node_label, label_count.second);
        })
      .template FlatMap<NodeIdLabel>(
        [target_partition_element_size](const std::pair<NodeLabel, uint32_t>& node_label, auto emit) {
          for (const Link& neighbor : node_label.first.first.links) {
            if (node_label.second < target_partition_element_size) {
              emit(NodeIdLabel { neighbor.target, node_label.first.second });
            }
          }
          emit(NodeIdLabel { node_label.first.first.id, node_label.first.second });
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

  auto node_labels = label_propagation(graph, 32, partition_element_target_size);

  auto cleaned_node_labels = node_labels
    .Keep()
    .Map([](const NodeIdLabel label) { return label.label; })
    .Uniq()
    .ZipWithIndex([](const Label label, const Label new_id) { return std::make_pair(label, new_id); })
    .InnerJoin(node_labels,
      [](const std::pair<Label, Label>& mapping) { return mapping.first; },
      [](const NodeIdLabel& label) { return label.label; },
      [](const std::pair<Label, Label>& mapping, const NodeIdLabel& label) {
        return NodeIdLabel { label.node, mapping.second };
      });

  size_t label_count = cleaned_node_labels.Keep().Size();

  auto label_counts = cleaned_node_labels
    .Keep()
    .Map([](const NodeIdLabel& label) { return std::make_pair(label.label, 1u); })
    .ReducePairToIndex([](const uint32_t count1, const uint32_t count2) { return count1 + count2; }, label_count)
    .Map([](const std::pair<Label, uint32_t>& label_count) { return label_count.second; })
    .Gather();

  std::vector<uint32_t> cluster_partition_element(context.my_rank() == 0 ? label_count : 0);
  if (context.my_rank() == 0) {
    Partitioning::distributeClusters(graph.node_count, label_counts, partition_size, cluster_partition_element);
  }

  return thrill::Distribute(context, cluster_partition_element)
    .ZipWithIndex([](const uint32_t partition, const Label label) { return std::make_pair(label, partition); })
    .InnerJoin(cleaned_node_labels,
      [](const std::pair<Label, Label>& label_partition) { return label_partition.first; },
      [](const NodeIdLabel& node_label) { return node_label.label; },
      [](const std::pair<Label, Label>& label_partition, const NodeIdLabel& node_label) {
        return NodePartition { node_label.node, label_partition.second };
      })
    .ReduceToIndex(
      [](const NodePartition& label) -> size_t { return label.node_id; },
      [](const NodePartition& label, const NodePartition&) { assert(false); return label; },
      graph.node_count);
}

