#pragma once

#include <thrill/api/collapse.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/fold_by_key.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_with_index.hpp>

#include <vector>
#include <sparsepp/spp.h>

#include "util/util.hpp"

#include "algo/partitioning.hpp"

#define LP_FIXED_RATIO 4

using Label = NodeId;

struct NodeIdLabel {
  NodeId node;
  Label label;
};

struct NodePartition {
  NodeId node_id;
  uint32_t partition;
};

bool nodeIncluded(const NodeId node, const uint32_t iteration) {
  uint32_t hash = Util::combined_hash(node, iteration / LP_FIXED_RATIO);
  return hash % LP_FIXED_RATIO == iteration % LP_FIXED_RATIO;
}

template<class Graph>
auto label_propagation(Graph& graph, uint32_t max_num_iterations, uint32_t target_partition_element_size) {
  using Node = typename Graph::Node;
  using Link = typename Node::LinkType;
  using NodeLabel = std::pair<Node, Label>;

  auto select_most_frequent_label = [node_count = graph.node_count](const auto& node_labels, uint32_t iteration) {
    return node_labels
      .template GroupToIndex<Label>(
        [](const NodeIdLabel& label) { return label.node; },
        [iteration](auto& iterator, const NodeId) {
          spp::sparse_hash_map<Label, uint32_t> label_counts;
          while (iterator.HasNext()) {
            label_counts[iterator.Next().label]++;
          }

          uint32_t win_counter = 1;
          uint32_t highest_occurence = 0;
          Label best_label = 0;
          for (const auto& pair : label_counts) {
            if (pair.second > highest_occurence) {
              highest_occurence = pair.second;
              win_counter = 1;
              best_label = pair.first;
            } else if (pair.second == highest_occurence) {
              if (Util::combined_hash(best_label, pair.first, iteration) % (win_counter + 1) == 0) {
                best_label = pair.first;
              }

              win_counter++;
            }
          }

          return best_label;
        },
        node_count);
  };

  auto node_labels = graph.nodes.Keep().Map([](const Node& node) { return NodeLabel(node, node.id); }).Collapse();

  for (uint32_t iteration = 0; iteration < max_num_iterations; iteration++) {
    auto included = [iteration](const NodeId id) { return nodeIncluded(id, iteration); };

    auto new_node_labels = (iteration < max_num_iterations / 3 ?
      select_most_frequent_label(node_labels
        .template FlatMap<NodeIdLabel>(
          [&included](const NodeLabel& node_label, auto emit) {
            for (const Link& neighbor : node_label.first.links) {
              if (included(neighbor.target)) {
                emit(NodeIdLabel { neighbor.target, node_label.second });
              }
            }
            emit(NodeIdLabel { node_label.first.id, node_label.second });
          }), iteration) :
      select_most_frequent_label(node_labels
        .template FoldByKey<std::vector<Node>>(thrill::NoDuplicateDetectionTag,
            [](const NodeLabel& node_label) { return node_label.second; },
            [](std::vector<Node>&& acc, const NodeLabel& node_label) {
              acc.push_back(node_label.first);
              return std::move(acc);
            })
        .template FlatMap<NodeIdLabel>(
          [target_partition_element_size, &included](const std::pair<Label, std::vector<Node>>& label_nodes, auto emit) {
            for (const Node& node : label_nodes.second) {
              for (const Link& neighbor : node.links) {
                if (label_nodes.second.size() < target_partition_element_size) {
                  if (included(neighbor.target)) {
                    emit(NodeIdLabel { neighbor.target, label_nodes.first });
                  }
                }
              }
              emit(NodeIdLabel { node.id, label_nodes.first });
            }
          }), iteration));

    size_t num_changed = new_node_labels
      .Keep()
      .Zip(node_labels, [](const Label& l1, const NodeLabel& l2) { return std::make_pair(l1, l2.second); })
      .Filter([](const std::pair<Label, Label>& labels) { return labels.first != labels.second; })
      .Size();

    node_labels = new_node_labels
      .Zip(graph.nodes.Keep(), [](const Label label, const Node& node) { return NodeLabel(node, label); });

    if (num_changed <= graph.node_count / (100 * LP_FIXED_RATIO)) {
      break;
    }
  }

  return node_labels.Map([](const NodeLabel& node_label) { return NodeIdLabel { node_label.first.id, node_label.second }; });
}

template<class Graph>
auto partition(const Graph& graph, const uint32_t partition_size) {
  if (partition_size == 1) {
    return graph.nodes.Map([](const typename Graph::Node& node) { return NodePartition { node.id, 0 }; }).Collapse();
  }

  auto& context = graph.nodes.context();
  NodeId partition_element_target_size = Partitioning::partitionElementTargetSize(graph.node_count, partition_size);

  auto labels_with_nodes = label_propagation(graph, 64, partition_element_target_size)
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

