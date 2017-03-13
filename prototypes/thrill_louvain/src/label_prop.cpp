#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/write_lines_one.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_with_index.hpp>

#include <thrill/common/cmdline_parser.hpp>

#include <ostream>
#include <iostream>
#include <vector>

#include "input.hpp"
#include "util.hpp"

#include "partitioning.hpp"

using NodeId = uint32_t;
using Label = uint32_t;

using Node = std::pair<NodeId, std::vector<NodeId>>;
using NodeLabel = std::pair<Node, Label>;

struct NodeIdLabel {
  NodeId node;
  Label label;
};

std::ostream& operator << (std::ostream& os, NodeIdLabel& node) {
  return os << node.node << " (" << node.label << ")";
}

auto label_propagation(thrill::DIA<Node>& nodes, uint32_t max_num_iterations, uint32_t target_partition_element_size) {
  size_t node_count = nodes.Keep().Size();
  auto node_labels = nodes.Map([](const Node& node) { return node.first; }).Collapse();

  for (uint32_t iteration = 0; iteration < max_num_iterations; iteration++) {
    auto label_counts = node_labels
      .Keep()
      .Map([](const Label label) { return std::make_pair(label, 1u); })
      .ReducePair([](const uint32_t count1, const uint32_t count2) { return count1 + count2; });

    auto new_node_labels = node_labels
      .Keep()
      .Zip(nodes.Keep(), [](const Label label, const Node& node) { return NodeLabel(node, label); })
      .InnerJoin(label_counts,
        [](const NodeLabel& node_label) { return node_label.second; },
        [](const std::pair<Label, uint32_t>& label_count) { return label_count.first; },
        [](const NodeLabel& node_label, const std::pair<Label, uint32_t>& label_count) {
          return std::make_pair(node_label, label_count.second);
        })
      .template FlatMap<NodeIdLabel>(
        [target_partition_element_size](const std::pair<NodeLabel, uint32_t>& node_label, auto emit) {
          for (const NodeId neighbor : node_label.first.first.second) {
            if (node_label.second < target_partition_element_size) {
              emit(NodeIdLabel { neighbor, node_label.first.second });
            }
          }
          emit(NodeIdLabel { node_label.first.first.first, node_label.first.second });
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
        node_count)
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

  return node_labels.Zip(nodes, [](const Label label, const Node& node) { return NodeIdLabel { node.first, label }; });
}

int main(int argc, char const *argv[]) {
  std::string graph_file = "";
  std::string out_file = "";
  uint32_t partition_size = 4;
  thrill::common::CmdlineParser cp;
  cp.AddParamString("graph", graph_file, "The graph to perform clustering on, in metis format");
  cp.AddUInt('k', "num-partitions", "unsigned int", partition_size, "Number of Partitions to split the graph into");
  cp.AddString('o', "out", "file", out_file, "Where to write the result");

  if (!cp.Process(argc, argv)) {
    return 1;
  }

  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    auto nodes = thrill::ReadLines(context, graph_file)
      .ZipWithIndex([](const std::string line, const size_t index) { return std::make_pair(line, index); })
      .Filter([](const std::pair<std::string, size_t>& node) { return node.second > 0; })
      .Map(
        [](const std::pair<std::string, size_t>& node) {
          std::istringstream line_stream(node.first);
          std::vector<NodeId> neighbors;
          NodeId neighbor;

          while (line_stream >> neighbor) {
            neighbors.push_back(neighbor - 1);
          }

          return Node(node.second - 1, neighbors);
        })
      .Cache();

    NodeId node_count = nodes.Keep().Size();
    NodeId partition_element_target_size = Partitioning::partitionElementTargetSize(node_count, partition_size);

    auto node_labels = label_propagation(nodes, 32, partition_element_target_size);

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
      Partitioning::distributeClusters(node_count, label_counts, partition_size, cluster_partition_element);
    }

    thrill::Distribute(context, cluster_partition_element)
      .ZipWithIndex([](const uint32_t partition, const Label label) { return std::make_pair(label, partition); })
      .InnerJoin(cleaned_node_labels,
        [](const std::pair<Label, Label>& label_partition) { return label_partition.first; },
        [](const NodeIdLabel& node_label) { return node_label.label; },
        [](const std::pair<Label, Label>& label_partition, const NodeIdLabel& node_label) {
          return NodeIdLabel { node_label.node, label_partition.second };
        })
      .ReduceToIndex(
        [](const NodeIdLabel& label) -> size_t { return label.node; },
        [](const NodeIdLabel& label, const NodeIdLabel&) { assert(false); return label; },
        node_count)
      .Map([](const NodeIdLabel& label) { return std::to_string(label.label); })
      .WriteLinesOne(out_file.empty() ? graph_file + ".part" : out_file);
  });
}

