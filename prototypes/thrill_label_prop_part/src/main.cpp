#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines_one.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/print.hpp>

#include <thrill/common/cmdline_parser.hpp>

#include <ostream>
#include <iostream>
#include <vector>
#include <map>

using NodeId = uint32_t;
using Label = uint32_t;

using Node = std::pair<NodeId, std::vector<NodeId>>;
using NodeLabel = std::pair<Node, Label>;
using NodeIdLabel = std::pair<NodeId, Label>;

template <typename T>
inline std::size_t hash_combine(const T& v) { return std::hash<T>{}(v); }

template <typename T, typename... Rest>
inline std::size_t hash_combine(const T& v, Rest... rest) {
  std::hash<T> hasher;
  std::size_t seed = hash_combine(rest...);
  return seed ^ (hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2));
}

inline uint64_t combine_u32ints(const uint32_t int1, const uint32_t int2) { return (uint64_t) int1 << 32 | int2; }

auto label_propagation(thrill::DIA<Node>& nodes, uint32_t max_num_iterations) {
  size_t node_count = nodes.Keep().Size();
  auto node_labels = nodes.Map([](const Node& node) { return node.first; }).Collapse();

  for (uint32_t iteration = 0; iteration < max_num_iterations; iteration++) {
    auto new_node_labels = node_labels
      .Keep()
      .Zip(nodes.Keep(), [](const Label label, const Node& node) { return NodeLabel(node, label); })
      .template FlatMap<NodeIdLabel>(
        [](const NodeLabel& node_label, auto emit) {
          for (const NodeId neighbor : node_label.first.second) {
            emit(NodeIdLabel(neighbor, node_label.second));
          }
        })
      .Map([](const NodeIdLabel& label) { return std::make_tuple(label, 1u, 1u); })
      .ReduceByKey(
        [](const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info) { return combine_u32ints(std::get<0>(label_info).first, std::get<0>(label_info).second); },
        [](const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info1, const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info2) {
          return std::make_tuple(std::get<0>(label_info1), std::get<1>(label_info1) + std::get<1>(label_info2), 1u);
        })
      .ReduceToIndex(
        [](const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info) -> size_t { return std::get<0>(label_info).first; },
        [iteration](const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info1, const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info2) {
          if (std::get<1>(label_info1) > std::get<1>(label_info2)) { // different label - one has more occurences
            return std::make_tuple(std::get<0>(label_info1), std::get<1>(label_info1), 1u);
          } else if (std::get<1>(label_info1) < std::get<1>(label_info2)) {
            return std::make_tuple(std::get<0>(label_info2), std::get<1>(label_info2), 1u);
          } else { // different labels, some occurence count, choose random
            uint32_t random_challenge_counter_sum = std::get<2>(label_info1) + std::get<2>(label_info2);
            if (hash_combine(std::get<0>(label_info1).second, std::get<0>(label_info2).second, iteration) % random_challenge_counter_sum < std::get<2>(label_info1)) {
              return std::make_tuple(std::get<0>(label_info1), std::get<1>(label_info1), random_challenge_counter_sum);
            } else {
              return std::make_tuple(std::get<0>(label_info2), std::get<1>(label_info2), random_challenge_counter_sum);
            }
          }
        },
        node_count)
      .Map([](const std::tuple<NodeIdLabel, uint32_t, uint32_t>& label_info) { return std::get<0>(label_info).second; });

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

  return node_labels.Zip(nodes, [](const Label label, const Node& node) { return NodeIdLabel(node.first, label); });
}

int main(int argc, char const *argv[]) {
  std::string graph_file = "";
  std::string out_file = "";
  uint32_t num_partitions = 4;
  thrill::common::CmdlineParser cp;
  cp.AddParamString("graph", graph_file, "The graph to perform clustering on, in metis format");
  cp.AddUInt('k', "num-partitions", "unsigned int", num_partitions, "Number of Partitions to split the graph into");
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

    auto node_labels = label_propagation(nodes, 128);

    uint32_t partition_count = num_partitions;
    NodeId partition_size = (node_count + partition_count - 1) / partition_count;

    node_labels
      .Sort([](const NodeIdLabel& node_label1, const NodeIdLabel& node_label2) { return node_label1.second < node_label2.second; })
      .ZipWithIndex([partition_size](const NodeIdLabel& node_label, size_t index) { return NodeIdLabel(node_label.first, index / partition_size); })
      .ReduceToIndex(
        [](const NodeIdLabel& label) -> size_t { return label.first; },
        [](const NodeIdLabel& label, const NodeIdLabel&) { throw "foo"; return label; },
        node_count)
      .Map([](const NodeIdLabel& node_label) { return std::to_string(node_label.second); })
      .WriteLinesOne(out_file.empty() ? graph_file + ".part" : out_file);
  });
}

