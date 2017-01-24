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
#include <thrill/api/join.hpp>

#include <ostream>
#include <iostream>
#include <vector>
#include <map>
#include <stdlib.h>

using NodeId = uint32_t;
using Label = uint32_t;

using NodeLabel = std::pair<NodeId, Label>;

std::ostream& operator << (std::ostream& os, NodeLabel& nl) {
  return os << nl.first << ": " << nl.second;
}

struct Edge {
  NodeId tail, head;
};

std::ostream& operator << (std::ostream& os, Edge& e) {
  return os << e.tail << " -> " << e.head;
}

#include <thrill/api/print.hpp> // Like WTF

auto partition(thrill::DIA<Edge>& edge_list) {
  auto node_labels = edge_list
    .Map([](const Edge& edge) { return edge.tail; })
    .ReduceByKey(
      [](const NodeId& id) { return id; },
      [](const NodeId& id, const NodeId&) { return id; })
    .Map([](const NodeId& id) { return NodeLabel(id, id); })
    .Collapse();

  for (int iteration = 0; iteration < 32; iteration++) {
    node_labels = edge_list
      .InnerJoinWith(node_labels,
        [](const Edge& edge) { return edge.head; },
        [](const NodeLabel& node_label) { return node_label.first; },
        [](const Edge& edge, const NodeLabel& node_label) {
          return std::make_pair(edge.tail, node_label.second);
        })
      .GroupByKey<NodeLabel>(
        [](const std::pair<NodeId, Label>& node_with_incoming_label) { return node_with_incoming_label.first; },
        [](auto& iterator, const NodeId node) {
          std::map<Label, uint32_t> label_counts;

          while(iterator.HasNext()) {
            label_counts[iterator.Next().second]++;
          }

          uint32_t max_count = 0;
          std::vector<Label> labels_with_max_count;
          for (const auto& label_count : label_counts) {
            if (label_count.second > max_count) {
              labels_with_max_count.clear();
              labels_with_max_count.push_back(label_count.first);
              max_count = label_count.second;
            }
          }

          return NodeLabel(node, labels_with_max_count[rand() % labels_with_max_count.size()]);
        });
  }

  return node_labels;
}

int main(int, char const *argv[]) {
  srand(42);

  return thrill::Run([&](thrill::Context& context) {
    auto edges = thrill::ReadLines(context, argv[1])
      .Filter([](const std::string& line) { return !line.empty() && line[0] != '#'; })
      .template FlatMap<Edge>(
        [](const std::string& line, auto emit) {
          std::istringstream line_stream(line);
          uint32_t tail, head;

          if (line_stream >> tail >> head) {
            tail--;
            head--;
            emit(Edge { tail, head });
            emit(Edge { head, tail });
          } else {
            die(std::string("malformatted edge: ") + line);
          }
        })
      .Sort([](const Edge & e1, const Edge & e2) {
        return (e1.tail == e2.tail && e1.head < e2.head) || (e1.tail < e2.tail);
      });

    partition(edges);
  });
}

