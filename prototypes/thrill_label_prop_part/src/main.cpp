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
#include <unordered_map>
#include <vector>
#include <set>
#include <map>

using NodeId = uint32_t;
using ClusterId = uint32_t;

using NodeCluster = std::pair<NodeId, ClusterId>;

std::ostream& operator << (std::ostream& os, NodeCluster& nc) {
  return os << nc.first << ": " << nc.second;
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
    .Map([](const NodeId& id) { return NodeCluster(id, id); });

  return node_labels;
}

int main(int, char const *argv[]) {
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

