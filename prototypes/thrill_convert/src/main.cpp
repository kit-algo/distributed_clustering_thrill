#include <thrill/api/print.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/print.hpp>

#include <ostream>
#include <iostream>
#include <unordered_map>

struct Edge {
  uint32_t tail, head;
};

struct Node {
  uint32_t id, degree;
};

std::ostream& operator << (std::ostream& os, const Edge& e) {
  return os << e.tail << " -> " << e.head;
}

void convert(thrill::Context& context, const std::string &input_path, const std::string &output_path) {
  auto input = thrill::ReadLines(context, input_path);
  auto edges = input
    .Filter([](const std::string& line) { return !line.empty() && line[0] != '#'; })
    .template FlatMap<Edge>(
      [](const std::string& line, auto emit) {
        std::istringstream line_stream(line);
        uint32_t tail, head;

        if (line_stream >> tail >> head) {
          emit(Edge { tail, head });
          emit(Edge { head, tail });
        } else {
          die(std::string("malformatted edge: ") + line);
        }
      })
    .Sort([](const Edge & e1, const Edge & e2) {
      return (e1.tail == e2.tail && e1.head < e2.head) || (e1.tail < e2.tail);
    });

  auto new_node_id_times_degree = edges
    .Keep()
    .Map([](const Edge & edge) { return Node {edge.tail, 1 }; })
    .ReduceByKey([](const Node & node) { return node.id; }, [](const Node & n1, const Node & n2) { return Node { n1.id, n1.degree + n2.degree }; })
    .Sort([](const Node & n1, const Node & n2) { return n1.id < n2.id; })
    .ZipWithIndex([](const Node & node, const uint32_t& index) { return Node { index, node.degree }; })
    .template FlatMap<uint32_t>(
      [](const Node & node, auto emit) {
        for (uint32_t i = 0; i < node.degree; i++) {
          emit(node.id);
        }
      });


  auto transformed_edges = edges
    .Zip(thrill::api::NoRebalanceTag, new_node_id_times_degree, [](const Edge & edge, const uint32_t & new_id) { return Edge { new_id, edge.head }; })
    .Map([](const Edge & edge) { return Edge { edge.head, edge.tail }; })
    .Sort(
      [](const Edge & e1, const Edge & e2) {
        return (e1.tail == e2.tail && e1.head < e2.head) || (e1.tail < e2.tail);
      })
    .Zip(thrill::api::NoRebalanceTag, new_node_id_times_degree, [](const Edge & edge, const uint32_t& new_id) { return Edge { new_id, edge.head }; });

  transformed_edges
    .GroupByKey<std::string>(
      [](const Edge& edge) { return edge.tail; },
      [](auto& iterator, const uint32_t) {
        std::ostringstream output;
        // output << node + 1 << ":";
        while(iterator.HasNext()) {
          uint32_t neighbor = iterator.Next().head;
          output << neighbor + 1 << " ";
        }
        return output.str();
      })
    .WriteLines(output_path);
}

int main(int, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    convert(context, argv[1], std::string(argv[1]) + ".graph");
  });
}

