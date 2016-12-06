#include <thrill/api/print.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/print.hpp>

#include <ostream>
#include <iostream>
#include <unordered_map>

struct Edge {
  uint32_t tail, head;
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

  auto id_mapping_pairs = edges
    .Keep()
    .Map([](const Edge & edge) { return edge.tail; })
    .ReduceByKey([](const uint32_t& tail) { return tail; }, [](const uint32_t& tail, const uint32_t&) { return tail; })
    .Sort([](const uint32_t& tail1, const uint32_t& tail2) { return tail1 < tail2; })
    .ZipWithIndex([](const uint32_t& tail, const uint32_t& index) { return std::make_pair(tail, index); })
    .AllGather();

  std::unordered_map<uint32_t, uint32_t> id_mapping(id_mapping_pairs.begin(), id_mapping_pairs.end());

  edges
    .GroupByKey<std::string>(
      [](const Edge& edge) { return edge.tail; },
      [&](auto& iterator, const uint32_t node) {
        std::ostringstream output;
        // output << id_mapping[node] + 1 << ":";
        while(iterator.HasNext()) {
          uint32_t neighbor = iterator.Next().head;
          output << id_mapping[neighbor] + 1 << " ";
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

