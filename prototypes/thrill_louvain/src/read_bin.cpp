#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/reduce_by_key.hpp>

#include <vector>
#include <cstdint>
#include <iostream>

struct Edge {
  uint32_t tail, head;
};

std::ostream& operator << (std::ostream& os, Edge& e) {
  return os << e.tail << " -> " << e.head;
}

namespace std {
  std::ostream& operator << (std::ostream& os, std::pair<uint32_t, uint32_t>& p) {
    return os << p.first << " -> " << p.second;
  }
} // std

int main(int, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    auto edges = thrill::ReadBinary<std::vector<uint32_t>>(context, argv[1])
      .ZipWithIndex(
        [](const std::vector<uint32_t>& neighbors, const uint32_t node) {
          return std::make_pair(node, neighbors);
        })
      .template FlatMap<Edge>(
        [](const std::pair<uint32_t, std::vector<uint32_t>>& node, auto emit) {
          for (const uint32_t neighbor : node.second) {
            emit(Edge { node.first, neighbor });
            emit(Edge { neighbor, node.first });
          }
        });

    edges.Print("edges");

    auto ground_proof = thrill::ReadBinary<std::pair<uint32_t, uint32_t>>(context, argv[2]);
    ground_proof.Print("ground_proof");

    edges.Map([](const Edge& edge) { return edge.tail; }).Uniq().Print("nodes");
  });
}

