#include <thrill/api/dia.hpp>
#include <thrill/api/write_lines_one.hpp>

#include <vector>
#include <algorithm>

#include "input.hpp"
#include "thrill_graph.hpp"

int main(int, char const *argv[]) {
  std::string file(argv[1]);
  std::string output = file.replace(file.end() - 6, file.end(), "-cleaned.graph");

  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    auto graph = Input::readToEdgeGraph(argv[1], context);

    auto cleanup_mapping = graph.edges
      .Keep()
      .Map([](const Edge& edge) { return edge.tail; })
      .Uniq()
      .Sort()
      .ZipWithIndex([](const NodeId old_id, const NodeId index) { return std::make_pair(old_id, index); });

    NodeId node_count = cleanup_mapping.Keep().Size();

    auto edges = graph.edges
      .InnerJoin(
        cleanup_mapping,
        [](const Edge& edge) { return edge.tail; },
        [](const std::pair<NodeId, NodeId>& mapping) { return mapping.first; },
        [](const Edge& edge, const std::pair<NodeId, NodeId>& mapping) { return Edge { mapping.second, edge.head }; })
      .InnerJoin(
        cleanup_mapping,
        [](const Edge& edge) { return edge.head; },
        [](const std::pair<NodeId, NodeId>& mapping) { return mapping.first; },
        [](const Edge& edge, const std::pair<NodeId, NodeId>& mapping) { return Edge { edge.tail, mapping.second }; });

    edgesToNodes(edges, node_count)
      .Map([](const NodeWithLinks& node) {
        std::stringstream ss;

        for (const auto& neighbor : node.links) {
          ss << neighbor.target + 1 << " ";
        }

        return ss.str();
      })
      .ZipWithIndex([](const std::string& s, size_t index) { return std::make_pair(s, index); })
      .template FlatMap<std::string>([node_count, edge_count = graph.total_weight](const std::pair<std::string, size_t>& output, auto emit) {
        if (output.second == 0) {
          std::stringstream ss;
          ss << node_count << ' ' << edge_count;
          emit(ss.str());
          emit(output.first);
        } else {
          emit(output.first);
        }
      })
      .WriteLinesOne(output);
  });
}

