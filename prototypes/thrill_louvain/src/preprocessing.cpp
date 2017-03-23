#include <thrill/api/dia.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/write_binary.hpp>

#include <vector>
#include <algorithm>

#include "input.hpp"

int main(int argc, char const *argv[]) {
  std::string graph_file(argv[1]);
  size_t glob_index = graph_file.find('*');
  if (glob_index != std::string::npos) {
    graph_file.erase(graph_file.begin() + glob_index);
  }
  std::string output = graph_file.replace(graph_file.begin() + graph_file.rfind('.'), graph_file.end(), "-preprocessed-#####.bin");

  std::string ground_truth_file = "";
  std::string ground_truth_output = "";
  if (argc > 2) {
    ground_truth_file = std::string(argv[2]);
    glob_index = ground_truth_file.find('*');
    if (glob_index != std::string::npos) {
      ground_truth_file.erase(ground_truth_file.begin() + glob_index);
    }
    ground_truth_output = ground_truth_file.replace(ground_truth_file.begin() + ground_truth_file.rfind('.'), ground_truth_file.end(), "-preprocessed-#####.bin");
  }

  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();
    auto graph = Input::readToEdgeGraph<false>(argv[1], context);
    thrill::common::hash<NodeId> hasher;
    auto cleanup_mapping = graph.edges
      .Keep()
      .Map([](const Edge& edge) { return edge.tail; })
      .Uniq() // Remove Degree Zero Nodes and wholes in ID range
      .Sort([&hasher](const NodeId id1, const NodeId id2) { return hasher(id1) < hasher(id2); }) // Shuffle
      .ZipWithIndex([](const NodeId old_id, const NodeId index) { return std::make_pair(old_id, index); });

    NodeId node_count = cleanup_mapping.Keep().Size();

    if (argc > 2) {
      Input::readClustering(argv[2], context)
        .InnerJoin(cleanup_mapping.Keep(),
          [](const NodeCluster& node_cluster) { return node_cluster.first; },
          [](const std::pair<NodeId, NodeId>& id_mapping) { return id_mapping.first; },
          [](const NodeCluster& node_cluster, const std::pair<NodeId, NodeId>& id_mapping) {
            return NodeCluster(id_mapping.second, node_cluster.second);
          })
        .WriteBinary(ground_truth_output);
    }

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
        [](const Edge& edge, const std::pair<NodeId, NodeId>& mapping) { return Edge { edge.tail, mapping.second }; })
      .Filter([](const Edge& edge) { return edge.tail <= edge.head; });

    edgesToNodes(edges, node_count)
      .Map(
        [](const NodeWithLinks& node) {
          std::vector<NodeId> neighbors(node.links.size());
          std::transform(node.links.begin(), node.links.end(), neighbors.begin(), [](const EdgeTarget& target) { return target.target; });
          std::sort(neighbors.begin(), neighbors.end());
          return neighbors;
        })
      .WriteBinary(output);
  });
}

