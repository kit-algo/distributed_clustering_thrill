#include <thrill/api/dia.hpp>
#include <thrill/api/rebalance.hpp>
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
    size_t old_node_count = graph.node_count;
    auto edges = graph.edges.Rebalance();
    thrill::common::hash<NodeId> hasher;
    auto cleanup_mapping = edges
      .Keep()
      .Map([old_node_count](const Edge& edge) { assert(edge.tail < old_node_count && edge.head < old_node_count); return edge.tail; })
      .Uniq() // Remove Degree Zero Nodes and holes in ID range
      .Sort([&hasher, old_node_count](const NodeId id1, const NodeId id2) { assert(id1 < old_node_count && id2 < old_node_count); return hasher(id1) < hasher(id2); }) // Shuffle
      .ZipWithIndex([old_node_count](const NodeId old_id, const NodeId index) { assert(old_node_count); return std::make_pair(old_id, index); });

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

    auto new_edges = edges
      .InnerJoin(
        cleanup_mapping,
        [](const Edge& edge) { return edge.tail; },
        [](const std::pair<NodeId, NodeId>& mapping) { return mapping.first; },
        [old_node_count, node_count](const Edge& edge, const std::pair<NodeId, NodeId>& mapping) { assert(edge.tail < old_node_count && edge.head < old_node_count && mapping.second < node_count && edge.tail == mapping.first); return Edge { mapping.second, edge.head }; })
      .InnerJoin(
        cleanup_mapping,
        [](const Edge& edge) { return edge.head; },
        [](const std::pair<NodeId, NodeId>& mapping) { return mapping.first; },
        [old_node_count, node_count](const Edge& edge, const std::pair<NodeId, NodeId>& mapping) { assert(edge.tail < node_count && edge.head < old_node_count && mapping.second < node_count && edge.head == mapping.first); return Edge { edge.tail, mapping.second }; })
      .Filter([node_count](const Edge& edge) { assert(edge.tail < node_count && edge.head < node_count && edge.tail != edge.head); return edge.tail <= edge.head; });

    edgesToNodes(new_edges, node_count)
      .Map(
        [node_count](const NodeWithLinks& node) {
          std::vector<NodeId> neighbors(node.links.size());
          std::transform(node.links.begin(), node.links.end(), neighbors.begin(), [](const EdgeTarget& target) { return target.target; });
          std::sort(neighbors.begin(), neighbors.end());
          for (NodeId node : neighbors) {
            assert(node < node_count);
          }
          return neighbors;
        })
      .WriteBinary(output);
  });
}

