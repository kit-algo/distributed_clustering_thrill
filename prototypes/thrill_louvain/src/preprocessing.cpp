#include <thrill/api/dia.hpp>
#include <thrill/api/fold_by_key.hpp>
#include <thrill/api/rebalance.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/zip.hpp>

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
    // size_t old_node_count = graph.node_count;
    thrill::common::hash<NodeId> hasher;

    auto nodes_with_new_ids = graph.edges
      .Rebalance()
      .template FoldByKey<std::vector<NodeId>>(thrill::NoDuplicateDetectionTag,
        [](const Edge& edge) { return edge.tail; },
        [](std::vector<NodeId>&& neighbors, const Edge& edge) {
          neighbors.push_back(edge.head);
          return std::move(neighbors);
        })
      .Sort([&hasher](const std::pair<NodeId, std::vector<NodeId>>& n1, const std::pair<NodeId, std::vector<NodeId>>& n2) { return hasher(n1.first) < hasher(n2.first); })
      .ZipWithIndex([](const std::pair<NodeId, std::vector<NodeId>>& node, NodeId new_index) { return std::make_pair(new_index, node); });

    auto cleanup_mapping = nodes_with_new_ids
      .Map([](const std::pair<NodeId, std::pair<NodeId, std::vector<NodeId>>>& new_id_and_node) { return std::make_pair(new_id_and_node.second.first, new_id_and_node.first); });

    // NodeId node_count = cleanup_mapping.Keep().Size();

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

    nodes_with_new_ids
      .template FlatMap<Edge>(
        [](const std::pair<NodeId, std::pair<NodeId, std::vector<NodeId>>>& new_id_and_node, auto emit) {
          for (NodeId neighbor : new_id_and_node.second.second) {
            emit(Edge { neighbor, new_id_and_node.first });
          }
        })
      .template FoldByKey<std::vector<NodeId>>(thrill::NoDuplicateDetectionTag,
        [](const Edge& edge) { return edge.tail; },
        [](std::vector<NodeId>&& neighbors, const Edge& edge) {
          neighbors.push_back(edge.head);
          return std::move(neighbors);
        })
      .Sort([&hasher](const std::pair<NodeId, std::vector<NodeId>>& n1, const std::pair<NodeId, std::vector<NodeId>>& n2) { return hasher(n1.first) < hasher(n2.first); })
      .Zip(cleanup_mapping,
        [](const std::pair<NodeId, std::vector<NodeId>>& node, const std::pair<NodeId, NodeId>& id_mapping) {
          assert(node.first == id_mapping.first);
          return std::make_pair(id_mapping.second, node.second);
        })
      .Map(
        [](const std::pair<NodeId, std::vector<NodeId>>& node) {
          std::vector<NodeId> neighbors;
          neighbors.reserve(node.second.size());
          for (NodeId id : node.second) {
            if (id > node.first) {
              neighbors.push_back(id);
            }
          }
          std::sort(neighbors.begin(), neighbors.end());
          return neighbors;
        })
      .WriteBinary(output);
  });
}

