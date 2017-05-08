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
  const std::string graph_file(argv[1]);
  std::string output = graph_file;
  size_t glob_index = output.find('*');
  if (glob_index != std::string::npos) {
    output.erase(output.begin() + glob_index);
  }
  output = output.replace(output.begin() + output.rfind('.'), output.end(), "-preprocessed-@@@@-#####.bin");

  std::string ground_truth_file = "";
  std::string ground_truth_output = "";
  if (argc > 2) {
    ground_truth_file = std::string(argv[2]);
    ground_truth_output = ground_truth_file;
    if (!ground_truth_output.empty()) {
      glob_index = ground_truth_output.find('*');
      if (glob_index != std::string::npos) {
        ground_truth_output.erase(ground_truth_output.begin() + glob_index);
      }
      ground_truth_output = ground_truth_output.replace(ground_truth_output.begin() + ground_truth_output.rfind('.'), ground_truth_output.end(), "-preprocessed-@@@@-#####.bin");
    }
  }

  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();
    auto graph = Input::readToEdgeGraph<false>(graph_file, context);
    // size_t old_node_count = graph.node_count;
    thrill::common::hash<NodeId> hasher;

    auto comparator = [&hasher](const std::pair<NodeId, std::vector<NodeId>>& n1, const std::pair<NodeId, std::vector<NodeId>>& n2) {
      size_t h1 = hasher(n1.first);
      size_t h2 = hasher(n2.first);
      return h1 < h2 || (h1 == h2 && n1.first < n2.first);
    };

    auto nodes_with_new_ids = graph.edges
      .Rebalance()
      .template FoldByKey<std::vector<NodeId>>(thrill::NoDuplicateDetectionTag,
        [](const Edge& edge) { return edge.tail; },
        [](std::vector<NodeId>&& neighbors, const Edge& edge) {
          neighbors.push_back(edge.head);
          return std::move(neighbors);
        })
      .Sort(comparator)
      .ZipWithIndex([](const std::pair<NodeId, std::vector<NodeId>>& node, NodeId new_index) { return std::make_pair(new_index, node); });

    auto cleanup_mapping = nodes_with_new_ids
      .Map([](const std::pair<NodeId, std::pair<NodeId, std::vector<NodeId>>>& new_id_and_node) { return std::make_pair(new_id_and_node.second.first, new_id_and_node.first); });

    // NodeId node_count = cleanup_mapping.Keep().Size();

    if (argc > 2 && !ground_truth_file.empty()) {
      Input::readClustering(ground_truth_file, context)
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
      .Sort(comparator)
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

