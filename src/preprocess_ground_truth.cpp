#include <thrill/api/dia.hpp>
#include <thrill/api/fold_by_key.hpp>
#include <thrill/api/rebalance.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/zip.hpp>

#include <vector>
#include <algorithm>

#include "util/thrill/input.hpp"

int main(int, char const *argv[]) {
  std::string ground_truth_file = "";
  std::string ground_truth_output = "";
  ground_truth_file = std::string(argv[1]);
  if (!ground_truth_file.empty()) {
    size_t glob_index = ground_truth_file.find('*');
    if (glob_index != std::string::npos) {
      ground_truth_file.erase(ground_truth_file.begin() + glob_index);
    }
    ground_truth_output = ground_truth_file.replace(ground_truth_file.begin() + ground_truth_file.rfind('.'), ground_truth_file.end(), "-preprocessed-@@@@-#####.bin");
  }

  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();
    thrill::common::hash<NodeId> hasher;

    auto comparator = [&hasher](const NodeCluster& n1, const NodeCluster& n2) {
      size_t h1 = hasher(n1.first);
      size_t h2 = hasher(n2.first);
      return h1 < h2 || (h1 == h2 && n1.first < n2.first);
    };

    Input::readClustering(argv[1], context)
      .Sort(comparator)
      .ZipWithIndex([](const NodeCluster& node_cluster, NodeId new_index) { return NodeCluster(new_index, node_cluster.second); })
      .WriteBinary(ground_truth_output);
  });
}

