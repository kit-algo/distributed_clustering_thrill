#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/group_by_key.hpp>

#include <thrill/common/stats_timer.hpp>

#include <ostream>
#include <iostream>
#include <vector>
#include <list>

#include "input.hpp"

namespace thrill {
namespace data {

template <typename Archive, typename T>
struct Serialization<Archive, std::list<T>>{
  static void Serialize(const std::list<T>& x, Archive& ar) {
    ar.PutVarint(x.size());
    for (typename std::list<T>::const_iterator it = x.begin(); it != x.end(); ++it)
      Serialization<Archive, T>::Serialize(*it, ar);
  }
  static std::list<T> Deserialize(Archive& ar) {
    size_t size = ar.GetVarint();
    std::list<T> out;
    for (size_t i = 0; i != size; ++i)
      out.emplace_back(Serialization<Archive, T>::Deserialize(ar));
    return out;
  }
  static constexpr bool   is_fixed_size = false;
  static constexpr size_t fixed_size = 0;
};

} // data
} // thrill

int main(int, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    auto graph = Input::readToNodeGraph(argv[1], context);

    thrill::common::StatsTimerBase<true> timer(/* autostart */ false);

    for (uint32_t i : { 1, 4, 16, 64, 256, 1024 }) {
      auto node_clusters = graph.nodes.Map([i](const NodeWithLinks& node) { return std::make_pair(node, node.id / i); });

      timer.Start();

      node_clusters
        .Keep()
        .Map([](const std::pair<NodeWithLinks, ClusterId>& node_cluster) { return std::make_pair(node_cluster.second, node_cluster.first.weightedDegree()); })
        .ReducePair([](const Weight weight1, const Weight weight2) { return weight1 + weight2; })
        .InnerJoin(node_clusters,
          [](const std::pair<ClusterId, Weight>& cluster_weight) { return cluster_weight.first; },
          [](const std::pair<NodeWithLinks, ClusterId>& node_cluster) { return node_cluster.second; },
          [](const std::pair<ClusterId, Weight>& cluster_weight, const std::pair<NodeWithLinks, ClusterId>& node_cluster) {
            return std::make_pair(node_cluster, cluster_weight.second);
          })
        .Execute();

      timer.Stop();

      if (context.my_rank() == 0) {
        std::cout << "Join " << i << ": " << timer.Milliseconds() << "ms" << std::endl;
      }

      timer.Reset();

      timer.Start();

      node_clusters
        .Keep()
        .Map([](const std::pair<NodeWithLinks, ClusterId>& node_cluster) { return std::make_pair(node_cluster.second, std::vector<NodeWithLinks>({ node_cluster.first })); })
        .template GroupByKey<std::pair<ClusterId, std::vector<NodeWithLinks>>>(
          [](const std::pair<ClusterId, std::vector<NodeWithLinks>>& pair) { return pair.first; },
          [](auto& iterator, const ClusterId cluster) {
            std::vector<NodeWithLinks> merged;
            while (iterator.HasNext()) {
              const std::pair<ClusterId, std::vector<NodeWithLinks>>& pair = iterator.Next();
              merged.insert(merged.end(), pair.second.begin(), pair.second.end());
            }
            return std::make_pair(cluster, merged);
          })
        // .ReduceByKey(
        //   [](const std::pair<ClusterId, std::vector<NodeWithLinks>>& pair) { return pair.first; },
        //   [](std::pair<ClusterId, std::vector<NodeWithLinks>> neighbors1, const std::pair<ClusterId, std::vector<NodeWithLinks>>& neighbors2) {
        //     neighbors1.second.insert(neighbors1.second.end(), neighbors2.second.begin(), neighbors2.second.end());
        //     return neighbors1;
        //   })
        .FlatMap([](const std::pair<ClusterId, std::vector<NodeWithLinks>>& cluster, auto emit) {
          Weight total_weight = 0;
          for (const NodeWithLinks& node : cluster.second) {
            total_weight += node.weightedDegree();
          }
          for (const NodeWithLinks& node : cluster.second) {
            emit(std::make_pair(std::make_pair(node, cluster.first), total_weight));
          }
        })
        .Execute();

      timer.Stop();

      if (context.my_rank() == 0) {
        std::cout << "Acc " << i << ": " << timer.Milliseconds() << "ms" << std::endl;
      }

      timer.Reset();
    }

  });
}

