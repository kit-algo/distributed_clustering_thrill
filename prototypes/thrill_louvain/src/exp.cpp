#include <thrill/api/cache.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/fold_by_key.hpp>
#include <thrill/api/zip.hpp>

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

    for (uint32_t i = 1; i < graph.node_count; i *= 8) {
      auto node_clusters = graph.nodes.Map([i](const NodeWithLinks& node) { return std::make_pair(node, node.id / i); }).Execute();

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
        .template GroupByKey<std::pair<ClusterId, std::vector<NodeWithLinks>>>(
          [](const std::pair<NodeWithLinks, ClusterId>& pair) { return pair.second; },
          [](auto& iterator, const ClusterId cluster) {
            std::vector<NodeWithLinks> merged;
            while (iterator.HasNext()) {
              const std::pair<NodeWithLinks, ClusterId>& pair = iterator.Next();
              merged.push_back(pair.first);
            }
            return std::make_pair(cluster, merged);
          })
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

      timer.Start();

      node_clusters
        .Keep()
        .template GroupByKey<std::pair<ClusterId, std::pair<Weight, std::vector<NodeId>>>>(
          [](const std::pair<NodeWithLinks, ClusterId>& pair) { return pair.second; },
          [](auto& iterator, const ClusterId cluster) {
            Weight acc = 0;
            std::vector<NodeId> nodes;
            while (iterator.HasNext()) {
              const NodeWithLinks& node = iterator.Next().first;
              acc = node.weightedDegree();
              nodes.push_back(node.id);
            }
            return std::make_pair(cluster, std::make_pair(acc, nodes));
          })
        .template FlatMap<std::pair<std::pair<NodeId, ClusterId>, Weight>>([](const std::pair<ClusterId, std::pair<Weight, std::vector<NodeId>>>& cluster, auto emit) {
          for (const NodeId id : cluster.second.second) {
            emit(std::make_pair(std::make_pair(id, cluster.first), cluster.second.first));
          }
        })
        .ReduceToIndex(
          [](const std::pair<std::pair<NodeId, ClusterId>, Weight>& node_cluster_weight) -> size_t { return node_cluster_weight.first.first; },
          [](const std::pair<std::pair<NodeId, ClusterId>, Weight>& ncw, const std::pair<std::pair<NodeId, ClusterId>, Weight>&) { assert(false); return ncw; },
          graph.node_count)
        .Zip(thrill::NoRebalanceTag, node_clusters, [](const std::pair<std::pair<NodeId, ClusterId>, Weight>& node_cluster_weight, const std::pair<NodeWithLinks, ClusterId>& node_cluster) {
          assert(node_cluster.first.id == node_cluster_weight.first.first);
          return std::make_pair(node_cluster, node_cluster_weight.second);
        })
        .Execute();

      timer.Stop();

      if (context.my_rank() == 0) {
        std::cout << "Acc Zip " << i << ": " << timer.Milliseconds() << "ms" << std::endl;
      }

      timer.Reset();

      timer.Start();

      node_clusters
        .Keep()
        .template FoldByKey<std::vector<NodeWithLinks>>(thrill::NoDuplicateDetectionTag,
          [](const std::pair<NodeWithLinks, ClusterId>& node_cluster) { return node_cluster.second; },
          [](std::vector<NodeWithLinks>&& acc, const std::pair<NodeWithLinks, ClusterId>& node_cluster) {
            acc.push_back(node_cluster.first);
            return std::move(acc);
          })
        .FlatMap([](const std::pair<ClusterId, std::vector<NodeWithLinks>>& cluster_nodes, auto emit) {
          Weight total_weight = 0;
          for (const NodeWithLinks& node : cluster_nodes.second) {
            total_weight += node.weightedDegree();
          }
          for (const NodeWithLinks& node : cluster_nodes.second) {
            emit(std::make_pair(std::make_pair(node, cluster_nodes.first), total_weight));
          }
        })
        .Execute();

      timer.Stop();

      if (context.my_rank() == 0) {
        std::cout << "Fold " << i << ": " << timer.Milliseconds() << "ms" << std::endl;
      }

      timer.Reset();

      timer.Start();

      node_clusters
        .Keep()
        .Map([](const std::pair<NodeWithLinks, ClusterId>& node_cluster) { return std::make_pair(node_cluster.second, std::vector<NodeWithLinks> { node_cluster.first }); })
        .ReduceByKey(
          [](const std::pair<ClusterId, std::vector<NodeWithLinks>>& cluster_nodes) { return cluster_nodes.first; },
          [](std::pair<ClusterId, std::vector<NodeWithLinks>>&& acc, const std::pair<ClusterId, std::vector<NodeWithLinks>>& nodes) {
            acc.second.insert(acc.second.end(), nodes.second.begin(), nodes.second.end());
            return std::move(acc);
          })
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
        std::cout << "Reduce with move " << i << ": " << timer.Milliseconds() << "ms" << std::endl;
      }

      timer.Reset();

      timer.Start();

      node_clusters
        .Keep()
        .Map([](const std::pair<NodeWithLinks, ClusterId>& node_cluster) { return std::make_pair(node_cluster.second, std::vector<NodeWithLinks> { node_cluster.first }); })
        .ReduceByKey(
          [](const std::pair<ClusterId, std::vector<NodeWithLinks>>& cluster_nodes) { return cluster_nodes.first; },
          [](const std::pair<ClusterId, std::vector<NodeWithLinks>> n1, const std::pair<ClusterId, std::vector<NodeWithLinks>>& n2) {
            std::vector<NodeWithLinks> nodes;
            nodes.insert(nodes.end(), n1.second.begin(), n1.second.end());
            nodes.insert(nodes.end(), n2.second.begin(), n2.second.end());
            return std::make_pair(n1.first, nodes);
          })
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
        std::cout << "Reduce with forced copying " << i << ": " << timer.Milliseconds() << "ms" << std::endl;
      }

      timer.Reset();
    }

  });
}

