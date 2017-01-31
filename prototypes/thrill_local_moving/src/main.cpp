#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/all_reduce.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/join.hpp>
#include <thrill/api/print.hpp>

#include <ostream>
#include <iostream>
#include <vector>
#include <map>

using NodeId = uint32_t;
using ClusterId = uint32_t;
using Weight = uint32_t;

using NodeCluster = std::pair<NodeId, ClusterId>;
using ClusterWeight = std::pair<ClusterId, Weight>;

struct Edge {
  NodeId tail, head;
};

std::ostream& operator << (std::ostream& os, Edge& e) {
  return os << e.tail << " -> " << e.head;
}

struct IncidentClusterInfo {
  ClusterId cluster;
  Weight incident_weight;
  Weight total_weight;
};

int64_t deltaModularity(const Weight node_degree,
                        const ClusterId current_cluster,
                        const ClusterId target_cluster,
                        const int64_t weight_between_node_and_current_cluster,
                        const int64_t weight_between_node_and_target_cluster,
                        int64_t current_cluster_incident_edges_weight,
                        int64_t target_cluster_incident_edges_weight,
                        const Weight total_weight) {

  if (target_cluster == current_cluster) {
    target_cluster_incident_edges_weight -= node_degree;
  }

  current_cluster_incident_edges_weight -= node_degree;

  int64_t e = (total_weight * 2 * (weight_between_node_and_target_cluster - weight_between_node_and_current_cluster));
  int64_t a = (target_cluster_incident_edges_weight - current_cluster_incident_edges_weight) * node_degree;
  return e - a;
}

auto local_moving(thrill::DIA<Edge>& edge_list, uint32_t num_iterations, Weight total_weight) {
  auto node_clusters = edge_list
    .Map([](const Edge& edge) { return edge.tail; })
    .ReduceByKey(
      [](const NodeId& id) { return id; },
      [](const NodeId& id, const NodeId&) { return id; })
    .Map([](const NodeId& id) { return NodeCluster(id, id); })
    .Collapse();

  auto cluster_weights = edge_list
    .Map([](const Edge& edge) { return ClusterWeight(edge.tail, 1); })
    .ReducePair(
      [](const Weight& weight1, const Weight& weight2) {
        return weight1 + weight2;
      });

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    auto node_cluster_weights = node_clusters
      .InnerJoinWith(cluster_weights,
        [](const NodeCluster& node_cluster) { return node_cluster.second; },
        [](const std::pair<ClusterId, Weight>& cluster_weight) { return cluster_weight.first; },
        [](const NodeCluster& node_cluster, const ClusterWeight& cluster_weight) { return std::make_pair(node_cluster.first, cluster_weight); });

    node_clusters = edge_list
      .InnerJoinWith(node_cluster_weights,
        [](const Edge& edge) { return edge.head; },
        [](const std::pair<NodeId, ClusterWeight>& node_cluster_weight) { return node_cluster_weight.first; },
        [](const Edge& edge, const std::pair<NodeId, ClusterWeight>& node_cluster_weight) {
          return std::make_pair(edge.tail, node_cluster_weight.second);
        })
      .Map([](const std::pair<NodeId, std::pair<ClusterId, Weight>>& node_with_incident_cluster_and_weight) {
        return std::make_pair(node_with_incident_cluster_and_weight.first, IncidentClusterInfo { node_with_incident_cluster_and_weight.second.first, 1, node_with_incident_cluster_and_weight.second.second }); })
      .ReduceByKey(
        [](const std::pair<NodeId, IncidentClusterInfo>& node_with_incident_cluster) -> uint64_t {
          return (uint64_t) node_with_incident_cluster.first << 32 | node_with_incident_cluster.second.cluster;
        },
        [](const std::pair<NodeId, IncidentClusterInfo>& node_with_incident_cluster1, const std::pair<NodeId, IncidentClusterInfo>& node_with_incident_cluster2) {
          return std::make_pair(node_with_incident_cluster1.first, IncidentClusterInfo { node_with_incident_cluster1.second.cluster, node_with_incident_cluster1.second.incident_weight + node_with_incident_cluster2.second.incident_weight , node_with_incident_cluster1.second.total_weight });
        })
      .InnerJoinWith(node_cluster_weights,
        [](const std::pair<NodeId, IncidentClusterInfo>& node_cluster) { return node_cluster.first; },
        [](const std::pair<NodeId, std::pair<ClusterId, Weight>>& node_cluster_weight) { return node_cluster_weight.first; },
        [](const std::pair<NodeId, IncidentClusterInfo>& node_cluster, const std::pair<NodeId, std::pair<ClusterId, Weight>>& node_cluster_weight) {
          return std::make_pair(node_cluster_weight, node_cluster.second);
        })
      .Map(
        [](const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, IncidentClusterInfo>& local_moving_edge) {
          return std::make_pair(local_moving_edge.first, std::vector<IncidentClusterInfo>({ local_moving_edge.second }));
        })
      .ReduceByKey(
        [](const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, std::vector<IncidentClusterInfo>>& local_moving_node) { return local_moving_node.first.first; },
        [](const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, std::vector<IncidentClusterInfo>>& local_moving_node1, const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, std::vector<IncidentClusterInfo>>& local_moving_node2) {
          std::vector<IncidentClusterInfo> tmp(local_moving_node1.second.begin(), local_moving_node1.second.end());
          tmp.insert(tmp.end(), local_moving_node2.second.begin(), local_moving_node2.second.end());
          return std::make_pair(local_moving_node1.first, tmp);
        })
      .Map(
        [&total_weight](const std::pair<std::pair<NodeId, std::pair<ClusterId, Weight>>, std::vector<IncidentClusterInfo>>& local_moving_node) {
          ClusterId best_cluster = local_moving_node.first.second.first;
          int64_t best_delta = 0;
          Weight degree = 0;
          Weight weight_between_node_and_current_cluster = 0;
          for (const IncidentClusterInfo& incident_cluster : local_moving_node.second) {
            degree += incident_cluster.incident_weight;
            if (incident_cluster.cluster == local_moving_node.first.second.first) {
              weight_between_node_and_current_cluster = incident_cluster.incident_weight;
            }
          }

          for (const IncidentClusterInfo& incident_cluster : local_moving_node.second) {
            int64_t delta = deltaModularity(degree, local_moving_node.first.second.first, incident_cluster.cluster,
                                            weight_between_node_and_current_cluster, incident_cluster.incident_weight,
                                            local_moving_node.first.second.second, incident_cluster.total_weight,
                                            total_weight);
            if (delta > best_delta) {
              best_delta = delta;
              best_cluster = incident_cluster.cluster;
            }
          }

          return NodeCluster(local_moving_node.first.first, best_cluster);
        })
      .Collapse();

    cluster_weights = edge_list
      .InnerJoinWith(node_clusters,
        [](const Edge& edge) { return edge.tail; },
        [](const NodeCluster& node_cluster) { return node_cluster.first; },
        [](const Edge&, const NodeCluster& node_cluster) { return ClusterWeight(node_cluster.second, 1); })
      .ReducePair([](const Weight weight1, const Weight weight2) { return weight1 + weight2; });

    // size_t cluster_count = node_clusters
    //   .Map([](const NodeCluster& node_cluster) { return node_cluster.second; })
    //   .ReduceByKey(
    //     [](const ClusterId cluster) { return cluster; },
    //     [](const ClusterId cluster, const ClusterId) {
    //       return cluster;
    //     })
    //   .Size();
    // std::cout << "Intermediate: " << cluster_count << std::endl;
  }

  return node_clusters;
}

int main(int, char const *argv[]) {
  return thrill::Run([&](thrill::Context& context) {
    Weight edge_count = 0;
    // context.enable_consume();

    auto edges = thrill::ReadLines(context, argv[1])
      .Filter([](const std::string& line) { return !line.empty() && line[0] != '#'; })
      .template FlatMap<Edge>(
        [&edge_count](const std::string& line, auto emit) {
          std::istringstream line_stream(line);
          NodeId tail, head;

          if (line_stream >> tail >> head) {
            tail--;
            head--;
            emit(Edge { tail, head });
            emit(Edge { head, tail });
            edge_count++;
          } else {
            die(std::string("malformatted edge: ") + line);
          }
        })
      .Collapse();


    auto node_clusters = local_moving(edges, 16, edge_count);

    size_t cluster_count = node_clusters
      .Map([](const NodeCluster& node_cluster) { return node_cluster.second; })
      .ReduceByKey(
        [](const ClusterId cluster) { return cluster; },
        [](const ClusterId cluster, const ClusterId) {
          return cluster;
        })
      .Size();
    std::cout << "Result: " << cluster_count << std::endl;
  });
}

