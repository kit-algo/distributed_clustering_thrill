#pragma once

#include "data/graph.hpp"
#include "data/cluster_store.hpp"

namespace Contraction {

using NodeId = typename Graph::NodeId;
using EdgeId = typename Graph::EdgeId;
using Weight = typename Graph::Weight;
using ClusterId = typename ClusterStore::ClusterId;

template<typename T>
T prefix_sum(std::vector<T>& elements) {
  T running_sum = 0;
  for (T& element : elements) {
    T next = element + running_sum;
    element = running_sum;
    running_sum = next;
  }
  return running_sum;
}

template<class GraphType, class ClusterStoreType>
Graph contract(const GraphType& graph, ClusterStoreType &clusters) {
  bool weighted = graph.isWeighted();
  const ClusterId cluster_count = clusters.rewriteClusterIds();
  std::vector<EdgeId> original_edge_degree_per_cluster(cluster_count + 1, 0);

  // count orig degrees for each cluster
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight) {
      original_edge_degree_per_cluster[clusters[neighbor]]++;
    });
  }

  // prefix sums to be able to do indexing
  EdgeId edge_count = prefix_sum(original_edge_degree_per_cluster);
  std::cout << edge_count << ", " << graph.getEdgeCount() << std::endl;
  assert(edge_count == graph.getEdgeCount() * 2);
  std::vector<NodeId> cluster_heads(edge_count);
  std::vector<Weight> cluster_weights(weighted ? edge_count : 0);

  // bucket sort
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight weight) {
      EdgeId index = original_edge_degree_per_cluster[clusters[neighbor]];
      cluster_heads[index] = clusters[node];
      if (weighted) {
        cluster_weights[index] = weight;
      }
      original_edge_degree_per_cluster[clusters[neighbor]]++;
    });
  }
  assert(*(original_edge_degree_per_cluster.end() - 1) == edge_count);
  assert(*(original_edge_degree_per_cluster.end() - 2) == edge_count);

  std::vector<EdgeId> meta_node_degrees(cluster_count + 1, 0);
  std::vector<bool> encountered_clusters(cluster_count, false);
  std::vector<ClusterId> encountered_clusters_list;

  EdgeId start = 0;
  for (ClusterId c = 0; c < cluster_count; c++) {
    EdgeId end = original_edge_degree_per_cluster[c];

    for (EdgeId edge = start; edge < end; edge++) {
      ClusterId neighbor = cluster_heads[edge];
      if (!encountered_clusters[neighbor]) {
        meta_node_degrees[neighbor]++;
        encountered_clusters[neighbor] = true;
        encountered_clusters_list.push_back(neighbor);
      }
    }

    if (encountered_clusters[c]) {
      meta_node_degrees[c]++;
    }

    for (ClusterId neighbor : encountered_clusters_list) {
      encountered_clusters[neighbor] = false;
    }
    encountered_clusters_list.clear();
    start = end;
  }

  EdgeId meta_edge_count = prefix_sum(meta_node_degrees);
  // initialze to non existing cluster id, so we can check if we already got the same edge
  std::vector<ClusterId> meta_graph_heads(meta_edge_count, cluster_count);
  std::vector<Weight> meta_graph_weights(meta_edge_count);

  start = 0;
  for (ClusterId c = 0; c < cluster_count; c++) {
    EdgeId end = original_edge_degree_per_cluster[c];

    for (EdgeId edge = start; edge < end; edge++) {
      ClusterId neighbor = cluster_heads[edge];
      Weight weight = weighted ? cluster_weights[edge] : 1;

      EdgeId meta_edge_index = meta_node_degrees[neighbor];
      if (meta_edge_index > 0 && (neighbor == 0 || meta_node_degrees[neighbor - 1] < meta_edge_index - 1) && meta_graph_heads[meta_edge_index - 1] == c) {
        meta_edge_index--;
      } else {
        meta_node_degrees[neighbor]++;
        if (c == neighbor) {
          meta_node_degrees[neighbor]++;
          meta_graph_heads[meta_edge_index] = c;
          meta_edge_index++;
        }
      }
      meta_graph_heads[meta_edge_index] = c;
      meta_graph_weights[meta_edge_index] += weight;
    }

    start = end;
  }

  for (EdgeId e = 0; e < meta_edge_count; e++) {
    if (meta_graph_weights[e] == 0) {
      assert(meta_graph_weights[e+1] % 2 == 0);
      meta_graph_weights[e+1] /= 2;
      meta_graph_weights[e] = meta_graph_weights[e+1];
    }
  }

  meta_node_degrees.pop_back();
  meta_node_degrees.insert(meta_node_degrees.begin(), 0);

  return Graph(std::move(meta_node_degrees), std::move(meta_graph_heads), std::move(meta_graph_weights));
}

} // Contraction
