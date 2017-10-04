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
  const ClusterId cluster_count = clusters.rewriteClusterIds();

  // we will need to efficiently iterate over all nodes but in order of their clustering
  std::vector<NodeId> node_ids_ordered_by_cluster(graph.getNodeCount());
  // we build the list up using bucket sort
  // we can count the number of nodes in each cluster
  // this allows us to use one vector for all buckets rather than one for each bucket
  std::vector<NodeId> cluster_node_counts(cluster_count, 0);
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    cluster_node_counts[clusters[node]]++;
  }
  prefix_sum(cluster_node_counts);
  for (NodeId node = 0; node < graph.getNodeCount(); node++) {
    node_ids_ordered_by_cluster[cluster_node_counts[clusters[node]]] = node;
    cluster_node_counts[clusters[node]]++;
  }

  // vector for meta node degrees, and later the meta first out
  std::vector<EdgeId> meta_node_degrees(cluster_count + 1, 0);
  // two vectors to emulate a more efficient map to store which neighbor clusters where already encountered
  std::vector<bool> encountered_clusters(cluster_count, false);
  std::vector<ClusterId> encountered_clusters_list;

  ClusterId current_cluster = clusters[node_ids_ordered_by_cluster[0]];
  // iterating over nodes ordered by clusters
  for (const NodeId node : node_ids_ordered_by_cluster) {
    // reset stuff whenever we come to a new cluster
    if (clusters[node] != current_cluster) {
      current_cluster = clusters[node];

      for (ClusterId neighbor : encountered_clusters_list) {
        encountered_clusters[neighbor] = false;
      }

      encountered_clusters_list.clear();
    }

    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight) {
      ClusterId neighbor_cluster = clusters[neighbor];
      if (!encountered_clusters[neighbor_cluster]) {
        meta_node_degrees[current_cluster]++;
        encountered_clusters[neighbor_cluster] = true;
        encountered_clusters_list.push_back(neighbor_cluster);

        // loops need to be represented twice
        if (current_cluster == neighbor_cluster) {
          meta_node_degrees[current_cluster]++;
        }
      }
    });
  }

  EdgeId meta_edge_count = prefix_sum(meta_node_degrees);
  // initialze to non existing cluster id, so we can check if we already got the same edge
  std::vector<ClusterId> meta_graph_heads(meta_edge_count, cluster_count);
  std::vector<Weight> meta_graph_weights(meta_edge_count);

  current_cluster = clusters[node_ids_ordered_by_cluster[0]];
  // iterating over nodes ordered by clusters
  for (const NodeId node : node_ids_ordered_by_cluster) {
    // reset stuff whenever we come to a new cluster
    if (clusters[node] != current_cluster) {
      current_cluster = clusters[node];
    }

    graph.forEachAdjacentNode(node, [&](NodeId neighbor, Weight weight) {
      ClusterId neighbor_cluster = clusters[neighbor];

      EdgeId meta_edge_index = meta_node_degrees[neighbor_cluster];
      if (meta_edge_index > 0 && (neighbor_cluster == 0 || meta_node_degrees[neighbor_cluster - 1] < meta_edge_index - 1) && meta_graph_heads[meta_edge_index - 1] == current_cluster) {
        meta_edge_index--;
      } else {
        meta_node_degrees[neighbor_cluster]++;
        if (current_cluster == neighbor_cluster) {
          meta_node_degrees[neighbor_cluster]++;
          meta_graph_heads[meta_edge_index] = current_cluster;
          meta_edge_index++;
        }
      }
      meta_graph_heads[meta_edge_index] = current_cluster;
      meta_graph_weights[meta_edge_index] += weight;
    });
  }

  for (EdgeId e = 0; e < meta_edge_count; e++) {
    if (meta_graph_weights[e] == 0) {
      assert(meta_graph_heads[e+1] == meta_graph_heads[e]);
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
