#pragma once

#include "cluster_store.hpp"
#include "graph.hpp"

#include <vector>
#include <unordered_map>
#include <assert.h>
#include <cstdint>
#include <math.h>

namespace Similarity {

using NodeId = typename Graph::NodeId;
using ClusterId = typename ClusterStore::ClusterId;

double adjustedRandIndex(const ClusterStore &c, const ClusterStore &d) {
  NodeId node_count = c.size();
  ClusterStore intersection(0, node_count);
  c.intersection(d, intersection);

  // TODO reduce sizes to actual number of clusters
  std::vector<uint32_t> c_cluster_sizes(node_count, 0);
  std::vector<uint32_t> d_cluster_sizes(node_count, 0);
  std::vector<uint32_t> intersection_cluster_sizes(node_count, 0);

  // precompute sizes for each cluster
  for (NodeId node = 0; node < node_count; node++) {
    c_cluster_sizes[c[node]]++;
    d_cluster_sizes[d[node]]++;
    intersection_cluster_sizes[intersection[node]]++;
  }

  uint32_t rand_index = 0;
  for (uint32_t s : intersection_cluster_sizes) {
    rand_index += s * (s - 1) / 2;
  }

  uint32_t sum_c = 0;
  for (uint32_t s : c_cluster_sizes) {
    sum_c += s * (s - 1) / 2;
  }

  uint32_t sum_d = 0;
  for (uint32_t s : d_cluster_sizes) {
    sum_d += s * (s - 1) / 2;
  }

  double maxIndex = 0.5 * (sum_c + sum_d);

  double expectedIndex = sum_c * sum_d / (node_count * (node_count-1) / 2);

  if (maxIndex == 0) { // both clusterings are singleton clusterings
    return 1.0;
  } else if (maxIndex == expectedIndex) { // both partitions contain one cluster the whole graph
    return 1.0;
  } else {
    return (rand_index - expectedIndex) / (maxIndex - expectedIndex);
  }
}

double entropy(const ClusterStore& clustering, std::vector<double> probabilities) {
  // $H(\zeta):=-\sum_{C\in\zeta}P(C)\cdot\log_{2}(P(C))$
  double h = 0.0;
  for (ClusterId cluster = 0; cluster < clustering.size(); cluster++) {
    if (probabilities[cluster] != 0) {
      h += probabilities[cluster] * log2(probabilities[cluster]);
    } // log(0) is not defined
  }
  h = -1.0 * h;

  assert(!std::isnan(h));

  return h;
}

double normalizedMutualInformation(const ClusterStore &c, const ClusterStore &d) {
  NodeId node_count = c.size();
  ClusterStore intersection(0, node_count);
  c.intersection(d, intersection);

  // TODO reduce sizes to actual number of clusters
  std::vector<uint32_t> c_cluster_sizes(node_count, 0);
  std::vector<uint32_t> d_cluster_sizes(node_count, 0);
  std::vector<uint32_t> intersection_cluster_sizes(node_count, 0);
  std::vector<ClusterId> intersection_to_c(node_count, 0);
  std::vector<ClusterId> intersection_to_d(node_count, 0);

  // precompute sizes for each cluster
  for (NodeId node = 0; node < node_count; node++) {
    c_cluster_sizes[c[node]]++;
    d_cluster_sizes[d[node]]++;

    ClusterId cluster = intersection[node];
    if (intersection_cluster_sizes[cluster] == 0) {
      intersection_to_c[cluster] = c[node];
      intersection_to_d[cluster] = d[node];
    }
    intersection_cluster_sizes[cluster] += 1;
  }

  // precompute cluster probabilities
  // TODO reduce sizes to actual number of clusters
  std::vector<double> p_c(node_count, 0.0);
  std::vector<double> p_d(node_count, 0.0);

  for (ClusterId cluster = 0; cluster < c_cluster_sizes.size(); cluster++) {
    p_c[cluster] = c_cluster_sizes[cluster] / (double) node_count;
  }

  for (ClusterId cluster = 0; cluster < d_cluster_sizes.size(); cluster++) {
    p_d[cluster] = d_cluster_sizes[cluster] / (double) node_count;
  }

  // calculate mutual information
  //     $MI(\zeta,\eta):=\sum_{C\in\zeta}\sum_{D\in\eta}\frac{|C\cap D|}{n}\cdot\log_{2}\left(\frac{|C\cap D|\cdot n}{|C|\cdot|D|}\right)$
  double MI = 0.0; // mutual information
  for (ClusterId intersection_cluster = 0; intersection_cluster < intersection_cluster_sizes.size(); intersection_cluster++) {
    if (intersection_cluster_sizes[intersection_cluster] > 0) {
      ClusterId c_cluster = intersection_to_c[intersection_cluster];
      ClusterId d_cluster = intersection_to_d[intersection_cluster];
      double factor1 = intersection_cluster_sizes[intersection_cluster] / (double) node_count;
      assert((c_cluster_sizes[c_cluster] * d_cluster_sizes[d_cluster]) != 0);
      double frac2 = (intersection_cluster_sizes[intersection_cluster] * node_count) / (double) (c_cluster_sizes[c_cluster] * d_cluster_sizes[d_cluster]);
      assert(frac2 != 0);
      double factor2 = log2(frac2);
      MI += factor1 * factor2;
    }
  }

  // sanity check
  assert(!std::isnan(MI));
  assert(MI >= 0.0);

  // compute entropy for both clusterings
  double h_c = entropy(c, p_c);
  double h_d = entropy(d, p_d);

  assert(!std::isnan(h_c));
  assert(!std::isnan(h_d));

  double h_sum = h_c + h_d;
  if (h_sum != 0) {
    return (2.0 * MI) / h_sum;
  } else {
    return .0;
  }
}

};
