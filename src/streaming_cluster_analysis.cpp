#include "data/cluster_store.hpp"
#include "util/io.hpp"
#include "util/logging.hpp"
#include "data/graph.hpp"

#include <iostream>
#include <string>
#include <numeric>
#include <assert.h>
#include <cstdint>

using NodeId = typename Graph::NodeId;
using EdgeId = typename Graph::EdgeId;
using Weight = typename Graph::Weight;
using ClusterId = typename ClusterStore::ClusterId;

using int128_t = __int128_t;

int main(int, char const *argv[]) {
  const NodeId node_count = std::stoi(argv[3]);
  ClusterStore clusters = IO::read_binary_clustering(argv[2], 100000);
  clusters.rewriteClusterIds();
  const ClusterId cluster_count = clusters.idRangeUpperBound();

  std::vector<Weight> inner_volumes(cluster_count, 0);
  std::vector<Weight> total_volumes(cluster_count, 0);
  std::vector<Weight> node_degrees(node_count, 0);
  Weight total_volume = 0;
  Weight total_cut = 0;

  IO::stream_bin_graph(argv[1], [&](const NodeId u, const NodeId v) {
    assert(u < node_count);
    assert(v < node_count);

    total_volume++;
    node_degrees[u]++;
    total_volumes[clusters[u]]++;

    assert(clusters[u] < cluster_count);
    assert(clusters[v] < cluster_count);
    if (clusters[u] == clusters[v]) {
      inner_volumes[clusters[u]]++;
    } else {
      total_cut++;
    }
  });


  Weight inner_sum = std::accumulate(inner_volumes.begin(), inner_volumes.end(), Weight(0));
  assert(inner_sum <= total_volume);
  assert(inner_sum + total_cut == total_volume);
  int128_t incident_sum = std::accumulate(total_volumes.begin(), total_volumes.end(), (int128_t) 0, [](const int128_t& agg, const Weight& elem) {
    return agg + (((int128_t) elem) * ((int128_t) elem));
  });

  assert(total_volume <= (1ull << 48));
  double modularity = (double(inner_sum) / double(total_volume)) - (double(incident_sum) / double(total_volume * total_volume));
  assert(modularity <= 1.0);
  assert(modularity >= -0.5);

  long double sum_p_log_p_w_alpha = 0;
  long double sum_p_log_p_cluster_cut = 0;
  long double sum_p_log_p_cluster_cut_plus_vol = 0;

  const auto plogp_rel = [total_volume](Weight w) -> double {
    if (w > 0) {
      double p = static_cast<double>(w) / total_volume;
      return p * log(p);
    }

    return 0;
  };

  for (NodeId node = 0; node < node_count; node++) {
    sum_p_log_p_w_alpha += plogp_rel(node_degrees[node]);
  }

  for (ClusterId c = 0; c < cluster_count; c++) {
    Weight cluster_cut = total_volumes[c] - inner_volumes[c];
    sum_p_log_p_cluster_cut += plogp_rel(cluster_cut);
    sum_p_log_p_cluster_cut_plus_vol += plogp_rel(cluster_cut + total_volumes[c]);
  }

  double map_equation = plogp_rel(total_cut) - 2 * sum_p_log_p_cluster_cut + sum_p_log_p_cluster_cut_plus_vol - sum_p_log_p_w_alpha;

  Logging::Id logging_id = Logging::getUnusedId();
  Logging::report("clustering", logging_id, "cluster_count", cluster_count);
  Logging::report("clustering", logging_id, "modularity", modularity);
  Logging::report("clustering", logging_id, "map_equation", map_equation);
}
