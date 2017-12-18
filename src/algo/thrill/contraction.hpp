#pragma once

#include "data/thrill/graph.hpp"

#include <routingkit/bit_vector.h>
#include <routingkit/id_mapper.h>

namespace Contraction {

std::vector<WeightedEdgeTarget> sort_and_merge_links(const std::vector<NodeWithWeightedLinks>& nodes, ClusterId own_cluster, ClusterId cluster_count) {
  RoutingKit::BitVector neighboring_cluster_ids(cluster_count);
  for (const NodeWithWeightedLinks& node : nodes) {
    for (const WeightedEdgeTarget& link : node.links) {
      neighboring_cluster_ids.set(link.target);
    }
  }

  size_t num_entries = neighboring_cluster_ids.population_count();
  if (neighboring_cluster_ids.is_set(own_cluster)) {
    num_entries++;
  }
  std::vector<WeightedEdgeTarget> output_links(num_entries);

  RoutingKit::LocalIDMapper id_mapper(neighboring_cluster_ids);
  for (const NodeWithWeightedLinks& node : nodes) {
    for (const WeightedEdgeTarget& link : node.links) {
      size_t index = id_mapper.to_local(link.target);
      if (neighboring_cluster_ids.is_set(own_cluster) && link.target > own_cluster) {
        index += 1;
      }

      output_links[index].target = link.target;
      output_links[index].weight += link.weight;
    }
  }

  if (neighboring_cluster_ids.is_set(own_cluster)) {
    size_t index = id_mapper.to_local(own_cluster);
    assert(output_links[index].target == own_cluster);
    assert(output_links[index].weight % 2 == 0);
    output_links[index].weight /= 2;
    output_links[index + 1].weight = output_links[index].weight;
    output_links[index + 1].target = own_cluster;
  }

  return output_links;
}

};
