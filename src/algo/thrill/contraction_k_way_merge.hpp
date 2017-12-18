#pragma once

#include "data/thrill/graph.hpp"

#include <routingkit/bit_vector.h>
#include <routingkit/id_mapper.h>

namespace ContractionKWayMerge {

std::vector<WeightedEdgeTarget> twoWayMerge(std::vector<WeightedEdgeTarget> x, std::vector<WeightedEdgeTarget> y) {
  size_t size_x = x.size();
  size_t size_y = y.size();

  std::vector<WeightedEdgeTarget> result;
  result.reserve(size_x + size_y);

  auto insert = [&](const WeightedEdgeTarget& element) {
    assert(result.empty() || element.target >= result.back().target);

    if (result.empty() || result.back().target != element.target) {
      result.push_back(element);
    } else {
      result.back().weight += element.weight;
    }
  };

  size_t index_x = 0;
  size_t index_y = 0;

  while (index_x < size_x && index_y < size_y) {
    if (index_x >= size_x) {
      insert(y[index_y]);
      index_y++;

    } else if (index_y >= size_y) {
      insert(x[index_x]);
      index_x++;

    } else {

      if (x[index_x].target <= y[index_y].target) {
        insert(x[index_x]);
        index_x++;
      } else {
        insert(y[index_y]);
        index_y++;
      }

    }
  }

  return result;
}

std::vector<WeightedEdgeTarget> kWayMerge(std::vector<std::vector<WeightedEdgeTarget>> containers) {
  if (containers.empty()) {
    return std::vector<WeightedEdgeTarget>();
  }
  if (containers.size() == 1) {
    return std::move(containers[0]);
  }

  std::sort(containers.begin(), containers.end(), [](const std::vector<WeightedEdgeTarget>& x, const std::vector<WeightedEdgeTarget>& y) { return x.size() < y.size(); });

  containers[0] = twoWayMerge(std::move(containers[0]), std::move(containers[1]));

  size_t next_intermediate = 0;
  size_t next_output = 1;
  size_t next_original_index = 2 % containers.size();

  auto done = [&]() {
    if (next_original_index != 0) { return false; }
    if (next_intermediate < next_output) {
      return next_output - next_intermediate == 1;
    } else {
      return next_output == 0 && next_intermediate == containers.size() - 1;
    }
  };

  auto next_smallest_list = [&]() {
    if (next_original_index != 0 && (next_intermediate == next_output || containers[next_original_index].size() < containers[next_intermediate].size())) {
      size_t ret_index = next_original_index;
      next_original_index = (next_original_index + 1) % containers.size();
      assert(next_original_index == 0 || next_original_index > next_output);
      assert(next_original_index == 0 || next_original_index > next_intermediate);
      return std::move(containers[ret_index]);
    } else {
      assert(next_intermediate != next_output);
      size_t ret_index = next_intermediate;
      next_intermediate = (next_intermediate + 1) % containers.size();
      return std::move(containers[ret_index]);
    }
  };

  while (!done()) {
    containers[next_output] = twoWayMerge(next_smallest_list(), next_smallest_list());
    next_output = (next_output + 1) % containers.size();
    assert(next_original_index == 0 || next_output < next_original_index);
    assert(next_output != next_intermediate);
  }

  return std::move(containers[next_intermediate]);
}

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
