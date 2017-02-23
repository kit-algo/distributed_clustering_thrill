#pragma once

#include <thrill/api/dia.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/size.hpp>

#include <vector>
#include <iostream>

#include "thrill_graph.hpp"

namespace Input {

DiaGraph<Edge> readEdgeListGraph(const std::string& file, thrill::Context& context) {
  auto edges = thrill::ReadLines(context, file)
    .Filter([](const std::string& line) { return !line.empty() && line[0] != '#'; })
    .template FlatMap<Edge>(
      [](const std::string& line, auto emit) {
        std::istringstream line_stream(line);
        NodeId tail, head;

        if (line_stream >> tail >> head) {
          emit(Edge { tail, head });
          emit(Edge { head, tail });
        } else {
          die(std::string("malformatted edge: ") + line);
        }
      })
    .Collapse();

  auto cleanup_mapping = edges
    .Keep()
    .Map([](const Edge& edge) { return edge.tail; })
    .Uniq()
    .Sort()
    .ZipWithIndex([](const NodeId old_id, const NodeId index) { return std::make_pair(old_id, index); });

  NodeId node_count = cleanup_mapping.Keep().Size();

  edges = edges
    .InnerJoin(
      cleanup_mapping,
      [](const Edge& edge) { return edge.tail; },
      [](const std::pair<NodeId, NodeId>& mapping) { return mapping.first; },
      [](const Edge& edge, const std::pair<NodeId, NodeId>& mapping) { return Edge { mapping.second, edge.head }; })
    .InnerJoin(
      cleanup_mapping,
      [](const Edge& edge) { return edge.head; },
      [](const std::pair<NodeId, NodeId>& mapping) { return mapping.first; },
      [](const Edge& edge, const std::pair<NodeId, NodeId>& mapping) { return Edge { edge.tail, mapping.second }; });

  Weight total_weight = edges.Keep().Size() / 2;

  return DiaGraph<Edge> { edges.Cache(), node_count, total_weight };
}

DiaGraph<Edge> readDimacsGraph(const std::string& file, thrill::Context& context) {
  using Node = std::pair<NodeId, std::vector<NodeId>>;

  auto lines = thrill::ReadLines(context, file);

  NodeId node_count = lines.Keep().Size() - 1;

  auto nodes = lines
    .ZipWithIndex([](const std::string line, const size_t index) { return std::make_pair(line, index); })
    .Filter([](const std::pair<std::string, size_t>& node) { return node.second > 0; })
    .Map(
      [](const std::pair<std::string, size_t>& node) {
        std::istringstream line_stream(node.first);
        std::vector<NodeId> neighbors;
        NodeId neighbor;

        while (line_stream >> neighbor) {
          neighbors.push_back(neighbor - 1);
        }

        return Node(node.second - 1, neighbors);
      });


  auto edges = nodes
    .template FlatMap<Edge>(
      [](const Node& node, auto emit) {
        for (const NodeId neighbor : node.second) {
          emit(Edge { node.first, neighbor });
        }
      });

  Weight total_weight = edges.Keep().Size() / 2;

  return DiaGraph<Edge> { edges.Cache(), node_count, total_weight };
}

DiaGraph<Edge> readBinaryGraph(const std::string& file, thrill::Context& context) {
  auto nodes = thrill::ReadBinary<std::vector<NodeId>>(context, file);

  NodeId node_count = nodes.Keep().Size();

  auto edges = nodes
    .ZipWithIndex(
      [](const std::vector<NodeId>& neighbors, const NodeId node) {
        return std::make_pair(node, neighbors);
      })
    .template FlatMap<Edge>(
      [](const std::pair<NodeId, std::vector<NodeId>>& node, auto emit) {
        for (const NodeId neighbor : node.second) {
          emit(Edge { node.first, neighbor });
          emit(Edge { neighbor, node.first });
        }
      });

  Weight total_weight = edges.Keep().Size() / 2;

  return DiaGraph<Edge> { edges.Cache(), node_count, total_weight };
}

bool ends_with(const std::string& value, const std::string& ending) {
  if (ending.size() > value.size()) return false;
  return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

DiaGraph<Edge> readGraph(const std::string& file, thrill::Context& context) {
  if (ends_with(file, ".graph")) {
    return readDimacsGraph(file, context);
  } else if (ends_with(file, ".txt")) {
    return readEdgeListGraph(file, context);
  } else if (ends_with(file, ".bin")) {
    return readBinaryGraph(file, context);
  } else {
    throw "unknown graph input";
  }
}

} // Input
