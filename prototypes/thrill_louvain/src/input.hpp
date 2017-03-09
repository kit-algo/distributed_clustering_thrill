#pragma once

#include <thrill/api/cache.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/zip_with_index.hpp>

#include <vector>
#include <iostream>

#include "thrill_graph.hpp"

namespace Input {

DiaEdgeGraph<Edge> readEdgeListToEdgeGraph(const std::string& file, thrill::Context& context) {
  auto raw_edges = thrill::ReadLines(context, file)
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
      });

  auto cleanup_mapping = raw_edges
    .Keep()
    .Map([](const Edge& edge) { return edge.tail; })
    .Uniq()
    .Sort()
    .ZipWithIndex([](const NodeId old_id, const NodeId index) { return std::make_pair(old_id, index); });

  NodeId node_count = cleanup_mapping.Keep().Size();

  auto edges = raw_edges
    .InnerJoin(
      cleanup_mapping,
      [](const Edge& edge) { return edge.tail; },
      [](const std::pair<NodeId, NodeId>& mapping) { return mapping.first; },
      [](const Edge& edge, const std::pair<NodeId, NodeId>& mapping) { return Edge { mapping.second, edge.head }; })
    .InnerJoin(
      cleanup_mapping,
      [](const Edge& edge) { return edge.head; },
      [](const std::pair<NodeId, NodeId>& mapping) { return mapping.first; },
      [](const Edge& edge, const std::pair<NodeId, NodeId>& mapping) { return Edge { edge.tail, mapping.second }; })
    .Cache();

  Weight total_weight = edges.Keep().Size() / 2;

  return DiaEdgeGraph<Edge> { edges, node_count, total_weight };
}

DiaGraph<NodeWithLinks, Edge> readEdgeListGraph(const std::string& file, thrill::Context& context) {
  auto graph = readEdgeListToEdgeGraph(file, context);
  return DiaGraph<NodeWithLinks, Edge> { edgesToNodes(graph.edges.Keep(), graph.node_count).Cache(), graph.edges, graph.node_count, graph.total_weight };
}

DiaNodeGraph<NodeWithLinks> readEdgeListToNodeGraph(const std::string& file, thrill::Context& context) {
  auto graph = readEdgeListToEdgeGraph(file, context);
  return DiaNodeGraph<NodeWithLinks> { edgesToNodes(graph.edges, graph.node_count).Cache(), graph.node_count, graph.total_weight };
}



DiaNodeGraph<NodeWithLinks> readDimacsToNodeGraph(const std::string& file, thrill::Context& context) {
  auto lines = thrill::ReadLines(context, file);

  NodeId node_count = lines.Keep().Size() - 1;

  auto nodes = lines
    .ZipWithIndex([](const std::string line, const size_t index) { return std::make_pair(line, index); })
    .Filter([](const std::pair<std::string, size_t>& node) { return node.second > 0; })
    .Map(
      [](const std::pair<std::string, size_t>& node) {
        std::istringstream line_stream(node.first);
        std::vector<EdgeTarget> neighbors;
        NodeId neighbor;

        while (line_stream >> neighbor) {
          neighbors.push_back(EdgeTarget { neighbor - 1 });
        }

        return NodeWithLinks { NodeId(node.second - 1), neighbors };
      })
    .Cache();

  Weight total_weight = nodes
    .Keep()
    .Map([](const NodeWithLinks& node) { return node.links.size(); })
    .Sum() / 2;

  return DiaNodeGraph<NodeWithLinks> { nodes, node_count, total_weight };
}

DiaGraph<NodeWithLinks, Edge> readDimacsGraph(const std::string& file, thrill::Context& context) {
  auto graph = readDimacsToNodeGraph(file, context);
  return DiaGraph<NodeWithLinks, Edge> { graph.nodes, nodesToEdges(graph.nodes.Keep()), graph.node_count, graph.total_weight };
}

DiaEdgeGraph<Edge> readDimacsToEdgeGraph(const std::string& file, thrill::Context& context) {
  auto graph = readDimacsToNodeGraph(file, context);
  return DiaEdgeGraph<Edge> { nodesToEdges(graph.nodes), graph.node_count, graph.total_weight };
}


DiaEdgeGraph<Edge> readBinaryToEdgeGraph(const std::string& file, thrill::Context& context) {
  auto input = thrill::ReadBinary<std::vector<NodeId>>(context, file);

  NodeId node_count = input.Keep().Size();

  auto edges = input
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
      })
    .Cache();

  Weight total_weight = edges.Keep().Size() / 2;

  return DiaEdgeGraph<Edge> { edges, node_count, total_weight };
}

DiaGraph<NodeWithLinks, Edge> readBinaryGraph(const std::string& file, thrill::Context& context) {
  auto graph = readBinaryToEdgeGraph(file, context);
  return DiaGraph<NodeWithLinks, Edge> { edgesToNodes(graph.edges.Keep(), graph.node_count), graph.edges, graph.node_count, graph.total_weight };
}

DiaNodeGraph<NodeWithLinks> readBinaryToNodeGraph(const std::string& file, thrill::Context& context) {
  auto graph = readBinaryToEdgeGraph(file, context);
  return DiaNodeGraph<NodeWithLinks> { edgesToNodes(graph.edges, graph.node_count).Cache(), graph.node_count, graph.total_weight };
}


bool ends_with(const std::string& value, const std::string& ending) {
  if (ending.size() > value.size()) return false;
  return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

DiaGraph<NodeWithLinks, Edge> readGraph(const std::string& file, thrill::Context& context) {
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

DiaEdgeGraph<Edge> readToEdgeGraph(const std::string& file, thrill::Context& context) {
  if (ends_with(file, ".graph")) {
    return readDimacsToEdgeGraph(file, context);
  } else if (ends_with(file, ".txt")) {
    return readEdgeListToEdgeGraph(file, context);
  } else if (ends_with(file, ".bin")) {
    return readBinaryToEdgeGraph(file, context);
  } else {
    throw "unknown graph input";
  }
}


DiaNodeGraph<NodeWithLinks> readToNodeGraph(const std::string& file, thrill::Context& context) {
  if (ends_with(file, ".graph")) {
    return readDimacsToNodeGraph(file, context);
  } else if (ends_with(file, ".txt")) {
    return readEdgeListToNodeGraph(file, context);
  } else if (ends_with(file, ".bin")) {
    return readBinaryToNodeGraph(file, context);
  } else {
    throw "unknown graph input";
  }
}

} // Input
