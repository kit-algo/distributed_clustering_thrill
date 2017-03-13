#include <thrill/api/write_lines_one.hpp>

#include <thrill/common/cmdline_parser.hpp>

#include <ostream>
#include <iostream>
#include <vector>

#include "input.hpp"
#include "thrill_partitioning.hpp"

int main(int argc, char const *argv[]) {
  std::string graph_file = "";
  std::string out_file = "";
  uint32_t partition_size = 4;
  thrill::common::CmdlineParser cp;
  cp.AddParamString("graph", graph_file, "The graph to perform clustering on");
  cp.AddUInt('k', "num-partitions", "unsigned int", partition_size, "Number of Partitions to split the graph into");
  cp.AddString('o', "out", "file", out_file, "Where to write the result");

  if (!cp.Process(argc, argv)) {
    return 1;
  }

  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    auto graph = Input::readToNodeGraph(graph_file, context);

    partition(graph, partition_size)
      .Map([](const NodeIdLabel& label) { return std::to_string(label.label); })
      .WriteLinesOne(out_file.empty() ? graph_file + ".part" : out_file);
  });
}

