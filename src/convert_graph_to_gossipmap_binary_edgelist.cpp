#include "util/io.hpp"
#include "util/logging.hpp"
#include "data/graph.hpp"

#include <tlx/cmdline_parser.hpp>

#include <iostream>
#include <string>
#include <cstdint>
#include <assert.h>

using NodeId = typename Graph::NodeId;

int main(int argc, char const *argv[]) {
  tlx::CmdlineParser cp;

  cp.set_description("This tool converts a series of thrill binary graph files"
		     "into the binary edge list format GossipMap understands"
		     "which consists of src/target pairs of 4 byte each."
		     "Note that every edge is printed twice to represent the"
		     "undirected input graph as directed graph.");
  cp.set_author("Michael Hamann <michael.hamann@kit.edu>");

  std::string graph_paths;
  cp.add_param_string("graph_pattern", graph_paths, "A glob pattern describing the input graph files.");

  std::string output_path;
  cp.add_param_string("output_path", output_path, "The path to the output file.");

  if (!cp.process(argc, argv)) {
    return -1;
  }

  cp.print_result();

  std::ofstream out_stream(output_path, std::ios::binary);

  IO::stream_bin_graph(graph_paths, [&](const NodeId u, const NodeId v) {

			static_assert(std::is_same<uint32_t, NodeId>::value, "Node type is not uint32 anymore, adjust code!");
			out_stream.write(reinterpret_cast<const char*>(&u), 4);
			out_stream.write(reinterpret_cast<const char*>(&v), 4);
    });

  out_stream.close();

  return 0;
}
