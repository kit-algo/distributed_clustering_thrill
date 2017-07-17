#include <thrill/api/write_lines_one.hpp>

#include <tlx/cmdline_parser.hpp>

#include <ostream>
#include <iostream>
#include <vector>

#include "util/thrill/input.hpp"
#include "algo/thrill/partitioning.hpp"
#include "util/logging.hpp"

int main(int argc, char const *argv[]) {
  std::string graph_file = "";
  std::string out_file = "";
  uint32_t partition_size = 4;
  tlx::CmdlineParser cp;
  cp.add_param_string("graph", graph_file, "The graph to perform clustering on");
  cp.add_unsigned('k', "num-partitions", "unsigned int", partition_size, "Number of Partitions to split the graph into");
  cp.add_string('o', "out", "file", out_file, "Where to write the result");

  if (!cp.process(argc, argv)) {
    return 1;
  }

  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();

    auto graph = Input::readToNodeGraph(graph_file, context);

    if (context.my_rank() == 0) {
      Logging::Id program_run_logging_id = Logging::getUnusedId();
      Logging::report("program_run", program_run_logging_id, "binary", argv[0]);
      Logging::report("program_run", program_run_logging_id, "hosts", context.num_hosts());
      Logging::report("program_run", program_run_logging_id, "total_workers", context.num_workers());
      Logging::report("program_run", program_run_logging_id, "workers_per_host", context.workers_per_host());
      Logging::report("program_run", program_run_logging_id, "graph", argv[1]);
      Logging::report("program_run", program_run_logging_id, "node_count", graph.node_count);
      Logging::report("program_run", program_run_logging_id, "edge_count", graph.total_weight);
      if (getenv("MOAB_JOBID")) {
        Logging::report("program_run", program_run_logging_id, "job_id", getenv("MOAB_JOBID"));
      }
    }

    partition(graph, partition_size)
      .Execute();
      // .Map([](const NodePartition& node_partition) { return std::to_string(node_partition.partition); })
      // .WriteLinesOne(out_file.empty() ? graph_file + ".part" : out_file);
  });
}

