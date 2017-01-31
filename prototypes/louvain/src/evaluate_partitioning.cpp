#include "graph.hpp"
#include "modularity.hpp"
#include "cluster_store.hpp"
#include "logging.hpp"
#include "io.hpp"

#include "boost/program_options.hpp"

#include <iostream>
#include <string>
#include <numeric>
#include <assert.h>
#include <random>
#include <chrono>

namespace po = boost::program_options;

using PartitionInput = std::pair<std::string, std::string>;

namespace std {
  istream& operator>>(istream& in, PartitionInput& ss) {
    string s;
    in >> s;
    const size_t sep = s.find(',');
    if (sep == string::npos) {
      throw po::error("invalid pair");
    } else {
      ss.first  = s.substr(0, sep);
      ss.second = s.substr(sep+1);
    }
    return in;
  }
}

int main(int argc, char const *argv[]) {
  po::options_description desc("Options");
  desc.add_options()
    ("graph", "The graph to perform clustering on, in metis format")
    ("seed,s", po::value<unsigned>(), "Fix random seed")
    ("partition,p", po::value<std::vector<PartitionInput>>()->composing(), "Partition with reporting UUID (comma seperated)")
    ("help", "produce help message");
  po::positional_options_description pos_desc;
  pos_desc.add("graph", 1);
  pos_desc.add("partition", -1);
  po::variables_map args;

  try {
    po::store(po::command_line_parser(argc, argv).options(desc).positional(pos_desc).run(), args); // can throw
  } catch(po::error& e) {
    std::cerr << "ERROR: " << e.what() << std::endl << std::endl;
    std::cerr << desc << std::endl;
    return 1;
  }

  if (args.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  if (args.count("seed")) {
    seed = args["seed"].as<unsigned>();
  }
  Modularity::rng = std::default_random_engine(seed);

  uint64_t run_id = Logging::getUnusedId();

  std::vector<std::vector<Graph::NodeId>> neighbors;
  Graph::EdgeId edge_count = IO::read_graph(args["graph"].as<std::string>(), neighbors);
  Graph graph(neighbors.size(), edge_count);
  graph.setEdgesByAdjacencyLists(neighbors);

  Logging::report("program_run", run_id, "binary", argv[0]);
  Logging::report("program_run", run_id, "graph", args["graph"].as<std::string>());
  Logging::report("program_run", run_id, "node_count", graph.getNodeCount());
  Logging::report("program_run", run_id, "edge_count", graph.getEdgeCount());
  Logging::report("program_run", run_id, "seed", seed);

  ClusterStore clusters(neighbors.size());

  for (const auto& partition_input : args["partition"].as<std::vector<PartitionInput>>()) {
    std::vector<uint32_t> node_partition_elements(graph.getNodeCount());
    IO::read_partition(partition_input.first, node_partition_elements);

    uint64_t algo_run_logging_id = Logging::getUnusedId();
    Logging::report("algorithm_run", algo_run_logging_id, "program_run_id", run_id);
    Logging::report("algorithm_run", algo_run_logging_id, "algorithm", "partitioned louvain");
    Logging::report("algorithm_run", algo_run_logging_id, "order", "original");
    Logging::report("algorithm_run", algo_run_logging_id, "partition_id", partition_input.second);

    Modularity::partitionedLouvain(graph, clusters, node_partition_elements, algo_run_logging_id);
  }
}

