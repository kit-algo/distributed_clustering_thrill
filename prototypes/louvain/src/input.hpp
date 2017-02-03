#pragma once

#include "graph.hpp"
#include "cluster_store.hpp"
#include "io.hpp"
#include "logging.hpp"

#include <memory>
#include <iostream>

#include "boost/program_options.hpp"

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

class Input {
private:

  po::variables_map args;
  po::options_description desc;
  int exit = 0;
  bool initialized = false;
  Logging::Id run_id;

  std::unique_ptr<Graph> graph;
  std::unique_ptr<ClusterStore> ground_proof;
  unsigned seed;

public:
  Input(int argc, char const *argv[], Logging::Id run_id) :
    desc("Options"),
    run_id(run_id),
    seed(std::chrono::system_clock::now().time_since_epoch().count()) {
    desc.add_options()
      ("graph", "The graph to perform clustering on, in metis format")
      ("ground-proof", "A ground proof clustering to compare to")
      ("partition,p", po::value<std::vector<PartitionInput>>()->composing(), "Partition with reporting UUID (comma seperated)")
      ("seed,s", po::value<unsigned>(), "Fix random seed")
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
      exit = 1;
    }

    Logging::report("program_run", run_id, "binary", argv[0]);
  }

  void initialize() {
    if (args.count("help")) {
      std::cout << desc << std::endl;
      return;
    }

    if (args.count("seed")) {
      seed = args["seed"].as<unsigned>();
    }

    std::vector<std::vector<Graph::NodeId>> neighbors;
    Graph::EdgeId edge_count = IO::read_graph(args["graph"].as<std::string>(), neighbors);
    graph = std::make_unique<Graph>(neighbors.size(), edge_count);
    graph->setEdgesByAdjacencyLists(neighbors);

    initialized = true;

    Logging::report("program_run", run_id, "graph", args["graph"].as<std::string>());
    Logging::report("program_run", run_id, "node_count", graph->getNodeCount());
    Logging::report("program_run", run_id, "edge_count", graph->getEdgeCount());
    Logging::report("program_run", run_id, "seed", seed);

    if (args.count("ground-proof")) {
      ground_proof = std::make_unique<ClusterStore>(graph->getNodeCount());
      IO::read_clustering(args["ground-proof"].as<std::string>(), *ground_proof);
      Logging::report("program_run", run_id, "ground_proof", args["ground-proof"].as<std::string>());
    }

  }

  int getExitCode() { return exit; }
  bool shouldRun() { return initialized; }
  unsigned getSeed() { return seed; }
  const Graph& getGraph() { return *graph; }
  bool isGroundProofAvailable() { if (ground_proof) { return true; } else { return false; } }
  const ClusterStore& getGroundProof() { return *ground_proof; }

  template<class F>
  void forEachPartition(F f) {
    for (const auto& partition_input : args["partition"].as<std::vector<PartitionInput>>()) {
      std::vector<uint32_t> node_partition_elements(graph->getNodeCount());
      IO::read_partition(partition_input.first, node_partition_elements);
      f(node_partition_elements, partition_input.second);
    }
  }

private:

};
