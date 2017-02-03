#pragma once

#include "graph.hpp"
#include "cluster_store.hpp"
#include "io.hpp"
#include "logging.hpp"

#include <memory>
#include <iostream>
#include <chrono>

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
  std::unordered_map<Graph::NodeId, Graph::NodeId> id_mapping;
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
      ("snap-format,f", po::bool_switch()->default_value(false), "Graph is in SNAP Edge List Format rather than DIMACS graph")
      ("help", "produce help message");
    po::positional_options_description pos_desc;
    pos_desc.add("graph", 1);
    pos_desc.add("partition", -1);

    try {
      po::store(po::command_line_parser(argc, argv).options(desc).positional(pos_desc).run(), args); // can throw
      po::notify(args);
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
    Graph::EdgeId edge_count = 0;
    if (args["snap-format"].as<bool>()) {
      establishIdMapping(args["graph"].as<std::string>());
      neighbors.resize(id_mapping.size());
      edge_count = IO::read_graph_txt(args["graph"].as<std::string>(), neighbors, id_mapping);
    } else {
      edge_count = IO::read_graph(args["graph"].as<std::string>(), neighbors);
    }
    graph = std::make_unique<Graph>(neighbors.size(), edge_count);
    graph->setEdgesByAdjacencyLists(neighbors);


    Logging::report("program_run", run_id, "graph", args["graph"].as<std::string>());
    Logging::report("program_run", run_id, "node_count", graph->getNodeCount());
    Logging::report("program_run", run_id, "edge_count", graph->getEdgeCount());
    Logging::report("program_run", run_id, "seed", seed);

    if (args.count("ground-proof")) {
      ground_proof = std::make_unique<ClusterStore>(graph->getNodeCount());
      if (args["snap-format"].as<bool>()) {
        IO::read_snap_clustering(args["ground-proof"].as<std::string>(), *ground_proof, id_mapping);
      } else {
        IO::read_clustering(args["ground-proof"].as<std::string>(), *ground_proof);
      }
      Logging::report("program_run", run_id, "ground_proof", args["ground-proof"].as<std::string>());
    }

    initialized = true;
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

  void establishIdMapping(const std::string& graph_file) {
    std::set<NodeId> node_ids;
    IO::open_file(graph_file, [&](auto& file) {
      std::string line;

      while (std::getline(file, line)) {
        if (!line.empty() && line[0] != '#') {
          std::istringstream line_stream(line);
          Graph::NodeId tail, head;
          if (line_stream >> tail >> head) {
            node_ids.insert(tail);
            node_ids.insert(head);
          }
        }
      }
    });

    NodeId new_id = 0;
    for (NodeId old_id : node_ids) {
      id_mapping[old_id] = new_id++;
    }
  }

};
