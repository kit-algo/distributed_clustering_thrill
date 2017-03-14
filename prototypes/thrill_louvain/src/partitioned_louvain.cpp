#include "thrill_local_moving.hpp"
#include "thrill_louvain.hpp"

int main(int argc, char const *argv[]) {
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  Modularity::rng = std::default_random_engine(seed);

  return Louvain::performAndEvaluate(argc, argv, "thrill partitioned louvain", [](const auto& graph) {
    return Louvain::louvain(graph, [](const auto& graph) {
      return LocalMoving::partitionedLocalMoving(graph);
    });
  });
}

