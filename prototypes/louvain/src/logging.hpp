#pragma once

#include <iostream>
#include <cstdint>
#include <vector>

#include "graph.hpp"
#include "cluster_store.hpp"
#include "similarity.hpp"

#define LOGGING_PREFIX "#LOG# "

namespace Logging {

typedef uint64_t Id;

Id id_counter = 0;

template<class IdType, class KeyType, class ValueType>
void report(const std::string & type, const IdType id, const KeyType & key, const ValueType & value) {
  std::cout << LOGGING_PREFIX << type << '/' << id << '/' << key << ": " << value << std::endl;
}

template<class IdType, class KeyType>
void report(const std::string & type, const IdType id, const KeyType & key, const bool & value) {
  std::cout << LOGGING_PREFIX << type << '/' << id << '/' << key << ": " << (value ? "true" : "false") << std::endl;
}

template<class IdType, class KeyType, class ValueType>
void report(const std::string & type, const IdType id, const KeyType & key, const std::vector<ValueType> & values) {
  std::cout << LOGGING_PREFIX << type << '/' << id << '/' << key << ": " << "[";
  for (const ValueType& value : values) {
    std::cout << value << ", ";
  }
  std::cout << "]" << std::endl;
}

Id getUnusedId() {
  return id_counter++;
}

void log_comparison_results(Logging::Id base_clustering_id, const ClusterStore & base_clusters, Logging::Id compare_clustering_id, const ClusterStore & compare_clusters) {
  Logging::Id comparison_id = Logging::getUnusedId();
  Logging::report("clustering_comparison", comparison_id, "base_clustering_id", base_clustering_id);
  Logging::report("clustering_comparison", comparison_id, "compare_clustering_id", compare_clustering_id);
  Logging::report("clustering_comparison", comparison_id, "NMI", Similarity::normalizedMutualInformation(base_clusters, compare_clusters));
  Logging::report("clustering_comparison", comparison_id, "ARI", Similarity::adjustedRandIndex(base_clusters, compare_clusters));
  std::pair<double, double> precision_recall = Similarity::weightedPrecisionRecall(base_clusters, compare_clusters);
  Logging::report("clustering_comparison", comparison_id, "Precision", precision_recall.first);
  Logging::report("clustering_comparison", comparison_id, "Recall", precision_recall.second);
}

}
