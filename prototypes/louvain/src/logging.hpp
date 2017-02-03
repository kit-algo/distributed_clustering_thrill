#pragma once

#include <iostream>
#include <cstdint>
#include <vector>

namespace Logging {

typedef uint64_t Id;

Id id_counter = 0;

template<class KeyType, class ValueType>
void report(const std::string & type, const Id id, const KeyType & key, const ValueType & value) {
  std::cout << type << '/' << id << '/' << key << ": " << value << std::endl;
}

template<class KeyType, class ValueType>
void report(const std::string & type, const Id id, const KeyType & key, const std::vector<ValueType> & values) {
  std::cout << type << '/' << id << '/' << key << ": " << "[";
  for (const ValueType& value : values) {
    std::cout << value << ", ";
  }
  std::cout << "]" << std::endl;
}

Id getUnusedId() {
  return id_counter++;
}

}
