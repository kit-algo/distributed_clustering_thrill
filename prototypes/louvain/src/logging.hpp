#pragma once

#include <iostream>
#include <cstdint>

namespace Logging {

uint64_t id_counter = 0;

template<class KeyType, class ValueType>
void report(const std::string & type, const uint64_t id, const KeyType & key, const ValueType & value) {
  std::cout << type << '/' << id << '/' << key << ": " << value << "\n";
}

uint64_t getUnusedId() {
  return id_counter++;
}

}
