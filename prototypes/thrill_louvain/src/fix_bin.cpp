#include <thrill/api/dia.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/write_binary.hpp>

#include <vector>
#include <algorithm>

#include "input.hpp"

int main(int, char const *argv[]) {
  std::string file(argv[1]);
  std::string output = file.replace(file.end() - 4, file.end(), "-sorted-#####.bin");

  return thrill::Run([&](thrill::Context& context) {
    context.enable_consume();
    thrill::ReadBinary<std::vector<NodeId>>(context, argv[1])
      .Map(
        [](std::vector<NodeId> neighbors) {
          std::sort(neighbors.begin(), neighbors.end());
          return neighbors;
        })
      .WriteBinary(output);
  });
}

