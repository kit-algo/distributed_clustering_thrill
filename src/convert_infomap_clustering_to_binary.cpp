#include <tlx/cmdline_parser.cpp>
#include <tlx/die.hpp>

#include <iostream>
#include <fstream>

int main(int argc, char const * argv[]) {
  tlx::CmdlineParser cp;

  cp.set_description("This tool converts an output file of Infomap or Gossipmap"
		     "into a binary clustering file.");
  cp.set_author("Michael Hamann <michael.hamann@kit.edu>");

  std::string input_path;
  cp.add_param_string("input_path", input_path, "The path of the input file.");

  std::string output_path;
  cp.add_param_string("output_path", output_path, "The path of the output file.");

  if (!cp.process(argc, argv)) {
    return -1;
  }

  cp.print_result();

  std::ofstream out_stream(output_path, std::ios::binary);

  std::ifstream in_stream(input_path);

  std::string line_buffer;

  while (std::getline(in_stream, line_buffer)) {
    uint32_t node_id, part_id;
    std::stringstream line_stream(line_buffer);
    line_stream >> node_id >> part_id;
    die_unless(line_stream.good());
    
    out_stream.write(reinterpret_cast<const char*>(&node_id), 4);
    out_stream.write(reinterpret_cast<const char*>(&part_id), 4);
  }

  in_stream.close();
  out_stream.close();

  return 0;
}
