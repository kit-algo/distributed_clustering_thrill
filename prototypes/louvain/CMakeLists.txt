cmake_minimum_required (VERSION 2.8)
project (Louvain)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src/")
include_directories("../../lib/thrill")

# Compiler-Options
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3") # optimize
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")  # debug symbols
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D _GLIBCXX_DEBUG -D _GLIBXX_DEBUG_PEDANTIC") # out of bounds asserts
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined") # sanitize all the things
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG") # disable asserts

# Executables
add_executable(louvain src/main.cpp ../../lib/thrill/thrill/common/string.cpp ../../lib/thrill/thrill/common/cmdline_parser.cpp)
add_executable(seq_louvain src/seq_louvain.cpp ../../lib/thrill/thrill/common/string.cpp ../../lib/thrill/thrill/common/cmdline_parser.cpp)
