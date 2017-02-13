cmake_minimum_required (VERSION 2.6)
project (ThrillLouvain)
include_directories("../../lib/thrill")
add_subdirectory (../../lib/thrill ../../../lib/thrill/build)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3") # optimize
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g") # debug symbols
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D _GLIBCXX_DEBUG -D _GLIBXX_DEBUG_PEDANTIC") # out of bounds asserts
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined") # sanitize all the things
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG") # disable asserts

add_executable(read src/main.cpp)
target_link_libraries(read thrill)
