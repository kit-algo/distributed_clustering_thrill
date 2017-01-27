cmake_minimum_required (VERSION 2.6)
project (louvain)
include_directories("../louvain/src")
include_directories("../../lib/thrill")
add_subdirectory (../../lib/thrill_upstream ../../../lib/thrill_upstream/build)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -Wextra")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3") # optimize
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g") # debug symbols
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D _GLIBCXX_DEBUG -D _GLIBXX_DEBUG_PEDANTIC") # out of bounds asserts
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined") # sanitize all the things
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG") # disable asserts

add_executable(thrill_louvain_no_join src/main.cpp)
target_link_libraries(thrill_louvain_no_join thrill)
