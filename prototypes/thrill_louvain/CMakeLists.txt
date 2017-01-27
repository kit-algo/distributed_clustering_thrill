cmake_minimum_required (VERSION 2.6)
project (ThrillLouvain)
include_directories("../louvain/src")
include_directories("../../lib/thrill")
add_subdirectory (../../lib/thrill ../../../lib/thrill/build)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -Wextra")

# Thrill stuff
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")
if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcrc32")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3") # optimize
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g") # debug symbols
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D _GLIBCXX_DEBUG -D _GLIBXX_DEBUG_PEDANTIC") # out of bounds asserts
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined") # sanitize all the things
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG") # disable asserts

add_executable(thrill_louvain src/main.cpp)
target_link_libraries(thrill_louvain thrill)
