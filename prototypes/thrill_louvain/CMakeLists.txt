cmake_minimum_required (VERSION 2.6)
project (ThrillLouvain)
include_directories("../louvain/src")
include_directories("../../lib/thrill")
add_subdirectory (../../lib/thrill thrill/build)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3") # optimize
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")  # debug symbols
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D _GLIBCXX_DEBUG -D _GLIBXX_DEBUG_PEDANTIC") # out of bounds asserts
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined") # sanitize all the things
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG") # disable asserts

add_executable(label_prop src/label_prop.cpp)
add_executable(local_moving src/local_moving.cpp)
add_executable(node_based_local_moving src/node_based_local_moving.cpp)
add_executable(partitioned_louvain src/partitioned_louvain.cpp)
add_executable(partitioned_louvain_no_join src/partitioned_louvain_no_join.cpp)
add_executable(exp src/exp.cpp)
add_executable(fix_bin src/fix_bin.cpp)

target_link_libraries(label_prop thrill)
target_link_libraries(local_moving thrill)
target_link_libraries(node_based_local_moving thrill)
target_link_libraries(partitioned_louvain thrill)
target_link_libraries(partitioned_louvain_no_join thrill)
target_link_libraries(exp thrill)
target_link_libraries(fix_bin thrill)
