cmake_minimum_required (VERSION 2.6)
project (ThrillLouvain)
include_directories("src")
include_directories("../louvain/src")
include_directories("../../lib/thrill")
include_directories("../../lib/sparsepp")
add_subdirectory (../../lib/thrill thrill/build)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3") # optimize
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")  # debug symbols
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D _GLIBCXX_DEBUG -D _GLIBXX_DEBUG_PEDANTIC") # out of bounds asserts
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined") # sanitize all the things
# set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG") # disable asserts

add_executable(dlslm_map_eq src/dlslm_map_eq.cpp)
add_executable(dlslm src/dlslm.cpp)
add_executable(dlplm src/dlplm.cpp)
add_executable(label_prop src/label_prop.cpp)
add_executable(preprocess src/preprocessing.cpp)
add_executable(distributed_clustering_analyser src/distributed_clustering_analyser.cpp)

set_target_properties(dlslm PROPERTIES COMPILE_FLAGS "-D STOP_MOVECOUNT -D FIXED_RATIO=4")
set_target_properties(dlslm_map_eq PROPERTIES COMPILE_FLAGS "-D STOP_MOVECOUNT -D FIXED_RATIO=4")

target_link_libraries(dlslm_map_eq thrill)
target_link_libraries(dlslm thrill)
target_link_libraries(dlplm thrill)
target_link_libraries(label_prop thrill)
target_link_libraries(preprocess thrill)
target_link_libraries(distributed_clustering_analyser thrill)
