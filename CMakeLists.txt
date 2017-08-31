cmake_minimum_required (VERSION 2.6)
project (ThrillLouvain)

include_directories("src")
include_directories("lib/thrill")
include_directories("lib/sparsepp")
include_directories("lib/vectorclass")
add_subdirectory(lib/thrill)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -Wall -Wextra")
SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O3") # abuse for dev builds
# set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D _GLIBCXX_DEBUG -D _GLIBXX_DEBUG_PEDANTIC") # out of bounds asserts
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined") # sanitize all the things

add_executable(dlslm_map_eq src/dlslm_map_eq.cpp)
add_executable(dlslm src/dlslm.cpp)
add_executable(dlplm src/dlplm.cpp)
add_executable(label_prop src/label_prop.cpp)
add_executable(preprocess src/preprocessing.cpp)
add_executable(distributed_clustering_analyser src/distributed_clustering_analyser.cpp)

set_target_properties(dlslm PROPERTIES COMPILE_FLAGS "-D STOP_MOVECOUNT -D FIXED_RATIO=4")
set_target_properties(dlslm_map_eq PROPERTIES COMPILE_FLAGS "-ffast-math -fopenmp -mavx2 -mfma -D STOP_MOVECOUNT -D FIXED_RATIO=4")

target_link_libraries(dlslm_map_eq thrill)
target_link_libraries(dlslm thrill)
target_link_libraries(dlplm thrill)
target_link_libraries(label_prop thrill)
target_link_libraries(preprocess thrill)
target_link_libraries(distributed_clustering_analyser thrill)

add_executable(seq_exp src/seq_exp.cpp)
add_executable(seq_louvain src/seq_louvain.cpp)
target_link_libraries(seq_exp thrill)
target_link_libraries(seq_louvain thrill)
