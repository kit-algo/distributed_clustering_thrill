cmake_minimum_required (VERSION 2.6)
project (ThrillLouvain)

include_directories("src")
include_directories("lib/thrill")
include_directories("lib/sparsepp")
include_directories("lib/vectorclass")
include_directories("lib/RoutingKit/include")
include_directories("lib/infomap/include")
add_subdirectory(lib/thrill)

add_custom_target(
   infomap_lib
   COMMAND make lib
   WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/lib/infomap/"
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -Wall -Wextra -fopenmp -mavx2 -mfma -ffast-math")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O3") # abuse for dev builds
# set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D _GLIBCXX_DEBUG -D _GLIBXX_DEBUG_PEDANTIC") # out of bounds asserts
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined") # sanitize all the things

add_executable(dlslm_map_eq src/dlslm_map_eq.cpp lib/RoutingKit/src/bit_select.cpp lib/RoutingKit/src/bit_vector.cpp lib/RoutingKit/src/id_mapper.cpp)
add_executable(dlslm src/dlslm.cpp lib/RoutingKit/src/bit_select.cpp lib/RoutingKit/src/bit_vector.cpp lib/RoutingKit/src/id_mapper.cpp)
add_executable(dlslm_with_seq src/dlslm.cpp lib/RoutingKit/src/bit_select.cpp lib/RoutingKit/src/bit_vector.cpp lib/RoutingKit/src/id_mapper.cpp)
add_executable(dlplm src/dlplm.cpp lib/RoutingKit/src/bit_select.cpp lib/RoutingKit/src/bit_vector.cpp lib/RoutingKit/src/id_mapper.cpp)
add_executable(label_prop src/label_prop.cpp lib/RoutingKit/src/bit_select.cpp lib/RoutingKit/src/bit_vector.cpp lib/RoutingKit/src/id_mapper.cpp)
add_executable(preprocess src/preprocessing.cpp)
add_executable(preprocess_ground_truth src/preprocess_ground_truth.cpp)
add_executable(distributed_clustering_analyser src/distributed_clustering_analyser.cpp lib/RoutingKit/src/bit_select.cpp lib/RoutingKit/src/bit_vector.cpp lib/RoutingKit/src/id_mapper.cpp)

set_target_properties(dlslm PROPERTIES COMPILE_FLAGS "-D STOP_MOVECOUNT -D FIXED_RATIO=4")
set_target_properties(dlslm_with_seq PROPERTIES COMPILE_FLAGS "-D STOP_MOVECOUNT -D FIXED_RATIO=4 -D SWITCH_TO_SEQ")
set_target_properties(dlslm_map_eq PROPERTIES COMPILE_FLAGS "-D STOP_MOVECOUNT -D FIXED_RATIO=4")

target_link_libraries(dlslm_map_eq thrill)
target_link_libraries(dlslm thrill)
target_link_libraries(dlslm_with_seq thrill)
target_link_libraries(dlplm thrill)
target_link_libraries(label_prop thrill)
target_link_libraries(preprocess thrill)
target_link_libraries(preprocess_ground_truth thrill)
target_link_libraries(distributed_clustering_analyser thrill)

add_executable(seq_exp src/seq_exp.cpp lib/RoutingKit/src/bit_select.cpp lib/RoutingKit/src/bit_vector.cpp lib/RoutingKit/src/id_mapper.cpp)
add_executable(seq_louvain src/seq_louvain.cpp lib/RoutingKit/src/bit_select.cpp lib/RoutingKit/src/bit_vector.cpp lib/RoutingKit/src/id_mapper.cpp)
add_executable(infomap src/infomap.cpp lib/RoutingKit/src/bit_select.cpp lib/RoutingKit/src/bit_vector.cpp lib/RoutingKit/src/id_mapper.cpp)
target_link_libraries(seq_exp thrill)
target_link_libraries(seq_louvain thrill)
target_link_libraries(infomap thrill "${CMAKE_SOURCE_DIR}/lib/infomap/lib/libInfomap.a")
add_dependencies(infomap infomap_lib)
