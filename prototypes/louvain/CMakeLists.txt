cmake_minimum_required (VERSION 2.8)
project (Louvain)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src/")

# LIBS

# Compiler-Options
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3") # optimize
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g") # debug symbols
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D _GLIBCXX_DEBUG -D _GLIBXX_DEBUG_PEDANTIC") # out of bounds asserts
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined") # sanitize all the things
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG") # disable asserts

# Executables
add_executable(louvain src/main.cpp)
add_executable(evaluate_partitioning src/evaluate_partitioning.cpp)
