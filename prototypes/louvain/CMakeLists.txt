cmake_minimum_required (VERSION 2.8)
project (Louvain)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src/")

# LIBS

# Compiler-Options
# -ggdb -D _GLIBCXX_DEBUG -DNDEBUG
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -std=c++14 -fopenmp ${FLAGS} -Wall -Wextra")

# MAIN
add_executable(louvain src/main.cpp)
