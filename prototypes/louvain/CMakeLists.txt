cmake_minimum_required (VERSION 2.8)
project (Louvain)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src/")

# LIBS

# Compiler-Options
# -ggdb -D _GLIBCXX_DEBUG -DNDEBUG -O3
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb -D _GLIBCXX_DEBUG -std=c++14 -fopenmp ${FLAGS} -Wall -Wextra")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++14 -fopenmp ${FLAGS} -Wall -Wextra")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -std=c++14 -fopenmp ${FLAGS} -Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -DNDEBUG -std=c++14 -fopenmp ${FLAGS} -Wall -Wextra")

# MAIN
add_executable(louvain src/main.cpp)
