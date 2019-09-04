cmake_minimum_required(VERSION 3.14)
project(tests)

set(CMAKE_CXX_STANDARD 17)

add_executable(simple_parallel_prefix lib/ff_bsp.hpp tests/parallel_prefix_simple.cpp)
add_executable(superstep_loop lib/ff_bsp.hpp tests/superstep_loop.cpp)
add_executable(shrinking_ds lib/ff_bsp.hpp tests/shrinking_ds.cpp)
add_executable(parallel_prefix lib/ff_bsp.hpp lib/bsp_pprefix.h tests/prefix_sum.cpp)
add_executable(bench bspedupack/bspbench.cpp lib/ff_bsp.hpp)
add_executable(aggregate tests/aggregate.cpp lib/ff_bsp.hpp)
add_executable(provah tests/provah.cpp)

include_directories(lib)
link_libraries(pthread)
include_directories(/usr/local/lib/ff)