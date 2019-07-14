cmake_minimum_required(VERSION 3.14)
project(tests)

set(CMAKE_CXX_STANDARD 17)

add_executable(parallel_prefix lib/ff_bsp.hpp tests/parallel_prefix.cpp)

include_directories(lib)
target_link_libraries(parallel_prefix pthread)
include_directories(/usr/local/lib/ff2)