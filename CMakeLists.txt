cmake_minimum_required(VERSION 3.20)
project(workstealer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(workstealer INTERFACE)
target_include_directories(workstealer INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(MSVC)
  target_compile_options(workstealer INTERFACE /W4 /permissive-)
else()
  target_compile_options(workstealer INTERFACE -Wall -Wextra -Wpedantic)
endif()

find_package(Threads REQUIRED)

add_executable(latency_bench bench/latency_bench.cpp)
target_link_libraries(latency_bench PRIVATE workstealer Threads::Threads)

add_executable(parallel_for_demo examples/parallel_for_demo.cpp)
target_link_libraries(parallel_for_demo PRIVATE workstealer Threads::Threads)
