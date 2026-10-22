cmake_minimum_required(VERSION 3.15)
project(rune_client_cpp_test_package CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(rune-client-cpp REQUIRED CONFIG)

add_executable(consumer consumer.cpp)
target_link_libraries(consumer PRIVATE rune::client)
