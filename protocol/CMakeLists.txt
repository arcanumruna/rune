cmake_minimum_required(VERSION 3.31)
project(rune_protocol)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(aeron)

add_library(rune_protocol INTERFACE)
target_include_directories(rune_protocol INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# CLion helper (see common): a STATIC target so the IDE indexes headers.
add_library(rune_protocol_dummy STATIC src/dummy.cpp)
target_include_directories(rune_protocol_dummy PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
    find_package(GTest REQUIRED)
    add_executable(rune_protocol_tests
            tests/protocol/SmokeTest.cpp
            tests/protocol/Crc32cTest.cpp)
    target_include_directories(rune_protocol_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(rune_protocol_tests PRIVATE GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_protocol_tests)
endif()
