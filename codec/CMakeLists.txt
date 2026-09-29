cmake_minimum_required(VERSION 3.25)
project(rune_codec CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(GTest REQUIRED)
add_library(rune_codec STATIC src/Serialize.cpp src/Record.cpp src/Validator.cpp)
target_include_directories(rune_codec PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
enable_testing()
add_executable(rune_codec_tests
    tests/SmokeTest.cpp
    tests/PrimTypeTest.cpp
    tests/DescriptorTest.cpp
    tests/FingerprintTest.cpp
    tests/SerializeTest.cpp
    tests/ToTextTest.cpp
    tests/RecordReaderTest.cpp
    tests/ValidatorTest.cpp)
target_link_libraries(rune_codec_tests PRIVATE rune_codec GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(rune_codec_tests)

find_package(benchmark QUIET)
if(benchmark_FOUND)
    add_executable(rune_codec_bench benchmark/ValidatorBenchmark.cpp)
    target_link_libraries(rune_codec_bench PRIVATE rune_codec benchmark::benchmark)
endif()
