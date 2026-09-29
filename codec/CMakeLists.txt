cmake_minimum_required(VERSION 3.25)
project(rune_codec CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(GTest REQUIRED)
add_library(rune_codec STATIC src/Serialize.cpp)
target_include_directories(rune_codec PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
enable_testing()
add_executable(rune_codec_tests
    tests/SmokeTest.cpp
    tests/PrimTypeTest.cpp
    tests/DescriptorTest.cpp
    tests/FingerprintTest.cpp
    tests/SerializeTest.cpp)
target_link_libraries(rune_codec_tests PRIVATE rune_codec GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(rune_codec_tests)
