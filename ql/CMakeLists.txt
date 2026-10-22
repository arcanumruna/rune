cmake_minimum_required(VERSION 3.25)
project(rune_ql CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── rune-codec sibling source lib ────────────────────────────────────────────
set(RUNE_CODEC_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../codec" CACHE PATH "Path to rune-codec source root")

add_library(rune_codec_dep STATIC
    ${RUNE_CODEC_ROOT}/src/Serialize.cpp
    ${RUNE_CODEC_ROOT}/src/Record.cpp
    ${RUNE_CODEC_ROOT}/src/Validator.cpp
    ${RUNE_CODEC_ROOT}/src/Generator.cpp
    ${RUNE_CODEC_ROOT}/src/AlterOps.cpp
    ${RUNE_CODEC_ROOT}/src/Evolve.cpp
    ${RUNE_CODEC_ROOT}/src/Lineage.cpp)
target_include_directories(rune_codec_dep PUBLIC ${RUNE_CODEC_ROOT}/include)

# ── rune_ql library ──────────────────────────────────────────────────────────
file(GLOB RUNE_QL_SOURCES CONFIGURE_DEPENDS src/*.cpp)
add_library(rune_ql STATIC ${RUNE_QL_SOURCES})
target_include_directories(rune_ql PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
# rune-ql includes only the std-only rune/protocol/CatalogTypes.h (F32).
set(RUNE_PROTOCOL_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../protocol/include" CACHE PATH "rune-protocol include dir")
target_include_directories(rune_ql PUBLIC ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_ql PUBLIC rune_codec_dep)

# ── Tests ────────────────────────────────────────────────────────────────────
if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    enable_testing()
    add_executable(rune_ql_tests
        tests/AstSmokeTest.cpp
        tests/ParserTest.cpp
        tests/BackendsTest.cpp
        tests/CatalogStatementsTest.cpp
        tests/ScriptTest.cpp
        tests/SecurityStatementsTest.cpp)
    target_link_libraries(rune_ql_tests PRIVATE rune_ql GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_ql_tests)
endif()

# ── Benchmarks ───────────────────────────────────────────────────────────────
find_package(benchmark QUIET)
if(benchmark_FOUND)
    add_executable(rune_ql_bench
        benchmark/QlBenchmark.cpp
        benchmark/ParseBench.cpp)
    target_link_libraries(rune_ql_bench PRIVATE rune_ql benchmark::benchmark)
endif()
