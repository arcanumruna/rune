cmake_minimum_required(VERSION 3.25)
project(rune_codec CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(GTest REQUIRED)
add_library(rune_codec STATIC
    src/Serialize.cpp
    src/Record.cpp
    src/Validator.cpp
    src/Generator.cpp
    src/Evolve.cpp
    src/AlterOps.cpp)
target_include_directories(rune_codec PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

# ── rune_catalog — the shared catalog-history library (spec §6): event model,
#    frozen per-formatVer apply rules, fingerprints, replayer, queries, .rcat.
#    Linked by the broker (AdminWorker), the client and the rune CLI.
set(RUNE_PROTOCOL_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include" CACHE PATH
    "rune-protocol include dir (rune/protocol/CatalogTypes.h: the one TopicConfig + RequestId)")
add_library(rune_catalog STATIC
    src/catalog/CatalogEvent.cpp)
target_include_directories(rune_catalog PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_catalog PUBLIC rune_codec)
target_compile_features(rune_catalog PUBLIC cxx_std_23)

# ── CLI tool: rune_codec_gen ─────────────────────────────────────────────────
add_executable(rune_codec_gen tools/rune_codec_gen_main.cpp)
target_link_libraries(rune_codec_gen PRIVATE rune_codec)

# ── Helper: make_characters_blob (generates tests/generated/characters.blob) ─
add_executable(make_characters_blob tools/make_characters_blob.cpp)
target_link_libraries(make_characters_blob PRIVATE rune_codec)

# ── Helper: make_fixedstr_sample_blob (generates tests/generated/fixedstr_sample.blob) ─
add_executable(make_fixedstr_sample_blob tools/make_fixedstr_sample_blob.cpp)
target_link_libraries(make_fixedstr_sample_blob PRIVATE rune_codec)

# ── Helper: make_str_keyed_blob (generates tests/generated/str_keyed.blob) ──
# Phase 2b Part C: a string-KEYED schema so the generated key() byte-span
# accessor is compiled and exercised (tests/GeneratedKeyTest.cpp).
add_executable(make_str_keyed_blob tools/make_str_keyed_blob.cpp)
target_link_libraries(make_str_keyed_blob PRIVATE rune_codec)

# ── Custom command: generate characters.gen.h from characters.blob ───────────
set(CHARACTERS_BLOB "${CMAKE_CURRENT_SOURCE_DIR}/tests/generated/characters.blob")
set(CHARACTERS_GEN_H "${CMAKE_CURRENT_BINARY_DIR}/tests/generated/characters.gen.h")

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tests/generated")

add_custom_command(
    OUTPUT  "${CHARACTERS_GEN_H}"
    COMMAND rune_codec_gen "${CHARACTERS_BLOB}" "${CHARACTERS_GEN_H}"
    DEPENDS rune_codec_gen "${CHARACTERS_BLOB}"
    COMMENT "Generating characters.gen.h from characters.blob"
)
add_custom_target(generate_characters_header DEPENDS "${CHARACTERS_GEN_H}")

# ── Custom command: generate fixedstr_sample.gen.h from fixedstr_sample.blob ─
set(FIXEDSTR_SAMPLE_BLOB "${CMAKE_CURRENT_SOURCE_DIR}/tests/generated/fixedstr_sample.blob")
set(FIXEDSTR_SAMPLE_GEN_H "${CMAKE_CURRENT_BINARY_DIR}/tests/generated/fixedstr_sample.gen.h")

add_custom_command(
    OUTPUT  "${FIXEDSTR_SAMPLE_GEN_H}"
    COMMAND rune_codec_gen "${FIXEDSTR_SAMPLE_BLOB}" "${FIXEDSTR_SAMPLE_GEN_H}"
    DEPENDS rune_codec_gen "${FIXEDSTR_SAMPLE_BLOB}"
    COMMENT "Generating fixedstr_sample.gen.h from fixedstr_sample.blob"
)
add_custom_target(generate_fixedstr_sample_header DEPENDS "${FIXEDSTR_SAMPLE_GEN_H}")

# ── Custom command: generate str_keyed.gen.h from str_keyed.blob (Phase 2b C) ─
set(STR_KEYED_BLOB "${CMAKE_CURRENT_SOURCE_DIR}/tests/generated/str_keyed.blob")
set(STR_KEYED_GEN_H "${CMAKE_CURRENT_BINARY_DIR}/tests/generated/str_keyed.gen.h")

add_custom_command(
    OUTPUT  "${STR_KEYED_GEN_H}"
    COMMAND rune_codec_gen "${STR_KEYED_BLOB}" "${STR_KEYED_GEN_H}"
    DEPENDS rune_codec_gen "${STR_KEYED_BLOB}"
    COMMENT "Generating str_keyed.gen.h from str_keyed.blob"
)
add_custom_target(generate_str_keyed_header DEPENDS "${STR_KEYED_GEN_H}")

enable_testing()
add_executable(rune_codec_tests
    tests/SmokeTest.cpp
    tests/PrimTypeTest.cpp
    tests/DescriptorTest.cpp
    tests/FingerprintTest.cpp
    tests/SerializeTest.cpp
    tests/ToTextTest.cpp
    tests/RecordReaderTest.cpp
    tests/ValidatorTest.cpp
    tests/GeneratorTest.cpp
    tests/GeneratedRoundTripTest.cpp
    tests/FixedStrLayoutTest.cpp
    tests/FixedStrSerializeTest.cpp
    tests/FixedStrReadValidateTest.cpp
    tests/FixedStrGeneratedRoundTripTest.cpp
    tests/EvolveTest.cpp
    tests/AlterOpsTest.cpp
    tests/MixedVersionReadTest.cpp
    tests/DecodeBoundaryMatrixTest.cpp
    tests/GeneratedKeyTest.cpp)
target_include_directories(rune_codec_tests PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/tests")
target_link_libraries(rune_codec_tests PRIVATE rune_codec GTest::gtest_main)
add_dependencies(rune_codec_tests generate_characters_header generate_fixedstr_sample_header
                 generate_str_keyed_header)
include(GoogleTest)
gtest_discover_tests(rune_codec_tests)

add_executable(rune_catalog_tests
    tests/catalog/CatalogEventTest.cpp)
target_link_libraries(rune_catalog_tests PRIVATE rune_catalog GTest::gtest_main)
gtest_discover_tests(rune_catalog_tests)

find_package(benchmark QUIET)
if(benchmark_FOUND)
    add_executable(rune_codec_bench benchmark/ValidatorBenchmark.cpp)
    target_link_libraries(rune_codec_bench PRIVATE rune_codec benchmark::benchmark)
endif()
