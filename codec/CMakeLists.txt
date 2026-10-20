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
set(RUNE_PROTOCOL_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../protocol/include" CACHE PATH
    "rune-protocol include dir (rune/protocol/CatalogTypes.h: the one TopicConfig + RequestId)")
add_library(rune_catalog STATIC
    src/catalog/CatalogEvent.cpp
    src/catalog/CatalogState.cpp
    src/catalog/CatalogFingerprint.cpp
    src/catalog/ApplyV1.cpp
    src/catalog/CatalogReplayer.cpp
    src/catalog/CatalogQueries.cpp
    src/catalog/CatalogFile.cpp
    src/catalog/RecordFile.cpp)
target_include_directories(rune_catalog PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_catalog PUBLIC rune_codec)
target_compile_features(rune_catalog PUBLIC cxx_std_23)

# ── rune_security — the shared authentication library (auth spec §3): event model,
#    frozen per-formatVer apply rules, fingerprints, permissions, replayer, .rsec.
#    Linked by the broker, the client and the rune CLI exactly like rune_catalog.
file(GLOB RUNE_SECURITY_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/security/*.cpp)
add_library(rune_security STATIC ${RUNE_SECURITY_SOURCES})
target_include_directories(rune_security PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(rune_security PUBLIC rune_catalog)
target_compile_features(rune_security PUBLIC cxx_std_23)

# ── Helper: make_catalog_golden (writes tests/catalog/golden/v<N>/basic.rcat + prints the
#    per-offset digests for tests/catalog/CatalogGoldenPins.h).  Run ONCE per formatVer.
add_executable(make_catalog_golden tools/make_catalog_golden.cpp)
target_link_libraries(make_catalog_golden PRIVATE rune_catalog)

# ── Helper: make_security_golden (writes tests/security/golden/v<N>/basic.rsec + prints the
#    per-offset digests for tests/security/SecurityGoldenPins.h).  Run ONCE per formatVer.
add_executable(make_security_golden tools/make_security_golden.cpp)
target_link_libraries(make_security_golden PRIVATE rune_security)

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

# ── Phase 2b Part B: allfixed_sample flyweight fixture (built, not checked in) ─
add_executable(make_allfixed_sample_blob tools/make_allfixed_sample_blob.cpp)
target_link_libraries(make_allfixed_sample_blob PRIVATE rune_codec)

set(ALLFIXED_SAMPLE_BLOB  "${CMAKE_CURRENT_BINARY_DIR}/tests/generated/allfixed_sample.blob")
set(ALLFIXED_SAMPLE_GEN_H "${CMAKE_CURRENT_BINARY_DIR}/tests/generated/allfixed_sample.gen.h")

add_custom_command(
    OUTPUT  "${ALLFIXED_SAMPLE_BLOB}"
    COMMAND make_allfixed_sample_blob "${ALLFIXED_SAMPLE_BLOB}"
    DEPENDS make_allfixed_sample_blob
    COMMENT "Generating allfixed_sample.blob (2b-B flyweight fixture)"
)
add_custom_command(
    OUTPUT  "${ALLFIXED_SAMPLE_GEN_H}"
    COMMAND rune_codec_gen "${ALLFIXED_SAMPLE_BLOB}" "${ALLFIXED_SAMPLE_GEN_H}"
    DEPENDS rune_codec_gen "${ALLFIXED_SAMPLE_BLOB}"
    COMMENT "Generating allfixed_sample.gen.h from allfixed_sample.blob"
)
add_custom_target(generate_allfixed_sample_header DEPENDS "${ALLFIXED_SAMPLE_GEN_H}")

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
    tests/GeneratedKeyTest.cpp
    tests/FlyweightTest.cpp
    tests/FlyweightGeneratedRoundTripTest.cpp)
target_include_directories(rune_codec_tests PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/tests")
target_link_libraries(rune_codec_tests PRIVATE rune_codec GTest::gtest_main)
add_dependencies(rune_codec_tests generate_characters_header generate_fixedstr_sample_header
                 generate_str_keyed_header generate_allfixed_sample_header)
include(GoogleTest)
gtest_discover_tests(rune_codec_tests)

add_executable(rune_catalog_tests
    tests/catalog/CatalogEventTest.cpp
    tests/catalog/CatalogApplyV1Test.cpp
    tests/catalog/CatalogTopicNameTest.cpp
    tests/catalog/CatalogFingerprintTest.cpp
    tests/catalog/CatalogReplayerTest.cpp
    tests/catalog/CatalogQueriesTest.cpp
    tests/catalog/CatalogGoldenTest.cpp)
target_link_libraries(rune_catalog_tests PRIVATE rune_catalog GTest::gtest_main)
target_compile_definitions(rune_catalog_tests PRIVATE
    RUNE_CATALOG_GOLDEN_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/catalog/golden")
gtest_discover_tests(rune_catalog_tests)

file(GLOB RUNE_SECURITY_TEST_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/security/*Test.cpp)
add_executable(rune_security_tests ${RUNE_SECURITY_TEST_SOURCES})
target_link_libraries(rune_security_tests PRIVATE rune_security GTest::gtest_main)
target_compile_definitions(rune_security_tests PRIVATE
    RUNE_SECURITY_GOLDEN_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/security/golden"
    RUNE_CATALOG_GOLDEN_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/catalog/golden")
gtest_discover_tests(rune_security_tests)

find_package(benchmark QUIET)
if(benchmark_FOUND)
    message(STATUS "rune_codec_bench: Google Benchmark ${benchmark_VERSION} found")
    add_executable(rune_codec_bench benchmark/ValidatorBenchmark.cpp benchmark/CatalogReplayBench.cpp benchmark/SecurityReplayBench.cpp)
    target_link_libraries(rune_codec_bench PRIVATE rune_security benchmark::benchmark)   # rune_catalog + rune_codec come PUBLIC via rune_security
else()
    message(STATUS "rune_codec_bench: Google Benchmark NOT found — rune_codec_bench is not built in this build dir")
endif()
