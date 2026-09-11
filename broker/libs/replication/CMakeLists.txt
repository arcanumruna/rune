add_library(rune_replication
    src/PartitionReplica.cpp
    src/FileRecordingStore.cpp
    src/PartitionRecorder.cpp
    src/PartitionReplay.cpp)
target_include_directories(rune_replication PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_PROTOCOL_INCLUDE})
# EpochFencing.h includes LeaderEpochCache.h from rune_storage; link it so
# consumers of rune_replication also get rune_storage headers transitively.
target_link_libraries(rune_replication PUBLIC rune_storage)

option(BUILD_REPLICATION_INTEGRATION_TESTING
    "Build replication cluster disaster/recovery integration tests (P3-6)" OFF)
if(BUILD_REPLICATION_INTEGRATION_TESTING)
    if(NOT BUILD_TESTING)
        find_package(GTest REQUIRED)
        include(GoogleTest)
    endif()
    set(REPLICATION_INTEG_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration)
    add_executable(rune_replication_integration_tests
        ${REPLICATION_INTEG_DIR}/ClusterDisasterRecoveryIntegrationTest.cpp)
    target_include_directories(rune_replication_integration_tests PRIVATE
        ${REPLICATION_INTEG_DIR})
    target_link_libraries(rune_replication_integration_tests PRIVATE
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_replication_integration_tests
        PROPERTIES TIMEOUT 60)
endif()

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_replication_tests
        tests/replication/ReplicationMessagesTest.cpp
        tests/replication/HighWatermarkTest.cpp
        tests/replication/IsrManagerTest.cpp
        tests/replication/EpochFencingTest.cpp
        tests/replication/PartitionReplicaTest.cpp
        tests/replication/PartitionRecorderTest.cpp
        tests/replication/PartitionReplayTest.cpp
        tests/replication/PartitionFailoverTest.cpp)
    target_link_libraries(rune_replication_tests PRIVATE rune_replication GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_replication_tests)
endif()

# ---------------------------------------------------------------------------
# Aeron Archive tests (P3-4) — gated behind BUILD_AERON_ARCHIVE_TESTS.
# OFF by default: the Aeron ArchivingMediaDriver is a Java process and
# requires a JVM + aeron-all JAR to be present at runtime.
# See docs/specs/2026-09-11-rune-archive-feasibility.md for details.
#
# Build+run (when Java is available):
#   cmake -DBUILD_AERON_ARCHIVE_TESTS=ON ..
#   cmake --build . -j2
#   ctest -R AeronArchive --timeout 60
# ---------------------------------------------------------------------------
option(BUILD_AERON_ARCHIVE_TESTS
    "Build real-Aeron Archive integration tests (requires Java + aeron-all JAR)" OFF)
if(BUILD_AERON_ARCHIVE_TESTS)
    if(NOT aeron_FOUND)
        message(FATAL_ERROR "BUILD_AERON_ARCHIVE_TESTS requires the aeron conan package")
    endif()
    if(NOT BUILD_TESTING AND NOT BUILD_INTEGRATION_TESTING)
        find_package(GTest REQUIRED)
        include(GoogleTest)
    endif()
    set(ARCHIVE_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests/archive)
    add_executable(rune_aeron_archive_tests
        ${ARCHIVE_TEST_DIR}/AeronArchiveRecordReplayTest.cpp)
    target_include_directories(rune_aeron_archive_tests PRIVATE
        ${ARCHIVE_TEST_DIR})
    target_link_libraries(rune_aeron_archive_tests PRIVATE
        rune_replication
        aeron::aeron
        GTest::gtest_main)
    gtest_discover_tests(rune_aeron_archive_tests
        PROPERTIES TIMEOUT 60)
endif()
