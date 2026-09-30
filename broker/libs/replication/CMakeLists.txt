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

# Task 4 (de-virtualised replica transport): the per-worker transport is
# header-only Aeron code the replica calls concretely.
if(aeron_FOUND)
    target_link_libraries(rune_replication PUBLIC aeron::aeron spdlog::spdlog)
    target_compile_definitions(rune_replication PUBLIC RUNE_REPLICATION_HAS_AERON=1)
endif()

# ---------------------------------------------------------------------------
# AeronPartitionTransport — compiled separately only when aeron is available.
# Consumers that want the real transport link rune_replication_aeron.
# The base rune_replication library remains Aeron-free (pure + InMemoryTransport).
# ---------------------------------------------------------------------------
if(aeron_FOUND)
    add_library(rune_replication_aeron
        src/AeronPartitionTransport.cpp)
    target_include_directories(rune_replication_aeron PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_NODE_INCLUDE})
    target_link_libraries(rune_replication_aeron PUBLIC
        rune_replication
        rune_consensus
        rune_metadata
        aeron::aeron)
    # spdlog PUBLIC because AeronPartitionTransport.h includes WorkerPool.h→Worker.h
    # which uses spdlog inline methods; consumers must see spdlog headers.
    target_link_libraries(rune_replication_aeron PUBLIC spdlog::spdlog)
    # rune_codec PUBLIC: Worker.h (included transitively via WorkerPool.h) now
    # includes rune/codec/Validator.h + rune/codec/Serialize.h for the Task 7
    # per-worker schema validation hook.  Any consumer of rune_replication_aeron
    # that includes Worker.h (directly or transitively) needs rune_codec headers.
    target_link_libraries(rune_replication_aeron PUBLIC rune_codec)
endif()

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
        tests/replication/FollowerSlotsTest.cpp
        tests/replication/NoDupFetchGateTest.cpp
        tests/replication/PartitionRecorderTest.cpp
        tests/replication/PartitionReplayTest.cpp
        tests/replication/PartitionFailoverTest.cpp
        tests/replication/AeronPartitionTransportTest.cpp
        tests/replication/DurabilityQuorumTest.cpp
        tests/replication/LeaderEpochCacheRestartTest.cpp)
    target_link_libraries(rune_replication_tests PRIVATE rune_replication GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_replication_tests)

    # ---------------------------------------------------------------------------
    # FollowerSync state-machine unit tests — STANDALONE.
    # FollowerSync.h is pure logic (no Aeron / storage / I/O), so this target
    # links ONLY the header include dir + gtest and builds on ANY platform
    # (including macOS where aeron is absent and rune_replication cannot link).
    # ---------------------------------------------------------------------------
    add_executable(rune_follower_sync_tests
        tests/replication/FollowerSyncTest.cpp)
    target_include_directories(rune_follower_sync_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(rune_follower_sync_tests PRIVATE GTest::gtest_main)
    gtest_discover_tests(rune_follower_sync_tests)

    # ---------------------------------------------------------------------------
    # Task 11: PSM re-rebuild on low-truncation tests (restart-durability trio).
    # ---------------------------------------------------------------------------
    add_executable(rune_psm_rerebuild_tests
        tests/replication/PsmReRebuildOnLowTruncationTest.cpp)
    target_link_libraries(rune_psm_rerebuild_tests PRIVATE rune_replication GTest::gtest_main)
    gtest_discover_tests(rune_psm_rerebuild_tests)

    # ---------------------------------------------------------------------------
    # AeronPartitionTransport in-process integration test (GR-1).
    # Gated behind BUILD_AERON_REPLICATION_TESTS (default ON when aeron is found).
    # Requires the embedded media driver (same as BUILD_BROKER_NODE_TESTS).
    # ---------------------------------------------------------------------------
    option(BUILD_AERON_REPLICATION_TESTS
        "Build AeronPartitionTransport in-process Aeron integration tests (GR-1)" ON)
    if(BUILD_AERON_REPLICATION_TESTS AND aeron_FOUND)
        add_executable(rune_replication_aeron_tests
            tests/integration/AeronPartitionTransportIntegrationTest.cpp)
        target_link_libraries(rune_replication_aeron_tests PRIVATE
            rune_replication_aeron
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_replication_aeron_tests
            PROPERTIES TIMEOUT 60)
    elseif(BUILD_AERON_REPLICATION_TESTS AND NOT aeron_FOUND)
        message(WARNING
            "BUILD_AERON_REPLICATION_TESTS=ON requires aeron; disabling rune_replication_aeron_tests")
    endif()
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
