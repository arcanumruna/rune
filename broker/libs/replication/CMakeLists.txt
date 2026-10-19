add_library(rune_replication
    src/PartitionReplica.cpp
    src/InternalIsrKnowledge.cpp
    src/FileRecordingStore.cpp
    src/PartitionRecorder.cpp
    src/PartitionReplay.cpp)
target_include_directories(rune_replication PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_PROTOCOL_INCLUDE})
# EpochFencing.h includes LeaderEpochCache.h from rune_storage; link it so
# consumers of rune_replication also get rune_storage headers transitively.
target_link_libraries(rune_replication PUBLIC rune_storage)
target_link_libraries(rune_replication PUBLIC rune_peerlink)   # authentication plan Task 9

# Task 4 (de-virtualised replica transport): the per-worker transport is
# header-only Aeron code the replica calls concretely.
if(aeron_FOUND)
    target_link_libraries(rune_replication PUBLIC aeron::aeron spdlog::spdlog)
    target_link_libraries(rune_replication PUBLIC rune_aeron_util)
    target_compile_definitions(rune_replication PUBLIC RUNE_REPLICATION_HAS_AERON=1)
endif()

# ---------------------------------------------------------------------------
# AeronPartitionTransport — part of rune_replication when aeron is available
# (Task 11 fix 2).  rune_replication_aeron is kept as a consumer-facing INTERFACE
# name.  Layering note (review m-d): its .cpp includes node HEADERS (header-
# inline ReplicaRegistry/WorkerPool routing), a replication→node header
# inversion that used to be confined to rune_replication_aeron; the only node
# SYMBOLS it needs are the worker io backends (rune_io_backend, a leaf lib).
# ---------------------------------------------------------------------------
if(aeron_FOUND)
    # Task 11 fix 2 (B2): AeronPartitionTransport is compiled INTO rune_replication
    # so PartitionReplica can call it directly (final class, TransportKind::Aeron)
    # — internal partitions' sends on the runtime duty cycle are never virtual.
    # Its .cpp only needs node HEADERS (all header-inline; no node link deps).
    target_sources(rune_replication PRIVATE src/AeronPartitionTransport.cpp)
    target_include_directories(rune_replication PRIVATE ${RUNE_NODE_INCLUDE})
    # rune_view: Worker.h (reached via the node headers above) now includes
    # rune/node/WorkerViewSet.h (Task 15), pulling rune/view/* headers.  rune_view
    # is a leaf lib (depends only on rune_codec) so this adds no circular dep.
    target_link_libraries(rune_replication PUBLIC rune_consensus rune_metadata rune_codec rune_view)
    # Kept as the consumer-facing name (same usage requirements as before).
    add_library(rune_replication_aeron INTERFACE)
    target_include_directories(rune_replication_aeron INTERFACE ${RUNE_NODE_INCLUDE})
    target_link_libraries(rune_replication_aeron INTERFACE
        rune_replication rune_consensus rune_metadata aeron::aeron spdlog::spdlog rune_codec rune_view)
    # F15 S4 (merged with T11 fix 2 in integration wave 2): Worker.h (reached via
    # the node headers above) holds the worker-owned read_committed state
    # (LsoTracker + AbortedTxnIndex, rune/transaction/ReadCommittedView.h), and
    # the header-inline worker code the transport instantiates calls into
    # rune_transaction (e.g. LsoTracker::reserve).  F15 had this edge on the
    # separate rune_replication_aeron library; T11 folded the transport INTO
    # rune_replication, so the edge moves here.  It closes a STATIC-library
    # cycle (rune_transaction → rune_coordination → rune_replication), which
    # CMake resolves by repeating the archives on the link line.
    target_link_libraries(rune_replication PUBLIC rune_transaction)
    # ...and the worker io backends that header-inline worker code submits to
    # (UringFlushBackend on Linux) — a leaf library, no cycle (libs/node).
    target_link_libraries(rune_replication PUBLIC rune_io_backend)
endif()

option(BUILD_REPLICATION_INTEGRATION_TESTING
    "Build replication cluster disaster/recovery integration tests (P3-6)" ON)
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
        PROPERTIES TIMEOUT 60 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
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
        tests/replication/LeaderEpochCacheRestartTest.cpp
        tests/replication/BatchPrefixTest.cpp
        tests/replication/LargeBatchServeTest.cpp
        tests/replication/OutboundHoldRouterTest.cpp
        tests/replication/F27OwedMemoSwapTest.cpp
        tests/replication/DiagFlagsTest.cpp
        tests/replication/LeaderEpochFenceTest.cpp
        tests/replication/IsrProposalIdTest.cpp
        tests/replication/TransientReplicaTest.cpp)
    target_link_libraries(rune_replication_tests PRIVATE rune_replication GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_replication_tests)

    # Zero-allocation proof for the leader's push/fetch paths (spec §1a.5, §3.5a.2).
    # Own executable: it replaces global operator new to count allocations.
    add_executable(rune_replication_zero_alloc_tests
        tests/replication/ZeroAllocReplicaTest.cpp)
    target_link_libraries(rune_replication_zero_alloc_tests PRIVATE rune_replication GTest::gtest_main)
    gtest_discover_tests(rune_replication_zero_alloc_tests)

    # ---------------------------------------------------------------------------
    # E2 / DUR-1: in-process disaster-recovery integration suite (default run).
    # Deterministic (injected clock, no sleeps), real Log + PartitionReplica +
    # FailoverElection; storage crash seam + transport fault seams.
    # ---------------------------------------------------------------------------
    add_executable(rune_dr_integration_tests
        tests/dr/DrIntegrationTest.cpp)
    target_include_directories(rune_dr_integration_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/dr)
    target_link_libraries(rune_dr_integration_tests PRIVATE
        rune_replication rune_metadata rune_consensus GTest::gtest_main)
    gtest_discover_tests(rune_dr_integration_tests PROPERTIES TIMEOUT 120)

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
            PROPERTIES TIMEOUT 60 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
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
        PROPERTIES TIMEOUT 60 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
endif()
