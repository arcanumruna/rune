add_library(rune_storage src/dummy.cpp src/MappedFile.cpp src/OffsetIndex.cpp src/LogSegment.cpp src/Log.cpp src/PartitionApi.cpp src/OffsetMap.cpp src/LogCleaner.cpp src/ProducerStateManager.cpp src/InMemoryLog.cpp
    # E5.1.2d: ProducePath moved from rune_gateway to rune_storage (leaf lib) to
    # eliminate the rune_node ↔ rune_gateway circular-dep workaround. Both
    # rune_gateway and rune_node depend on rune_storage, so this is a true leaf
    # location callable from both without any circular dependency.
    src/ProducePath.cpp)
target_include_directories(rune_storage PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_PROTOCOL_INCLUDE})
# ProducePath.cpp uses spdlog (warn/error/debug logging).
find_package(spdlog QUIET)
if(spdlog_FOUND)
    target_link_libraries(rune_storage PUBLIC spdlog::spdlog)
endif()

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_storage_tests tests/storage/SmokeTest.cpp tests/storage/MappedFileTest.cpp tests/storage/OffsetIndexTest.cpp tests/storage/LogSegmentTest.cpp tests/storage/LogTest.cpp tests/storage/RetentionTest.cpp tests/storage/LeaderEpochCacheTest.cpp tests/storage/LeaderEpochCachePersistTest.cpp tests/storage/ProducerStateManagerTest.cpp tests/storage/PartitionApiTest.cpp tests/storage/OffsetMapTest.cpp tests/storage/LogCleanerTest.cpp tests/storage/SkimpyOffsetMapTest.cpp tests/storage/LogReadViewTest.cpp tests/storage/ThrottlerTest.cpp tests/storage/CleanerOffsetCheckpointTest.cpp tests/storage/ProducerIdScanTest.cpp tests/storage/RecoveryPointTest.cpp tests/storage/SegmentLifecycleHotpathTest.cpp)
    add_executable(rune_in_memory_log_tests tests/storage/InMemoryLogTest.cpp
        tests/storage/InMemoryLogReadViewTest.cpp)
    target_link_libraries(rune_in_memory_log_tests PRIVATE rune_storage GTest::gtest_main)
    gtest_discover_tests(rune_in_memory_log_tests)
    add_executable(rune_in_memory_log_ring_tests tests/storage/InMemoryLogRingTest.cpp)
    target_link_libraries(rune_in_memory_log_ring_tests PRIVATE rune_storage GTest::gtest_main)
    gtest_discover_tests(rune_in_memory_log_ring_tests)
    # D3b r2: the cleaner tests write the PRODUCTION txn marker bytes
    # (rune::transaction::ControlMarker::build) — a cross-library contract.
    target_link_libraries(rune_storage_tests PRIVATE rune_storage rune_transaction GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_storage_tests)
endif()

option(BUILD_INTEGRATION_TESTING "Build storage integration tests (no external deps — real filesystem only)" ON)
if(BUILD_INTEGRATION_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_storage_integration_tests
        tests/integration/EndToEndProduceConsumeIntegrationTest.cpp
        tests/integration/RestartRecoveryIntegrationTest.cpp
        tests/integration/CrashTornTailIntegrationTest.cpp
        tests/integration/RetentionIntegrationTest.cpp
        tests/integration/RollingStressIntegrationTest.cpp
        tests/integration/MixedWorkloadIntegrationTest.cpp)
    target_include_directories(rune_storage_integration_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_storage_integration_tests PRIVATE rune_storage GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_storage_integration_tests)
endif()
