add_library(rune_coordination
    src/OffsetStore.cpp
    src/GroupCoordinator.cpp
    src/Assignor.cpp
    src/ReplicatedOffsetStore.cpp)
target_include_directories(rune_coordination PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${RUNE_PROTOCOL_INCLUDE})
# rune_coordination uses OffsetStore (filesystem) and GroupCoordinator which
# depend on rune_storage types (Log, LogCleaner, OffsetMap) used in integration
# tests.  Link rune_storage so the integration target gets its headers too.
# CG-2: ReplicatedOffsetStore depends on rune_replication (PartitionReplica).
target_link_libraries(rune_coordination PUBLIC rune_storage rune_replication)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_coordination_tests
        tests/coordination/OffsetStoreTest.cpp
        tests/coordination/OffsetMessagesTest.cpp
        tests/coordination/GroupCoordinatorTest.cpp
        tests/coordination/AssignorTest.cpp
        tests/coordination/RebalanceTest.cpp
        tests/coordination/ReplicatedOffsetStoreTest.cpp)
    target_link_libraries(rune_coordination_tests PRIVATE
        rune_coordination
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_coordination_tests)
endif()

# ---------------------------------------------------------------------------
# Consumer-group + compaction end-to-end integration tests (P4-5).
# Gated behind BUILD_COORDINATION_INTEGRATION_TESTING (OFF by default) to
# mirror the replication integration target pattern.
# ---------------------------------------------------------------------------
option(BUILD_COORDINATION_INTEGRATION_TESTING
    "Build consumer-group + compaction end-to-end integration tests (P4-5)" OFF)
if(BUILD_COORDINATION_INTEGRATION_TESTING)
    if(NOT BUILD_TESTING)
        find_package(GTest REQUIRED)
        include(GoogleTest)
    endif()
    set(COORD_INTEG_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration)
    add_executable(rune_coordination_integration_tests
        ${COORD_INTEG_DIR}/ConsumerGroupIntegrationTest.cpp
        ${COORD_INTEG_DIR}/CompactionIntegrationTest.cpp)
    target_include_directories(rune_coordination_integration_tests PRIVATE
        ${COORD_INTEG_DIR}
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_coordination_integration_tests PRIVATE
        rune_coordination
        GTest::gtest_main)
    gtest_discover_tests(rune_coordination_integration_tests
        PROPERTIES TIMEOUT 60)
endif()
