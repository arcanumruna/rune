add_library(rune_storage src/dummy.cpp src/MappedFile.cpp src/OffsetIndex.cpp src/LogSegment.cpp src/Log.cpp src/PartitionApi.cpp)
target_include_directories(rune_storage PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_PROTOCOL_INCLUDE})

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_storage_tests tests/storage/SmokeTest.cpp tests/storage/MappedFileTest.cpp tests/storage/OffsetIndexTest.cpp tests/storage/LogSegmentTest.cpp tests/storage/LogTest.cpp tests/storage/RetentionTest.cpp tests/storage/LeaderEpochCacheTest.cpp tests/storage/ProducerStateTest.cpp tests/storage/PartitionApiTest.cpp)
    target_link_libraries(rune_storage_tests PRIVATE rune_storage GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_storage_tests)
endif()

option(BUILD_INTEGRATION_TESTING "Build storage integration tests (no external deps — real filesystem only)" OFF)
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
