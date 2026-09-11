add_library(rune_replication
    src/PartitionReplica.cpp)
target_include_directories(rune_replication PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_PROTOCOL_INCLUDE})
# EpochFencing.h includes LeaderEpochCache.h from rune_storage; link it so
# consumers of rune_replication also get rune_storage headers transitively.
target_link_libraries(rune_replication PUBLIC rune_storage)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_replication_tests
        tests/replication/ReplicationMessagesTest.cpp
        tests/replication/HighWatermarkTest.cpp
        tests/replication/IsrManagerTest.cpp
        tests/replication/EpochFencingTest.cpp
        tests/replication/PartitionReplicaTest.cpp)
    target_link_libraries(rune_replication_tests PRIVATE rune_replication GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_replication_tests)
endif()
