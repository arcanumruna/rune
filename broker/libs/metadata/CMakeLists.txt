# rune_metadata — header-only in-memory assignment model (PartitionAssignment, mergeChange).
# Consumed by: MSM apply, balancer, replication bridge, routing image.

add_library(rune_metadata INTERFACE)
target_include_directories(rune_metadata INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_partition_assignment_tests
        tests/PartitionAssignmentTest.cpp)
    target_link_libraries(rune_partition_assignment_tests PRIVATE
        rune_metadata
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_partition_assignment_tests)
endif()
