# rune_metadata — in-memory assignment model (PartitionAssignment, mergeChange)
# + MetadataStateMachine apply / delta / snapshot.
# Consumed by: balancer, replication bridge, routing image.

# Header-only portion (PartitionAssignment, AssignmentMap, interfaces)
add_library(rune_metadata INTERFACE)
target_include_directories(rune_metadata INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# MetadataStateMachine — compiled library (consumes rune-protocol headers)
add_library(rune_metadata_msm STATIC
    src/MetadataStateMachine.cpp)

# StaticBalancer — compiled library (co-location-aware initial assignment)
add_library(rune_static_balancer STATIC
    src/StaticBalancer.cpp)
target_include_directories(rune_static_balancer PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_include_directories(rune_static_balancer PRIVATE
    ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_static_balancer PUBLIC rune_metadata)
target_include_directories(rune_metadata_msm PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_include_directories(rune_metadata_msm PRIVATE
    ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_metadata_msm PUBLIC rune_metadata)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    add_executable(rune_partition_assignment_tests
        tests/PartitionAssignmentTest.cpp)
    target_link_libraries(rune_partition_assignment_tests PRIVATE
        rune_metadata
        GTest::gtest_main)

    add_executable(rune_metadata_state_machine_tests
        tests/MetadataStateMachineTest.cpp)
    target_link_libraries(rune_metadata_state_machine_tests PRIVATE
        rune_metadata_msm
        GTest::gtest_main)
    target_include_directories(rune_metadata_state_machine_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})

    add_executable(rune_static_balancer_tests
        tests/StaticBalancerTest.cpp)
    target_link_libraries(rune_static_balancer_tests PRIVATE
        rune_static_balancer
        GTest::gtest_main)
    target_include_directories(rune_static_balancer_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})

    include(GoogleTest)
    gtest_discover_tests(rune_partition_assignment_tests)
    gtest_discover_tests(rune_metadata_state_machine_tests)
    gtest_discover_tests(rune_static_balancer_tests)
endif()
