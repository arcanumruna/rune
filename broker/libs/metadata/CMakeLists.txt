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

# DynamicBalancer — delta rebalance (bit-packed reverse index, preferred-leader-in-ISR, co-partition unit move)
add_library(rune_dynamic_balancer STATIC
    src/DynamicBalancer.cpp)
target_include_directories(rune_dynamic_balancer PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_include_directories(rune_dynamic_balancer PRIVATE
    ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_dynamic_balancer PUBLIC
    rune_static_balancer
    rune_metadata
    rune_replication
    rune_consensus)
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

    add_executable(rune_routing_image_tests
        tests/RoutingImageTest.cpp)
    target_link_libraries(rune_routing_image_tests PRIVATE
        rune_metadata
        GTest::gtest_main)

    add_executable(rune_replica_position_table_tests
        tests/ReplicaPositionTableTest.cpp)
    target_link_libraries(rune_replica_position_table_tests PRIVATE
        rune_metadata
        rune_replication
        GTest::gtest_main)

    add_executable(rune_membership_state_tests
        tests/MembershipStateTest.cpp)
    target_link_libraries(rune_membership_state_tests PRIVATE
        rune_metadata
        GTest::gtest_main)

    add_executable(rune_control_plane_timer_tests
        tests/ControlPlaneTimerTest.cpp)
    target_link_libraries(rune_control_plane_timer_tests PRIVATE
        rune_metadata
        GTest::gtest_main)
    target_include_directories(rune_control_plane_timer_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})

    add_executable(rune_failover_election_tests
        tests/FailoverElectionTest.cpp)
    target_link_libraries(rune_failover_election_tests PRIVATE
        rune_metadata
        rune_replication
        rune_consensus
        GTest::gtest_main)

    add_executable(rune_dynamic_balancer_tests
        tests/DynamicBalancerTest.cpp)
    target_link_libraries(rune_dynamic_balancer_tests PRIVATE
        rune_dynamic_balancer
        GTest::gtest_main)
    target_include_directories(rune_dynamic_balancer_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})

    add_executable(rune_controlled_shutdown_tests
        tests/ControlledShutdownTest.cpp)
    target_link_libraries(rune_controlled_shutdown_tests PRIVATE
        rune_dynamic_balancer
        GTest::gtest_main)
    target_include_directories(rune_controlled_shutdown_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})

    add_executable(rune_shard_router_tests
        tests/ShardRouterTest.cpp)
    target_link_libraries(rune_shard_router_tests PRIVATE
        rune_metadata
        GTest::gtest_main)

    include(GoogleTest)
    gtest_discover_tests(rune_partition_assignment_tests PROPERTIES TIMEOUT 60)
    gtest_discover_tests(rune_metadata_state_machine_tests)
    gtest_discover_tests(rune_static_balancer_tests)
    gtest_discover_tests(rune_routing_image_tests)
    gtest_discover_tests(rune_replica_position_table_tests)
    gtest_discover_tests(rune_membership_state_tests)
    gtest_discover_tests(rune_control_plane_timer_tests)
    gtest_discover_tests(rune_failover_election_tests)
    gtest_discover_tests(rune_dynamic_balancer_tests)
    gtest_discover_tests(rune_controlled_shutdown_tests)
    gtest_discover_tests(rune_shard_router_tests)
endif()
