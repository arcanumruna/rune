add_library(rune_node
    src/NodeConfig.cpp
    src/BrokerNode.cpp)

target_include_directories(rune_node PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# BrokerNode needs: consensus engine, gateway (BR-7a), replication (GR-2/GR-3), Aeron, spdlog
target_link_libraries(rune_node PUBLIC rune_consensus rune_gateway)
if(aeron_FOUND)
    target_link_libraries(rune_node PUBLIC aeron::aeron rune_replication_aeron)
    # Aeron's embedded media driver calls uuid_generate() from libuuid on Linux;
    # macOS provides it in system libs and the ConanCenter aeron recipe doesn't
    # propagate it. Link it PUBLIC so every driver-embedding consumer (rune_broker
    # and the BrokerNode/UDP-cluster test executables) picks it up. --no-as-needed
    # keeps libuuid regardless of link order relative to aeron's static archive.
    if(UNIX AND NOT APPLE)
        target_link_libraries(rune_node PUBLIC
            -Wl,--no-as-needed uuid -Wl,--as-needed)
    endif()
endif()
target_link_libraries(rune_node PRIVATE spdlog::spdlog)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    # Unit tests (NodeConfig only — no Aeron driver)
    add_executable(rune_node_tests
        tests/node/NodeConfigTest.cpp)
    target_link_libraries(rune_node_tests PRIVATE
        rune_node
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_node_tests)

    # ---------------------------------------------------------------------------
    # BrokerNode integration tests — embed the Aeron media driver.
    # Gated behind BUILD_BROKER_NODE_TESTS (default ON when BUILD_TESTING=ON)
    # so CI can disable if the embedded driver cannot start in the environment.
    # ---------------------------------------------------------------------------
    option(BUILD_BROKER_NODE_TESTS
        "Build BrokerNode integration tests (embedded Aeron media driver)" ON)
    if(BUILD_BROKER_NODE_TESTS)
        if(NOT aeron_FOUND)
            message(WARNING
                "BUILD_BROKER_NODE_TESTS=ON requires aeron; disabling BrokerNodeTest")
        else()
            add_executable(rune_broker_node_tests
                tests/node/BrokerNodeTest.cpp)
            target_link_libraries(rune_broker_node_tests PRIVATE
                rune_node
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_broker_node_tests
                PROPERTIES TIMEOUT 60)
        endif()
    endif()

    # ---------------------------------------------------------------------------
    # UDP cluster election integration test (BR-3) — 3 nodes over aeron:udp.
    # Gated behind BUILD_NODE_UDP_TESTS (default ON when BUILD_BROKER_NODE_TESTS=ON).
    # Set to OFF if 3 in-process embedded drivers prove infeasible in the environment.
    # ---------------------------------------------------------------------------
    option(BUILD_NODE_UDP_TESTS
        "Build 3-node UDP cluster election test (3 embedded Aeron drivers in-process)" ON)
    if(BUILD_NODE_UDP_TESTS)
        if(NOT aeron_FOUND)
            message(WARNING
                "BUILD_NODE_UDP_TESTS=ON requires aeron; disabling UdpClusterElectionTest")
        else()
            add_executable(rune_udp_cluster_tests
                tests/integration/UdpClusterElectionTest.cpp)
            target_link_libraries(rune_udp_cluster_tests PRIVATE
                rune_node
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_udp_cluster_tests
                PROPERTIES TIMEOUT 120)
        endif()
    endif()

    # ---------------------------------------------------------------------------
    # BrokerNode+Gateway integration tests (BR-7a) — single-node with live gateway.
    # Gated behind BUILD_BROKER_NODE_TESTS (reuses that gate).
    # ---------------------------------------------------------------------------
    if(BUILD_BROKER_NODE_TESTS AND aeron_FOUND)
        add_executable(rune_broker_node_gateway_tests
            tests/node/BrokerNodeGatewayTest.cpp)
        target_link_libraries(rune_broker_node_gateway_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_broker_node_gateway_tests
            PROPERTIES TIMEOUT 120)
    endif()

    # ---------------------------------------------------------------------------
    # Gateway↔Replication wiring integration tests (GR-2/GR-3) — 3-node in-process
    # over real aeron:udp with replication ports.
    # Gated behind BUILD_NODE_UDP_TESTS (same env requirement as UDP cluster tests).
    # ---------------------------------------------------------------------------
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_gateway_replication_wiring_tests
            tests/integration/GatewayReplicationWiringTest.cpp)
        target_link_libraries(rune_gateway_replication_wiring_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_replication_wiring_tests
            PROPERTIES TIMEOUT 180)
    endif()

    # ---------------------------------------------------------------------------
    # GR-4: Gateway produce → leader PartitionReplica (Mode A onLeaderLogAppended).
    # Unit + integration tests (3-node in-process, real aeron:udp, no Docker).
    # Gated behind BUILD_NODE_UDP_TESTS (same env requirement as GR-3 tests).
    # ---------------------------------------------------------------------------
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_gateway_produce_replication_tests
            tests/integration/GatewayProduceReplicationTest.cpp)
        target_link_libraries(rune_gateway_produce_replication_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_produce_replication_tests
            PROPERTIES TIMEOUT 180)
    endif()

    # ---------------------------------------------------------------------------
    # GR-5: acks=All deferred-ack via non-blocking pending-ack queue + min-ISR gate.
    # Unit + integration tests (3-node in-process, real aeron:udp, no Docker).
    # Gated behind BUILD_NODE_UDP_TESTS (same env requirement as GR-3/GR-4 tests).
    # ---------------------------------------------------------------------------
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_gateway_acks_all_deferred_ack_tests
            tests/integration/GatewayAcksAllDeferredAckTest.cpp)
        target_link_libraries(rune_gateway_acks_all_deferred_ack_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_acks_all_deferred_ack_tests
            PROPERTIES TIMEOUT 240)
    endif()
endif()
