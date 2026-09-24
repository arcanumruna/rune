cmake_minimum_required(VERSION 3.20)

# rune_gateway — ClientGateway + SessionTable (BR-4) + TopicRegistry (GB-1)
# E5.1.2d: ProducePath.cpp was moved to rune_storage (leaf lib) — it used to
# live here as a workaround for the rune_node ↔ rune_gateway circular dep.
# Now both rune_gateway and rune_node depend on rune_storage which holds the
# implementation, cleanly eliminating the circular-dep workaround.
add_library(rune_gateway
    src/ClientGateway.cpp
    src/TopicRegistry.cpp)

target_include_directories(rune_gateway PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# rune-protocol headers (sibling repo, header-only)
target_include_directories(rune_gateway PUBLIC
    ${RUNE_PROTOCOL_INCLUDE})

# TopicRegistry depends on rune_storage (Log).
# NodeConfig/TopicSpec and ReplicaRegistry headers are included via RUNE_NODE_INCLUDE
# (raw path) to avoid a circular target dependency (rune_node → PUBLIC rune_gateway → rune_node).
# rune_replication is linked PUBLIC so ReplicaRegistry.h (which includes PartitionReplica.h)
# compiles correctly and consumers of rune_gateway also get rune_replication headers.
target_link_libraries(rune_gateway PUBLIC rune_storage rune_replication rune_transaction rune_metadata rune_consensus)
target_include_directories(rune_gateway PUBLIC ${RUNE_NODE_INCLUDE})

if(aeron_FOUND)
    target_link_libraries(rune_gateway PUBLIC aeron::aeron)
endif()

target_link_libraries(rune_gateway PRIVATE spdlog::spdlog rune_coordination)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    # -------------------------------------------------------------------------
    # SessionTable unit tests (no Aeron, no driver)
    # -------------------------------------------------------------------------
    add_executable(rune_gateway_session_tests
        tests/gateway/SessionTableTest.cpp)
    target_link_libraries(rune_gateway_session_tests PRIVATE
        rune_gateway
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_gateway_session_tests)

    # -------------------------------------------------------------------------
    # TopicRegistry unit tests (GB-1) — no Aeron, real filesystem + rune_storage.
    # -------------------------------------------------------------------------
    add_executable(rune_gateway_topic_registry_tests
        tests/gateway/TopicRegistryTest.cpp)
    target_link_libraries(rune_gateway_topic_registry_tests PRIVATE
        rune_gateway
        GTest::gtest_main)
    gtest_discover_tests(rune_gateway_topic_registry_tests)

    # -------------------------------------------------------------------------
    # G0.1: ClientGateway egress retry queue unit tests — no Aeron, no driver.
    # Uses the test-only makeForTest() factory + FakePublication seam.
    # -------------------------------------------------------------------------
    add_executable(rune_gateway_egress_tests
        tests/gateway/ClientGatewayEgressTest.cpp)
    target_link_libraries(rune_gateway_egress_tests PRIVATE
        rune_gateway
        GTest::gtest_main)
    gtest_discover_tests(rune_gateway_egress_tests)

    # -------------------------------------------------------------------------
    # G1.1: ClientGateway hot-path regression guards — leaderView_ call count
    # and minIsr_ per-cycle snapshot.  No Aeron, no driver.
    # -------------------------------------------------------------------------
    add_executable(rune_gateway_hotpath_tests
        tests/gateway/ClientGatewayHotPathTest.cpp)
    target_link_libraries(rune_gateway_hotpath_tests PRIVATE
        rune_gateway
        GTest::gtest_main)
    gtest_discover_tests(rune_gateway_hotpath_tests)

    # -------------------------------------------------------------------------
    # E5.1: MetadataResponse ingressStreamId unit tests — no Aeron, no driver.
    # Verifies PartitionEntry.ingressStreamId = base + ShardRouter.workerForPartition.
    # -------------------------------------------------------------------------
    add_executable(rune_gateway_metadata_response_tests
        tests/gateway/MetadataResponseTest.cpp)
    target_link_libraries(rune_gateway_metadata_response_tests PRIVATE
        rune_gateway
        rune_node
        spdlog::spdlog
        GTest::gtest_main)
    gtest_discover_tests(rune_gateway_metadata_response_tests)

    # -------------------------------------------------------------------------
    # ClientGateway integration tests — embedded Aeron driver + ipc loopback.
    # Gated behind BUILD_GATEWAY_TESTS (default ON when BUILD_TESTING=ON).
    # -------------------------------------------------------------------------
    option(BUILD_GATEWAY_TESTS
        "Build ClientGateway ipc-loopback integration tests (embedded Aeron media driver)" ON)

    if(BUILD_GATEWAY_TESTS)
        if(NOT aeron_FOUND)
            message(WARNING
                "BUILD_GATEWAY_TESTS=ON requires aeron; disabling gateway integration tests")
        else()
            add_executable(rune_gateway_loopback_tests
                tests/integration/GatewayLoopbackTest.cpp)
            target_link_libraries(rune_gateway_loopback_tests PRIVATE
                rune_gateway
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_gateway_loopback_tests
                PROPERTIES TIMEOUT 90)

            # BR-5: session timeout reaping tests (injected clock)
            add_executable(rune_gateway_timeout_tests
                tests/integration/GatewayTimeoutTest.cpp)
            target_link_libraries(rune_gateway_timeout_tests PRIVATE
                rune_gateway
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_gateway_timeout_tests
                PROPERTIES TIMEOUT 90)

            # BR-5: leader-awareness REDIRECT tests
            add_executable(rune_gateway_redirect_tests
                tests/integration/GatewayRedirectTest.cpp)
            target_link_libraries(rune_gateway_redirect_tests PRIVATE
                rune_gateway
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_gateway_redirect_tests
                PROPERTIES TIMEOUT 90)

            # BR-7a fix: async pending-connect drop/timeout/eviction tests
            add_executable(rune_gateway_async_drop_tests
                tests/integration/GatewayAsyncDropTest.cpp)
            target_link_libraries(rune_gateway_async_drop_tests PRIVATE
                rune_gateway
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_gateway_async_drop_tests
                PROPERTIES TIMEOUT 120)

            # GB-2: Produce handler — leader-local append + offset assignment tests
            add_executable(rune_gateway_produce_tests
                tests/integration/GatewayProduceTest.cpp)
            target_link_libraries(rune_gateway_produce_tests PRIVATE
                rune_gateway
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_gateway_produce_tests
                PROPERTIES TIMEOUT 120)

            # GB-3: Subscribe handler — egress-paced push + back-pressure exactly-once tests
            add_executable(rune_gateway_subscribe_tests
                tests/integration/GatewaySubscribeTest.cpp)
            target_link_libraries(rune_gateway_subscribe_tests PRIVATE
                rune_gateway
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_gateway_subscribe_tests
                PROPERTIES TIMEOUT 180)

            # GB-4: Data-op REDIRECT to leader (Produce/Subscribe require leader)
            add_executable(rune_gateway_data_op_redirect_tests
                tests/integration/GatewayDataOpRedirectTest.cpp)
            target_link_libraries(rune_gateway_data_op_redirect_tests PRIVATE
                rune_gateway
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_gateway_data_op_redirect_tests
                PROPERTIES TIMEOUT 120)

            # TX-2: Idempotent-producer dedup integration tests.
            add_executable(rune_gateway_idempotent_tests
                tests/integration/ProduceIdempotentTest.cpp)
            target_link_libraries(rune_gateway_idempotent_tests PRIVATE
                rune_gateway
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_gateway_idempotent_tests
                PROPERTIES TIMEOUT 120)

            # TX-4 Task 9 + Step 6: read_committed LSO bound + aborted-txn filter +
            # control-marker skip in pushSubscriptions + group push path.
            add_executable(rune_gateway_read_committed_push_tests
                tests/integration/ReadCommittedPushTest.cpp)
            target_link_libraries(rune_gateway_read_committed_push_tests PRIVATE
                rune_gateway
                rune_transaction
                rune_replication
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_gateway_read_committed_push_tests
                PROPERTIES TIMEOUT 180)

            # MD1 Task 9: per-partition routing + partition-leader REDIRECT tests.
            add_executable(rune_gateway_per_partition_routing_tests
                tests/integration/GatewayPerPartitionRoutingTest.cpp)
            target_link_libraries(rune_gateway_per_partition_routing_tests PRIVATE
                rune_gateway
                rune_metadata
                rune_replication
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_gateway_per_partition_routing_tests
                PROPERTIES TIMEOUT 180)

            # MD1 Task 10: Metadata request/response integration tests.
            add_executable(rune_metadata_request_tests
                tests/integration/MetadataRequestTest.cpp)
            target_link_libraries(rune_metadata_request_tests PRIVATE
                rune_gateway
                rune_metadata
                rune_replication
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_metadata_request_tests
                PROPERTIES TIMEOUT 180)
        endif()
    endif()
endif()
