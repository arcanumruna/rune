add_library(rune_node
    src/NodeConfig.cpp
    src/BrokerNode.cpp
    src/GroupCoordinationService.cpp
    src/TransactionCoordinationService.cpp)

target_include_directories(rune_node PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# BrokerNode needs: consensus engine, gateway (BR-7a), replication (GR-2/GR-3), Aeron, spdlog
# rune_coordination: GroupCoordinator + ReplicatedOffsetStore (CG-3)
# rune_transaction: TransactionCoordinator + ReplicatedTxnStateStore (TX-3)
target_link_libraries(rune_node PUBLIC rune_consensus rune_gateway rune_coordination rune_transaction)
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

    # RoleCounter wire-encoding unit tests (pure header — no Aeron driver).
    add_executable(rune_role_counter_tests
        tests/node/RoleCounterTest.cpp)
    target_link_libraries(rune_role_counter_tests PRIVATE
        rune_node
        GTest::gtest_main)
    gtest_discover_tests(rune_role_counter_tests)

    # ---------------------------------------------------------------------------
    # CG-3: GroupCoordinationService unit tests — injected clock, fake leader toggle.
    # No Aeron required.
    # ---------------------------------------------------------------------------
    add_executable(rune_group_coordination_service_tests
        tests/node/GroupCoordinationServiceTest.cpp)
    target_link_libraries(rune_group_coordination_service_tests PRIVATE
        rune_node
        rune_coordination
        GTest::gtest_main)
    gtest_discover_tests(rune_group_coordination_service_tests)

    # ---------------------------------------------------------------------------
    # TX-3: TransactionCoordinationService unit tests — injected leader toggle,
    # single-node PartitionReplica (minIsr=1), no Aeron required.
    # ---------------------------------------------------------------------------
    add_executable(rune_txn_coordination_service_tests
        tests/node/TransactionCoordinationServiceTest.cpp)
    target_link_libraries(rune_txn_coordination_service_tests PRIVATE
        rune_node
        rune_transaction
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_txn_coordination_service_tests)

    # ---------------------------------------------------------------------------
    # CG-3: GroupCoordinationService integration tests — drive real Join→doWork→
    # Sync→OffsetCommit→OffsetFetch through GroupCoordinationService against real
    # GroupCoordinator + ReplicatedOffsetStore.
    # No Aeron required.
    # ---------------------------------------------------------------------------
    add_executable(rune_group_coordination_integration_tests
        tests/integration/GroupHandlerIntegrationTest.cpp)
    target_link_libraries(rune_group_coordination_integration_tests PRIVATE
        rune_node
        rune_coordination
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_group_coordination_integration_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # CG-4: GroupPushDelivery integration tests — assignment→push-subscription
    # translation, resume-from-committed offset, eager revoke on rebalance, and
    # Assign egress control frame encode/decode.
    # Uses an embedded Aeron IPC driver (no network ports) to drive the real
    # ClientGateway push path (onEgressFrame capture, no live Aeron subscribers).
    # Gated behind aeron_FOUND (same gate as the broker-node tests that embed a driver).
    # ---------------------------------------------------------------------------
    if(aeron_FOUND)
        add_executable(rune_group_push_delivery_tests
            tests/integration/GroupPushDeliveryTest.cpp)
        target_link_libraries(rune_group_push_delivery_tests PRIVATE
            rune_node
            rune_coordination
            rune_replication
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_group_push_delivery_tests
            PROPERTIES TIMEOUT 120)
    endif()

    # ---------------------------------------------------------------------------
    # CG-2: ReplicatedOffsetStore integration tests — 3-in-process-node cluster
    # using InMemoryTransport (no Aeron required).  Tests offset durability across
    # a leader kill + Log replay (offset-survives-leader-kill scenario).
    # ---------------------------------------------------------------------------
    add_executable(rune_offset_replication_tests
        tests/integration/OffsetReplicationTest.cpp)
    target_link_libraries(rune_offset_replication_tests PRIVATE
        rune_coordination
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_offset_replication_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # TX-3: ReplicatedTxnStateStore integration tests — 3-in-process-node cluster
    # using InMemoryTransport.  Tests txn-state durability across a leader kill +
    # Log replay (TxnStateSurvivesLeaderKillAndRebuild scenario).
    # ---------------------------------------------------------------------------
    add_executable(rune_txn_state_replication_tests
        tests/integration/TxnStateReplicationTest.cpp)
    target_link_libraries(rune_txn_state_replication_tests PRIVATE
        rune_transaction
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_txn_state_replication_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # TX-2b: ProducerStateManager durability across restart and leader failover.
    # Uses InMemoryTransport (3-in-process nodes, no Aeron required).
    # Verifies that PSM is rebuilt from the partition Log so a duplicate produce
    # after failover is still detected as Duplicate (no re-append).
    # ---------------------------------------------------------------------------
    add_executable(rune_producer_state_durability_tests
        tests/integration/ProducerStateDurabilityTest.cpp)
    target_link_libraries(rune_producer_state_durability_tests PRIVATE
        rune_storage
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_producer_state_durability_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # TX-4 Task 8: TxnMarkerTest — control marker writes (ControlMarker::writeMarkers)
    # + coordinator state machine integration.  No Aeron required.
    # ---------------------------------------------------------------------------
    add_executable(rune_txn_marker_tests
        tests/integration/TxnMarkerTest.cpp)
    target_link_libraries(rune_txn_marker_tests PRIVATE
        rune_node
        rune_transaction
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_txn_marker_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # CP-2: CompactionDutyCycle integration tests — NodeRuntimeAgent bounded
    # compaction pass wired as duty-cycle step 6.5.  No Aeron required.
    # Tests: leader-only compaction, non-leader no-op, Delete-policy skip,
    #        active-segment-never-touched invariant.
    # ---------------------------------------------------------------------------
    add_executable(rune_compaction_duty_cycle_tests
        tests/integration/CompactionDutyCycleTest.cpp)
    target_link_libraries(rune_compaction_duty_cycle_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        GTest::gtest_main)
    gtest_discover_tests(rune_compaction_duty_cycle_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # CP-3: CompactionPolicy integration tests — cleanup.policy routing:
    #   Compact-never-size-deletes, tombstone delete-retention, CompactDelete
    #   ordering (compact-first-then-delete), Delete-never-compacts,
    #   producer-state survival under CompactDelete.
    # ---------------------------------------------------------------------------
    add_executable(rune_compaction_policy_tests
        tests/integration/CompactionPolicyTest.cpp)
    target_link_libraries(rune_compaction_policy_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        GTest::gtest_main)
    gtest_discover_tests(rune_compaction_policy_tests
        PROPERTIES TIMEOUT 120)

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

            # RoleCounter publish→read integration test: BrokerNode publishes the
            # Aeron node-role/term counters; CncFileReader reads them back (the
            # rune_role path).  Embeds the media driver → gated with the others.
            add_executable(rune_role_counter_publish_tests
                tests/integration/RoleCounterPublishTest.cpp)
            target_link_libraries(rune_role_counter_publish_tests PRIVATE
                rune_node
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_role_counter_publish_tests
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
    # DR-BUG-4: UdpMessageSink peer resolution must be non-fatal + self-healing
    # (an unresolvable peer host must never abort startup/send).  Uses one
    # embedded Aeron media driver in-process.  Gated behind BUILD_BROKER_NODE_TESTS.
    # ---------------------------------------------------------------------------
    if(BUILD_BROKER_NODE_TESTS AND aeron_FOUND)
        add_executable(rune_udp_message_sink_resolve_tests
            tests/integration/UdpMessageSinkResolveTest.cpp)
        target_link_libraries(rune_udp_message_sink_resolve_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_udp_message_sink_resolve_tests
            PROPERTIES TIMEOUT 120)
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

    # ---------------------------------------------------------------------------
    # GR-6: subscribe/push delivery bounded by high-watermark (no uncommitted reads).
    # Unit tests (InMemoryTransport, no Aeron) + integration test (3-node in-process
    # over real aeron:udp).  No Docker.
    # Gated behind BUILD_NODE_UDP_TESTS (same env requirement as GR-3..GR-5 tests).
    # ---------------------------------------------------------------------------
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_gateway_subscribe_hw_bound_tests
            tests/integration/GatewaySubscribeHwBoundTest.cpp)
        target_link_libraries(rune_gateway_subscribe_hw_bound_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_subscribe_hw_bound_tests
            PROPERTIES TIMEOUT 240)
    endif()

    # ---------------------------------------------------------------------------
    # GR-7: Follower catch-up / truncation / rejoin under failover (durability crux).
    # Unit tests (InMemoryTransport, deterministic clock) + integration tests
    # (3-node in-process, real aeron:udp).  No Docker.
    # Gated behind BUILD_NODE_UDP_TESTS (same env requirement as GR-3..GR-6 tests).
    # ---------------------------------------------------------------------------
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_gateway_failover_replication_tests
            tests/integration/GatewayFailoverReplicationTest.cpp)
        target_link_libraries(rune_gateway_failover_replication_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_failover_replication_tests
            PROPERTIES TIMEOUT 300)
    endif()

    # ---------------------------------------------------------------------------
    # TX-2b fix round 1 (A): HW-bound unit test — no Aeron required.
    #
    # DivergentTail_HWBoundRebuild_NoFalseDuplicate:
    #   Directly proves the CRITICAL fix: HW-bounded rebuildFromLog() must NOT
    #   seed PSM from uncommitted (above-HW) batches.  A producer retry of a
    #   never-committed sequence must be Accepted (not false-Duplicate).
    #   Fails against the pre-fix LEO-bound code.
    #
    # Links only rune_storage (no rune_node/rune_consensus that require Aeron).
    # TX2B_UNIT_ONLY suppresses the integration-only sections of the shared
    # source file (Aeron headers, BrokerNode, integration test function).
    # ---------------------------------------------------------------------------
    add_executable(rune_psm_hw_bound_tests
        tests/integration/GatewayPsmFailoverTest.cpp)
    target_compile_definitions(rune_psm_hw_bound_tests PRIVATE
        TX2B_UNIT_ONLY=1)
    target_link_libraries(rune_psm_hw_bound_tests PRIVATE
        rune_storage
        GTest::gtest_main)
    gtest_discover_tests(rune_psm_hw_bound_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # TX-2b fix round 1 (B): gateway-driven integration test — requires Aeron.
    #
    # FailoverDedup_HandleProducePath:
    #   3 BrokerNodes, real aeron:udp.  Produces idempotent batches via the real
    #   handleProduce path; kills the leader; verifies the new leader's PSM rebuild
    #   (doWork() wasLeader_ transition) deduplicates re-sends and accepts new seqs.
    # Gated behind BUILD_NODE_UDP_TESTS + aeron_FOUND (same as GR-7).
    # ---------------------------------------------------------------------------
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_gateway_psm_failover_tests
            tests/integration/GatewayPsmFailoverTest.cpp)
        target_link_libraries(rune_gateway_psm_failover_tests PRIVATE
            rune_node
            rune_storage
            rune_replication
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_psm_failover_tests
            PROPERTIES TIMEOUT 300)
    endif()
endif()
