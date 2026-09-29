add_library(rune_node
    src/NodeConfig.cpp
    src/BrokerNode.cpp
    src/ReplicationMetadataBridge.cpp
    src/GroupCoordinationService.cpp
    src/TransactionCoordinationService.cpp
    src/PortableFlushBackend.cpp
    src/SchemaRegistry.cpp
    # UringFlushBackend.cpp compiles to an empty TU on non-Linux (#ifdef __linux__).
    src/UringFlushBackend.cpp)
# E5.1.2d: ProducePath.cpp was moved from libs/gateway/src/ to libs/storage/src/
# (compiled into rune_storage, a true leaf lib). Both rune_gateway and rune_node
# depend on rune_storage, so the circular-dep workaround is eliminated.

target_include_directories(rune_node PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${RUNE_PROTOCOL_INCLUDE})

# BrokerNode needs: consensus engine, gateway (BR-7a), replication (GR-2/GR-3), Aeron, spdlog
# rune_coordination: GroupCoordinator + ReplicatedOffsetStore (CG-3)
# rune_transaction: TransactionCoordinator + ReplicatedTxnStateStore (TX-3)
# rune_metadata_msm: MetadataStateMachine (MD1 Task 4)
# rune_dynamic_balancer: DynamicBalancer + FailoverElection (GR-7 dead-leader reassign,
#   now ODR-used via MetadataService::reassignDeadPartitionLeaders from NodeRuntimeAgent)
target_link_libraries(rune_node PUBLIC rune_consensus rune_gateway rune_coordination rune_transaction rune_metadata_msm rune_static_balancer rune_dynamic_balancer)
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
# spdlog is PUBLIC because Worker.h (a PUBLIC header) uses spdlog calls inline.
# E2 inc2a: Worker.h was extended with inline execute methods that log via spdlog.
target_link_libraries(rune_node PUBLIC spdlog::spdlog)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    # TSan suppressions for vendored Aeron client internals (conductor↔poll
    # fence-based ordering TSan cannot model). Applied as a CTest ENVIRONMENT to
    # the Aeron-based node test targets so `ctest` runs are clean while rune-side
    # races still fail. See cmake/tsan-aeron.supp for the exact (aeron::-only)
    # signatures — no rune symbol is suppressed.
    #
    # NOTE: CTest ENVIRONMENT *replaces* TSAN_OPTIONS (it does not merge), so we
    # fold in halt_on_error=1 here to match the Milestone-E1 plan's prescription
    # (stop at the first real race). Without this, these targets would fall back
    # to TSan's default halt_on_error=0 (races still reported + non-zero exit, but
    # not stopped at first). A real rune-side race still fails the build either way.
    set(RUNE_TSAN_AERON_SUPP
        "${CMAKE_SOURCE_DIR}/cmake/tsan-aeron.supp")
    set(RUNE_AERON_TSAN_ENV
        "TSAN_OPTIONS=halt_on_error=1 suppressions=${RUNE_TSAN_AERON_SUPP}")

    # Unit tests (NodeConfig + PortableFlushBackend + OneToOneByteRing + SchemaCodecSmoke + SchemaRegistry)
    add_executable(rune_node_tests
        tests/node/NodeConfigTest.cpp
        tests/node/PortableFlushBackendTest.cpp
        tests/node/OneToOneByteRingTest.cpp
        tests/node/SchemaCodecSmokeTest.cpp
        tests/node/SchemaRegistryTest.cpp
        )
    target_link_libraries(rune_node_tests PRIVATE
        rune_node
        rune_codec
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_node_tests)

    # ---------------------------------------------------------------------------
    # E5.1.2b: ProducePathTest — unit tests for the shared handleProduceBatch
    # helper (ProducePath.h/.cpp).  Tests the core batch produce semantics
    # (restamp, CRC, idempotent dedup, min-ISR gate, append, PSM update) against
    # a real Log + ProducerStateManager.  No Aeron required.
    # ---------------------------------------------------------------------------
    add_executable(rune_produce_path_tests
        tests/node/ProducePathTest.cpp)
    target_include_directories(rune_produce_path_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_produce_path_tests PRIVATE
        rune_node
        GTest::gtest_main)
    gtest_discover_tests(rune_produce_path_tests
        PROPERTIES TIMEOUT 60)

    # RoleCounter wire-encoding unit tests (pure header — no Aeron driver).
    add_executable(rune_role_counter_tests
        tests/node/RoleCounterTest.cpp)
    target_link_libraries(rune_role_counter_tests PRIVATE
        rune_node
        GTest::gtest_main)
    gtest_discover_tests(rune_role_counter_tests)

    # ---------------------------------------------------------------------------
    # MD1 Task 7: ReplicationMetadataBridge per-partition role application tests.
    # Tests the FINDING-1 split: INTERNAL partitions follow cluster-consensus
    # leadership; USER partitions follow the committed AssignmentMap.
    # No Aeron required (uses MockTransport).
    # ---------------------------------------------------------------------------
    add_executable(rune_bridge_per_partition_tests
        tests/node/ReplicationMetadataBridgePerPartitionTest.cpp)
    target_include_directories(rune_bridge_per_partition_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_bridge_per_partition_tests PRIVATE
        rune_node
        rune_metadata_msm
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_bridge_per_partition_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # Task 12: ReplicationMetadataBridge churn tests (Note B fix + TSan).
    # Tests: delete-delta tears down replica, stray-replica quarantine,
    # leader<->follower churn from the changing committed map, follower
    # epoch-boundary truncation on handoff, concurrent apply-vs-lookup TSan-clean.
    # No Aeron required (uses MockTransport + on-disk Log).
    # ---------------------------------------------------------------------------
    add_executable(rune_bridge_churn_tests
        tests/node/ReplicationMetadataBridgeChurnTest.cpp)
    target_include_directories(rune_bridge_churn_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_bridge_churn_tests PRIVATE
        rune_node
        rune_metadata_msm
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_bridge_churn_tests
        PROPERTIES TIMEOUT 120)

    # ---------------------------------------------------------------------------
    # E1 Task 2: Worker unit tests — drives per-replica duty steps for owned
    # shards only (shared-nothing, no Aeron required).
    # ---------------------------------------------------------------------------
    add_executable(rune_worker_tests
        tests/node/WorkerTest.cpp)
    target_link_libraries(rune_worker_tests PRIVATE
        rune_node
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_worker_tests)

    # ---------------------------------------------------------------------------
    # E1 Task 3: WorkerPool unit tests — N workers, deterministic single-owner
    # assignment via ShardRouter, co-partition affinity, assignFromRegistry.
    # No Aeron required.
    # ---------------------------------------------------------------------------
    add_executable(rune_worker_pool_tests
        tests/node/WorkerPoolTest.cpp)
    target_link_libraries(rune_worker_pool_tests PRIVATE
        rune_node
        rune_metadata
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_worker_pool_tests)

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
    # E3 Task 4: AsyncDurability integration tests — Worker Strict-tier produce
    # defers the ack until the IoBackend (PortableFlushBackend) fsync completion
    # is reaped in doWork.  Three tests:
    #   1. StrictProduceAcksOnlyAfterFlushCompletion
    #   2. FlushFailureErrorsTheAckNeverOK
    #   3. SubmitRingFullHoldsProduce
    # No Aeron required (disk-backed Log + PartitionReplica + Worker directly).
    # ---------------------------------------------------------------------------
    add_executable(rune_async_durability_tests
        tests/integration/AsyncDurabilityTest.cpp)
    target_link_libraries(rune_async_durability_tests PRIVATE
        rune_node
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_async_durability_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # E3 Task 8a: UringDurabilityTest — raw io_uring backend integration tests.
    # The test source is entirely wrapped in #if defined(__linux__); on macOS
    # this binary compiles and links but registers ZERO tests (gtest main exits 0).
    # Run the real tests on the Hetzner Linux box.
    # ---------------------------------------------------------------------------
    add_executable(rune_uring_durability_tests
        tests/integration/UringDurabilityTest.cpp)
    target_link_libraries(rune_uring_durability_tests PRIVATE
        rune_node
        GTest::gtest_main)
    gtest_discover_tests(rune_uring_durability_tests
        PROPERTIES TIMEOUT 60)

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
    # E1 Task 7 (EXIT): WorkerPoolParity integration tests.
    #
    # Two tests:
    #   1. MultiWorkerBrokerReachesLeaderAndServes — real BrokerNode at N=4 workers;
    #      asserts co-group affinity (topic0.p and topic1.p share a worker ∀p) and
    #      >1 distinct worker ids used (proves N>1 is real, not collapsed to 1).
    #   2. DynamicReassignmentReconcilesOwnership — ports the NodeDeath_NoCommittedLoss
    #      rebalance/failover driver from RebalanceFailoverIntegrationTest verbatim;
    #      asserts no committed loss, single leader, serves after failover.
    #
    # The BrokerNode test embeds the Aeron media driver → gated behind aeron_FOUND.
    # ---------------------------------------------------------------------------
    if(aeron_FOUND)
        add_executable(rune_worker_pool_parity_tests
            tests/integration/WorkerPoolParityTest.cpp)
        target_include_directories(rune_worker_pool_parity_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_worker_pool_parity_tests PRIVATE
            rune_node
            rune_replication
            rune_metadata
            rune_dynamic_balancer
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_worker_pool_parity_tests
            PROPERTIES TIMEOUT 120)
    endif()

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
    # MD1 Task 4: MetadataService / MetadataStateMachine replication integration
    # tests — 5 test cases:
    #   1. SingleNode: HW-gated visibility.
    #   2. 3-node: majority-HW visibility on all followers.
    #   3. Restart: snapshot + replay yields identical map.
    #   4. Apply off request path: no lock stall.
    #   5. Metadata replica role follows cluster leadership.
    #
    # Uses InMemoryTransport (no Aeron required).
    # ---------------------------------------------------------------------------
    add_executable(rune_metadata_replication_tests
        tests/integration/MetadataReplicationTest.cpp)
    target_include_directories(rune_metadata_replication_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_metadata_replication_tests PRIVATE
        rune_node
        rune_metadata_msm
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_metadata_replication_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # Milestone D Phase-2 Task 14 (EXIT): rebalance + failover in-process.
    # Deterministic control-plane tick (ControlTickRecord) → DynamicBalancer /
    # FailoverElection over MembershipState::eligibleNodes() → emitDynamicAssignment
    # → real 3-node KIP-101 handoff with NO committed loss (majority + all-in-sync
    # + memory topics).  Uses InMemoryTransport (no Aeron).
    # ---------------------------------------------------------------------------
    add_executable(rune_rebalance_failover_tests
        tests/integration/RebalanceFailoverIntegrationTest.cpp)
    target_include_directories(rune_rebalance_failover_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_rebalance_failover_tests PRIVATE
        rune_node
        rune_metadata_msm
        rune_static_balancer
        rune_dynamic_balancer
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_rebalance_failover_tests
        PROPERTIES TIMEOUT 120)

    # ---------------------------------------------------------------------------
    # MD1 Task 6: StaticAssignmentEmit integration tests — on becoming the
    # metadata-consensus leader, MetadataService runs StaticBalancer::assign()
    # over configured topics and appends the resulting assignment records to
    # the metadata replica in bounded batches.
    #
    # Test cases: leader-claim emission visible on all nodes, idempotency guard,
    # bounded batching (batchSize), follower no-op.
    #
    # Uses InMemoryTransport (no Aeron required).
    # ---------------------------------------------------------------------------
    add_executable(rune_static_assignment_emit_tests
        tests/integration/StaticAssignmentEmitTest.cpp)
    target_include_directories(rune_static_assignment_emit_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_static_assignment_emit_tests PRIVATE
        rune_node
        rune_metadata_msm
        rune_static_balancer
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_static_assignment_emit_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # MD1-13 Round-2: BootstrapRoutingImage tests.
    #
    # Tests 1-3 (no Aeron): buildStaticRoutingImage() correctness when
    #   committedMap() is empty — non-null, deterministic, balanced assignment.
    #   No redirect loop: every partition has a valid leaderNodeId in the image.
    #
    # Tests 4-5 (Aeron gated): acks=All during bootstrap → NotLeader (retriable);
    #   acks=Leader during bootstrap → OK (local append).  Uses the
    #   applyProduceForTest seam + injectTestSession + setOnEgressFrame.
    # ---------------------------------------------------------------------------
    add_executable(rune_bootstrap_routing_image_tests
        tests/integration/BootstrapRoutingImageTest.cpp)
    target_include_directories(rune_bootstrap_routing_image_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_bootstrap_routing_image_tests PRIVATE
        rune_node
        rune_metadata_msm
        rune_static_balancer
        rune_replication
        GTest::gtest_main)
    if(aeron_FOUND)
        target_compile_definitions(rune_bootstrap_routing_image_tests PRIVATE
            RUNE_AERON_AVAILABLE=1)
        target_link_libraries(rune_bootstrap_routing_image_tests PRIVATE
            aeron::aeron)
    endif()
    gtest_discover_tests(rune_bootstrap_routing_image_tests
        PROPERTIES TIMEOUT 120)

    # ---------------------------------------------------------------------------
    # MD1 Task 11: Multi-leader produce integration (Phase-1 exit criterion).
    #
    # 3-node in-process cluster: static assignment places partition leaders on
    # DIFFERENT nodes; concurrent multi-leader produce all records land and HW
    # advances per-partition on each partition's own leader; co-partition group
    # co-location asserted end-to-end; majority-quorum ack (acks=all) proven;
    # records readable per-partition HW-bounded.
    #
    # Uses InMemoryTransport (no Aeron required).
    # ---------------------------------------------------------------------------
    add_executable(rune_multi_leader_produce_tests
        tests/integration/MultiLeaderProduceTest.cpp)
    target_include_directories(rune_multi_leader_produce_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_multi_leader_produce_tests PRIVATE
        rune_node
        rune_metadata_msm
        rune_static_balancer
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_multi_leader_produce_tests
        PROPERTIES TIMEOUT 120)

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
    # CP-4: CompactionCorrectness integration tests — producer-state dedup
    #   survives compaction, deterministic replicas, offset-store rebuild from
    #   compacted log, HW/LEO unchanged by compaction.
    # ---------------------------------------------------------------------------
    add_executable(rune_compaction_correctness_tests
        tests/integration/CompactionCorrectnessTest.cpp)
    target_link_libraries(rune_compaction_correctness_tests PRIVATE
        rune_node
        rune_coordination
        rune_replication
        rune_storage
        GTest::gtest_main)
    gtest_discover_tests(rune_compaction_correctness_tests
        PROPERTIES TIMEOUT 120)

    # ---------------------------------------------------------------------------
    # CP-6: CompactionCleanerAgent integration tests — LogCleanerAgent dedicated
    #   off-thread compaction: off-duty-thread compaction, filthiest-first selection,
    #   per-cycle byte budget, append P99 unperturbed, swap safety under role change.
    # No Aeron required.
    # ---------------------------------------------------------------------------
    add_executable(rune_compaction_cleaner_agent_tests
        tests/integration/CompactionCleanerAgentTest.cpp)
    target_link_libraries(rune_compaction_cleaner_agent_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        GTest::gtest_main)
    # RUN_SERIAL: this binary contains latency assertions (e.g.
    # AppendP99UnperturbedByCompaction) that measure append p99 while compaction
    # runs.  Under `ctest -jN` on a small host, concurrent test processes steal CPU
    # and perturb the p99, causing a false failure (the test passes standalone).
    # Serialising it guarantees the measurement reflects compaction interference
    # only, not ambient scheduler contention.
    gtest_discover_tests(rune_compaction_cleaner_agent_tests
        PROPERTIES TIMEOUT 120 RUN_SERIAL TRUE)

    # ---------------------------------------------------------------------------
    # CP-9: RetentionCleanerAgent integration tests — separate cleaner-cadence
    #   retention: time (largestTimestamp), size, log-start-offset; async delete
    #   (.deleted rename + delayed physical remove); retention off append thread;
    #   single-writer safety; compact+delete combined without double-delete.
    # No Aeron required.
    # ---------------------------------------------------------------------------
    add_executable(rune_retention_cleaner_agent_tests
        tests/integration/RetentionCleanerAgentTest.cpp)
    target_link_libraries(rune_retention_cleaner_agent_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        GTest::gtest_main)
    gtest_discover_tests(rune_retention_cleaner_agent_tests
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
            target_include_directories(rune_broker_node_tests PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/tests)
            target_link_libraries(rune_broker_node_tests PRIVATE
                rune_node
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_broker_node_tests
                PROPERTIES TIMEOUT 60
                ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # RoleCounter publish→read integration test: BrokerNode publishes the
            # Aeron node-role/term counters; CncFileReader reads them back (the
            # rune_role path).  Embeds the media driver → gated with the others.
            add_executable(rune_role_counter_publish_tests
                tests/integration/RoleCounterPublishTest.cpp)
            target_include_directories(rune_role_counter_publish_tests PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/tests)
            target_link_libraries(rune_role_counter_publish_tests PRIVATE
                rune_node
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_role_counter_publish_tests
                PROPERTIES TIMEOUT 60
                ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
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
            target_include_directories(rune_udp_cluster_tests PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/tests)
            target_link_libraries(rune_udp_cluster_tests PRIVATE
                rune_node
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_udp_cluster_tests
                PROPERTIES TIMEOUT 120
                ENVIRONMENT "${RUNE_AERON_TSAN_ENV}"
                RESOURCE_LOCK rune_udp_ports)  # serialize ephemeral-UDP-port suites
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
    # E5.1.2a: WorkerIngress unit tests — per-worker Aeron subscription +
    # zero-copy poll delivery mechanism.  Uses an embedded IPC Aeron driver
    # (same fixture as GatewayLoopbackTest).  Gated behind BUILD_BROKER_NODE_TESTS.
    # ---------------------------------------------------------------------------
    if(BUILD_BROKER_NODE_TESTS AND aeron_FOUND)
        add_executable(rune_worker_ingress_tests
            tests/node/WorkerIngressTest.cpp)
        target_link_libraries(rune_worker_ingress_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_worker_ingress_tests
            PROPERTIES TIMEOUT 60
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

        # read-rework T3/T4 I1: WorkerEgress — per-worker dynamic-MDC egress
        # publication (UDP MDC driver).  Sole-writer send + fan-out delivery.
        add_executable(rune_worker_egress_tests
            tests/node/WorkerEgressTest.cpp)
        target_link_libraries(rune_worker_egress_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_worker_egress_tests
            PROPERTIES TIMEOUT 60
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

        # read-rework T3/T4 I2: worker-local subscription table fed by
        # Subscribe/Unsubscribe WorkerOps (no driver — SPSC ring + doWork only).
        add_executable(rune_worker_subscribe_tests
            tests/node/WorkerSubscribeTest.cpp)
        target_link_libraries(rune_worker_subscribe_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_worker_subscribe_tests
            PROPERTIES TIMEOUT 60
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

        # read-rework: internal SPSC O2O ring (tryClaim/commit/abort zero-copy).
        add_executable(rune_o2o_ring_tests
            tests/node/O2ORingBufferTest.cpp)
        target_link_libraries(rune_o2o_ring_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_o2o_ring_tests
            PROPERTIES TIMEOUT 60
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

        # read-rework T3/T4 I3a: in-worker read+encode+push loop over its own
        # dynamic-MDC egress (UDP MDC driver + real disk Log + subscriber).
        add_executable(rune_worker_push_tests
            tests/node/WorkerPushTest.cpp)
        target_link_libraries(rune_worker_push_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_worker_push_tests
            PROPERTIES TIMEOUT 120
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
    endif()

    # ---------------------------------------------------------------------------
    # E5.1.2d: WorkerProducePathTest — per-worker ingress produce path tests.
    #
    # Tests the full produce path: WorkerIngress → handleProduceBatch → append →
    # ack via ret_.  Three tests:
    #   1. AcksLeader_ImmediateAck  — acks=0: ack fires immediately on append.
    #   2. AcksAll_DeferredAckOnHWAdvance — acks=1: deferred until HW passes.
    #   3. G_A6_Backpressure_NoDropOnFullRet — structural backpressure: worker
    #      holds (not drops) acks when ret_ is full; resumes on drain.
    #
    # Uses an embedded IPC Aeron driver (same fixture as WorkerIngressTest).
    # Gated behind BUILD_BROKER_NODE_TESTS (same as WorkerIngressTest).
    # ---------------------------------------------------------------------------
    if(BUILD_BROKER_NODE_TESTS AND aeron_FOUND)
        add_executable(rune_worker_produce_path_tests
            tests/node/WorkerProducePathTest.cpp)
        target_include_directories(rune_worker_produce_path_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_worker_produce_path_tests PRIVATE
            rune_node
            rune_replication
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_worker_produce_path_tests
            PROPERTIES TIMEOUT 120
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
    endif()

    # ---------------------------------------------------------------------------
    # E5.1.3: WorkerPerWorkerIngressE2ETest — per-worker ingress end-to-end tests.
    #
    # Tests with perWorkerIngressActive_=true (flag ON):
    #   1. MultiPartitionMultiKey_EndToEnd — two workers/partitions, correct routing.
    #   2. ReservationPoll_MoreThanOnePerCycle — >1 produce per doWork (E5.1.3b).
    #   3. G_A6_ReservationGate_BackpressureStillHolds — G-A6 preserved under load.
    #   4. AcksAll_DeferredUntilHW — acks=all on single-node leader.
    #   5. AcksAll_TimeoutErrorIsInternal — timeout yields errorCode=6, not 7 (E5.1.3a).
    #   6. WorkerOwnsLogSync_NoGatewayRace — R7 structural: worker owns log sync.
    #
    # Uses embedded IPC Aeron driver (same fixture as WorkerProducePathTest).
    # Gated behind BUILD_BROKER_NODE_TESTS (same gate as WorkerProducePathTest).
    # ---------------------------------------------------------------------------
    if(BUILD_BROKER_NODE_TESTS AND aeron_FOUND)
        add_executable(rune_worker_per_worker_ingress_e2e_tests
            tests/node/WorkerPerWorkerIngressE2ETest.cpp)
        target_include_directories(rune_worker_per_worker_ingress_e2e_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_worker_per_worker_ingress_e2e_tests PRIVATE
            rune_node
            rune_replication
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_worker_per_worker_ingress_e2e_tests
            PROPERTIES TIMEOUT 120
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
    endif()

    # ---------------------------------------------------------------------------
    # E5.1.5: IngressMigrationTest — exactly-once produce across partition
    # leadership reassignment when per-worker ingress is active (flag ON).
    #
    # Three tests:
    #   1. ExOwner_RejectsWithNotLeader — ex-owner returns NotLeader (retriable).
    #   2. PsmReconstructionOnOwnershipGain_DedupsAcrossMove — PSM rebuild on
    #      BecomeLeader; retry of last committed seq returns original baseOffset.
    #   3. ExactlyOnceAcrossMove_RetrySpanningMove — adversarial retry spanning
    #      the ownership move; exactly kN+1 records in the final log.
    #
    # Uses embedded IPC Aeron driver (same fixture as WorkerProducePathTest).
    # Gated behind BUILD_BROKER_NODE_TESTS + aeron_FOUND.
    # TSan: TSAN_OPTIONS=halt_on_error=0 suppressions=cmake/tsan-aeron.supp
    # ---------------------------------------------------------------------------
    if(BUILD_BROKER_NODE_TESTS AND aeron_FOUND)
        add_executable(rune_ingress_migration_tests
            tests/integration/IngressMigrationTest.cpp)
        target_include_directories(rune_ingress_migration_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_ingress_migration_tests PRIVATE
            rune_node
            rune_replication
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_ingress_migration_tests
            PROPERTIES TIMEOUT 120
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
    endif()

    # BrokerNode+Gateway integration tests (BR-7a) — single-node with live gateway.
    # Gated behind BUILD_BROKER_NODE_TESTS (reuses that gate).
    # ---------------------------------------------------------------------------
    if(BUILD_BROKER_NODE_TESTS AND aeron_FOUND)
        add_executable(rune_broker_node_gateway_tests
            tests/node/BrokerNodeGatewayTest.cpp)
        target_include_directories(rune_broker_node_gateway_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_broker_node_gateway_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_broker_node_gateway_tests
            PROPERTIES TIMEOUT 120
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
    endif()

    # ---------------------------------------------------------------------------
    # Gateway↔Replication wiring integration tests (GR-2/GR-3) — 3-node in-process
    # over real aeron:udp with replication ports.
    # Gated behind BUILD_NODE_UDP_TESTS (same env requirement as UDP cluster tests).
    # ---------------------------------------------------------------------------
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_gateway_replication_wiring_tests
            tests/integration/GatewayReplicationWiringTest.cpp)
        target_include_directories(rune_gateway_replication_wiring_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_gateway_replication_wiring_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_replication_wiring_tests
            PROPERTIES TIMEOUT 180
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
    endif()

    # ---------------------------------------------------------------------------
    # GR-4: Gateway produce → leader PartitionReplica (Mode A onLeaderLogAppended).
    # Unit + integration tests (3-node in-process, real aeron:udp, no Docker).
    # Gated behind BUILD_NODE_UDP_TESTS (same env requirement as GR-3 tests).
    # ---------------------------------------------------------------------------
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_gateway_produce_replication_tests
            tests/integration/GatewayProduceReplicationTest.cpp)
        target_include_directories(rune_gateway_produce_replication_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_gateway_produce_replication_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_produce_replication_tests
            PROPERTIES TIMEOUT 180
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
    endif()

    # ---------------------------------------------------------------------------
    # GR-5: acks=All deferred-ack via non-blocking pending-ack queue + min-ISR gate.
    # Unit + integration tests (3-node in-process, real aeron:udp, no Docker).
    # Gated behind BUILD_NODE_UDP_TESTS (same env requirement as GR-3/GR-4 tests).
    # ---------------------------------------------------------------------------
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_gateway_acks_all_deferred_ack_tests
            tests/integration/GatewayAcksAllDeferredAckTest.cpp)
        target_include_directories(rune_gateway_acks_all_deferred_ack_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_gateway_acks_all_deferred_ack_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_acks_all_deferred_ack_tests
            PROPERTIES TIMEOUT 240
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
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
        target_include_directories(rune_gateway_subscribe_hw_bound_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_gateway_subscribe_hw_bound_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_subscribe_hw_bound_tests
            PROPERTIES TIMEOUT 240
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
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
        # RESOURCE_LOCK: these tests bind ephemeral UDP ports discovered via a
        # bind/close/return-number probe (inherent TOCTOU).  Running two such
        # real-BrokerNode UDP suites concurrently races for the same freed port
        # → intermittent "bind: Address already in use".  Serialize all UDP
        # failover suites on a shared lock so ctest -jN never overlaps them.
        gtest_discover_tests(rune_gateway_failover_replication_tests
            PROPERTIES TIMEOUT 300 RESOURCE_LOCK rune_udp_ports)
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
            PROPERTIES TIMEOUT 300 RESOURCE_LOCK rune_udp_ports)
    endif()

    # ---------------------------------------------------------------------------
    # CP-5 Phase 1: CompactionRetentionSmoke — end-to-end integration gate.
    #
    # Cohesive smoke suite that drives a REAL BrokerNode (NodeRuntimeAgent +
    # LogCleanerAgent on their own threads, config-driven coalescing on) for 10
    # scenarios:
    #   1. CompactHappyPath_LatestPerKey
    #   2. TombstoneLifecycle_KIP534
    #   3. DeleteRetention_TimeAndSize
    #   4. CompactDelete_BothRun_NoDoubleDelete
    #   5. InternalTopic_Recovery_CompactedLog
    #   6. IdempotentDedup_SurvivesRetention
    #   7. RestartResume_Checkpoint
    #   8. HwLeo_NonPerturbation
    #   9. AppendDuringCompaction_Correctness
    #  10. Failover_HwBounded_NoResurrection
    #
    # Requires: BrokerNode (embedded Aeron driver).
    # Gated behind BUILD_BROKER_NODE_TESTS + aeron_FOUND (same as BrokerNodeTest).
    # ---------------------------------------------------------------------------
    if(BUILD_BROKER_NODE_TESTS AND aeron_FOUND)
        add_executable(rune_compaction_retention_smoke_tests
            tests/integration/CompactionRetentionSmokeTest.cpp)
        target_include_directories(rune_compaction_retention_smoke_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_compaction_retention_smoke_tests PRIVATE
            rune_node
            rune_coordination
            rune_replication
            rune_storage
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_compaction_retention_smoke_tests
            PROPERTIES TIMEOUT 300
            ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
    endif()

    # ---------------------------------------------------------------------------
    # CP-5 Phase 4: CompactionStress — in-process compaction+retention stress/
    #   endurance test at scale.
    #
    # Three tests:
    #   1. ManyPartitions_LatestPerKey_Stress
    #        300 partitions (200 compact + 50 compact+delete + 50 delete),
    #        20 keys/partition, 30 records/key → 180 000 total records.
    #        Asserts: latest-per-key, HW/LEO invariant, no divergence,
    #        bounded segment count, checkpoint advancement.
    #
    #   2. ConcurrentCleaner_TSan_Stress
    #        30 compact partitions, 10 keys, 20 records/key.
    #        LogCleanerAgent on its own thread (AgentRunner), duty-cycle on test
    #        thread.  TSan build: run under -fsanitize=thread → 0 races.
    #        Asserts same properties as (1); wall budget 30 s.
    #
    #   3. CheckpointCoalescing_Bounded_Stress
    #        Single partition, 15 keys, 50 records/key, 1500 cleaner cycles.
    #        Asserts: checkpoint monotone, segment count bounded by coalescing,
    #        latest-per-key, no divergence.
    #
    # No Aeron required (no BrokerNode; uses standalone Log + PartitionReplica
    # + LogCleanerAgent + NodeRuntimeAgent directly).
    # ---------------------------------------------------------------------------
    add_executable(rune_compaction_stress_tests
        tests/integration/CompactionStressTest.cpp)
    target_link_libraries(rune_compaction_stress_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        GTest::gtest_main)
    gtest_discover_tests(rune_compaction_stress_tests
        PROPERTIES TIMEOUT 180)
endif()
