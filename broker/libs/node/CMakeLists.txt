add_library(rune_node
    src/NodeConfig.cpp
    src/StorageIdentity.cpp
    src/PemFile.cpp
    src/BrokerNode.cpp
    src/EmbeddedDriver.cpp
    src/PeerResolverHub.cpp
    src/HostDefaults.cpp
    src/FdBudget.cpp
    src/ShardAgent.cpp
    src/ReplicationMetadataBridge.cpp
    src/GroupCoordinationService.cpp
    src/TransactionCoordinationService.cpp
    src/SchemaRegistry.cpp
    src/ViewRegistry.cpp
    src/AdminWorker.cpp
    src/AdminLogPartition.cpp
    src/CatalogPartition.cpp
    src/CatalogCheckpoint.cpp
    src/CatalogApplier.cpp
    src/SecurityApplier.cpp
    src/SecurityAdmin.cpp
    src/PeerLinkAgent.cpp
    src/CatalogReadServer.cpp
    src/CatalogEffectFrames.cpp
    src/ViewSnapshotIoAgent.cpp
    src/WorkerViewSet.cpp
    src/TopicDirReconcile.cpp)

# The worker's io backends, as their own leaf library: Worker.h's header-inline
# code (instantiated by rune_replication's AeronPartitionTransport.cpp too) calls
# UringFlushBackend::submit/submitFadvise, so rune_replication must link these
# symbols without linking rune_node (which depends on it).  Linux-only failure
# before (macOS compiles UringFlushBackend.cpp to an empty TU): replication-only
# test executables did not link.
add_library(rune_io_backend
    src/PortableFlushBackend.cpp
    # UringFlushBackend.cpp compiles to an empty TU on non-Linux (#ifdef __linux__).
    src/UringFlushBackend.cpp)
target_include_directories(rune_io_backend PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_io_backend PUBLIC rune_consensus spdlog::spdlog)
target_link_libraries(rune_node PUBLIC rune_io_backend)
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
# Task 13: ViewSnapshotIoAgent peeks the Task-12 delta wire header's O field
# (rune::view::kViewCheckpointMagic) -- node -> view is the sanctioned
# dependency direction (libs/view must NEVER depend on libs/node).
target_link_libraries(rune_node PUBLIC rune_view)
# Catalog history Task 7: the CatalogApplier folds __catalog through the shared
# rune_catalog library (decode / apply / fingerprints — one set of frozen rules).
target_link_libraries(rune_node PUBLIC rune_catalog)
# Authentication plan Task 9: the SecurityApplier folds __security through rune_security; cluster keys via rune_peerlink.
target_link_libraries(rune_node PUBLIC rune_security rune_peerlink)
if(aeron_FOUND)
    target_link_libraries(rune_node PUBLIC aeron::aeron rune_replication_aeron rune_aeron_util)
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
# UDP GSO/GRO Aeron transport bindings (EmbeddedDriver, HostDefaults probe).
target_link_libraries(rune_node PUBLIC rune_gso)

# A7 park-until-work + epoll-first polling (review L4: ONE copy, shared with rune-client-cpp): the rune-protocol
# sibling C source, like rune_gso.  Aeron's internal driver headers are C; they include siblings relative to
# <root>, <root>/aeron, <root>/aeronmd (same probe as rune_gso).  Non-Linux compiles a stub (0 fds -> timed park).
add_library(rune_driver_wake STATIC ${RUNE_PROTOCOL_INCLUDE}/../src/net/rune_driver_wake.c)
set_target_properties(rune_driver_wake PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON POSITION_INDEPENDENT_CODE ON)
target_include_directories(rune_driver_wake PUBLIC ${RUNE_PROTOCOL_INCLUDE})
if(UNIX AND NOT APPLE)
    target_link_libraries(rune_driver_wake PRIVATE aeron::aeron)
    foreach(_rune_aeron_inc IN LISTS aeron_INCLUDE_DIRS)
        target_include_directories(rune_driver_wake PRIVATE
            "${_rune_aeron_inc}" "${_rune_aeron_inc}/aeron" "${_rune_aeron_inc}/aeronmd")
    endforeach()
endif()
target_link_libraries(rune_node PUBLIC rune_driver_wake)

# DR-15 fix round 1: the embedded drivers' cache-only peer name resolver (C glue over Aeron's internal
# name-resolver vtable; the cache is C++ PeerResolutionInbox).  Every platform (it is not a Linux feature).
add_library(rune_peer_name_resolver STATIC src/c/rune_peer_name_resolver.c)
set_target_properties(rune_peer_name_resolver PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON
                      POSITION_INDEPENDENT_CODE ON)
target_include_directories(rune_peer_name_resolver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(rune_peer_name_resolver PRIVATE aeron::aeron)
foreach(_rune_aeron_inc IN LISTS aeron_INCLUDE_DIRS)
    target_include_directories(rune_peer_name_resolver PRIVATE
        "${_rune_aeron_inc}" "${_rune_aeron_inc}/aeron" "${_rune_aeron_inc}/aeronmd")
endforeach()
target_link_libraries(rune_node PUBLIC rune_peer_name_resolver)

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
        "${RUNE_BROKER_DIR}/cmake/tsan-aeron.supp")

    # Task 13 (spec §3.4): worker-side ack sink + announce-before-accept (no driver).
    add_executable(rune_session_ack_sink_tests tests/node/SessionAckSinkTest.cpp)
    target_link_libraries(rune_session_ack_sink_tests PRIVATE rune_node GTest::gtest_main)
    gtest_discover_tests(rune_session_ack_sink_tests)
    add_executable(rune_session_announcer_tests tests/node/SessionAnnouncerTest.cpp tests/node/PeerLinkFanoutTest.cpp)
    target_link_libraries(rune_session_announcer_tests PRIVATE rune_node rune_metadata GTest::gtest_main)
    gtest_discover_tests(rune_session_announcer_tests)
    # Authentication Task 13: the resumable WRITE-bitmap shipper and the slot-indexed worker session crypto (no driver).
    add_executable(rune_permission_shipper_tests tests/node/PermissionShipperTest.cpp
                   tests/node/WorkerSessionCryptoRegressionTest.cpp)
    target_link_libraries(rune_permission_shipper_tests PRIVATE rune_node rune_metadata GTest::gtest_main)
    gtest_discover_tests(rune_permission_shipper_tests)
    if(UNIX AND NOT APPLE)
        # Real-driver UDP GSO/GRO bindings tests (Linux kernels only: UDP_SEGMENT/UDP_GRO).
        add_executable(rune_udp_gso_tests tests/node/UdpGsoBindingsTest.cpp)
        target_link_libraries(rune_udp_gso_tests PRIVATE rune_gso rune_node aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_udp_gso_tests PROPERTIES LABELS "linux-only")   # review B-I1: checkpoint stage c
    endif()
    set(RUNE_AERON_TSAN_ENV
        "TSAN_OPTIONS=halt_on_error=1 suppressions=${RUNE_TSAN_AERON_SUPP}")

    # Unit tests (NodeConfig + PortableFlushBackend + OneToOneByteRing + SchemaCodecSmoke + SchemaRegistry + AdminWorker)
    add_executable(rune_node_tests
        tests/node/NodeConfigTest.cpp
        tests/node/StorageIdentityTest.cpp
        tests/node/StorageGateTest.cpp
        tests/node/HostDefaultsTest.cpp
        tests/node/FdBudgetTest.cpp
        tests/node/PortableFlushBackendTest.cpp
        tests/node/OneToOneByteRingTest.cpp
        tests/node/SchemaCodecSmokeTest.cpp
        tests/node/SchemaRegistryTest.cpp
        tests/node/ViewRegistryTest.cpp
        tests/node/AdminWorkerTest.cpp
        tests/node/AdminIsrChannelTest.cpp
        tests/node/SignalControlTest.cpp
        tests/node/LeaderObserverTest.cpp
        tests/node/OwnerThreadTestTaskTest.cpp
        tests/node/ThreadAffinityTest.cpp
        tests/node/ParkUntilWorkIdleTest.cpp
        tests/node/RingDoorbellTest.cpp
        tests/node/ViewSnapshotIoAgentTest.cpp
        tests/node/WorkerViewSetTest.cpp
        tests/node/TopicDirReconcileTest.cpp
        tests/node/TopicNameTest.cpp
        tests/node/ViewReadinessRegressionTest.cpp
        tests/node/ViewRecoveryRegressionTest.cpp
        tests/node/CatalogApplierTest.cpp
        tests/node/ViewSinkRegressionTest.cpp
        tests/node/SecurityApplierTest.cpp
        tests/node/SecurityReadServerTest.cpp
        tests/node/SecurityWritePathTest.cpp
        tests/node/SecurityAnswerGateTest.cpp
        tests/node/SecurityRingCodecTest.cpp
        tests/node/CatalogWritePathTest.cpp
        tests/node/CatalogLeadershipTest.cpp
        tests/node/CatalogReadServerTest.cpp
        tests/node/CatalogGatesRegressionTest.cpp
        tests/node/WorkerOwnedIndexRegressionTest.cpp
        tests/node/WorkerOpWireRegressionTest.cpp
        tests/node/WorkerDroppedTopicRegressionTest.cpp
        tests/node/ReplicationFrameLimitsRegressionTest.cpp
        )
    target_include_directories(rune_node_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_node_tests PRIVATE
        rune_node
        rune_codec
        rune_view
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_node_tests PROPERTIES LABELS small)
    rune_regression_batches(rune_node_tests 4)   # defined in the root CMakeLists.txt

    # Catalog history Task 7 (F12): the admin thread's catalog steady state never
    # allocates.  Replaces the global operator new (binary-wide) — own binary, like
    # rune_ddl_stress_tests / rune_transient_alloc_regression_tests.  Task 23: + the
    # idle-admin, produce placement-gate and GetViewRow gates.
    add_executable(rune_catalog_alloc_regression_tests
        tests/node/CatalogAllocRegressionTest.cpp)
    target_include_directories(rune_catalog_alloc_regression_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_catalog_alloc_regression_tests PRIVATE
        rune_node
        rune_codec
        rune_view
        GTest::gtest_main)
    gtest_discover_tests(rune_catalog_alloc_regression_tests)

    # Authentication Task 16 (hard rule 4): sealed inter-broker replication allocates nothing in steady state.  Replaces the
    # global operator new (binary-wide) - own binary; counts only the test thread's allocations.
    if(aeron_FOUND)
        add_executable(rune_peer_frame_alloc_regression_tests
            tests/node/PeerFrameAllocRegressionTest.cpp)
        target_link_libraries(rune_peer_frame_alloc_regression_tests PRIVATE
            rune_node
            rune_replication
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_peer_frame_alloc_regression_tests
            PROPERTIES TIMEOUT 120 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
    endif()

    # Authentication Task 9 (hard rule 4): an idle SecurityApplier pass allocates nothing.  Replaces the global
    # operator new (binary-wide) — own binary, like the catalog's.
    add_executable(rune_security_alloc_regression_tests
        tests/node/SecurityAllocRegressionTest.cpp)
    target_include_directories(rune_security_alloc_regression_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_security_alloc_regression_tests PRIVATE
        rune_node
        rune_codec
        rune_view
        GTest::gtest_main)
    gtest_discover_tests(rune_security_alloc_regression_tests)

    # Catalog history Task 20: the interaction matrix — the expectation table, the
    # 3-broker harness (3 AdminWorkers + replicas over one FilteringTransport, real
    # appliers, a fake control side and gateway, the client rules of Task 5) and the
    # 324-cell parameterized unit matrix.  Task 21 (stress) and Task 22 append to it.
    add_executable(rune_catalog_matrix_tests
        tests/node/CatalogInteractionMatrixRegressionTest.cpp
        tests/node/TempDirHygieneRegressionTest.cpp)
    target_include_directories(rune_catalog_matrix_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_catalog_matrix_tests PRIVATE
        rune_node
        rune_codec
        rune_view
        GTest::gtest_main)
    gtest_discover_tests(rune_catalog_matrix_tests PROPERTIES TIMEOUT 600 LABELS small)
    rune_regression_batches(rune_catalog_matrix_tests 2)

    # Catalog history Task 21: the seeded, model-checked stress (64 seeds x 400 steps against the independent
    # CatalogModel).  Its own binary: long-running (each seed is one ctest case).
    add_executable(rune_catalog_stress_tests
        tests/node/CatalogStressRegressionTest.cpp)
    target_include_directories(rune_catalog_stress_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_catalog_stress_tests PRIVATE
        rune_node
        rune_codec
        rune_view
        GTest::gtest_main)
    gtest_discover_tests(rune_catalog_stress_tests PROPERTIES TIMEOUT 900)

    # ---------------------------------------------------------------------------
    # E5 Task 2: WorkerReplicationValidator unit tests — pure symmetric-worker-count
    # validator (header-only, no Aeron required).
    # ---------------------------------------------------------------------------
    add_executable(rune_worker_replication_validator_tests
        tests/node/WorkerReplicationValidatorTest.cpp)
    target_link_libraries(rune_worker_replication_validator_tests PRIVATE
        rune_node
        GTest::gtest_main)
    gtest_discover_tests(rune_worker_replication_validator_tests)

    # ---------------------------------------------------------------------------
    # E5 Task 5: PerWorkerReplicationWiring unit tests — BrokerNode wiring logic.
    #
    # Tests:
    #   1. FlagOffUsesLegacyTransportForAll — zero planes; legacy for all.
    #   2. FlagOnUserPartsOnPlanesInternalOnLegacy — W planes; user→plane; internal→legacy.
    #   3. *Regression*: MismatchedPeerWorkerCountRefusesStart — THROWS.
    #   4. *Regression*: UnresolvedPlaneAbortsStart — THROWS.
    #   5. SingleNodeNoPeersSkipsPlaneConstruction — no planes on single-node.
    #   6. WorkerReplicationTransportPointerRoundTrip — pointer type compiles.
    #
    # Test seam: FakePubRegistry (inline) injected via WorkerReplicationTransportImpl<>.
    # WorkerReplicationTransport.h includes Aeron headers → gated behind aeron_FOUND.
    # No Aeron driver started, no filesystem, no network.
    # ---------------------------------------------------------------------------
    if(aeron_FOUND)
        add_executable(rune_per_worker_replication_wiring_tests
            tests/node/PerWorkerReplicationWiringTest.cpp)
        target_link_libraries(rune_per_worker_replication_wiring_tests PRIVATE
            rune_node
            rune_replication
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_per_worker_replication_wiring_tests)
    endif()

    # ---------------------------------------------------------------------------
    # E5 Task 3: WorkerReplicationTransport unit tests — send path (per-worker
    # sub + per-peer pubs, direct-claim encode).
    #
    # Tests (no Aeron driver required):
    #   1. SendFetchResponseSelectsPeerPubAndEncodesFrame — exact peer routing +
    #      byte-identical SBE frame (round-trip decode).
    #   2. SendFetchRequestBroadcastsToAllPeers — sendFetchRequest hits ALL peers.
    #   3. SendOleRequestRoutesToLeader — OLE request to single target peer.
    #   4. SendOleResponseRoutesToFollower — OLE response to single target peer.
    #   5. SendReconcileRequestStampsReconcileTemplateId — FetchRequest +
    #      ReconcileFetchReq templateId in place.
    #   6. SendReconcileResponseStampsReconcileTemplateId — FetchResponse +
    #      ReconcileFetchResp templateId in place.
    #   7. ResolvedTrueWhenFakeRegistryHasPeers — resolved() gate.
    #   8. SendFetchResponseEmptyRecords — HW-only update (zero records).
    #   9. SendFetchRequestBroadcastsToThreePeers — 3-peer broadcast regression.
    #  10. WorkerIdAccessor — workerId() returns configured id.
    #
    # Test seam: FakePubRegistry (SharedFakePubRegistry) injected via the
    # WorkerReplicationTransportImpl<PubRegistry> template — no Aeron driver.
    # The WorkerReplicationTransport.h header includes Aeron headers (tryClaim
    # etc.) so the target must link aeron::aeron for include-path resolution.
    # Gated behind aeron_FOUND (not BUILD_BROKER_NODE_TESTS: no driver started).
    # ---------------------------------------------------------------------------
    if(aeron_FOUND)
        add_executable(rune_worker_replication_transport_tests
            tests/node/WorkerReplicationTransportTest.cpp
            tests/node/ReplicaTransportKindTest.cpp
            tests/node/F27SendNeverDroppedRegressionTest.cpp)
        target_link_libraries(rune_worker_replication_transport_tests PRIVATE
            rune_node
            rune_replication
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_worker_replication_transport_tests)
    endif()

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
        rune_view
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_worker_tests)

    # Phase 2c Tasks 6-7: FilterIndex (value -> members) and FilterGroup (member state machine, shared scan,
    # rebuild delivery, position notes). No Aeron. AllocWindow.cpp replaces the global operator new for this
    # binary (the zero-allocation gates); later filter test binaries link it the same way.
    add_executable(rune_filter_engine_tests
        tests/node/FilterIndexTest.cpp
        tests/node/FilterGroupTest.cpp
        tests/support/AllocWindow.cpp)
    target_include_directories(rune_filter_engine_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${RUNE_CLIENT_ROOT}/examples)
    target_link_libraries(rune_filter_engine_tests PRIVATE
        rune_node
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_filter_engine_tests PROPERTIES LABELS small)
    rune_regression_batches(rune_filter_engine_tests 2)

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
        # Catalog history Task 17: the tests define their topics through TestCluster (tests/support).
        target_include_directories(rune_worker_pool_parity_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${RUNE_PROTOCOL_INCLUDE})
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
    # Task 6: SchemaPersistenceTest — __topic_schemas log + AdminWorker wiring.
    #
    # Tests (on-host, no Aeron / media driver):
    #   1. SchemaRecord_LandsInLog
    #   2. RebuildFromLog_RecoveryAfterRestart
    #   3. DuplicateDefineTopic_IdempotentOnLog
    #   4. ValidateTrue_PushValidatorsCalled
    #   5. Regression_RULING1_TopicIdIsReturnedByCreateTopic
    #
    # NOT tested here (Docker-gated / Task 8):
    #   - majority-durable commit (Aeron follower replicas required)
    #   - full client DDL e2e (gateway required)
    # ---------------------------------------------------------------------------
    add_executable(rune_schema_persistence_tests
        tests/integration/SchemaPersistenceTest.cpp)
    target_include_directories(rune_schema_persistence_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_schema_persistence_tests PRIVATE
        rune_node
        rune_replication
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_schema_persistence_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # Task 8a: SchemaGatewayTest — gateway → admin ring → replica → response
    # DDL round-trip (RULING-2 E/F), plus SchemaGatewayRegression.DdlDoesNotBlockProduce.
    # On-host (no Aeron); real-Aeron majority-durable + failover deferred to 8b.
    # ---------------------------------------------------------------------------
    add_executable(rune_schema_gateway_tests
        tests/integration/SchemaGatewayTest.cpp)
    target_include_directories(rune_schema_gateway_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_schema_gateway_tests PRIVATE
        rune_node
        rune_gateway
        rune_replication
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_schema_gateway_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # Task 19: CreateViewRegressionTest — CREATE VIEW control plane (create-and-run
    # path): compile + validate + keyed/Compact sink + __views persist + respond +
    # fan-out install onto a real WorkerViewSet.  On-host (no Aeron / media driver).
    # ---------------------------------------------------------------------------
    add_executable(rune_create_view_tests
        tests/integration/CreateViewRegressionTest.cpp)
    target_include_directories(rune_create_view_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_create_view_tests PRIVATE
        rune_node
        rune_view
        rune_replication
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_create_view_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # DDL conformance plan: DdlHarness suites — fix regressions (B1-B4, KL2), the
    # Occupant x Op matrix and the pair x timing matrix.  On-host (no Aeron),
    # deterministic.  Every suite name contains "Regression".
    # ---------------------------------------------------------------------------
    # Catalog history Task 10: DdlHarness.h is built on CatalogDdlRig (the
    # __catalog write path); the suites below are ported to it — every former cell
    # restored or explicitly ruled out (task-10 report).
    add_executable(rune_ddl_conformance_tests
        tests/node/DdlHarnessRegressionTest.cpp
        tests/node/DdlLeadershipRegressionTest.cpp
        tests/node/DdlDedupeRegressionTest.cpp
        tests/node/DdlOccupantMatrixRegressionTest.cpp
        tests/node/DdlPairTimingMatrixRegressionTest.cpp
        tests/node/DdlFinalFixRegressionTest.cpp)
    target_include_directories(rune_ddl_conformance_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_ddl_conformance_tests PRIVATE
        rune_node
        rune_view
        rune_replication
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_ddl_conformance_tests
        DISCOVERY_TIMEOUT 120
        PROPERTIES TIMEOUT 120 LABELS small)
    rune_regression_batches(rune_ddl_conformance_tests 3)

    # DDL conformance plan Task 10: seeded model-checked DDL stress + the admin
    # steady-state zero-alloc gate.  Own executable (replaces global operator new).
    # Ported to the catalog write path by catalog history Task 10.
    add_executable(rune_ddl_stress_tests
        tests/node/DdlStressRegressionTest.cpp)
    target_include_directories(rune_ddl_stress_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_ddl_stress_tests PRIVATE
        rune_node
        rune_view
        rune_replication
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_ddl_stress_tests
        PROPERTIES TIMEOUT 180)

    # ---------------------------------------------------------------------------
    # Task 20: GetViewRowRegressionTest — the GetViewRow point-read path (real
    # Worker op/return + WorkerViewSet) + the openView fingerprint handshake
    # (real AdminWorker + __topic_schemas/__views).  On-host (no Aeron / media
    # driver), deterministic.
    # ---------------------------------------------------------------------------
    add_executable(rune_get_view_row_tests
        tests/integration/GetViewRowRegressionTest.cpp)
    target_include_directories(rune_get_view_row_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_get_view_row_tests PRIVATE
        rune_node
        rune_view
        rune_replication
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_get_view_row_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # Task 7: ProduceValidationTest — per-worker schema validation hook.
    #
    # Tests (on-host, no Aeron / media driver):
    #   1. SchemaUpdate_Regression_ValidatorInstalledOnWorkerThread
    #   2. SchemaUpdate_Regression_WellFormedRecord_AppendProceeds
    #   3. SchemaUpdate_Regression_MalformedRecord_ValidationFailed
    #   4. SchemaUpdate_Regression_BadEnumRecord_ValidationFailed
    #   5. SchemaUpdate_Regression_NoValidator_FallsThrough
    #
    # Task 12: version-keyed validators_ — each writer version gets its own
    # Validator; a record is selected/validated by ITS OWN writer version:
    #   6. WorkerValidator.ValidatesRecordAgainstItsOwnSchemaVersion
    #   7. ProduceValidationRegression.UnknownSchemaVersionRejectedRegression
    #
    # Uses InMemoryLog + Worker directly (no Aeron required).
    # ---------------------------------------------------------------------------
    add_executable(rune_produce_validation_tests
        tests/integration/ProduceValidationTest.cpp)
    target_include_directories(rune_produce_validation_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_produce_validation_tests PRIVATE
        rune_node
        rune_replication
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_produce_validation_tests
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
    # MD1 Task 6 (catalog-history Task 18: CatalogAssignmentEmit): assignment-emit integration tests — on becoming the
    # metadata-consensus leader, MetadataService runs StaticBalancer::assign()
    # over configured topics and appends the resulting assignment records to
    # the metadata replica in bounded batches.
    #
    # Test cases: leader-claim emission visible on all nodes, idempotency guard,
    # bounded batching (batchSize), follower no-op.
    #
    # Uses InMemoryTransport (no Aeron required).
    # ---------------------------------------------------------------------------
    add_executable(rune_catalog_assignment_emit_tests
        tests/integration/CatalogAssignmentEmitTest.cpp)
    target_include_directories(rune_catalog_assignment_emit_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_catalog_assignment_emit_tests PRIVATE
        rune_node
        rune_metadata_msm
        rune_static_balancer
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_catalog_assignment_emit_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # Task 8b (RULING-4, SINGLE-NODE): RuntimeTopicReplica integration test — a
    # RUNTIME-defined topic (DefineTopic → AdminWorker → control-hook createTopic)
    # becomes produce/consume-capable because the control thread creates +
    # registers its user PartitionReplica and emits its assignment so the bridge
    # promotes it to Leader.  Proves: register replica (bumps generation) →
    # addRuntimeTopic emits assignment → bridge promotes to Leader → produce
    # appends (LEO advances) → consume reads back.  Uses InMemoryTransport (no
    # Aeron); the full real-Aeron socket e2e is Task 13 (Docker).
    # ---------------------------------------------------------------------------
    add_executable(rune_runtime_topic_replica_tests
        tests/integration/RuntimeTopicReplicaTest.cpp)
    target_include_directories(rune_runtime_topic_replica_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_runtime_topic_replica_tests PRIVATE
        rune_node
        rune_metadata_msm
        rune_static_balancer
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_runtime_topic_replica_tests
        PROPERTIES TIMEOUT 60)

    # ---------------------------------------------------------------------------
    # MD1-13 Round-2: bootstrap routing image tests (renamed CatalogBootstrapRoutingImage by catalog-history Task 18).
    #
    # Tests 1-3 (no Aeron): buildStaticRoutingImage() correctness when
    #   committedMap() is empty — non-null, deterministic, balanced assignment.
    #   No redirect loop: every partition has a valid leaderNodeId in the image.
    #
    # Tests 4-5 (Aeron gated): acks=All during bootstrap → NotLeader (retriable);
    #   acks=Leader during bootstrap → OK (local append).  Uses the
    #   applyProduceForTest seam + injectTestSession + setOnEgressFrame.
    # ---------------------------------------------------------------------------
    add_executable(rune_catalog_bootstrap_routing_image_tests
        tests/integration/CatalogBootstrapRoutingImageTest.cpp)
    target_include_directories(rune_catalog_bootstrap_routing_image_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_catalog_bootstrap_routing_image_tests PRIVATE
        rune_node
        rune_metadata_msm
        rune_static_balancer
        rune_replication
        GTest::gtest_main)
    if(aeron_FOUND)
        target_compile_definitions(rune_catalog_bootstrap_routing_image_tests PRIVATE
            RUNE_AERON_AVAILABLE=1)
        target_link_libraries(rune_catalog_bootstrap_routing_image_tests PRIVATE
            aeron::aeron)
    endif()
    gtest_discover_tests(rune_catalog_bootstrap_routing_image_tests
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
    # RUN_SERIAL + LABELS latency: AppendP99UnperturbedByCompaction measures append
    # p99 while compaction runs.  Under `ctest -jN` on a small host, concurrent test
    # processes steal CPU and perturb the p99, causing a false failure (the test
    # passes standalone).  Serialising it guarantees the measurement reflects
    # compaction interference only, not ambient scheduler contention.  Only that
    # latency test is serial (scripts/run-regression.sh stage b); the rest of the
    # binary is functional and runs in parallel (2026-10-15).
    set(RUNE_CLEANER_LATENCY_TESTS "CompactionCleanerAgent.AppendP99UnperturbedByCompaction")
    gtest_discover_tests(rune_compaction_cleaner_agent_tests
        TEST_FILTER "${RUNE_CLEANER_LATENCY_TESTS}"
        PROPERTIES TIMEOUT 120 RUN_SERIAL TRUE LABELS latency)
    gtest_discover_tests(rune_compaction_cleaner_agent_tests
        TEST_FILTER "-${RUNE_CLEANER_LATENCY_TESTS}"
        PROPERTIES TIMEOUT 120)

    # ---------------------------------------------------------------------------
    # Duty-pass latency gate (*LatencyGate* + *Regression*): drives the REAL
    # production worker duty pass (shard-mode Worker + fused NodeRuntimeAgent)
    # while the real off-thread LogCleanerAgent rolls / retains / compacts /
    # truncates, and fails when the pass MAX or p99.9 exceeds the per-build-type
    # budget (Release vs Debug constants in the test).  RUN_SERIAL: a latency
    # measurement must not share the host with other test processes.
    # Benchmark mode: RUNE_LATENCY_GATE_BENCH=1 (prints, never fails).
    # No Aeron required.
    # ---------------------------------------------------------------------------
    add_executable(rune_duty_pass_latency_gate_tests
        tests/integration/DutyPassLatencyGateTest.cpp)
    target_link_libraries(rune_duty_pass_latency_gate_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_duty_pass_latency_gate_tests
        PROPERTIES TIMEOUT 1800 RUN_SERIAL TRUE LABELS latency)

    # Hot-path B (2026-10-05): deterministic regressions behind the latency-gate
    # findings F5 / F7 / F8 (PushRead zero-copy + per-pass bound, async epoch
    # durability, TruncatePending pass cost).  No Aeron required.
    add_executable(rune_hotpath_b_regression_tests
        tests/integration/HotPathBRegressionTest.cpp)
    target_link_libraries(rune_hotpath_b_regression_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_hotpath_b_regression_tests)
    # Combined fix program tasks 2 / 6: worker direct-ingress enforcement + client control batches (no Aeron).
    add_executable(rune_worker_enforcement_regression_tests
        tests/integration/WorkerEnforcementRegressionTest.cpp)
    target_link_libraries(rune_worker_enforcement_regression_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_worker_enforcement_regression_tests)
    # D23 (D19-D21 review L2): the hot-path B tests that replace the global
    # operator new / delete (binary-wide) run in their own binary.
    add_executable(rune_hotpath_b_alloc_regression_tests
        tests/integration/HotPathBAllocRegressionTest.cpp)
    target_link_libraries(rune_hotpath_b_alloc_regression_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_hotpath_b_alloc_regression_tests)
    # Phase 2b E2: the worker data path of TRANSIENT (InMemoryLog) partitions —
    # produce, push reads, read_committed, full ring, evicted cursors.  No Aeron.
    add_executable(rune_transient_data_path_tests
        tests/integration/TransientDataPathTest.cpp)
    target_link_libraries(rune_transient_data_path_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_transient_data_path_tests)

    # E2 final fix wave (I3): the K-partition transient failover latency gate —
    # every worker pass timed while K full 8 MiB rings rebuild their
    # read_committed state.  RUN_SERIAL: a latency measurement must not share the
    # host.  Benchmark mode: RUNE_LATENCY_GATE_BENCH=1 (prints, never fails).
    add_executable(rune_transient_failover_latency_gate_tests
        tests/integration/TransientFailoverLatencyGateTest.cpp)
    target_link_libraries(rune_transient_failover_latency_gate_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    # Only the gate itself is serial (stage b of scripts/run-regression.sh); the
    # binary's functional fairness / skip-walk / judging tests run in parallel.
    set(RUNE_TRANSIENT_FAILOVER_GATE
        "TransientFailoverLatencyGateRegression.KTransientPartitionsBecomingLeaderNeverStallAWorkerPass")
    gtest_discover_tests(rune_transient_failover_latency_gate_tests
        TEST_FILTER "${RUNE_TRANSIENT_FAILOVER_GATE}"
        PROPERTIES TIMEOUT 600 RUN_SERIAL TRUE LABELS latency)
    gtest_discover_tests(rune_transient_failover_latency_gate_tests
        TEST_FILTER "-${RUNE_TRANSIENT_FAILOVER_GATE}"
        PROPERTIES TIMEOUT 600)

    # Phase 2b E2: replaces the global operator new (binary-wide) — own binary.
    add_executable(rune_transient_alloc_regression_tests
        tests/integration/TransientDataPathAllocTest.cpp)
    target_link_libraries(rune_transient_alloc_regression_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_transient_alloc_regression_tests)

    # Phase 2b E2: seeded model-checked stress over a transient partition's shared ring.
    add_executable(rune_transient_stress_tests tests/integration/TransientDataPathStressTest.cpp)
    target_include_directories(rune_transient_stress_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_transient_stress_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_transient_stress_tests PROPERTIES TIMEOUT 300)

    # ---------------------------------------------------------------------------
    # CP-9: RetentionCleanerAgent integration tests — separate cleaner-cadence
    #   retention: time (largestTimestamp), size, log-start-offset; async delete
    #   (.deleted rename + delayed physical remove); retention off append thread;
    #   single-writer safety; compact+delete combined without double-delete.
    # No Aeron required.
    # ---------------------------------------------------------------------------
    # Catalog history Task 8 (ruling 5): an AlterTopicConfig reaches the cleaner as
    # a per-topic policy override (its view ring) — the replica config is untouched.
    add_executable(rune_log_cleaner_policy_override_tests
        tests/node/LogCleanerPolicyOverrideTest.cpp)
    target_link_libraries(rune_log_cleaner_policy_override_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        GTest::gtest_main)
    gtest_discover_tests(rune_log_cleaner_policy_override_tests PROPERTIES TIMEOUT 120)

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
            # No RESOURCE_LOCK (2026-10-15): every UDP port a real-BrokerNode suite binds
            # comes from the collision-proof allocator (aeronutil/testing/FreePort.h —
            # flock-owned per-process port blocks), so these suites run in parallel.
            gtest_discover_tests(rune_broker_node_tests
                PROPERTIES TIMEOUT 60
                ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # DDL conformance plan Task 12: DDL serialization + name collisions over
            # a REAL single-node BrokerNode and real Aeron (T1/T2/T3-T4/T7) — on the
            # __catalog write path since catalog history Task 10.
            add_executable(rune_ddl_broker_tests
                tests/integration/DdlConformanceBrokerRegressionTest.cpp)
            target_include_directories(rune_ddl_broker_tests PRIVATE
                ${RUNE_PROTOCOL_INCLUDE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests)
            target_link_libraries(rune_ddl_broker_tests PRIVATE
                rune_node
                rune_view
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_ddl_broker_tests
                PROPERTIES TIMEOUT 180
                ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # EmbeddedDriver: one driver, one pumping thread (spec §3.6).
            add_executable(rune_embedded_driver_tests tests/node/EmbeddedDriverTest.cpp)
            target_link_libraries(rune_embedded_driver_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_embedded_driver_tests
                PROPERTIES TIMEOUT 60 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            add_executable(rune_aeron_pump_tests tests/node/AeronPumpInvokerTest.cpp
                tests/node/UnresolvedPeerRegressionTest.cpp)
            target_link_libraries(rune_aeron_pump_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_aeron_pump_tests PROPERTIES TIMEOUT 60 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # A9: worker ring doorbells (op / session / ret_ space wait), parked shard runner wake,
            # ADMIN_ACTION retry on worker claims — REAL invoker driver.
            add_executable(rune_worker_doorbell_tests tests/node/WorkerDoorbellTest.cpp)
            target_link_libraries(rune_worker_doorbell_tests PRIVATE rune_node rune_replication aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_worker_doorbell_tests PROPERTIES TIMEOUT 120 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            add_executable(rune_shard_agent_tests tests/node/ShardAgentTest.cpp)
            target_link_libraries(rune_shard_agent_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_shard_agent_tests PROPERTIES TIMEOUT 60 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # Task 13 (spec §3.4): worker-side acks on the worker's own session
            # publication, REAL invoker driver over aeron:ipc.
            add_executable(rune_shard_worker_ack_tests tests/node/ShardWorkerAckTest.cpp)
            target_link_libraries(rune_shard_worker_ack_tests PRIVATE rune_node rune_replication rune_codec aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_shard_worker_ack_tests PROPERTIES TIMEOUT 120 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # Authentication Task 13 (spec §6, D30): the shard worker's WRITE grants, a REAL invoker driver over aeron:ipc.
            add_executable(rune_worker_write_grants_tests tests/node/WorkerWriteGrantsTest.cpp)
            target_link_libraries(rune_worker_write_grants_tests PRIVATE rune_node rune_replication rune_codec aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_worker_write_grants_tests PROPERTIES TIMEOUT 120 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # Authentication Task 13 (hard rule 4): the check allocates nothing, gateway path and shard worker path.
            # Replaces the global operator new (binary-wide) - its own binary.
            add_executable(rune_auth_alloc_regression_tests tests/node/AuthAllocRegressionTest.cpp)
            target_include_directories(rune_auth_alloc_regression_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
            target_link_libraries(rune_auth_alloc_regression_tests PRIVATE rune_node rune_replication rune_codec rune_view
                                  rune_catalog aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_auth_alloc_regression_tests PROPERTIES TIMEOUT 300 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # D8c: credit-based gateway pause of a slow gateway-routed client — a
            # real gateway in front of a real worker with worker-side acks.
            add_executable(rune_gateway_produce_credit_tests tests/integration/GatewayProduceCreditTest.cpp)
            target_link_libraries(rune_gateway_produce_credit_tests PRIVATE rune_node rune_replication aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_gateway_produce_credit_tests PROPERTIES TIMEOUT 120 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # Spec §3.1 never-block: follower flush / owned-log sync / Strict tier
            # go through the worker's io backend; background-token routing.
            add_executable(rune_shard_nonblocking_tests tests/integration/ShardNonBlockingTest.cpp)
            target_link_libraries(rune_shard_nonblocking_tests PRIVATE rune_node rune_replication aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_shard_nonblocking_tests PROPERTIES TIMEOUT 120 LABELS small ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
            rune_regression_batches(rune_shard_nonblocking_tests 1 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # A10 group commit: one durability round per worker (covers every dirty log, completes as a
            # unit, Strict acks wait for their covering round, fail-stop, never an inline sync).
            add_executable(rune_group_commit_round_tests tests/integration/GroupCommitRoundTest.cpp)
            target_link_libraries(rune_group_commit_round_tests PRIVATE rune_node rune_replication aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_group_commit_round_tests PROPERTIES TIMEOUT 120 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # F15 (EOS marker / LSO correctness): S0 failing *Regression* tests
            # (design note 2026-10-01-f15-eos-marker-lso-design.md).
            add_executable(rune_f15_eos_regression_tests tests/integration/F15EosRegressionTest.cpp)
            target_link_libraries(rune_f15_eos_regression_tests PRIVATE rune_node rune_replication aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_f15_eos_regression_tests PROPERTIES TIMEOUT 120 LABELS small ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
            rune_regression_batches(rune_f15_eos_regression_tests 3 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

            # D9 (cross-node transaction markers): gateway unit + 3-node
            # in-process integration over the real cluster-plane transport.
            add_executable(rune_d9_cross_node_marker_tests tests/integration/D9CrossNodeMarkerTest.cpp)
            target_include_directories(rune_d9_cross_node_marker_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
            target_link_libraries(rune_d9_cross_node_marker_tests PRIVATE rune_node rune_replication aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_d9_cross_node_marker_tests PROPERTIES TIMEOUT 120 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")

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
                PROPERTIES TIMEOUT 180
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
                ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
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

        # Phase 2c Task 8: the Worker serves FILTERED subscriptions through FilterGroups (FilterSchema op, the filter
        # tail on Subscribe, member binding, the filtered push pass); same rig as rune_worker_push_tests.
        add_executable(rune_worker_filter_tests
            tests/node/WorkerFilterPushTest.cpp)
        target_include_directories(rune_worker_filter_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${RUNE_CLIENT_ROOT}/examples)
        target_link_libraries(rune_worker_filter_tests PRIVATE
            rune_node
            rune_codec
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_worker_filter_tests
            PROPERTIES TIMEOUT 180
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
        # Catalog history Task 17: the tests define their topics through TestCluster (tests/support).
        target_include_directories(rune_gateway_failover_replication_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_gateway_failover_replication_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        # Ports from the collision-proof allocator (FreePort.h): runs in parallel.
        gtest_discover_tests(rune_gateway_failover_replication_tests
            PROPERTIES TIMEOUT 300)
    endif()

    # ---------------------------------------------------------------------------
    # DUR-1 round-4 follow-up LOW-B: the consensus candidacy gate + template-58
    # knowledge exchange + internal-ISR term source driven end to end over real
    # Aeron (in-process BrokerNodes on loopback, no Docker).  *Regression*.
    # ---------------------------------------------------------------------------
    # Authentication Task 15: the inter-broker link handshakes (agent over one driver; three in-process brokers).
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_peer_link_agent_tests
            tests/integration/PeerLinkAgentTest.cpp
            tests/integration/PeerSealedClusterTest.cpp)
        target_include_directories(rune_peer_link_agent_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_peer_link_agent_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_peer_link_agent_tests
            PROPERTIES TIMEOUT 240)
    endif()

    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_consensus_candidacy_tests
            tests/integration/ConsensusCandidacyElectionTest.cpp)
        target_include_directories(rune_consensus_candidacy_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_consensus_candidacy_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_consensus_candidacy_tests
            PROPERTIES TIMEOUT 180)
    endif()

    # ---------------------------------------------------------------------------
    # E5 Task 7: PerWorkerReplicationTest — in-process per-worker replication
    # integration + failover suite (perWorkerReplication flag ON).
    #
    # 3 BrokerNodes with perWorkerReplication=true, workerCount=2, replBaseStreamId=200.
    # Real aeron:udp — no Docker, no InMemoryTransport.
    #
    # Tests:
    #   AcksAllThreeNodeReplicatesAndCommits — FetchRequest(1)/FetchResponse(2) E2E.
    #   RollingLeaderKillNoCommittedLossRegression (*Regression*) — OLE+reconcile(3-6).
    #   EpochChangeTruncatesDivergentTailOnly — divergent-tail truncation path.
    #
    # Ports from the collision-proof allocator (FreePort.h): runs in parallel.
    # ---------------------------------------------------------------------------
    if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)
        add_executable(rune_per_worker_replication_integration_tests
            tests/integration/PerWorkerReplicationTest.cpp)
        # Catalog history Task 17: the tests define their topics through TestCluster (tests/support).
        target_include_directories(rune_per_worker_replication_integration_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_per_worker_replication_integration_tests PRIVATE
            rune_node
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_per_worker_replication_integration_tests
            PROPERTIES TIMEOUT 360)

        # Self-contained worker shards (spec §3.1): per-worker invoker driver +
        # client, ShardAgent pass, shard replication planes over real UDP.
        add_executable(rune_shard_mode_tests tests/integration/ShardModeTest.cpp)
        target_include_directories(rune_shard_mode_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_shard_mode_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_shard_mode_tests PROPERTIES TIMEOUT 360)

        # Phase 2b E2: TRANSIENT topics end to end in-process over real Aeron —
        # shard + legacy produce/subscribe, 3-node acks=all, restart (empty).
        add_executable(rune_transient_topic_data_path_tests tests/integration/TransientTopicDataPathTest.cpp)
        target_include_directories(rune_transient_topic_data_path_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_transient_topic_data_path_tests PRIVATE rune_node rune_view aeron::aeron
                              GTest::gtest_main)
        gtest_discover_tests(rune_transient_topic_data_path_tests PROPERTIES TIMEOUT 360)

        # Catalog history Task 11 (F7): the REAL client (rune-client-cpp, embedded by
        # the root CMakeLists) against a REAL in-process single-node BrokerNode.
        # Task 15: rune_cli_lib — the real `rune` CLI against the real nodes.
        add_executable(rune_client_broker_tests tests/client/ClientDdlBrokerRegressionTest.cpp
                       tests/client/ClientCatalogReadBrokerRegressionTest.cpp
                       tests/client/RuneCliBrokerRegressionTest.cpp
                       tests/client/ClientConnectRoleRegressionTest.cpp
                       tests/client/ClientDdlClusterRegressionTest.cpp
                       tests/client/ClientLoginBrokerRegressionTest.cpp
                       tests/client/RuneCliSecurityBrokerRegressionTest.cpp
                       tests/client/ClientSessionRevokedBrokerRegressionTest.cpp
                       tests/client/ClientAuthClusterRegressionTest.cpp
                       tests/client/RuneConsoleBrokerRegressionTest.cpp
                       tests/client/ClaimBrokerRegressionTest.cpp
                       tests/client/ProducerIdBrokerRegressionTest.cpp)
        # Phase 2b B Task 8b: ClaimBrokerRegressionTest compiles against the client's build-time generated all-fixed
        # `position.gen.h` (rune_gen_claim_schemas); the embedded client builds it under its own binary dir.
        add_dependencies(rune_client_broker_tests generate_claim_schema_headers)
        target_include_directories(rune_client_broker_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests
                                   ${CMAKE_BINARY_DIR}/rune-client/claim-gen)
        target_link_libraries(rune_client_broker_tests PRIVATE rune_node rune_view rune_codec rune_catalog
                              rune_client_api rune_cli_lib aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_client_broker_tests PROPERTIES TIMEOUT 600)

        # Authentication Task 11: the SCRAM login in the session handshake against ONE real authenticating broker
        # (raw frames: AuthWireClient + the rune-crypto SCRAM client).
        add_executable(rune_auth_login_tests tests/node/AuthLoginTest.cpp)
        target_include_directories(rune_auth_login_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_auth_login_tests PRIVATE rune_node rune_view rune_codec rune_catalog aeron::aeron
                              GTest::gtest_main)
        gtest_discover_tests(rune_auth_login_tests PROPERTIES TIMEOUT 600)

        # Authentication Task 12: the per-session permission tables and the enforcement of every request type against
        # ONE real authenticating broker (raw frames): AuthEnforcement (the §2.1 map), AuthVisibility (hidden == missing),
        # CatalogPrincipal (the catalog / security events record the authenticated user).
        add_executable(rune_auth_enforcement_tests tests/node/AuthEnforcementTest.cpp
                       tests/node/AuthVisibilityTest.cpp tests/node/CatalogPrincipalTest.cpp
                       tests/node/AuthLiveChangeTest.cpp tests/node/SecurityHaltTest.cpp)
        target_include_directories(rune_auth_enforcement_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_auth_enforcement_tests PRIVATE rune_node rune_view rune_codec rune_catalog aeron::aeron
                              GTest::gtest_main)
        gtest_discover_tests(rune_auth_enforcement_tests PROPERTIES TIMEOUT 600)

        # Authentication Task 17: the 720-cell request x grant x timing interaction matrix (spec section 11) against ONE
        # real authenticating broker (shard runtime); the expectations are support/AuthMatrixExpectations.h.
        add_executable(rune_auth_matrix_tests tests/node/AuthInteractionMatrixRegressionTest.cpp)
        target_include_directories(rune_auth_matrix_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_auth_matrix_tests PRIVATE rune_node rune_view rune_codec rune_catalog aeron::aeron
                              GTest::gtest_main)
        gtest_discover_tests(rune_auth_matrix_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 900 LABELS small)
        rune_regression_batches(rune_auth_matrix_tests 4)   # the 720 cells run in the *Regression* gate as 4 sharded batches

        # Authentication Task 13: shard-mode produce straight to the worker ingress against ONE real authenticating
        # broker (the worker's WRITE bitmaps shipped on the lifecycle ring) + the zero-allocation gates.
        add_executable(rune_shard_auth_produce_tests tests/integration/ShardAuthProduceTest.cpp)
        target_include_directories(rune_shard_auth_produce_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_shard_auth_produce_tests PRIVATE rune_node rune_view rune_codec rune_catalog aeron::aeron
                              GTest::gtest_main)
        gtest_discover_tests(rune_shard_auth_produce_tests PROPERTIES TIMEOUT 600)

        # Authentication Task 19: THREE real authenticating brokers (sealed peer links): __security replication and
        # digests, logins via every broker, the 100 ms revoke gate, leader kill mid security DDL, peer keys, rotation.
        add_executable(rune_auth_cluster_tests tests/integration/AuthClusterIntegrationTest.cpp)
        target_include_directories(rune_auth_cluster_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_auth_cluster_tests PRIVATE rune_node rune_view rune_codec rune_catalog aeron::aeron
                              GTest::gtest_main)
        gtest_discover_tests(rune_auth_cluster_tests PROPERTIES TIMEOUT 900)

        # Authentication Task 18: seeded, model-checked stress - racing security DDL vs produce / subscribe / create against
        # an independent permission model (support/PermissionModel.h).
        add_executable(rune_auth_stress_tests tests/node/AuthStressRegressionTest.cpp tests/node/AuthDdlAnswerStallRegressionTest.cpp)
        target_include_directories(rune_auth_stress_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_auth_stress_tests PRIVATE rune_node rune_view rune_codec rune_catalog aeron::aeron
                              GTest::gtest_main)
        gtest_discover_tests(rune_auth_stress_tests PROPERTIES TIMEOUT 900)

        # DR client bugs (2026-10-15): the REAL client against 3 REAL in-process brokers, a leader SIGKILLed mid-op
        # (seed failover, metadata from any live broker, re-route; produce / subscribe / group / txn / DDL / view row).
        # Phase 2b F Task 11: EosBrokerRegressionTest.cpp - the unified transactional Producer against 3 real brokers
        # (every path, fencing, in-doubt commit, leader crash, abort-rewind-redo, plain dedup across a reconnect);
        # it kills brokers, so it lives here (RUN_SERIAL).  It reads client-cpp's TxnMatrixExpectations.h and the
        # build-time generated all-fixed position.gen.h (typed claim through ProducerRef).
        add_executable(rune_client_failover_broker_tests tests/client/ClientFailoverBrokerRegressionTest.cpp
                       tests/client/EosBrokerRegressionTest.cpp
                       tests/client/FilterBrokerRegressionTest.cpp)
        add_dependencies(rune_client_failover_broker_tests generate_claim_schema_headers)
        target_include_directories(rune_client_failover_broker_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests
                                   ${RUNE_CLIENT_ROOT}/tests ${CMAKE_BINARY_DIR}/rune-client/claim-gen
                                   ${CMAKE_BINARY_DIR}/rune-client/claim-gen/gen ${RUNE_CLIENT_ROOT}/examples)
        target_link_libraries(rune_client_failover_broker_tests PRIVATE rune_node rune_view rune_codec rune_catalog
                              rune_client_api aeron::aeron GTest::gtest_main)
        target_compile_definitions(rune_client_failover_broker_tests PRIVATE
            RUNE_CRYPTO_TESTDATA_DIR="${RUNE_CRYPTO_TESTDATA_GEN}")   # fix round 3: TLS failover cells
        gtest_discover_tests(rune_client_failover_broker_tests PROPERTIES TIMEOUT 600 RUN_SERIAL TRUE)

        # Catalog history Task 11: the DDL route (Redirect / NotLeader / Internal when
        # halted) and the halted controller's resignation, 3 brokers over real Aeron.
        add_executable(rune_catalog_leadership_broker_tests tests/integration/CatalogLeadershipBrokerTest.cpp)
        target_include_directories(rune_catalog_leadership_broker_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_catalog_leadership_broker_tests PRIVATE rune_node rune_view rune_codec aeron::aeron
                              GTest::gtest_main)
        gtest_discover_tests(rune_catalog_leadership_broker_tests PROPERTIES TIMEOUT 360)

        # Catalog history Task 12: a Subscribe on __catalog served by every broker
        # (leader, follower, halted) over real Aeron — backlog, marker, live, close.
        add_executable(rune_catalog_read_broker_tests tests/integration/CatalogReadBrokerTest.cpp)
        target_include_directories(rune_catalog_read_broker_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_catalog_read_broker_tests PRIVATE rune_node rune_view rune_codec aeron::aeron
                              GTest::gtest_main)
        gtest_discover_tests(rune_catalog_read_broker_tests PROPERTIES TIMEOUT 360)

        # Catalog history Task 6: the __catalog partition on every broker —
        # real replication on its admin-thread plane, controller-ISR leader
        # (never unclean), new / wiped brokers refetch from 0; the admin/control
        # role + ISR relay units.
        add_executable(rune_catalog_replication_tests tests/integration/CatalogReplicationTest.cpp
                tests/integration/StorageLossClusterTest.cpp
                tests/integration/SecurityReplicationTest.cpp
                tests/integration/AuthBootstrapWindowTest.cpp)
        target_include_directories(rune_catalog_replication_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_catalog_replication_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_catalog_replication_tests PROPERTIES TIMEOUT 360)

        # Catalog history Task 22: the interaction matrix's 3-node slice (32 cells: runtime DDL through every
        # broker, a catalog-leader crash mid-DDL, data through every broker) + the binding regressions B1/B3/B4,
        # 3 BrokerNodes over real Aeron.  Closes the E2 multi-node DDL carry-over.
        add_executable(rune_catalog_integration_tests tests/integration/CatalogClusterIntegrationTest.cpp)
        target_include_directories(rune_catalog_integration_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_catalog_integration_tests PRIVATE rune_node rune_view rune_codec rune_catalog
                              aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_catalog_integration_tests PROPERTIES TIMEOUT 600)

        # Catalog history Task 8: the control thread executes the catalog's effects
        # (topics created / dropped at catalog ids, placement add / remove, config
        # override, view uninstall; every effect idempotent) on a real BrokerNode.
        add_executable(rune_catalog_effects_tests tests/node/CatalogEffectsTest.cpp)
        target_include_directories(rune_catalog_effects_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_catalog_effects_tests PRIVATE rune_node rune_view rune_codec aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_catalog_effects_tests PROPERTIES TIMEOUT 360)

        # Catalog history Task 16: the broker boots from __catalog (the one boot
        # replay on the startup thread, topics built from the catalog, dropped ids
        # tombstoned, the topic-dir reconcile) — real single-node and 3-node brokers.
        add_executable(rune_catalog_boot_tests tests/node/CatalogBootTest.cpp)
        target_include_directories(rune_catalog_boot_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_catalog_boot_tests PRIVATE rune_node rune_view rune_codec aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_catalog_boot_tests PROPERTIES TIMEOUT 360)

        # Catalog history Task 17: TestCluster::defineTopic — the in-process way to create a
        # topic now that the static topic list / NodeConfig::topics are gone (DDL over the wire on real
        # single-node and 3-node brokers), and the FD budget counted from the catalog's topics.
        add_executable(rune_test_cluster_tests tests/node/TestClusterTest.cpp)
        target_include_directories(rune_test_cluster_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_test_cluster_tests PRIVATE rune_node rune_view rune_codec aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_test_cluster_tests PROPERTIES TIMEOUT 360)

        # Leader-epoch monotonicity across restarts (Kafka parity): full cold
        # restart, single-node restart, stale same-epoch tail on a follower.
        add_executable(rune_epoch_restart_tests tests/integration/EpochRestartRegressionTest.cpp)
        target_include_directories(rune_epoch_restart_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_epoch_restart_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_epoch_restart_tests PROPERTIES TIMEOUT 360)

        # Controlled shutdown (Kafka parity): a gracefully stopping node hands
        # its partitions (and the controller its consensus leadership) off
        # before it exits.
        add_executable(rune_controlled_shutdown_e2e_tests tests/integration/ControlledShutdownRegressionTest.cpp)
        target_include_directories(rune_controlled_shutdown_e2e_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_controlled_shutdown_e2e_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_controlled_shutdown_e2e_tests PROPERTIES TIMEOUT 360)
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
    target_include_directories(rune_psm_hw_bound_tests PRIVATE
        ${RUNE_BROKER_DIR}/libs/aeronutil/include)   # header-only testing/FreePort.h
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
        # Catalog history Task 17: the tests define their topics through TestCluster (tests/support).
        target_include_directories(rune_gateway_psm_failover_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_gateway_psm_failover_tests PRIVATE
            rune_node
            rune_storage
            rune_replication
            aeron::aeron
            GTest::gtest_main)
        gtest_discover_tests(rune_gateway_psm_failover_tests
            PROPERTIES TIMEOUT 300)
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
