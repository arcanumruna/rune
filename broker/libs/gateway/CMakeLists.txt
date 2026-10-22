cmake_minimum_required(VERSION 3.20)

# rune_gateway — ClientGateway + SessionTable (BR-4) + TopicRegistry (GB-1)
# E5.1.2d: ProducePath.cpp was moved to rune_storage (leaf lib) — it used to
# live here as a workaround for the rune_node ↔ rune_gateway circular dep.
# Now both rune_gateway and rune_node depend on rune_storage which holds the
# implementation, cleanly eliminating the circular-dep workaround.
add_library(rune_gateway
    src/ClientGateway.cpp
    src/TopicRegistry.cpp
    src/TopicDirMarker.cpp
    src/SecurityView.cpp
    src/LoginThrottle.cpp
    src/SessionAuth.cpp
    src/PermissionTable.cpp
    src/PermissionPool.cpp
    src/Authorizer.cpp
    src/LiveSecurityChanges.cpp
    src/ScramServer.cpp
    src/FilterAdmission.cpp)

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
target_link_libraries(rune_gateway PUBLIC rune_storage rune_replication rune_transaction rune_metadata rune_consensus rune_aeron_util)
target_include_directories(rune_gateway PUBLIC ${RUNE_NODE_INCLUDE})
# client<->broker TLS/AEAD (Task 7): SessionTable.h (public header) now holds a
# Session::CryptoState built from rune_crypto's AeadCipher; PUBLIC so every
# consumer of this header (rune_node, its tests/integration/*, and everything
# PUBLIC-linking rune_node transitively) gets rune_crypto's include dir too —
# see the ★ BUILD/LINKAGE TRAP comment on rune_crypto's add_library() in the
# top-level CMakeLists.txt.
target_link_libraries(rune_gateway PUBLIC rune_crypto)
# Authentication plan Task 9: the control thread's SecurityView (rune_security) and the cluster keys (rune_peerlink).
target_link_libraries(rune_gateway PUBLIC rune_security rune_peerlink)
# rune_codec PUBLIC: Worker.h (included transitively via WorkerPool.h) now
# includes rune/codec/Validator.h for the Task 7 per-worker schema validation
# hook.  Consumers that include Worker.h via RUNE_NODE_INCLUDE need rune_codec headers.
target_link_libraries(rune_gateway PUBLIC rune_codec)
# rune_view PUBLIC: Worker.h (included transitively via WorkerPool.h) now
# includes rune/node/WorkerViewSet.h (Task 15 view-fold integration), which
# pulls rune/view/* headers.  Consumers that include Worker.h via
# RUNE_NODE_INCLUDE need rune_view headers.  rune_view is a leaf lib (depends
# only on rune_codec) so this introduces no circular dependency.
target_link_libraries(rune_gateway PUBLIC rune_view)

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

    # Authentication Task 11: the brute-force curve (D14) - pure, no Aeron.
    add_executable(rune_gateway_login_throttle_tests tests/LoginThrottleTest.cpp)
    target_link_libraries(rune_gateway_login_throttle_tests PRIVATE rune_gateway GTest::gtest_main)
    gtest_discover_tests(rune_gateway_login_throttle_tests)

    # Authentication Task 11: the broker half of SCRAM as pure functions.
    add_executable(rune_gateway_scram_server_tests tests/ScramServerTest.cpp)
    target_include_directories(rune_gateway_scram_server_tests PRIVATE ${RUNE_BROKER_DIR}/libs/node/tests)
    target_link_libraries(rune_gateway_scram_server_tests PRIVATE rune_gateway GTest::gtest_main)
    gtest_discover_tests(rune_gateway_scram_server_tests)

    # Authentication Task 11: the gateway's login state machine (gate, SCRAM legs, throttle, deadline, counters) - no Aeron.
    add_executable(rune_gateway_auth_login_tests tests/gateway/AuthLoginGatewayTest.cpp)
    target_include_directories(rune_gateway_auth_login_tests PRIVATE ${RUNE_BROKER_DIR}/libs/node/tests)
    target_link_libraries(rune_gateway_auth_login_tests PRIVATE rune_gateway rune_node spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_auth_login_tests)

    # Authentication Task 12: the per-session permission table, the object index and the Authorizer - pure, no Aeron.
    add_executable(rune_gateway_permission_table_tests tests/PermissionTableTest.cpp)
    target_link_libraries(rune_gateway_permission_table_tests PRIVATE rune_gateway GTest::gtest_main)
    gtest_discover_tests(rune_gateway_permission_table_tests)

    # Authentication Task 14: what a permission refresh ends for one session - pure, no Aeron.
    add_executable(rune_gateway_live_security_changes_tests tests/LiveSecurityChangesTest.cpp)
    target_link_libraries(rune_gateway_live_security_changes_tests PRIVATE rune_gateway GTest::gtest_main)
    gtest_discover_tests(rune_gateway_live_security_changes_tests)

    # Authentication Task 9: the control thread's SecurityView (D29).
    add_executable(rune_gateway_security_view_tests
        tests/gateway/SecurityViewTest.cpp)
    target_link_libraries(rune_gateway_security_view_tests PRIVATE
        rune_gateway
        GTest::gtest_main)
    gtest_discover_tests(rune_gateway_security_view_tests)

    # -------------------------------------------------------------------------
    # client<->broker TLS/AEAD (Task 7 handshake + Task 8 data-plane AEAD):
    # gateway server-role crypto — Aeron-free (no ClientGateway/SessionTable
    # involved; see the file's own header comment for why). Links rune_crypto
    # + GTest ONLY (not rune_gateway, which would drag in <Aeron.h>). Task 8
    # added direct #includes of rune/gateway/{CryptoState,CryptoFrame}.h (both
    # aeron-free — CryptoState was extracted out of SessionTable.h specifically
    # so this target could use the REAL type) and rune/protocol/client/
    # {CryptoEnvelope,MessageHeader}.h, so this target needs this lib's own
    # include/ dir and the protocol include dir too (rune_gateway itself gets
    # both PUBLIC above; this target deliberately skips linking rune_gateway).
    # Task 8b additionally #includes rune/node/RecordsSeal.h (the Records-push
    # seal step — aeron-free by construction, unlike RecordsPush.h itself, see
    # that header's own comment), so this target also needs libs/node's
    # include/ dir; it still deliberately skips linking rune_node (which would
    # drag in <Aeron.h> via RecordsPush.h's #include <ExclusivePublication.h>).
    # -------------------------------------------------------------------------
    add_executable(rune_gateway_crypto_tests
        tests/GatewayCryptoTest.cpp)
    target_include_directories(rune_gateway_crypto_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../node/include
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_gateway_crypto_tests PRIVATE
        rune_crypto
        GTest::gtest_main)
    target_compile_definitions(rune_gateway_crypto_tests PRIVATE
        RUNE_CRYPTO_TESTDATA_DIR="${RUNE_CRYPTO_TESTDATA_GEN}")
    gtest_discover_tests(rune_gateway_crypto_tests)

    # -------------------------------------------------------------------------
    # Task 15: SECURITY REGRESSION SUITE (named `*Regression*` so it is
    # selected as the run-first priority gate per CLAUDE.md rule 6/7).
    # Consolidates the tamper/AAD/replay/cross-session/per-sender/downgrade/
    # handshake security properties proven piecemeal across
    # rune_gateway_crypto_tests + rune_gateway_require_tls_tests, plus the
    # gaps GatewayCryptoRegressionTest.cpp's own file header documents.
    # Same aeron-free build shape as rune_gateway_crypto_tests above (links
    # rune_crypto + GTest only; needs this lib's + libs/node's include/ dirs
    # + the protocol include dir, but deliberately skips linking
    # rune_gateway/rune_node to stay off <Aeron.h>).
    # -------------------------------------------------------------------------
    add_executable(rune_gateway_crypto_regression_tests
        tests/GatewayCryptoRegressionTest.cpp)
    target_include_directories(rune_gateway_crypto_regression_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../node/include
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_gateway_crypto_regression_tests PRIVATE
        rune_crypto
        GTest::gtest_main)
    target_compile_definitions(rune_gateway_crypto_regression_tests PRIVATE
        RUNE_CRYPTO_TESTDATA_DIR="${RUNE_CRYPTO_TESTDATA_GEN}")
    gtest_discover_tests(rune_gateway_crypto_regression_tests)

    # -------------------------------------------------------------------------
    # Task 9: require_tls / shard-mode downgrade-safety decision logic
    # (ConnectDowngradePolicy.h) + cert/key FILE PATH -> PEM TEXT loading
    # (rune/node/PemFile.{h,cpp}). Aeron-free for the same reason as
    # rune_gateway_crypto_tests above: compiles PemFile.cpp directly (NOT
    # rune_node, which would drag in Worker.h -> RecordsPush.h -> Aeron) +
    # rune_crypto + GTest only.
    # -------------------------------------------------------------------------
    add_executable(rune_gateway_require_tls_tests
        tests/RequireTlsDowngradeTest.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../node/src/PemFile.cpp)
    target_include_directories(rune_gateway_require_tls_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../node/include
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_gateway_require_tls_tests PRIVATE
        rune_crypto
        GTest::gtest_main)
    target_compile_definitions(rune_gateway_require_tls_tests PRIVATE
        RUNE_CRYPTO_TESTDATA_DIR="${RUNE_CRYPTO_TESTDATA_GEN}")
    gtest_discover_tests(rune_gateway_require_tls_tests)

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
    # TopicRegistry runtime tests (Task 5) — createTopic + regression guards.
    # No Aeron, real filesystem + rune_storage.
    # -------------------------------------------------------------------------
    add_executable(rune_gateway_topic_registry_runtime_tests
        tests/gateway/TopicRegistryRuntimeTest.cpp)
    target_link_libraries(rune_gateway_topic_registry_runtime_tests PRIVATE
        rune_gateway
        GTest::gtest_main)
    gtest_discover_tests(rune_gateway_topic_registry_runtime_tests)

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

    # Task 13 (spec §3.4): post-ack notifications + session close on every worker.
    add_executable(rune_gateway_shard_notify_tests tests/gateway/ShardNotifyTest.cpp)
    target_link_libraries(rune_gateway_shard_notify_tests PRIVATE rune_gateway rune_node spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_shard_notify_tests)

    # 2026-10-05: record-egress publication lifetime (retire-on-ack) — no Aeron.
    add_executable(rune_gateway_record_egress_lifetime_tests tests/gateway/RecordEgressLifetimeTest.cpp)
    target_link_libraries(rune_gateway_record_egress_lifetime_tests PRIVATE rune_gateway rune_node spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_record_egress_lifetime_tests)

    # 2026-10-05: (session, correlationId) = ONE subscription (duplicate Subscribe dedupe) — no Aeron.
    add_executable(rune_gateway_subscribe_dedupe_tests tests/gateway/SubscribeDedupeTest.cpp)
    target_link_libraries(rune_gateway_subscribe_dedupe_tests PRIVATE rune_gateway rune_node spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_subscribe_dedupe_tests)

    # Phase 2c Task 9: filtered Subscribe admission, caps, refusal details, ordered schema fan-out - no Aeron.
    add_executable(rune_gateway_filter_subscribe_tests tests/gateway/FilterSubscribeGatewayTest.cpp)
    target_link_libraries(rune_gateway_filter_subscribe_tests PRIVATE rune_gateway rune_node rune_codec spdlog::spdlog GTest::gtest_main)
    target_include_directories(rune_gateway_filter_subscribe_tests PRIVATE ${RUNE_CLIENT_ROOT}/examples)
    gtest_discover_tests(rune_gateway_filter_subscribe_tests)

    # DDL conformance final review C1: an uncommitted runtime topic serves nothing — no Aeron.
    add_executable(rune_gateway_topic_servability_tests tests/gateway/TopicServabilityTest.cpp)
    target_link_libraries(rune_gateway_topic_servability_tests PRIVATE rune_gateway rune_node spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_topic_servability_tests)

    # Catalog history Task 8: C1 generalized (applied AND placed) + dropped topics — no Aeron.
    add_executable(rune_gateway_catalog_tests tests/gateway/CatalogGatewayTest.cpp)
    target_link_libraries(rune_gateway_catalog_tests PRIVATE rune_gateway rune_node spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_catalog_tests)

    # Issues #109 / #110 / #196: the D24 gate waits for the workers' enforcement-fence acknowledgements - no Aeron.
    add_executable(rune_gateway_security_fence_tests tests/gateway/SecurityEnforcementFenceTest.cpp)
    target_include_directories(rune_gateway_security_fence_tests PRIVATE ${RUNE_BROKER_DIR}/libs/node/tests)
    target_link_libraries(rune_gateway_security_fence_tests PRIVATE rune_gateway rune_node spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_security_fence_tests)

    # Authentication Task 10: the bootstrap proof (D4), the __security route, Subscribe -7 and the pre-auth gate - no Aeron.
    add_executable(rune_gateway_security_tests tests/gateway/SecurityGatewayTest.cpp)
    target_include_directories(rune_gateway_security_tests PRIVATE ${RUNE_BROKER_DIR}/libs/node/tests)
    target_link_libraries(rune_gateway_security_tests PRIVATE rune_gateway rune_node spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_security_tests)

    # Phase 2b E2: TRANSIENT partitions served through their owning worker (C1-gated) — no Aeron.
    add_executable(rune_gateway_transient_tests tests/gateway/TransientGatewayTest.cpp)
    target_link_libraries(rune_gateway_transient_tests PRIVATE rune_gateway rune_node rune_replication rune_storage
                          rune_transaction spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_transient_tests)

    # Phase 2b E2: the transient interaction matrix (1008 TEST_P cells) — no Aeron.
    add_executable(rune_gateway_transient_matrix_tests tests/gateway/TransientInteractionMatrixTest.cpp)
    target_include_directories(rune_gateway_transient_matrix_tests PRIVATE ${RUNE_BROKER_DIR}/libs/node/tests)
    target_link_libraries(rune_gateway_transient_matrix_tests PRIVATE rune_gateway rune_node rune_replication
                          rune_storage rune_transaction spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_transient_matrix_tests DISCOVERY_TIMEOUT 120 PROPERTIES LABELS small)
    rune_regression_batches(rune_gateway_transient_matrix_tests 1)

    # D4: a follower redirects group / txn coordinator requests to the leader — no Aeron.
    add_executable(rune_gateway_coordinator_redirect_tests tests/gateway/CoordinatorRedirectTest.cpp)
    target_link_libraries(rune_gateway_coordinator_redirect_tests PRIVATE rune_gateway spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_coordinator_redirect_tests)

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
                rune_node   # F19: worker-return push path (WorkerPool)
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

            # SPIKE (throwaway): per-worker MDC egress mechanic de-risk (T3/T4).
            add_executable(rune_mdc_egress_spike_tests
                tests/integration/MdcEgressSpikeTest.cpp)
            target_link_libraries(rune_mdc_egress_spike_tests PRIVATE
                rune_gateway
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_mdc_egress_spike_tests
                PROPERTIES TIMEOUT 90)

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
