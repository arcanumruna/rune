cmake_minimum_required(VERSION 3.31)
project(rune_client_cpp)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# rune-protocol is header-only; consumed via the sibling source include path.
set(RUNE_PROTOCOL_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include"
    CACHE PATH "rune-protocol include dir")

find_package(aeron)
# macOS/armv8: aeron ConanCenter recipe is unsupported there (see conanfile.py),
# so it's skipped and find_package finds nothing.  Provide a stub INTERFACE target
# so `target_link_libraries(... aeron::aeron)` resolve and CMake CONFIGURE succeeds
# for IDE loading on a Mac.  Real builds/tests run on Linux with the real aeron.
if(NOT TARGET aeron::aeron)
    message(WARNING "aeron not found — creating a STUB aeron::aeron INTERFACE target "
                    "(configure-only; client build/run requires real aeron on Linux).")
    add_library(rune_aeron_stub INTERFACE)
    add_library(aeron::aeron ALIAS rune_aeron_stub)
endif()
find_package(spdlog REQUIRED)

option(ENABLE_COVERAGE "Enable coverage instrumentation" OFF)
if(ENABLE_COVERAGE)
    add_compile_options(--coverage -O0 -g)
    add_link_options(--coverage)
endif()

option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
endif()

option(BUILD_BENCHMARKS "Build benchmarks" OFF)
if(BUILD_BENCHMARKS)
    find_package(benchmark REQUIRED)
endif()

# ── rune-fr flight recorder (sibling static lib) ─────────────────────────────
# Mirrors the broker's wiring so the client can be traced with the SAME rune-fr
# (compile-in default; runtime-disabled until RUNE_FR_ENABLE=1).
option(ENABLE_FLIGHT_RECORDER "Compile in rune-fr flight-recorder instrumentation" ON)
set(RUNE_FR_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../rune-fr" CACHE PATH "Path to the rune-fr sibling repo")
add_library(rune_fr STATIC
    ${RUNE_FR_DIR}/src/Calibration.cpp
    ${RUNE_FR_DIR}/src/Enable.cpp
    ${RUNE_FR_DIR}/src/Registry.cpp
    ${RUNE_FR_DIR}/src/Report.cpp)
target_include_directories(rune_fr PUBLIC ${RUNE_FR_DIR}/include)
if(aeron_FOUND)
    target_link_libraries(rune_fr PUBLIC aeron::aeron)
endif()
target_compile_features(rune_fr PUBLIC cxx_std_23)
if(ENABLE_FLIGHT_RECORDER)
    target_compile_definitions(rune_fr PUBLIC RUNE_FR=1)
else()
    target_compile_definitions(rune_fr PUBLIC RUNE_FR=0)
endif()

# ── rune-codec (0a) — consumed as a SIBLING source static lib (like rune-fr) ──
set(RUNE_CODEC_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rune-codec"
    CACHE PATH "Path to the rune-codec sibling repo")
add_library(rune_codec STATIC
    ${RUNE_CODEC_ROOT}/src/Serialize.cpp
    ${RUNE_CODEC_ROOT}/src/Record.cpp
    ${RUNE_CODEC_ROOT}/src/Validator.cpp
    ${RUNE_CODEC_ROOT}/src/Generator.cpp)
target_include_directories(rune_codec PUBLIC ${RUNE_CODEC_ROOT}/include)
target_compile_features(rune_codec PUBLIC cxx_std_23)

# ── rune_client — trivial header-only/interface library ──────────────────────
add_library(rune_client INTERFACE)
target_include_directories(rune_client INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(rune_client INTERFACE spdlog::spdlog)
if(aeron_FOUND)
    target_link_libraries(rune_client INTERFACE aeron::aeron)
endif()

# ── rune_client_transport — AeronTransport (compiled, links aeronmd C driver) ─
if(aeron_FOUND)
    add_library(rune_client_transport STATIC
        src/transport/AeronTransport.cpp
    )
    target_include_directories(rune_client_transport PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(rune_client_transport PUBLIC aeron::aeron spdlog::spdlog)

    # Linux CI portability: libuuid is needed on Linux but not macOS (no-op there).
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_transport PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── rune_client_agent — ClientAgent (CC-5, compiled, links transport) ─────────
if(aeron_FOUND)
    add_library(rune_client_agent STATIC
        src/agent/ClientAgent.cpp
    )
    target_include_directories(rune_client_agent PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(rune_client_agent PUBLIC
        rune_client_transport
        aeron::aeron
        spdlog::spdlog
        rune_codec
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_agent PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── rune_client_producer — Producer (CB-2 + TX-5, compiled, links agent) ──────
if(aeron_FOUND)
    add_library(rune_client_producer STATIC
        src/producer/ProducerImpl.cpp
        src/producer/Producer.cpp
        src/producer/ProducerHub.cpp
        src/producer/BatchBuilder.cpp
        src/producer/TransactionalProducerImpl.cpp
        src/producer/TransactionalProducer.cpp
    )
    target_include_directories(rune_client_producer PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(rune_client_producer PUBLIC
        rune_client_agent
        aeron::aeron
        spdlog::spdlog
        rune_fr
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_producer PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── rune_client_subscriber — Subscription + SubscriberHub (CB-4) ──────────────
if(aeron_FOUND)
    add_library(rune_client_subscriber STATIC
        src/subscriber/Subscription.cpp
        src/subscriber/SubscriberHub.cpp
    )
    target_include_directories(rune_client_subscriber PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(rune_client_subscriber PUBLIC
        rune_client_agent
        aeron::aeron
        spdlog::spdlog
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_subscriber PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── rune_client_group — GroupConsumer + GroupConsumerHub (CG-5) ───────────────
add_library(rune_client_group STATIC
    src/group/GroupConsumer.cpp
    src/group/GroupConsumerHub.cpp
)
target_include_directories(rune_client_group PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(rune_client_group PUBLIC
    spdlog::spdlog
    rune_codec
)
if(aeron_FOUND)
    target_link_libraries(rune_client_group PUBLIC aeron::aeron)
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_group PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── rune_client_api — Client (CC-6, compiled, links agent) ────────────────────
if(aeron_FOUND)
    add_library(rune_client_api STATIC
        src/Client.cpp
    )
    target_include_directories(rune_client_api PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(rune_client_api PUBLIC
        rune_client_group
        rune_client_subscriber
        rune_client_producer
        rune_client_agent
        aeron::aeron
        spdlog::spdlog
    )

    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_api PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── Examples ──────────────────────────────────────────────────────────────────
option(BUILD_EXAMPLES "Build example programs" OFF)
if(BUILD_EXAMPLES AND aeron_FOUND)
    add_executable(rune_client_ping
        examples/rune_client_ping.cpp
    )
    target_include_directories(rune_client_ping PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_client_ping PRIVATE
        rune_client_api
    )
    # Linux: link libuuid (aeron embedded driver).
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_ping PRIVATE ${UUID_LIB})
        endif()
    endif()

    # CB-5 C — E2E produce/subscribe example vs 3-node cluster
    add_executable(rune_produce_subscribe
        examples/rune_produce_subscribe.cpp
    )
    target_include_directories(rune_produce_subscribe PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_produce_subscribe PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_produce_subscribe PRIVATE ${UUID_LIB})
        endif()
    endif()

    # Network-hop throughput load client (batched/pipelined produce over Aeron).
    add_executable(rune_produce_throughput
        examples/rune_produce_throughput.cpp
    )
    target_include_directories(rune_produce_throughput PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_produce_throughput PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
        rune_fr
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_produce_throughput PRIVATE ${UUID_LIB})
        endif()
    endif()

    # Network-hop READ throughput load client (subscribe/catch-up/tail over Aeron).
    add_executable(rune_consume_throughput
        examples/rune_consume_throughput.cpp
    )
    target_include_directories(rune_consume_throughput PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_consume_throughput PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB_CONSUME uuid)
        if(UUID_LIB_CONSUME)
            target_link_libraries(rune_consume_throughput PRIVATE ${UUID_LIB_CONSUME})
        endif()
    endif()

    # DR-1 — configurable disaster-recovery driver (rune-broker/dr-e2e harness)
    add_executable(dr_client
        examples/dr_client.cpp
    )
    target_include_directories(dr_client PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(dr_client PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(dr_client PRIVATE ${UUID_LIB})
        endif()
    endif()
endif()

# ── Benchmarks ────────────────────────────────────────────────────────────────
if(BUILD_BENCHMARKS AND aeron_FOUND)
    add_executable(rune_client_bench
        bench/ClientPingBench.cpp
    )
    target_include_directories(rune_client_bench PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_client_bench PRIVATE
        rune_client_api
        benchmark::benchmark
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_bench PRIVATE ${UUID_LIB})
        endif()
    endif()

    # CB-5 D — Produce RTT + subscribe delivery latency benchmark
    add_executable(rune_client_produce_bench
        bench/ClientProduceBench.cpp
    )
    target_include_directories(rune_client_produce_bench PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_client_produce_bench PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
        benchmark::benchmark
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_produce_bench PRIVATE ${UUID_LIB})
        endif()
    endif()

    # Accumulator engine micro-bench (in-process, no Aeron) — client batching throughput
    add_executable(rune_producer_accum_bench
        bench/ProducerAccumBench.cpp
    )
    target_include_directories(rune_producer_accum_bench PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_producer_accum_bench PRIVATE
        rune_client_api
        rune_client_producer
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_producer_accum_bench PRIVATE ${UUID_LIB})
        endif()
    endif()

    # CG-7 — GroupConsumer hot-path benchmarks (in-process, no Aeron)
    add_executable(rune_client_group_bench
        bench/GroupConsumerBench.cpp
    )
    target_include_directories(rune_client_group_bench PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_client_group_bench PRIVATE
        rune_client_group
        benchmark::benchmark
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_group_bench PRIVATE ${UUID_LIB})
        endif()
    endif()
endif()

# ── Tests ─────────────────────────────────────────────────────────────────────
if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    include(GoogleTest)

    # Conformance vector directory — the sibling rune-protocol repo.
    set(RUNE_CONFORMANCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/conformance")
    if(NOT EXISTS "${RUNE_CONFORMANCE_DIR}/vectors")
        message(FATAL_ERROR
            "CC-2: conformance vector directory not found: ${RUNE_CONFORMANCE_DIR}/vectors\n"
            "  The sibling rune-protocol repo must be present at ${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol")
    endif()

    add_executable(rune_client_tests
        tests/SmokeTest.cpp
        tests/ConformanceTest.cpp
        tests/DataConformanceTest.cpp
    )
    target_link_libraries(rune_client_tests PRIVATE rune_client GTest::gtest_main)
    target_include_directories(rune_client_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_compile_definitions(rune_client_tests PRIVATE
        RUNE_CONFORMANCE_DIR="${RUNE_CONFORMANCE_DIR}"
    )

    gtest_discover_tests(rune_client_tests)

    # ── Schema codec smoke test (0b Task 9) — no Aeron dependency ───────────────
    add_executable(rune_schema_codec_smoke_tests
        tests/SchemaCodecSmokeTest.cpp
    )
    target_link_libraries(rune_schema_codec_smoke_tests PRIVATE rune_codec GTest::gtest_main)
    gtest_discover_tests(rune_schema_codec_smoke_tests)

    # ── Session state machine tests (CC-4) — no Aeron dependency ──────────────
    add_executable(rune_session_tests
        tests/session/SessionMachineTest.cpp
    )
    target_link_libraries(rune_session_tests PRIVATE rune_client GTest::gtest_main)
    target_include_directories(rune_session_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})

    gtest_discover_tests(rune_session_tests)

    # ── Transport tests (CC-3) — embedded Aeron driver + ipc loopback echo ──
    if(aeron_FOUND)
        add_executable(rune_transport_tests
            tests/transport/AeronTransportTest.cpp
        )
        target_link_libraries(rune_transport_tests PRIVATE
            rune_client_transport
            GTest::gtest_main
        )

        gtest_discover_tests(rune_transport_tests)
    endif()

    # ── Agent tests (CC-5) — single-writer duty-cycle + ipc gateway loop ──────
    if(aeron_FOUND)
        add_executable(rune_agent_tests
            tests/agent/ClientAgentTest.cpp
        )
        target_include_directories(rune_agent_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_agent_tests PRIVATE
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_agent_tests)
    endif()

    # ── Task 1.3: Multi-node partition routing + multi-session registry ───────
    # Pure unit test: agent routing table (setRoute/routeEndpointFor) + session
    # registry (sessionForEndpoint/sessionCount) + MetadataCache.  No live driver.
    if(aeron_FOUND)
        add_executable(rune_multinode_routing_tests
            tests/agent/MultiNodeRoutingTest.cpp
        )
        target_include_directories(rune_multinode_routing_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_multinode_routing_tests PRIVATE
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_multinode_routing_tests)
    endif()

    # ── Client API tests (CC-6) — connect/ping futures + ipc gateway ──────────
    if(aeron_FOUND)
        add_executable(rune_client_api_tests
            tests/client/ClientTest.cpp
        )
        target_include_directories(rune_client_api_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_client_api_tests PRIVATE
            rune_client_api
            GTest::gtest_main
        )

        gtest_discover_tests(rune_client_api_tests)
    endif()

    # ── Task 10: Schema DDL round-trip tests (defineTopic / getTopicSchema) ─────
    # Uses the embedded Aeron IPC loopback gateway (same pattern as ClientTest).
    if(aeron_FOUND)
        add_executable(rune_schema_client_tests
            tests/client/ClientSchemaTest.cpp
        )
        target_include_directories(rune_schema_client_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_schema_client_tests PRIVATE
            rune_client_api
            GTest::gtest_main
        )

        gtest_discover_tests(rune_schema_client_tests)
    endif()

    # ── Redirect-follow tests (CB-5 B) — data-op redirect follow ──────────────
    if(aeron_FOUND)
        add_executable(rune_redirect_tests
            tests/client/RedirectTest.cpp
        )
        target_include_directories(rune_redirect_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_redirect_tests PRIVATE
            rune_client_api
            rune_client_producer
            rune_client_subscriber
            GTest::gtest_main
        )

        gtest_discover_tests(rune_redirect_tests)

        # ── Milestone D Phase-2 Task 14 (Note A) — bare NotLeader is retriable ──
        add_executable(rune_notleader_retry_tests
            tests/client/ClientNotLeaderRetryTest.cpp
        )
        target_include_directories(rune_notleader_retry_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_notleader_retry_tests PRIVATE
            rune_client_api
            rune_client_producer
            rune_client_subscriber
            GTest::gtest_main
        )

        gtest_discover_tests(rune_notleader_retry_tests)

        # ── MD1 Task 10 review — metadata routing against a real gateway ──────
        add_executable(rune_metadata_routing_tests
            tests/client/MetadataRoutingTest.cpp
        )
        target_include_directories(rune_metadata_routing_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_metadata_routing_tests PRIVATE
            rune_client_api
            rune_client_producer
            rune_client_subscriber
            GTest::gtest_main
        )
        gtest_discover_tests(rune_metadata_routing_tests)
    endif()

    # ── Producer tests (CB-2) — auto-batch, partitioning, acks, idempotence ──
    if(aeron_FOUND)
        add_executable(rune_producer_tests
            tests/producer/ProducerTest.cpp
        )
        target_include_directories(rune_producer_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_producer_tests PRIVATE
            rune_client_producer
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_producer_tests)
    endif()

    # ── Producer race regression tests (DR-BUG-1) ─────────────────────────────
    # Exercises the connect → immediate first-produce path with NO settle delay.
    # Must pass cleanly and TSan-clean after the lock-free snapshot fix.
    if(aeron_FOUND)
        add_executable(rune_producer_race_tests
            tests/producer/ProducerRaceTest.cpp
        )
        target_include_directories(rune_producer_race_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_producer_race_tests PRIVATE
            rune_client_api
            rune_client_producer
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_producer_race_tests)
    endif()

    # ── TX-5 Transactional producer + read_committed consumer tests ─────────────
    if(aeron_FOUND)
        add_executable(rune_txn_producer_tests
            tests/producer/TransactionalProducerTest.cpp
        )
        target_include_directories(rune_txn_producer_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_txn_producer_tests PRIVATE
            rune_client_producer
            rune_client_group
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_txn_producer_tests)
    endif()

    # ── BatchBuilder tests (CB-3) — Tier-2 explicit atomic batch ──────────────
    if(aeron_FOUND)
        add_executable(rune_batch_builder_tests
            tests/producer/BatchBuilderTest.cpp
        )
        target_include_directories(rune_batch_builder_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_batch_builder_tests PRIVATE
            rune_client_producer
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_batch_builder_tests)
    endif()

    # ── E5.1: Per-partition ingress-stream routing tests ──────────────────────
    # Pure unit test: ProducerImpl::updateIngressRoute + MetadataCache round-trip.
    # No real Aeron transport — sendFn is a fake lambda that records streamId calls.
    if(aeron_FOUND)
        add_executable(rune_per_worker_routing_tests
            tests/producer/ClientPerWorkerRoutingTest.cpp
        )
        target_include_directories(rune_per_worker_routing_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_per_worker_routing_tests PRIVATE
            rune_client_producer
            GTest::gtest_main
        )

        gtest_discover_tests(rune_per_worker_routing_tests)
    endif()

    # ── Producer batching instrumentation tests (Gate-2) ─────────────────────
    # Tests: framesSent/recordsSent counters + RUNE_LINGER_US/BATCH_BYTES/BATCH_RECORDS env knobs.
    if(aeron_FOUND)
        add_executable(rune_producer_batching_tests
            tests/producer/ProducerBatchingTest.cpp
        )
        target_include_directories(rune_producer_batching_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_producer_batching_tests PRIVATE
            rune_client_producer
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_producer_batching_tests)
    endif()

    # ── Subscriber tests (CB-4) — push RecordHandler, offset tracking, unsub ──
    if(aeron_FOUND)
        add_executable(rune_subscriber_tests
            tests/subscriber/SubscriberTest.cpp
        )
        target_include_directories(rune_subscriber_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_subscriber_tests PRIVATE
            rune_client_api
            rune_client_subscriber
            rune_client_producer
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_subscriber_tests)
    endif()

    # ── MD1 Task 10: Metadata cache + partition router tests ──────────────────
    # No Aeron dependency: MetadataCache and PartitionRouter are header-only
    # data structures; MetadataIntegrationTest uses the protocol codec directly.
    add_executable(rune_metadata_client_tests
        tests/metadata/MetadataCacheTest.cpp
        tests/metadata/PartitionRouterTest.cpp
        tests/metadata/MetadataIntegrationTest.cpp
    )
    target_include_directories(rune_metadata_client_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_metadata_client_tests PRIVATE
        rune_client
        GTest::gtest_main
    )
    gtest_discover_tests(rune_metadata_client_tests)

    # ── Group consumer tests (CG-5) — state machine, records routing, commit ──
    # No Aeron dependency: GroupConsumerHub is self-contained (no transport).
    add_executable(rune_group_tests
        tests/group/GroupConsumerTest.cpp
    )
    target_include_directories(rune_group_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_group_tests PRIVATE
        rune_client_group
        GTest::gtest_main
    )

    gtest_discover_tests(rune_group_tests)

    # ── Group consumer TSan tests (CG-5) ─────────────────────────────────────
    # Targets: commit() wait-free path vs runner doWork (hot path).
    # Build with -fsanitize=thread if a TSan build dir is configured.
    # This target is only added when ENABLE_TSAN is explicitly set.
    option(ENABLE_TSAN "Enable ThreadSanitizer for group consumer tests" OFF)
    if(ENABLE_TSAN)
        add_executable(rune_group_tsan_tests
            tests/group/GroupConsumerTest.cpp
        )
        target_include_directories(rune_group_tsan_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_group_tsan_tests PRIVATE
            rune_client_group
            GTest::gtest_main
        )
        target_compile_options(rune_group_tsan_tests PRIVATE -fsanitize=thread -g -O1)
        target_link_options(rune_group_tsan_tests PRIVATE -fsanitize=thread)

        gtest_discover_tests(rune_group_tsan_tests)
    endif()
endif()
