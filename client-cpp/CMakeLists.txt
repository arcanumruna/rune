cmake_minimum_required(VERSION 3.31)
project(rune_client_cpp)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# rune-protocol is header-only; consumed via the sibling source include path.
set(RUNE_PROTOCOL_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include)

find_package(aeron)
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
