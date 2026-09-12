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
endif()
