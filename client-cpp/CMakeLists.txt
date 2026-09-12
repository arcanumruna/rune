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
endif()
