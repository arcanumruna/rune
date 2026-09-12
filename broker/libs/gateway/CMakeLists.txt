cmake_minimum_required(VERSION 3.20)

# rune_gateway — ClientGateway + SessionTable (BR-4) + TopicRegistry (GB-1)
add_library(rune_gateway
    src/ClientGateway.cpp
    src/TopicRegistry.cpp)

target_include_directories(rune_gateway PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# rune-protocol headers (sibling repo, header-only)
target_include_directories(rune_gateway PUBLIC
    ${RUNE_PROTOCOL_INCLUDE})

# TopicRegistry depends on rune_storage (Log).
# NodeConfig/TopicSpec headers are included via RUNE_NODE_INCLUDE (raw path) to avoid
# a circular target dependency (rune_node → PUBLIC rune_gateway → rune_node).
target_link_libraries(rune_gateway PUBLIC rune_storage)
target_include_directories(rune_gateway PUBLIC ${RUNE_NODE_INCLUDE})

if(aeron_FOUND)
    target_link_libraries(rune_gateway PUBLIC aeron::aeron)
endif()

target_link_libraries(rune_gateway PRIVATE spdlog::spdlog)

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
        endif()
    endif()
endif()
