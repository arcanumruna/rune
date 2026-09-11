cmake_minimum_required(VERSION 3.20)

# rune_gateway — ClientGateway + SessionTable (BR-4)
add_library(rune_gateway
    src/ClientGateway.cpp)

target_include_directories(rune_gateway PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# rune-protocol headers (sibling repo, header-only)
target_include_directories(rune_gateway PUBLIC
    ${RUNE_PROTOCOL_INCLUDE})

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
        endif()
    endif()
endif()
