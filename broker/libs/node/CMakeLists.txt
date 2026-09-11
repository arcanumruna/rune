add_library(rune_node
    src/NodeConfig.cpp
    src/BrokerNode.cpp)

target_include_directories(rune_node PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# BrokerNode needs: consensus engine, Aeron (embedded driver + client), spdlog
target_link_libraries(rune_node PUBLIC rune_consensus)
if(aeron_FOUND)
    target_link_libraries(rune_node PUBLIC aeron::aeron)
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
        endif()
    endif()
endif()
