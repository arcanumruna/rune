add_library(rune_node
    src/NodeConfig.cpp)
target_include_directories(rune_node PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_node_tests
        tests/node/NodeConfigTest.cpp)
    target_link_libraries(rune_node_tests PRIVATE
        rune_node
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_node_tests)
endif()
