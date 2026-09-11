add_library(rune_replication INTERFACE)
target_include_directories(rune_replication INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_PROTOCOL_INCLUDE})

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_replication_tests
        tests/replication/ReplicationMessagesTest.cpp
        tests/replication/HighWatermarkTest.cpp)
    target_link_libraries(rune_replication_tests PRIVATE rune_replication GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_replication_tests)
endif()
