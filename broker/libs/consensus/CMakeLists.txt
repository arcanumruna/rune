add_library(rune_consensus src/PersistentState.cpp)
target_include_directories(rune_consensus PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_PROTOCOL_INCLUDE})

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_consensus_tests
        tests/consensus/TypesTest.cpp
        tests/consensus/PersistentStateTest.cpp)
    target_link_libraries(rune_consensus_tests PRIVATE rune_consensus GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_consensus_tests)
endif()
