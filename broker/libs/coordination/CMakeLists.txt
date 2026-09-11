add_library(rune_coordination
    src/OffsetStore.cpp)
target_include_directories(rune_coordination PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_coordination_tests
        tests/coordination/OffsetStoreTest.cpp
        tests/coordination/OffsetMessagesTest.cpp)
    target_link_libraries(rune_coordination_tests PRIVATE
        rune_coordination
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_coordination_tests)
endif()
