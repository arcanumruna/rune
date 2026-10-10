add_library(rune_view INTERFACE)   # header-only until Task 2 adds a .cpp; switch to STATIC then
target_include_directories(rune_view INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_view INTERFACE rune_codec)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_view_tests tests/view/ViewTypesTest.cpp)
    target_link_libraries(rune_view_tests PRIVATE rune_view rune_codec GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_view_tests)
endif()
