add_library(rune_view STATIC
    src/ViewRow.cpp
    src/ViewTable.cpp
    src/ViewPlan.cpp
    src/PlanCompiler.cpp
    src/ViewFold.cpp
    src/ViewCheckpoint.cpp
    src/AggState.cpp
    src/AggFold.cpp
    src/PlanSources.cpp)
target_include_directories(rune_view PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${RUNE_PROTOCOL_INCLUDE}
    # ViewFold.h consumes rune::storage::LogReadView (a header-only struct;
    # no link against rune_storage's heavy deps -- aeron et al -- needed).
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../storage/include>)
target_link_libraries(rune_view PUBLIC rune_codec)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_view_tests tests/view/ViewTypesTest.cpp tests/view/ViewRowTest.cpp
        tests/view/ViewTableTest.cpp tests/view/ViewPlanTest.cpp tests/view/PlanCompilerTest.cpp
        tests/view/ViewFoldTest.cpp tests/view/ViewCheckpointTest.cpp
        tests/view/AggStateTest.cpp
        tests/view/AggFoldTest.cpp
        tests/view/AggRetractableTest.cpp
        tests/view/PlanSourcesTest.cpp)
    target_link_libraries(rune_view_tests PRIVATE rune_view rune_codec GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_view_tests)
endif()
