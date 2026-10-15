# rune_aeron_util — header-only Aeron helpers shared by consensus/replication/
# gateway/node (spec §3.6 invoker pumping).  INTERFACE: no sources.
add_library(rune_aeron_util INTERFACE)
target_include_directories(rune_aeron_util INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(rune_aeron_util INTERFACE aeron::aeron)
target_compile_features(rune_aeron_util INTERFACE cxx_std_23)

if(BUILD_TESTING)
    # Task 11 fix 2 (B3): templated fragment assembler — reassembly regression.
    find_package(GTest REQUIRED)
    add_executable(rune_aeronutil_tests tests/FragmentAssemblerTest.cpp
        tests/FreePortRegressionTest.cpp)   # 2026-10-15: the parallel-safe test port allocator
    target_link_libraries(rune_aeronutil_tests PRIVATE rune_aeron_util GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_aeronutil_tests PROPERTIES TIMEOUT 120)
endif()
