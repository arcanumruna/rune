add_library(rune_storage src/dummy.cpp src/MappedFile.cpp)
target_include_directories(rune_storage PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_PROTOCOL_INCLUDE})

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_storage_tests tests/storage/SmokeTest.cpp tests/storage/MappedFileTest.cpp)
    target_link_libraries(rune_storage_tests PRIVATE rune_storage GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_storage_tests)
endif()
