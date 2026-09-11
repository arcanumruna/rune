add_library(rune_transaction
    src/TransactionCoordinator.cpp)
target_include_directories(rune_transaction PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
# ProducerIdAllocator.h is header-only (no dependency on rune_storage/rune_coordination).
# If P5-3 needs Types from coordination, link it then. For now keep it standalone.

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_transaction_tests
        tests/transaction/TransactionCoordinatorTest.cpp
        tests/transaction/ProducerIdAllocatorTest.cpp)
    target_link_libraries(rune_transaction_tests PRIVATE
        rune_transaction
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_transaction_tests)
endif()
