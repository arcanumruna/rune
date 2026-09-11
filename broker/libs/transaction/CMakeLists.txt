add_library(rune_transaction
    src/TransactionCoordinator.cpp
    src/TransactionLog.cpp
    src/ControlMarker.cpp)
target_include_directories(rune_transaction PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
# TransactionLog uses POSIX I/O (open/pread/pwrite/fsync/ftruncate).
# No additional link targets needed on Linux/macOS.
# TransactionMessages.h is header-only. ControlMarker depends on
# TransactionCoordinator (already in rune_transaction).

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_transaction_tests
        tests/transaction/TransactionCoordinatorTest.cpp
        tests/transaction/ProducerIdAllocatorTest.cpp
        tests/transaction/TransactionLogTest.cpp
        tests/transaction/TransactionMessagesTest.cpp
        tests/transaction/ControlMarkerTest.cpp)
    target_link_libraries(rune_transaction_tests PRIVATE
        rune_transaction
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_transaction_tests)
endif()
