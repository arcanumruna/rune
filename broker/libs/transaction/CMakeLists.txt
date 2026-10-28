add_library(rune_transaction
    src/TransactionCoordinator.cpp
    src/TransactionLog.cpp
    src/ControlMarker.cpp
    src/ReadCommittedView.cpp
    src/ReplicatedTxnStateStore.cpp)
target_include_directories(rune_transaction PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${RUNE_PROTOCOL_INCLUDE})
# TransactionLog uses POSIX I/O (open/pread/pwrite/fsync/ftruncate).
# No additional link targets needed on Linux/macOS.
# TransactionMessages.h is header-only. ControlMarker depends on
# TransactionCoordinator (already in rune_transaction).
# ReadCommittedView scans rune::storage::Log — link rune_storage (no cycle:
# rune_storage has no dep on rune_transaction).
target_link_libraries(rune_transaction PUBLIC rune_storage rune_coordination)

# ---------------------------------------------------------------------------
# Exactly-once EOS end-to-end integration tests (P5-5).
# Gated behind BUILD_TRANSACTION_INTEGRATION_TESTING (ON by default — it rotted while OFF) to
# mirror the replication / coordination integration target pattern.
# ---------------------------------------------------------------------------
option(BUILD_TRANSACTION_INTEGRATION_TESTING
    "Build exactly-once EOS end-to-end integration tests (P5-5)" ON)   # ON: ms-fast; OFF let it rot (14 stale failures, 2026-10-03)
if(BUILD_TRANSACTION_INTEGRATION_TESTING)
    if(NOT BUILD_TESTING)
        find_package(GTest REQUIRED)
        include(GoogleTest)
    endif()
    set(TXN_INTEG_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration)
    add_executable(rune_transaction_integration_tests
        ${TXN_INTEG_DIR}/ExactlyOnceIntegrationTest.cpp
        ${TXN_INTEG_DIR}/LeaderFailoverEosTest.cpp)
    target_include_directories(rune_transaction_integration_tests PRIVATE
        ${TXN_INTEG_DIR}
        ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_transaction_integration_tests PRIVATE
        rune_transaction
        rune_replication
        rune_coordination
        GTest::gtest_main)
    gtest_discover_tests(rune_transaction_integration_tests
        PROPERTIES TIMEOUT 60)
endif()

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_transaction_tests
        tests/transaction/TransactionCoordinatorTest.cpp
        tests/transaction/ProducerIdAllocatorTest.cpp
        tests/transaction/TransactionLogTest.cpp
        tests/transaction/TransactionMessagesTest.cpp
        tests/transaction/ControlMarkerTest.cpp
        tests/transaction/ReadCommittedViewTest.cpp
        tests/transaction/ControlBatchBeFormatTest.cpp
        tests/transaction/ReplicatedTxnStateStoreTest.cpp
        tests/transaction/ProducerIdWatermarkRecoveryTest.cpp
        tests/transaction/F15TxnRegressionTest.cpp
        tests/transaction/NsTxnLimitRegressionTest.cpp)
    target_link_libraries(rune_transaction_tests PRIVATE
        rune_transaction
        rune_replication
        GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_transaction_tests)
endif()
