add_library(rune_view STATIC
    src/ViewRow.cpp
    src/ViewTable.cpp
    src/ViewPlan.cpp
    src/PlanCompiler.cpp
    src/ViewFold.cpp
    src/ViewCheckpoint.cpp
    src/AggState.cpp
    src/AggFold.cpp
    src/PlanSources.cpp
    src/KeyedFold.cpp
    src/CheckEval.cpp
    src/UniqueClaims.cpp
    src/TxnOverlay.cpp)
target_include_directories(rune_view PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${RUNE_PROTOCOL_INCLUDE}
    # ViewFold.h consumes rune::storage::LogReadView (a header-only struct;
    # no link against rune_storage's heavy deps -- aeron et al -- needed).
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../storage/include>
    # FoldSkip.h consumes the header-only rune/transaction/AbortedTxnIndex.h (#591); no link against rune_transaction.
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../transaction/include>)
target_link_libraries(rune_view PUBLIC rune_codec)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_view_tests tests/view/ViewTypesTest.cpp tests/view/ViewRowTest.cpp
        tests/view/ViewTableTest.cpp tests/view/ViewPlanTest.cpp tests/view/PlanCompilerTest.cpp
        tests/view/ViewFoldTest.cpp tests/view/ViewCheckpointTest.cpp
        tests/view/AggStateTest.cpp
        tests/view/AggFoldTest.cpp
        tests/view/AggRetractableTest.cpp
        tests/view/PlanSourcesTest.cpp
        tests/view/CheckedWritesPlanV2Test.cpp
        tests/view/LegacyFoldReference.cpp
        tests/view/CheckedWritesPrepareApplyTest.cpp
        tests/view/CheckedWritesAbortedFoldMatrixTest.cpp
        tests/view/CheckedWritesViewTableTest.cpp
        tests/view/CheckedWritesViewTablePrefixTest.cpp
        tests/view/CheckedWritesViewCheckpointV3Test.cpp
        tests/view/CheckedWritesKeyedFoldTest.cpp
        tests/view/CheckedWritesKeyedAggTest.cpp
        tests/view/CheckedWritesCheckEvalTest.cpp
        tests/view/CheckedWritesUniqueFoldTest.cpp
        tests/view/CheckedWritesTxnOverlayTest.cpp
        tests/view/CheckedWritesTxnFoldTest.cpp
        ../node/tests/support/AllocWindow.cpp)
    target_include_directories(rune_view_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../node/tests/support)
    target_link_libraries(rune_view_tests PRIVATE rune_view rune_codec GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_view_tests)
    # T7 review 5, N2: the overlay's spare row reads as zeroes when malloc scribbles fresh blocks, as ASan does.
    add_test(NAME CheckedWritesTxnOverlaySpareRowUnderScribbleRegression
             COMMAND rune_view_tests --gtest_filter=*.ShortOverlayRowsNeverReachTheSpareRow:*.OverlayRowsShorterThanATableRowAreCapacityExhausted:*.ReserveZeroesOnlyTheSpareRowRegression)
    set_tests_properties(CheckedWritesTxnOverlaySpareRowUnderScribbleRegression PROPERTIES ENVIRONMENT "MallocScribble=1;MallocPreScribble=1;MALLOC_PERTURB_=170")
endif()
