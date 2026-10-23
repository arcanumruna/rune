# Per-process temp root for every gtest binary (issue #375): see test_support/TestTmpRoot.cpp.
#
# include() this from the top-level CMakeLists.txt (the monorepo root, or client-cpp/ when it is the top-level
# project) when tests are built.  At the end of the top-level directory's configure, every EXECUTABLE target in
# every directory that links gtest directly (GTest::gtest_main / GTest::gtest / GTest::gmock_main / gtest_main)
# gets TestTmpRoot.cpp as a private source -- no per-target edits.
include_guard(GLOBAL)

set(RUNE_TEST_TMP_ROOT_SRC "${CMAKE_CURRENT_LIST_DIR}/test_support/TestTmpRoot.cpp"
    CACHE INTERNAL "per-process test temp root source (issue #375)")

function(_rune_test_tmp_root_wire_dir dir)
    get_property(_targets DIRECTORY "${dir}" PROPERTY BUILDSYSTEM_TARGETS)
    foreach(_t IN LISTS _targets)
        get_target_property(_type ${_t} TYPE)
        if(NOT _type STREQUAL "EXECUTABLE")
            continue()
        endif()
        get_target_property(_wired ${_t} RUNE_TEST_TMP_ROOT_WIRED)
        if(_wired)
            continue()
        endif()
        get_target_property(_libs ${_t} LINK_LIBRARIES)
        if(NOT _libs)
            continue()
        endif()
        set(_gtest FALSE)
        foreach(_l IN ITEMS GTest::gtest_main GTest::gtest GTest::gmock_main gtest_main)
            if(_l IN_LIST _libs)
                set(_gtest TRUE)
            endif()
        endforeach()
        if(_gtest)
            target_sources(${_t} PRIVATE "${RUNE_TEST_TMP_ROOT_SRC}")
            set_target_properties(${_t} PROPERTIES RUNE_TEST_TMP_ROOT_WIRED TRUE)
            set_property(GLOBAL APPEND PROPERTY RUNE_TEST_TMP_ROOT_TARGETS ${_t})
        endif()
    endforeach()
    get_property(_subdirs DIRECTORY "${dir}" PROPERTY SUBDIRECTORIES)
    foreach(_d IN LISTS _subdirs)
        _rune_test_tmp_root_wire_dir("${_d}")
    endforeach()
endfunction()

function(_rune_test_tmp_root_wire_all)
    _rune_test_tmp_root_wire_dir("${CMAKE_SOURCE_DIR}")
    get_property(_all GLOBAL PROPERTY RUNE_TEST_TMP_ROOT_TARGETS)
    list(LENGTH _all _n)
    message(STATUS "rune test tmp root: wired into ${_n} gtest executables")
endfunction()

cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL _rune_test_tmp_root_wire_all)

# Issue #577: the per-process root covers only paths built from temp_directory_path() / $TMPDIR.  This ctest scans the
# test sources of every subproject and fails on a literal "/tmp/... (the deliberately host-wide paths are allowlisted
# in the script, each with a comment at its site).  Registered by whichever top-level project includes this file.
if(NOT TEST TestTmpRootRegression.NoTestSourceHardCodesTmpRegression)
    get_filename_component(_rune_repo_root "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
    add_test(NAME TestTmpRootRegression.NoTestSourceHardCodesTmpRegression
             COMMAND ${CMAKE_COMMAND} -DRUNE_REPO_ROOT=${_rune_repo_root}
                     -P ${CMAKE_CURRENT_LIST_DIR}/test_support/NoHardCodedTmp.cmake)
endif()
