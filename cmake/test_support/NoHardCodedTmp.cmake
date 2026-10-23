# Issue #577: no test source builds a path from a literal "/tmp/..." -- test data paths come from
# std::filesystem::temp_directory_path(), so they land under the per-process root of #375
# (TestTmpRoot.cpp) and are removed at exit / swept after a kill.
#
#   cmake -DRUNE_REPO_ROOT=<repo> -P NoHardCodedTmp.cmake
#
# Scans every C++ source under a tests/, bench/, benchmark/ or testing/ directory of the subprojects
# below and fails (exit != 0) on any line holding the literal "/tmp/ or "/tmp" that is not on the allowlist.
# The allowlist holds the deliberately HOST-WIDE paths only; each site carries a comment saying why.
cmake_minimum_required(VERSION 3.20)

if(NOT RUNE_REPO_ROOT)
    message(FATAL_ERROR "NoHardCodedTmp: pass -DRUNE_REPO_ROOT=<repo root>")
endif()

# Host-wide by design: FreePort.h's port-lock dir must be shared by every test process on the host.
set(_allow
    "\"/tmp/rune-test-ports-")

set(_subprojects broker client-cpp codec protocol ql crypto)
set(_bad "")
set(_scanned 0)
foreach(_sp IN LISTS _subprojects)
    file(GLOB_RECURSE _files LIST_DIRECTORIES false
         "${RUNE_REPO_ROOT}/${_sp}/*.cpp" "${RUNE_REPO_ROOT}/${_sp}/*.h" "${RUNE_REPO_ROOT}/${_sp}/*.hpp")
    foreach(_f IN LISTS _files)
        file(RELATIVE_PATH _rel "${RUNE_REPO_ROOT}" "${_f}")
        # Build trees and fetched dependencies are not sources.
        if(_rel MATCHES "(^|/)(build[^/]*|cmake-build[^/]*|_deps|third_party|\\.git)/")
            continue()
        endif()
        if(NOT _rel MATCHES "(^|/)(tests|bench|benchmark|testing)/")
            continue()
        endif()
        math(EXPR _scanned "${_scanned} + 1")
        file(STRINGS "${_f}" _lines REGEX "\"/tmp[/\"]")
        foreach(_l IN LISTS _lines)
            set(_ok FALSE)
            foreach(_a IN LISTS _allow)
                string(FIND "${_l}" "${_a}" _pos)
                if(NOT _pos EQUAL -1)
                    set(_ok TRUE)
                endif()
            endforeach()
            if(NOT _ok)
                string(STRIP "${_l}" _s)
                string(APPEND _bad "  ${_rel}: ${_s}\n")
            endif()
        endforeach()
    endforeach()
endforeach()

if(_scanned EQUAL 0)
    message(FATAL_ERROR "NoHardCodedTmp: no test source found under ${RUNE_REPO_ROOT} (vacuous scan)")
endif()
if(NOT _bad STREQUAL "")
    message(FATAL_ERROR "NoHardCodedTmp (#577): test sources hard-code \"/tmp/\"; build the path from "
                        "std::filesystem::temp_directory_path() instead:\n${_bad}")
endif()
message(STATUS "NoHardCodedTmp: ${_scanned} test sources scanned, no hard-coded \"/tmp/\"")
