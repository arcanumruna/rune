# Issue #577: no test source builds a path from a literal "/tmp/..." -- test data paths come from
# std::filesystem::temp_directory_path(), so they land under the per-process root of #375
# (TestTmpRoot.cpp) and are removed at exit / swept after a kill.
#
#   cmake -DRUNE_REPO_ROOT=<repo> -P NoHardCodedTmp.cmake
#
# Scans every C++ source (.cpp .cc .cxx .h .hh .hpp .hxx .ipp .inl .tpp) under a tests/, bench/, benchmark/ or
# testing/ directory of the subprojects below and fails (exit != 0) on any string literal that starts with /tmp:
# "/tmp/...", "/tmp", and raw strings R"(/tmp/...)" / R"d(/tmp/...)d".  The allowlist holds the deliberately
# HOST-WIDE paths only (each site carries a comment saying why); an allowlisted literal is removed from its line
# before the line is checked again, so it never hides another literal on the same line.
cmake_minimum_required(VERSION 3.20)

if(NOT RUNE_REPO_ROOT)
    message(FATAL_ERROR "NoHardCodedTmp: pass -DRUNE_REPO_ROOT=<repo root>")
endif()

# Host-wide by design: FreePort.h's port-lock dir must be shared by every test process on the host.
set(_allow
    "\"/tmp/rune-test-ports-")

set(_subprojects broker client-cpp codec protocol ql crypto)
set(_exts cpp cc cxx h hh hpp hxx ipp inl tpp)
# A literal that starts with /tmp: an ordinary string ("/tmp/ or "/tmp") or a raw string (R"(/tmp or R"delim(/tmp).
set(_tmp_re "(\"|R\"[^(\" ]*\\()/tmp[/\")]")
set(_bad "")
set(_scanned 0)
foreach(_sp IN LISTS _subprojects)
    set(_globs "")
    foreach(_e IN LISTS _exts)
        list(APPEND _globs "${RUNE_REPO_ROOT}/${_sp}/*.${_e}")
    endforeach()
    file(GLOB_RECURSE _files LIST_DIRECTORIES false ${_globs})
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
        file(STRINGS "${_f}" _lines REGEX "${_tmp_re}")
        foreach(_l IN LISTS _lines)
            set(_rest "${_l}")
            foreach(_a IN LISTS _allow)
                string(REPLACE "${_a}" "" _rest "${_rest}")
            endforeach()
            if(_rest MATCHES "${_tmp_re}")
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
