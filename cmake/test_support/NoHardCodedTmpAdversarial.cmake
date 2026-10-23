# Adversarial review of PR #597 (#577): the NoHardCodedTmp scan must catch every form of a literal /tmp path in a test
# source, and let only the allowlisted host-wide literal through.  Each case is a fixture tree with one source, scanned
# (plus a clean companion) by NoHardCodedTmp.cmake; the case passes when the scan's verdict (exit 0 = clean) is the expected one.
#
#   cmake -DSCANNER=<NoHardCodedTmp.cmake> -DWORK_DIR=<scratch dir in the build tree> -P NoHardCodedTmpAdversarial.cmake
cmake_minimum_required(VERSION 3.20)
if(NOT SCANNER OR NOT WORK_DIR)
    message(FATAL_ERROR "NoHardCodedTmpAdversarial: pass -DSCANNER=<script> -DWORK_DIR=<dir>")
endif()

set(_q "\"")
set(_tmp "/tmp")   # assembled, so this file never holds the literal it hunts for
set(_failures "")
set(_n 0)

# case(<name> <relative file> <content> <expect: CLEAN|DIRTY>)
function(_case name rel content expect)
    set(_root "${WORK_DIR}/${name}")
    file(REMOVE_RECURSE "${_root}")
    if(NOT rel STREQUAL "")
        file(WRITE "${_root}/${rel}" "${content}\n")
        # A clean companion test source: every case but `vacuous` is a non-empty scan, so a verdict is never the
        # vacuous-tree refusal in disguise.
        file(WRITE "${_root}/protocol/tests/Companion.cpp" "int companion = 0;\n")
    else()
        file(MAKE_DIRECTORY "${_root}/broker")
    endif()
    execute_process(COMMAND ${CMAKE_COMMAND} -DRUNE_REPO_ROOT=${_root} -P ${SCANNER}
                    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
    if(_rc EQUAL 0)
        set(_got CLEAN)
    else()
        set(_got DIRTY)
    endif()
    if(NOT _got STREQUAL expect)
        set(_failures "${_failures}  ${name}: expected ${expect}, the scan said ${_got}\n${_err}\n" PARENT_SCOPE)
    endif()
    file(REMOVE_RECURSE "${_root}")
endfunction()

_case(clean        "broker/tests/A.cpp"  "auto p = std::filesystem::temp_directory_path() / ${_q}x${_q};"            CLEAN)
_case(plain        "broker/tests/A.cpp"  "std::string p = ${_q}${_tmp}/x${_q};"                                      DIRTY)
_case(bare         "codec/tests/A.h"     "fs::path p(${_q}${_tmp}${_q});"                                             DIRTY)
_case(raw          "ql/tests/A.cpp"      "const char* p = R${_q}(${_tmp}/x)${_q};"                                    DIRTY)
_case(raw_delim    "crypto/tests/A.cpp"  "const char* p = R${_q}xy(${_tmp}/x)xy${_q};"                                DIRTY)
_case(cc_ext       "client-cpp/tests/A.cc" "std::string p = ${_q}${_tmp}/x${_q};"                                     DIRTY)
_case(hpp_testing  "broker/libs/x/include/testing/A.hpp" "std::string p = ${_q}${_tmp}/x${_q};"                       DIRTY)
_case(allowlisted  "broker/tests/A.h"    "const char* d = ${_q}${_tmp}/rune-test-ports-10000x100${_q};"               CLEAN)
_case(allow_bypass "broker/tests/A.cpp"  "f(${_q}${_tmp}/rune-test-ports-${_q}, ${_q}${_tmp}/leak${_q});"            DIRTY)
_case(not_tmp      "broker/tests/A.cpp"  "std::string p = ${_q}${_tmp}foo/x${_q};"                                    CLEAN)
_case(non_test_dir "broker/src/A.cpp"    "std::string p = ${_q}${_tmp}/x${_q};"                                       CLEAN)
_case(vacuous      ""                    ""                                                                           DIRTY)

file(REMOVE_RECURSE "${WORK_DIR}")
if(NOT _failures STREQUAL "")
    message(FATAL_ERROR "NoHardCodedTmpAdversarial: the scan got these cases wrong:\n${_failures}")
endif()
message(STATUS "NoHardCodedTmpAdversarial: every case matched")
