cmake_minimum_required(VERSION 3.31)
project(rune LANGUAGES C CXX)

# Monorepo root: one configure for the broker tree.
#
# Every subproject also builds on its own from its subdirectory (cmake -S codec, -S crypto, ...).
# The broker compiles the protocol/codec/crypto/ql/client-cpp sources itself, under the same
# target names those subprojects use (rune_codec, rune_crypto, rune_ql, rune_client_api, ...), and
# embeds client-cpp for the client<->broker tests.  Adding those subprojects next to it in the
# same configure would define every target twice, so by default the root hosts:
#   broker/    (broker, libs, all broker tests, embedded client library)
#   protocol/  (wire format tests; its targets do not overlap the broker's)
# and codec/, crypto/, ql/, client-cpp/ are built from their own directories (RUNE_BUILD_* can
# select any one of them, or protocol/broker, but the broker excludes the other four).
#
# flight-recorder lives in its own repository: pass -DFLIGHT_RECORDER_DIR=<checkout>, or CMake
# clones the pinned commit with FetchContent.
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTING "Build tests" OFF)
option(BUILD_BENCHMARKS "Build benchmarks" OFF)
if(BUILD_TESTING)
    enable_testing()
    # Issue #375: every gtest binary runs under its own TMPDIR root, removed at exit and swept when its owner is dead.
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/RuneTestTmpRoot.cmake)
endif()

option(RUNE_BUILD_PROTOCOL "Build protocol/ (wire format, header-only + tests)" ON)
option(RUNE_BUILD_BROKER   "Build broker/ (broker, libs, tests, embedded client library)" ON)
option(RUNE_BUILD_CODEC    "Build codec/ standalone (not together with the broker)" OFF)
option(RUNE_BUILD_CRYPTO   "Build crypto/ standalone (not together with the broker)" OFF)
option(RUNE_BUILD_QL       "Build ql/ standalone (not together with the broker)" OFF)
option(RUNE_BUILD_CLIENT   "Build client-cpp/ standalone (not together with the broker)" OFF)

if(RUNE_BUILD_BROKER AND (RUNE_BUILD_CODEC OR RUNE_BUILD_CRYPTO OR RUNE_BUILD_QL OR RUNE_BUILD_CLIENT))
    message(FATAL_ERROR "broker/ already compiles codec, crypto, ql and client-cpp under the same "
                        "target names; build those subprojects from their own directories "
                        "(or set -DRUNE_BUILD_BROKER=OFF).")
endif()

if(RUNE_BUILD_PROTOCOL)
    add_subdirectory(protocol)
endif()
if(RUNE_BUILD_BROKER)
    add_subdirectory(broker)
endif()
foreach(_p codec crypto ql client-cpp)
    string(TOUPPER "${_p}" _u)
    string(REPLACE "CLIENT-CPP" "CLIENT" _u "${_u}")
    if(RUNE_BUILD_${_u})
        add_subdirectory(${_p})
    endif()
endforeach()
