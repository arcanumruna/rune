cmake_minimum_required(VERSION 3.31)
project(rune_broker)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# rune-protocol is header-only; consumed via the sibling source include path in Phase 1.
set(RUNE_PROTOCOL_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include)

find_package(aeron)
find_package(spdlog REQUIRED)

option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
endif()

add_subdirectory(libs/storage)

add_executable(rune_broker src/main.cpp)
target_link_libraries(rune_broker PRIVATE rune_storage spdlog::spdlog)
