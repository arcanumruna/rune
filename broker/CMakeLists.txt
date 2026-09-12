cmake_minimum_required(VERSION 3.31)
project(rune_broker)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# rune-protocol is header-only; consumed via the sibling source include path in Phase 1.
set(RUNE_PROTOCOL_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include)

find_package(aeron)
find_package(spdlog REQUIRED)

option(ENABLE_COVERAGE "Enable coverage instrumentation" OFF)
if(ENABLE_COVERAGE)
    add_compile_options(--coverage -O0 -g)
    add_link_options(--coverage)
endif()

option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
endif()

option(BUILD_BENCHMARKS "Build benchmarks" OFF)
if(BUILD_BENCHMARKS)
    find_package(benchmark REQUIRED)
endif()

add_subdirectory(libs/storage)
add_subdirectory(libs/consensus)
add_subdirectory(libs/replication)
add_subdirectory(libs/coordination)
add_subdirectory(libs/transaction)
add_subdirectory(libs/node)
add_subdirectory(libs/gateway)

add_executable(rune_broker src/main.cpp)
target_link_libraries(rune_broker PRIVATE rune_node rune_storage spdlog::spdlog)
if(aeron_FOUND)
    target_link_libraries(rune_broker PRIVATE aeron::aeron)
    # Aeron's embedded media driver calls uuid_generate() from libuuid on Linux;
    # macOS provides this in its system libs, so the dependency only surfaces here.
    # The ConanCenter aeron recipe does not propagate it to the static driver link.
    # --no-as-needed guards against the linker discarding libuuid when it is
    # ordered before aeron's static driver archive on the link line.
    if(UNIX AND NOT APPLE)
        target_link_libraries(rune_broker PRIVATE
            -Wl,--no-as-needed uuid -Wl,--as-needed)
    endif()
endif()

if(BUILD_BENCHMARKS)
    add_executable(rune_broker_benchmark
        benchmark/BenchmarkMain.cpp
        benchmark/StorageBenchmark.cpp
        benchmark/MappedFileBenchmark.cpp
        benchmark/OffsetIndexBenchmark.cpp
        benchmark/LogSegmentBenchmark.cpp
        benchmark/LogBenchmark.cpp
        benchmark/RetentionBenchmark.cpp
        benchmark/PartitionApiBenchmark.cpp
        benchmark/LeaderEpochCacheBenchmark.cpp
        benchmark/ProducerStateBenchmark.cpp
        benchmark/ClusterMemberBenchmark.cpp
        benchmark/NodeStateFileBenchmark.cpp
        benchmark/TermLogBenchmark.cpp
        benchmark/ConsensusMessagesBenchmark.cpp
        benchmark/ElectionBenchmark.cpp
        benchmark/ConsensusAgentBenchmark.cpp
        benchmark/ReplicationBenchmark.cpp
        benchmark/CoordinationBenchmark.cpp
        benchmark/TransactionBenchmark.cpp)
    target_include_directories(rune_broker_benchmark PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_broker_benchmark PRIVATE rune_storage rune_consensus rune_replication rune_coordination rune_transaction benchmark::benchmark)
endif()
