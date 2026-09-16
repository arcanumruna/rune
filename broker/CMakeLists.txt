cmake_minimum_required(VERSION 3.31)
project(rune_broker)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# rune-protocol is header-only; consumed via the sibling source include path in Phase 1.
set(RUNE_PROTOCOL_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include)

# rune_node include path — exposed as a CMake variable so gateway (which rune_node links)
# can include NodeConfig.h / TopicSpec without creating a circular target dependency.
set(RUNE_NODE_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/libs/node/include)

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
    # libuuid (needed by aeron's embedded driver on Linux) is linked PUBLIC on
    # rune_node so it propagates here and to the node test targets. See
    # libs/node/CMakeLists.txt.
endif()

# ── ping_probe — standalone raw-Aeron probe for BR-7b docker E2E ─────────────
# Enabled by -DBUILD_PING_PROBE=ON (used in Dockerfile.probe).
# Links against aeron (embedded driver + client) and the rune-protocol headers.
# Not built by default to avoid polluting the standard build.
option(BUILD_PING_PROBE "Build ping_probe (BR-7b docker e2e probe)" OFF)
if(BUILD_PING_PROBE)
    if(NOT aeron_FOUND)
        message(FATAL_ERROR "BUILD_PING_PROBE=ON requires aeron")
    endif()
    add_executable(ping_probe docker/ping-probe/ping_probe.cpp)
    target_include_directories(ping_probe PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(ping_probe PRIVATE aeron::aeron spdlog::spdlog)
    if(UNIX AND NOT APPLE)
        target_link_libraries(ping_probe PRIVATE -Wl,--no-as-needed uuid -Wl,--as-needed)
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
        benchmark/TransactionBenchmark.cpp
        benchmark/GatewayBench.cpp
        benchmark/GatewayDataBench.cpp
        benchmark/ProduceMajorityAckBench.cpp
        benchmark/GroupCoordinationBench.cpp)
    target_include_directories(rune_broker_benchmark PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_broker_benchmark PRIVATE rune_storage rune_consensus rune_replication rune_coordination rune_transaction rune_gateway rune_node benchmark::benchmark)
    target_include_directories(rune_broker_benchmark PRIVATE ${RUNE_NODE_INCLUDE})
    if(aeron_FOUND)
        target_link_libraries(rune_broker_benchmark PRIVATE aeron::aeron)
    endif()
endif()
