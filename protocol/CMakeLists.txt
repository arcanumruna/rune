cmake_minimum_required(VERSION 3.31)
project(rune_protocol)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# clangd / tooling: export compile_commands.json (C++23 flags reach the IDE).
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(aeron)

add_library(rune_protocol INTERFACE)
target_include_directories(rune_protocol INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# CLion helper (see common): a STATIC target so the IDE indexes headers.
add_library(rune_protocol_dummy STATIC src/dummy.cpp)
target_include_directories(rune_protocol_dummy PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
    find_package(GTest REQUIRED)
    add_executable(rune_protocol_tests
            tests/protocol/SmokeTest.cpp
            tests/protocol/Crc32cTest.cpp
            tests/protocol/VarintTest.cpp
            tests/protocol/RecordBatchTest.cpp
            tests/protocol/PartitionHashTest.cpp
            tests/protocol/MetadataRecordsTest.cpp
            tests/protocol/CreateTopicRecordTest.cpp
            tests/protocol/NodeJoinRecordTest.cpp
            tests/protocol/ControlTickRecordTest.cpp
            tests/protocol/client/MessageHeaderTest.cpp
            tests/protocol/client/CryptoEnvelopeTest.cpp
            tests/protocol/client/ControlMessagesConnectTest.cpp
            tests/protocol/client/ControlMessagesKeepaliveDisconnectErrorTest.cpp
            tests/protocol/client/PingMessagesTest.cpp
            tests/protocol/client/DataMessagesTest.cpp
            tests/protocol/client/UnknownProducerIdTest.cpp
            tests/protocol/client/GroupMessagesTest.cpp
            tests/protocol/client/OffsetMessagesTest.cpp
            tests/protocol/client/ConformanceTest.cpp
            tests/protocol/client/DispatchTest.cpp
            tests/protocol/client/TxnProtocolTest.cpp
            tests/protocol/client/MetadataMessagesTest.cpp
            tests/protocol/client/SchemaMessagesTest.cpp
            tests/protocol/client/SchemaConformanceTest.cpp
            tests/protocol/client/ViewMessagesTest.cpp
            tests/protocol/internal/RingSbeTest.cpp
            tests/protocol/net/UdpGsoPlanTest.cpp)
    target_include_directories(rune_protocol_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_compile_definitions(rune_protocol_tests PRIVATE
            RUNE_CONFORMANCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/conformance")
    target_link_libraries(rune_protocol_tests PRIVATE GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_protocol_tests)
endif()

option(BUILD_BENCHMARKS "Build benchmarks" OFF)
if(BUILD_BENCHMARKS)
    find_package(benchmark REQUIRED)
    add_executable(rune_protocol_benchmark
            benchmark/BenchmarkMain.cpp
            benchmark/Crc32cBenchmark.cpp
            benchmark/VarintBenchmark.cpp
            benchmark/RecordBatchBenchmark.cpp
            benchmark/ClientCodecBenchmark.cpp)
    target_include_directories(rune_protocol_benchmark PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_CURRENT_SOURCE_DIR}/benchmark)
    target_link_libraries(rune_protocol_benchmark PRIVATE benchmark::benchmark)
endif()
