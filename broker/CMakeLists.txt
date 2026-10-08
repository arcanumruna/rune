cmake_minimum_required(VERSION 3.31)
project(rune_broker)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# rune-protocol is header-only; consumed via the sibling source include path in Phase 1.
set(RUNE_PROTOCOL_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include"
    CACHE PATH "rune-protocol include dir")

# rune_node include path — exposed as a CMake variable so gateway (which rune_node links)
# can include NodeConfig.h / TopicSpec without creating a circular target dependency.
set(RUNE_NODE_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/libs/node/include)

find_package(aeron)
# The embedded Aeron media driver calls uuid_generate() (libuuid) on Linux and the
# ConanCenter recipe does not propagate it: attach it to the imported target ONCE,
# so every driver-embedding executable (broker, every test) links on Linux.
if(aeron_FOUND AND UNIX AND NOT APPLE AND TARGET aeron::aeron)
    set_property(TARGET aeron::aeron APPEND PROPERTY INTERFACE_LINK_LIBRARIES
        "-Wl,--no-as-needed" uuid "-Wl,--as-needed")
endif()
# macOS/armv8: the aeron ConanCenter recipe is unsupported there (see conanfile.py),
# so aeron is skipped and find_package finds nothing.  Provide a stub INTERFACE
# target so the many `target_link_libraries(... aeron::aeron)` calls still resolve
# and CMake CONFIGURE succeeds for IDE loading/indexing on a Mac.  The broker is
# only built/run on Linux, where the real aeron::aeron target is used.
if(NOT TARGET aeron::aeron)
    message(WARNING "aeron not found — creating a STUB aeron::aeron INTERFACE target "
                    "(configure-only; broker build/run requires real aeron on Linux).")
    add_library(rune_aeron_stub INTERFACE)
    add_library(aeron::aeron ALIAS rune_aeron_stub)
endif()
find_package(spdlog REQUIRED)

# ── rune-fr — ultra-low-overhead flight recorder ─────────────────────────────
# Consumed as a SIBLING source lib (like rune-protocol; no conan publish gate).
# Header-first + 4 small .cpp; depends only on aeron's header-only AtomicBuffer/
# RecordDescriptor (already a broker dep).
#
# COMPILE FLAG: -DENABLE_FLIGHT_RECORDER=ON (default) compiles the instrumentation
# in (runtime-disabled by default → ~0 cost until rune_fr::enable()).  Build with
# -DENABLE_FLIGHT_RECORDER=OFF to compile EVERY span/mark out to nothing (RUNE_FR=0)
# — a production build with zero recorder footprint.  At RUN time (ENABLE build),
# arm with RUNE_FR_ENABLE and choose which paths with RUNE_FR_CATEGORIES.
option(ENABLE_FLIGHT_RECORDER "Compile in rune-fr flight-recorder instrumentation" ON)
set(RUNE_FR_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../rune-fr" CACHE PATH "Path to the rune-fr sibling repo")
add_library(rune_fr STATIC
    ${RUNE_FR_DIR}/src/Calibration.cpp
    ${RUNE_FR_DIR}/src/Enable.cpp
    ${RUNE_FR_DIR}/src/Registry.cpp
    ${RUNE_FR_DIR}/src/Report.cpp)
target_include_directories(rune_fr PUBLIC ${RUNE_FR_DIR}/include)
target_link_libraries(rune_fr PUBLIC aeron::aeron)
target_compile_features(rune_fr PUBLIC cxx_std_23)
# PUBLIC so the RUNE_FR macro reaches every file that includes FlightRecorder.h.
if(ENABLE_FLIGHT_RECORDER)
    target_compile_definitions(rune_fr PUBLIC RUNE_FR=1)
else()
    target_compile_definitions(rune_fr PUBLIC RUNE_FR=0)
endif()

# ── rune-crypto — client<->broker TLS/AEAD (Task 7) ──────────────────────────
# Consumed as a SIBLING source lib, exactly like rune-fr/rune-codec above (no
# conan publish gate yet).  Header-first (TlsSession.h/Aead.h/Cipher.h) + 3
# small .cpp.
#
# ★ BUILD/LINKAGE TRAP (task-7-interfaces.md, Task-5 precedent on the client
#   side): rune/gateway/SessionTable.h is already part of rune_gateway's
#   PUBLIC interface (it includes <Aeron.h>) and now also includes
#   rune/crypto/Aead.h + Cipher.h for Session::CryptoState.  Every downstream
#   target that compiles a TU including SessionTable.h/ClientGateway.h
#   therefore needs rune_crypto's include dir transitively — rune_node does
#   (BrokerNode.h/NodeRuntimeAgent.h/GroupCoordinationService.cpp/
#   TransactionCoordinationService.cpp all #include ClientGateway.h directly;
#   so do several libs/node/tests/integration/*.cpp).  rune_crypto + OpenSSL
#   are therefore linked PUBLIC on rune_gateway below (never PRIVATE — that
#   was exactly the client's Task-5 fix-round-1 Critical).  rune_node already
#   links rune_gateway PUBLIC (see its CMakeLists.txt), so the PUBLIC chain
#   propagates to rune_broker and every test/benchmark executable without any
#   further change here.
find_package(OpenSSL REQUIRED)
set(RUNE_CRYPTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rune-crypto"
    CACHE PATH "Path to the rune-crypto sibling repo")
add_library(rune_crypto STATIC
    ${RUNE_CRYPTO_ROOT}/src/Caps.cpp
    ${RUNE_CRYPTO_ROOT}/src/Aead.cpp
    ${RUNE_CRYPTO_ROOT}/src/TlsSession.cpp)
target_include_directories(rune_crypto PUBLIC ${RUNE_CRYPTO_ROOT}/include)
target_link_libraries(rune_crypto PUBLIC OpenSSL::SSL OpenSSL::Crypto)
target_compile_features(rune_crypto PUBLIC cxx_std_23)

# ── rune_gso — UDP GSO/GRO Aeron transport bindings ──────────────────────────
# rune-protocol sibling C source (spec 2026-10-03-aeron-udp-gso-gro).  Linux does GSO/GRO; other platforms
# compile a no-op (create() returns NULL -> stock Aeron).
enable_language(C)
add_library(rune_gso STATIC ${RUNE_PROTOCOL_INCLUDE}/../src/net/rune_gso_bindings.c)
set_target_properties(rune_gso PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON POSITION_INDEPENDENT_CODE ON)
target_include_directories(rune_gso PUBLIC ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_gso PUBLIC aeron::aeron)
if(UNIX AND NOT APPLE)
    # Aeron's internal driver headers include siblings relative to <root>, <root>/aeron, <root>/aeronmd
    # (pinned by the plan's Task 2 Step 1 probe against the conan aeron package).
    foreach(_rune_aeron_inc IN LISTS aeron_INCLUDE_DIRS)
        target_include_directories(rune_gso PRIVATE
            "${_rune_aeron_inc}" "${_rune_aeron_inc}/aeron" "${_rune_aeron_inc}/aeronmd")
    endforeach()
endif()

# ── rune-codec (0a) — consumed as a SIBLING source static lib (like rune-fr) ──
set(RUNE_CODEC_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rune-codec"
    CACHE PATH "Path to the rune-codec sibling repo")
add_library(rune_codec STATIC
    ${RUNE_CODEC_ROOT}/src/Serialize.cpp
    ${RUNE_CODEC_ROOT}/src/Record.cpp
    ${RUNE_CODEC_ROOT}/src/Validator.cpp
    ${RUNE_CODEC_ROOT}/src/Generator.cpp)
target_include_directories(rune_codec PUBLIC ${RUNE_CODEC_ROOT}/include)
target_compile_features(rune_codec PUBLIC cxx_std_23)

option(ENABLE_COVERAGE "Enable coverage instrumentation" OFF)
if(ENABLE_COVERAGE)
    add_compile_options(--coverage -O0 -g)
    add_link_options(--coverage)
endif()

option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()

    # F4 regression guard: a media driver is driven by exactly ONE thread
    # (spec Sec 3.6). This statically re-detects the "caller thread pumps
    # aeron_driver_main_do_work() in a wait-for-cnc.dat loop while a dedicated
    # thread ALSO pumps it" two-pumper race across every source file (test
    # harnesses, benchmarks, tools) outside libs/node/src/EmbeddedDriver.cpp,
    # so the bug class fixed in F4 cannot silently come back in a new/copied
    # harness. See scripts/check_single_driver_pumper.py for the exact rule.
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(NOT Python3_Interpreter_FOUND)
        message(FATAL_ERROR
            "Python3 interpreter not found, but BUILD_TESTING=ON requires it for "
            "the DriverPumpGuardSelfTestRegression / NoCallerSideDriverPumpingRegression "
            "ctests (scripts/check_single_driver_pumper.py). Silently dropping this guard "
            "is how the F4 two-pumper race regresses unnoticed; install Python3 or "
            "configure with -DBUILD_TESTING=OFF.")
    endif()
    # Self-test first: pins that the detector itself still catches every known
    # antipattern shape (braced, braceless, pump-in-condition/increment) and
    # stays false-positive-free on every known legitimate shape, independent
    # of what the current tree looks like.
    add_test(NAME DriverPumpGuardSelfTestRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_single_driver_pumper.py
            --selftest)
    add_test(NAME NoCallerSideDriverPumpingRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_single_driver_pumper.py
            ${CMAKE_CURRENT_SOURCE_DIR})

    # F9a guard (user law 2026-10-01): no std::atomic / mutex / lock /
    # thread_local in production code (libs/**, src/**; test code excluded)
    # outside scripts/atomics_allowlist.txt — ring-buffer internals, the
    # LogCleaner channel, and TEMPORARY slice-B / unscoped entries.  Stale
    # allowlist entries fail too, so converted sites must leave the list.
    add_test(NAME NoAtomicsGuardSelfTestRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_atomics.py --selftest)
    add_test(NAME NoAtomicsOrMutexesOutsideAllowlistRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_atomics.py
            ${CMAKE_CURRENT_SOURCE_DIR})
endif()

# gtest_discover_tests() defaults to running each test binary with
# --gtest_list_tests at BUILD time, under a hardcoded 5s DISCOVERY_TIMEOUT.
# Under load (e.g. two large builds running in parallel on the same box),
# that listing step can exceed 5s and fail the whole build even though the
# binary itself is fine — seen on rune_per_worker_replication_integration_tests
# and rune_gateway_psm_failover_tests (idle machine: both list in <1s).
# Setting DISCOVERY_MODE to PRE_TEST here, before any add_subdirectory() that
# calls gtest_discover_tests(), moves discovery to `ctest` invocation time
# instead of build time for every call below (none pass DISCOVERY_MODE
# explicitly), so machine load at build time can never fail the build.
set(CMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE PRE_TEST)

option(BUILD_BENCHMARKS "Build benchmarks" OFF)
if(BUILD_BENCHMARKS)
    find_package(benchmark REQUIRED)
endif()

# TSan environment for every Aeron-using test target (vendored Aeron client
# internals are suppressed via cmake/tsan-aeron.supp; rune code is not).
set(RUNE_TSAN_AERON_SUPP "${CMAKE_SOURCE_DIR}/cmake/tsan-aeron.supp")
set(RUNE_AERON_TSAN_ENV "TSAN_OPTIONS=halt_on_error=1 suppressions=${RUNE_TSAN_AERON_SUPP}")
# In a TSan build, link a default-options object into EVERY executable so the
# Aeron suppressions apply even to test targets registered without the env.
if(CMAKE_CXX_FLAGS MATCHES "-fsanitize=thread")
    configure_file(${CMAKE_SOURCE_DIR}/cmake/tsan_default_options.cpp.in
                   ${CMAKE_BINARY_DIR}/rune_tsan_default_options.cpp @ONLY)
    add_library(rune_tsan_default_options OBJECT ${CMAKE_BINARY_DIR}/rune_tsan_default_options.cpp)
    link_libraries(rune_tsan_default_options)
endif()

add_subdirectory(libs/aeronutil)
add_subdirectory(libs/metadata)
add_subdirectory(libs/storage)
add_subdirectory(libs/consensus)
add_subdirectory(libs/replication)
add_subdirectory(libs/coordination)
add_subdirectory(libs/transaction)
add_subdirectory(libs/node)
add_subdirectory(libs/gateway)

# Flight-recorder instrumentation: link rune_fr PUBLIC into every instrumented
# lib so the headers propagate to dependents (and their tests).
target_link_libraries(rune_storage     PUBLIC rune_fr)
target_link_libraries(rune_consensus   PUBLIC rune_fr)
target_link_libraries(rune_replication PUBLIC rune_fr)
target_link_libraries(rune_node        PUBLIC rune_fr rune_codec)
target_link_libraries(rune_gateway     PUBLIC rune_fr)

add_executable(rune_broker src/main.cpp)
target_link_libraries(rune_broker PRIVATE rune_node rune_storage rune_fr spdlog::spdlog)
if(aeron_FOUND)
    target_link_libraries(rune_broker PRIVATE aeron::aeron)
    # libuuid (needed by aeron's embedded driver on Linux) is linked PUBLIC on
    # rune_node so it propagates here and to the node test targets. See
    # libs/node/CMakeLists.txt.
endif()

# F9a fix round 1: real process-directed signal delivery to the broker binary
# (spawn rune_broker, kill(pid, SIGINT/SIGTERM), expect a clean exit 0).
if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_broker_process_tests tests/process/BrokerProcessSignalTest.cpp)
    target_link_libraries(rune_broker_process_tests PRIVATE GTest::gtest_main)
    target_compile_definitions(rune_broker_process_tests PRIVATE
        RUNE_BROKER_BIN="$<TARGET_FILE:rune_broker>")
    add_dependencies(rune_broker_process_tests rune_broker)
    include(GoogleTest)
    gtest_discover_tests(rune_broker_process_tests PROPERTIES RUN_SERIAL TRUE)
endif()

# ── rune_role — standalone Aeron node-role counter reader ────────────────────
# Reads the machine-readable leadership signal (RoleCounter.h) out of a broker's
# CnC file and prints one line.  The dr-e2e harness runs this via `docker exec`
# to detect the leader WITHOUT scraping "is LEADER" log lines (log-level
# independent).  Header-only dep on rune/node/RoleCounter.h; links aeron only.
if(aeron_FOUND)
    add_executable(rune_role src/rune_role.cpp)
    target_include_directories(rune_role PRIVATE ${RUNE_NODE_INCLUDE})
    target_link_libraries(rune_role PRIVATE aeron::aeron)
    if(UNIX AND NOT APPLE)
        target_link_libraries(rune_role PRIVATE -Wl,--no-as-needed uuid -Wl,--as-needed)
    endif()
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
    # ── RoutingImageBench — standalone (header-only dep, fast to build) ─────────
    add_executable(RoutingImageBench
        benchmark/RoutingImageBench.cpp)
    target_link_libraries(RoutingImageBench PRIVATE
        rune_metadata
        benchmark::benchmark_main)

    add_executable(rune_broker_benchmark
        benchmark/BenchmarkMain.cpp
        benchmark/StorageBenchmark.cpp
        benchmark/MappedFileBenchmark.cpp
        benchmark/OffsetIndexBenchmark.cpp
        benchmark/LogSegmentBenchmark.cpp
        benchmark/LogBenchmark.cpp
        benchmark/InMemoryLogBenchmark.cpp
        benchmark/ThrottlerBenchmark.cpp
        benchmark/ProducePathBenchmark.cpp
        benchmark/ProducerIdScanBenchmark.cpp
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
        benchmark/GroupCoordinationBench.cpp
        benchmark/CompactionBenchmark.cpp
        benchmark/MetadataApplyBench.cpp
        benchmark/RoutingImageBench.cpp
        benchmark/MultiLeaderProduceBench.cpp
        benchmark/WorkerProduceLatencyBench.cpp
        benchmark/ProduceValidateBench.cpp
        benchmark/FusedRuntimePassBench.cpp
        benchmark/WorkerReturnCodecBench.cpp
        benchmark/SessionAckSinkBench.cpp
        benchmark/RcBench.cpp
        benchmark/F15WorkerPsmBench.cpp
        benchmark/WaiterQueueBench.cpp)
    target_include_directories(rune_broker_benchmark PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    # FusedRuntimePassBench drives a real broker with the shard tests' wire client.
    target_include_directories(rune_broker_benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libs/node/tests/support)
    target_link_libraries(rune_broker_benchmark PRIVATE rune_storage rune_consensus rune_replication rune_coordination rune_transaction rune_gateway rune_node rune_metadata_msm benchmark::benchmark)
    target_include_directories(rune_broker_benchmark PRIVATE ${RUNE_NODE_INCLUDE})
    if(aeron_FOUND)
        target_link_libraries(rune_broker_benchmark PRIVATE aeron::aeron)
    endif()
endif()
