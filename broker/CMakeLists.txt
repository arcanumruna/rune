cmake_minimum_required(VERSION 3.31)
project(rune_broker)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# clangd / tooling: export compile_commands.json (C++23 flags reach the IDE).
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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
    ${RUNE_CRYPTO_ROOT}/src/TlsSession.cpp
    ${RUNE_CRYPTO_ROOT}/src/Kdf.cpp
    ${RUNE_CRYPTO_ROOT}/src/Scram.cpp)
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
    ${RUNE_CODEC_ROOT}/src/Generator.cpp
    # schema-evolution (durable tags): materializeAsOf/checkCompatibility/applyOps
    # + the ALTER-op wire codec shared with rune-ql. Required by SchemaRegistry's
    # per-version materialization (Task 9).
    ${RUNE_CODEC_ROOT}/src/Evolve.cpp
    ${RUNE_CODEC_ROOT}/src/AlterOps.cpp)
target_include_directories(rune_codec PUBLIC ${RUNE_CODEC_ROOT}/include)
target_compile_features(rune_codec PUBLIC cxx_std_23)

# ── rune_catalog — the shared catalog-history library (catalog history spec §6):
#    event model, frozen per-formatVer apply rules, fingerprints, the verifying
#    replayer.  Compiled from the rune-codec sibling's sources (ruling 1) so the
#    broker, the client and the CLI link byte-identical rules.
file(GLOB RUNE_CATALOG_SOURCES CONFIGURE_DEPENDS ${RUNE_CODEC_ROOT}/src/catalog/*.cpp)
add_library(rune_catalog STATIC ${RUNE_CATALOG_SOURCES})
target_include_directories(rune_catalog PUBLIC ${RUNE_CODEC_ROOT}/include ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_catalog PUBLIC rune_codec)
target_compile_features(rune_catalog PUBLIC cxx_std_23)

# ── rune_security — the shared authentication library (auth spec §3, plan D1): event
#    model, frozen v1 apply rules, fingerprints, permissions, verifying replayer, .rsec.
#    Compiled from the rune-codec sibling's sources, exactly like rune_catalog.
file(GLOB RUNE_SECURITY_SOURCES CONFIGURE_DEPENDS ${RUNE_CODEC_ROOT}/src/security/*.cpp)
add_library(rune_security STATIC ${RUNE_SECURITY_SOURCES})
target_include_directories(rune_security PUBLIC ${RUNE_CODEC_ROOT}/include)
target_link_libraries(rune_security PUBLIC rune_catalog)
target_compile_features(rune_security PUBLIC cxx_std_23)

# Task 9 / spec §11: RUNE_TLS — the client<->broker TLS/AEAD enablement gate.
# OFF (default): NodeConfig.cpp never reads RUNE_TLS_CERT_FILE/RUNE_TLS_KEY_FILE/
# RUNE_REQUIRE_TLS from the environment (guarded by the RUNE_TLS_ENABLED compile
# definition below) — tlsCertFile/tlsKeyFile/requireTls stay at their empty/false
# defaults no matter what an operator sets, so a RUNE_TLS=OFF build is
# byte-identical plaintext (the crypto core itself, Tasks 5-8/8b, is already
# always compiled in — this option gates ONLY the config path that could ever
# arm it). ON: the broker can be deployed with cert/key files + require_tls.
option(RUNE_TLS "Enable the RUNE_TLS client<->broker TLS/AEAD config path (cert/key file loading, require_tls, SIGHUP reload)" OFF)

option(ENABLE_COVERAGE "Enable coverage instrumentation" OFF)
if(ENABLE_COVERAGE)
    add_compile_options(--coverage -O0 -g)
    add_link_options(--coverage)
endif()

# Catalog history Task 8 fix round 1 (M8): Apple's ld warns "ignoring duplicate
# libraries" whenever CMake repeats a static library on a link line (it does so
# on purpose for the mutually-dependent rune_* static libraries).  The repeat is
# required on other linkers and harmless here; silence the noise so a real
# warning in the build output stands out.
if(APPLE)
    include(CheckLinkerFlag)
    check_linker_flag(CXX "LINKER:-no_warn_duplicate_libraries" RUNE_LD_NO_WARN_DUP_LIBS)
    if(RUNE_LD_NO_WARN_DUP_LIBS)
        add_link_options("LINKER:-no_warn_duplicate_libraries")
    endif()
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

    # Catalog history spec §8 / F5: topics exist only through DDL.  No mention of the retired topics env var may remain
    # in a tracked or untracked-not-ignored file (docs/superpowers/** excepted) beyond the explicit, counted allowlist in
    # scripts/rune_topics_allowlist.txt; a tracked binary containing the string (committed build
    # output) is a hit too.
    add_test(NAME NoRuneTopicsGateRegression.RepositoryHasNoRuneTopicsMention
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_rune_topics.py
            ${CMAKE_CURRENT_SOURCE_DIR})
    # The gate's own self-test: every way it could silently pass (odd paths, untracked files, stale allowlist rows).
    add_test(NAME NoRuneTopicsGateRegression.GateSelfTest
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/test_check_no_rune_topics.py)
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

# rune_regression_batches() — batched *Regression* runs of binaries made of many tiny tests.
#
# gtest_discover_tests() makes one ctest test (one process) per gtest test.  For a binary of thousands of
# millisecond tests the process — exec under Rosetta, dyld, gtest registering every parameterized test of
# the binary — costs 50-150 ms, 10-40x the test itself: rune_ddl_conformance_tests' 1901 *Regression*
# tests took ~300 ctest slot-seconds and run in ~10 s in one process.
#
# rune_regression_batches(<target> <shards> [TIMEOUT <s>] [ENVIRONMENT <env>...]) adds <shards> ctest
# tests "batch:<target>:<k>of<shards>", each running the binary once over its gtest shard k
# (GTEST_TOTAL_SHARDS / GTEST_SHARD_INDEX) of --gtest_filter=*Regression* — exactly the tests
# `ctest -R Regression` selects from that binary — labelled `batch`.  The binary's individual discovered
# tests stay (per-task `ctest -R <filter>` runs keep working) and carry LABELS small; the regression gate
# (scripts/run-regression.sh) runs the batches instead of them: -R "Regression|^batch:" -LE "latency|small".
# A plain `ctest -R Regression` still runs every test individually (the batches do not match it).
function(rune_regression_batches target shards)
    cmake_parse_arguments(RB "" "TIMEOUT" "ENVIRONMENT" ${ARGN})
    if(NOT RB_TIMEOUT)
        set(RB_TIMEOUT 900)
    endif()
    math(EXPR last "${shards} - 1")
    foreach(i RANGE 0 ${last})
        math(EXPR n "${i} + 1")
        set(name "batch:${target}:${n}of${shards}")
        add_test(NAME "${name}" COMMAND $<TARGET_FILE:${target}> "--gtest_filter=*Regression*")
        set_tests_properties("${name}" PROPERTIES
            LABELS batch
            TIMEOUT ${RB_TIMEOUT}
            ENVIRONMENT "GTEST_TOTAL_SHARDS=${shards};GTEST_SHARD_INDEX=${i};${RB_ENVIRONMENT}")
    endforeach()
endfunction()


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
add_subdirectory(libs/view)
add_subdirectory(libs/consensus)
add_subdirectory(libs/replication)
add_subdirectory(libs/coordination)
add_subdirectory(libs/transaction)
add_subdirectory(libs/node)
if(RUNE_TLS)
    target_compile_definitions(rune_node PRIVATE RUNE_TLS_ENABLED)
endif()
add_subdirectory(libs/gateway)

# ── Catalog history Task 11 (F7): the REAL client, embedded for
# rune_client_broker_tests (the client's blocking *AndWait DDL against a real
# in-process BrokerNode).  Required under BUILD_TESTING — never a silent skip.
# The client's own `if(NOT TARGET …)` guards (Task 5) reuse the broker's copies
# of the shared sibling targets (rune_fr / rune_codec / rune_crypto / rune_gso /
# rune_driver_wake as rune_client_driver_wake) — after libs/ so they all exist;
# its own tests and benchmarks are not built here (BUILD_TESTING / BUILD_BENCHMARKS
# are shadowed OFF for the subdirectory only).
set(RUNE_CLIENT_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rune-client-cpp" CACHE PATH
    "Path to the rune-client-cpp sibling repo (rune_client_broker_tests)")
if(BUILD_TESTING)
    if(NOT EXISTS "${RUNE_CLIENT_ROOT}/CMakeLists.txt")
        message(FATAL_ERROR "RUNE_CLIENT_ROOT (${RUNE_CLIENT_ROOT}) has no CMakeLists.txt — the rune-client-cpp "
                            "sibling is required for rune_client_broker_tests (BUILD_TESTING=ON)")
    endif()
    set(_rune_saved_build_testing ${BUILD_TESTING})
    set(_rune_saved_build_benchmarks ${BUILD_BENCHMARKS})
    set(BUILD_TESTING OFF)
    set(BUILD_BENCHMARKS OFF)
    # Exactly one rune_driver_wake.c per link (fix round 0): the client's
    # rune_client_driver_wake IS the broker's rune_driver_wake (libs/node).
    if(NOT TARGET rune_driver_wake)
        message(FATAL_ERROR "rune_driver_wake (libs/node) must be defined before the client is embedded — "
                            "rune_client_driver_wake aliases it (one rune_driver_wake.c per link)")
    endif()
    add_library(rune_client_driver_wake ALIAS rune_driver_wake)
    add_subdirectory(${RUNE_CLIENT_ROOT} ${CMAKE_BINARY_DIR}/rune-client EXCLUDE_FROM_ALL)
    set(BUILD_TESTING ${_rune_saved_build_testing})
    set(BUILD_BENCHMARKS ${_rune_saved_build_benchmarks})
    if(NOT TARGET rune_client_api)
        message(FATAL_ERROR "the embedded rune-client-cpp defined no rune_client_api (aeron not found?) — "
                            "rune_client_broker_tests needs it")
    endif()
endif()


# Flight-recorder instrumentation: link rune_fr PUBLIC into every instrumented
# lib so the headers propagate to dependents (and their tests).
target_link_libraries(rune_storage     PUBLIC rune_fr)
target_link_libraries(rune_consensus   PUBLIC rune_fr)
target_link_libraries(rune_replication PUBLIC rune_fr)
target_link_libraries(rune_node        PUBLIC rune_fr rune_codec)
target_link_libraries(rune_gateway     PUBLIC rune_fr)

# client<->broker TLS/AEAD (Task 8b): the node header Worker.h now reaches
# <rune/crypto/Aead.h> via RecordsPush.h -> RecordsSeal.h (the Records-push seal
# step, raw-crypto-params so it stays decoupled from rune_gateway).  Every lib
# that COMPILES a node-header chain reaching Worker.h therefore needs
# rune_crypto's include dir.  rune_node owns Worker.h (PUBLIC so rune_broker +
# rune_gateway dependents inherit it); rune_replication compiles node headers via
# RUNE_NODE_INCLUDE WITHOUT linking rune_node, so it needs rune_crypto directly.
# rune_gateway already links rune_crypto PUBLIC (libs/gateway/CMakeLists.txt).
# Unconditional (not RUNE_TLS-gated): the crypto headers/sources are always
# compiled; RUNE_TLS only gates the RUNE_TLS_ENABLED runtime env parsing.
target_link_libraries(rune_node        PUBLIC rune_crypto)
target_link_libraries(rune_replication PUBLIC rune_crypto)

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
    add_executable(rune_broker_process_tests tests/process/BrokerProcessSignalTest.cpp
                                          tests/process/StorageFormatProcessTest.cpp)
    target_link_libraries(rune_broker_process_tests PRIVATE GTest::gtest_main)
    target_include_directories(rune_broker_process_tests PRIVATE ${CMAKE_SOURCE_DIR}/libs/aeronutil/include)
    target_compile_definitions(rune_broker_process_tests PRIVATE
        RUNE_BROKER_BIN="$<TARGET_FILE:rune_broker>")
    add_dependencies(rune_broker_process_tests rune_broker)
    include(GoogleTest)
    # Not RUN_SERIAL (2026-10-15): every port the spawned broker binds comes from the collision-proof
    # allocator (FreePort.h) and its dirs are pid-scoped, so it runs in parallel with everything else.
    gtest_discover_tests(rune_broker_process_tests)
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

# ── Task 23 fix rounds 1+2 (R-I2, R-N1, R-N2): the BenchGate.h self-tests ────────────────────────────────
# Registered in EVERY test tree (BUILD_TESTING alone: Debug, ASan, TSan), not only where the benchmarks are built, so the gate
# tests exist at every checkpoint.  Google Benchmark is a hard requirement of a test tree: without it the gate self-tests would
# silently disappear.  `ctest -N -R BenchGateRegression` must list exactly 4 tests (checkpoint check).
if(BUILD_TESTING)
    find_package(benchmark CONFIG)
    if(NOT benchmark_FOUND)
        message(FATAL_ERROR "BenchGateRegression needs google-benchmark (conan): a test tree without it would silently drop the gate self-tests")
    endif()
    if(NOT TARGET rune_bench_gate_selftest)
        add_executable(rune_bench_gate_selftest
            benchmark/BenchmarkMain.cpp
            benchmark/BenchGateRegressionBench.cpp)
        target_link_libraries(rune_bench_gate_selftest PRIVATE benchmark::benchmark)
    endif()
    set(_gate_bin "$<TARGET_FILE:rune_bench_gate_selftest>")
    add_test(NAME BenchGateRegression.PassingGateExitsZeroWithAggregates
        COMMAND sh -c "out=$(${_gate_bin} --benchmark_filter=BM_GatePassesRegression --benchmark_repetitions=3 --benchmark_report_aggregates_only=true 2>&1); rc=$?; echo \"$out\"; test $rc -eq 0 && echo \"$out\" | grep -q _median")
    # A gate that fails on repetition 0 only: no crash (exit exactly 1, not a signal), the aggregates are printed.
    add_test(NAME BenchGateRegression.GateFailedOnFirstRepetitionExitsOneWithoutCrashing
        COMMAND sh -c "out=$(${_gate_bin} --benchmark_filter=BM_GateFirstRepetitionFailsRegression --benchmark_repetitions=3 --benchmark_report_aggregates_only=true 2>&1); rc=$?; echo \"$out\"; test $rc -eq 1 && echo \"$out\" | grep -q _median && echo \"$out\" | grep -q 'BENCH GATE FAILED'")
    # R-N1: a skipped (errored) run - a setup failure or an in-loop abort - fails the process too: exit exactly 2.
    add_test(NAME BenchGateRegression.SkippedRunExitsTwo
        COMMAND sh -c "out=$(${_gate_bin} --benchmark_filter=BM_GateSkippedRunRegression --benchmark_repetitions=3 2>&1); rc=$?; echo \"$out\"; test $rc -eq 2 && echo \"$out\" | grep -q 'BENCH SKIPPED'")
    # A tripped gate turns into a nonzero exit code.
    add_test(NAME BenchGateRegression.AlwaysFailingGateIsNonzero
        COMMAND ${_gate_bin} --benchmark_filter=BM_GateAlwaysFailsRegression)
    set_tests_properties(BenchGateRegression.AlwaysFailingGateIsNonzero PROPERTIES WILL_FAIL TRUE)
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
        benchmark/WaiterQueueBench.cpp
        benchmark/ViewTableBench.cpp
        benchmark/ViewFoldBench.cpp
        benchmark/ViewCheckpointBench.cpp
        benchmark/ViewFoldSourceBench.cpp
        benchmark/CatalogBootReplayBench.cpp
        benchmark/CatalogHotPathBench.cpp)
    target_include_directories(rune_broker_benchmark PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    # FusedRuntimePassBench drives a real broker with the shard tests' wire client.
    target_include_directories(rune_broker_benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libs/node/tests/support)
    target_link_libraries(rune_broker_benchmark PRIVATE rune_storage rune_consensus rune_replication rune_coordination rune_transaction rune_gateway rune_node rune_metadata_msm rune_view benchmark::benchmark)
    target_include_directories(rune_broker_benchmark PRIVATE ${RUNE_NODE_INCLUDE})
    if(aeron_FOUND)
        target_link_libraries(rune_broker_benchmark PRIVATE aeron::aeron)
    endif()
endif()
