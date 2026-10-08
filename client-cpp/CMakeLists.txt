cmake_minimum_required(VERSION 3.31)
project(rune_client_cpp)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# rune-protocol is header-only; consumed via the sibling source include path.
set(RUNE_PROTOCOL_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include"
    CACHE PATH "rune-protocol include dir")

find_package(aeron)
# macOS/armv8: aeron ConanCenter recipe is unsupported there (see conanfile.py),
# so it's skipped and find_package finds nothing.  Provide a stub INTERFACE target
# so `target_link_libraries(... aeron::aeron)` resolve and CMake CONFIGURE succeeds
# for IDE loading on a Mac.  Real builds/tests run on Linux with the real aeron.
if(NOT TARGET aeron::aeron)
    message(WARNING "aeron not found — creating a STUB aeron::aeron INTERFACE target "
                    "(configure-only; client build/run requires real aeron on Linux).")
    add_library(rune_aeron_stub INTERFACE)
    add_library(aeron::aeron ALIAS rune_aeron_stub)
endif()
find_package(spdlog REQUIRED)
# client<->broker TLS/AEAD (Task 5): TLS 1.3 handshake + AEAD (rune-crypto, below).
find_package(OpenSSL REQUIRED)

# Task 9 / spec §11: RUNE_TLS — the client<->broker TLS/AEAD enablement gate,
# mirroring the broker's RUNE_TLS CMake option for a consistent build matrix.
# Unlike the broker, the client has no env-driven TLS config to gate: its
# crypto core (Tasks 5-8/8b) is already always compiled in, and
# offerCrypto/tlsTrustPem/tlsExpectHost/authToken/requireCrypto
# (ClientConfig, mapped into session::SessionConfig by Client.cpp's
# toAgentConfig()) are plain programmatic fields an application sets in code
# — they default to false/empty regardless of this option, so a caller who
# never touches them gets today's byte-identical plaintext connect either
# way. This option exists so RUNE_TLS=ON/OFF can be passed uniformly across
# both repos' build scripts; it does not currently change this repo's compiled
# output (reserved for a future env-var-driven default, symmetric with the
# broker's RUNE_TLS_CERT_FILE/RUNE_TLS_KEY_FILE/RUNE_REQUIRE_TLS).
option(RUNE_TLS "Enable the RUNE_TLS client<->broker TLS/AEAD config path (build-matrix parity with the broker; no effect on this repo's compiled output today)" OFF)

option(ENABLE_COVERAGE "Enable coverage instrumentation" OFF)
if(ENABLE_COVERAGE)
    add_compile_options(--coverage -O0 -g)
    add_link_options(--coverage)
endif()

option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()

    # F8a shared-nothing guard: no std::atomic / mutex / lock / condition_variable
    # / thread_local anywhere in src/ or include/ except the ring implementation's
    # own memory ordering (M2ORingBuffer) and the TEMPORARY "SLICE-B" caps listed
    # in scripts/check_no_atomics_or_mutexes.py.  Self-test first: pins that the
    # detector still catches every banned shape and stays false-positive-free,
    # independent of the current tree.
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(NOT Python3_Interpreter_FOUND)
        message(FATAL_ERROR
            "Python3 interpreter not found, but BUILD_TESTING=ON requires it for the "
            "NoAtomicsOrMutexesGuardSelfTestRegression / "
            "NoAtomicsOrMutexesOutsideAllowlistRegression ctests "
            "(scripts/check_no_atomics_or_mutexes.py). Install Python3 or configure "
            "with -DBUILD_TESTING=OFF.")
    endif()
    add_test(NAME NoAtomicsOrMutexesGuardSelfTestRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_atomics_or_mutexes.py --selftest)
    add_test(NAME NoAtomicsOrMutexesOutsideAllowlistRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_atomics_or_mutexes.py
            ${CMAKE_CURRENT_SOURCE_DIR})
    # F8c C7: move-only handles — no std::shared_ptr / weak_ptr / make_shared in
    # src/ or include/ except the Aeron transport objects (exact allowlist).
    # (Renamed in C9 fix round 1 from NoSharedPtrInPublicHandlesGuard*: it scans
    # the whole tree; the handle-header-only check is NoSharedPtrHandlesGuard.)
    add_test(NAME NoSharedOwnershipOutsideTransportGuardSelfTestRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_shared_ptr_handles.py --selftest)
    add_test(NAME NoSharedOwnershipOutsideTransportGuardRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_shared_ptr_handles.py
            ${CMAKE_CURRENT_SOURCE_DIR})
    # F8c C9: no std::future / promise / shared_future / packaged_task / async /
    # launch / #include <future> in src/ include/ examples/ bench/ tools/ (the
    # script self-tests its fixtures first), and no shared ownership in the
    # public handle headers — allowlist or not.
    add_test(NAME NoFuturesOrPromisesGuardRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_atomics_or_mutexes.py --futures
            ${CMAKE_CURRENT_SOURCE_DIR})
    add_test(NAME NoSharedPtrHandlesGuardRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_shared_ptr_handles.py --handles
            ${CMAKE_CURRENT_SOURCE_DIR})
    # T15b design §7: the client-shard sources are under both guards (atomics /
    # mutex / futures, and shared ownership) — the run fails if one is not scanned.
    set(RUNE_SHARD_SOURCES
        "src/shard/ClientShard.cpp,src/shard/ShardConfig.cpp,include/rune/client/shard/ClientShard.h,include/rune/client/shard/ShardConfig.h,include/rune/client/shard/ShardMap.h,include/rune/client/shard/ShardState.h")
    add_test(NAME NoAtomicsMutexFuturesSharedPtrInShardSourcesRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_atomics_or_mutexes.py
            --expect-covered=${RUNE_SHARD_SOURCES} ${CMAKE_CURRENT_SOURCE_DIR})
    add_test(NAME NoAtomicsMutexFuturesSharedPtrInShardSourcesSharedOwnershipRegression
        COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_no_shared_ptr_handles.py
            --expect-covered=${RUNE_SHARD_SOURCES} ${CMAKE_CURRENT_SOURCE_DIR})
endif()

option(BUILD_BENCHMARKS "Build benchmarks" OFF)
if(BUILD_BENCHMARKS)
    find_package(benchmark REQUIRED)
endif()

# ── rune-fr flight recorder (sibling static lib) ─────────────────────────────
# Mirrors the broker's wiring so the client can be traced with the SAME rune-fr
# (compile-in default; runtime-disabled until RUNE_FR_ENABLE=1).
option(ENABLE_FLIGHT_RECORDER "Compile in rune-fr flight-recorder instrumentation" ON)
set(RUNE_FR_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../rune-fr" CACHE PATH "Path to the rune-fr sibling repo")
add_library(rune_fr STATIC
    ${RUNE_FR_DIR}/src/Calibration.cpp
    ${RUNE_FR_DIR}/src/Enable.cpp
    ${RUNE_FR_DIR}/src/Registry.cpp
    ${RUNE_FR_DIR}/src/Report.cpp)
target_include_directories(rune_fr PUBLIC ${RUNE_FR_DIR}/include)
if(aeron_FOUND)
    target_link_libraries(rune_fr PUBLIC aeron::aeron)
endif()
target_compile_features(rune_fr PUBLIC cxx_std_23)
if(ENABLE_FLIGHT_RECORDER)
    target_compile_definitions(rune_fr PUBLIC RUNE_FR=1)
else()
    target_compile_definitions(rune_fr PUBLIC RUNE_FR=0)
endif()

# ── rune-crypto (client<->broker TLS/AEAD, Task 5) — sibling source static lib ──
# Mirrors the rune-fr wiring above: TlsSession (TLS 1.3 memory-BIO handshake) +
# AeadCipher (seal/open) + Caps (CPU cipher selection), consumed by SessionMachine.h
# (header-only) and, once Task 6 lands, the produce/egress seal/open seams.
set(RUNE_CRYPTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rune-crypto"
    CACHE PATH "Path to the rune-crypto sibling repo")
add_library(rune_crypto STATIC
    ${RUNE_CRYPTO_ROOT}/src/Caps.cpp
    ${RUNE_CRYPTO_ROOT}/src/Aead.cpp
    ${RUNE_CRYPTO_ROOT}/src/TlsSession.cpp)
target_include_directories(rune_crypto PUBLIC ${RUNE_CRYPTO_ROOT}/include)
target_link_libraries(rune_crypto PUBLIC OpenSSL::SSL OpenSSL::Crypto)
target_compile_features(rune_crypto PUBLIC cxx_std_23)

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

# ── rune-ql — DDL parser + code-gen backends (sibling source static lib) ──────
# Mirrors the rune-codec wiring above.  rune-ql's sources include rune-codec
# headers, so we reuse the rune_codec target (already defined above) rather
# than re-defining it, avoiding duplicate-target errors.
set(RUNE_QL_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rune-ql"
    CACHE PATH "Path to the rune-ql sibling repo")
file(GLOB RUNE_QL_SRCS "${RUNE_QL_ROOT}/src/*.cpp")
add_library(rune_ql STATIC ${RUNE_QL_SRCS})
target_include_directories(rune_ql PUBLIC ${RUNE_QL_ROOT}/include)
target_link_libraries(rune_ql PUBLIC rune_codec)
target_compile_features(rune_ql PUBLIC cxx_std_23)

# ── rune_client — trivial header-only/interface library ──────────────────────
add_library(rune_client INTERFACE)
target_include_directories(rune_client INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(rune_client INTERFACE spdlog::spdlog)
if(aeron_FOUND)
    target_link_libraries(rune_client INTERFACE aeron::aeron)
endif()

# ── rune_gso — UDP GSO/GRO Aeron transport bindings ──────────────────────────
# rune-protocol sibling C source (rune-broker spec 2026-10-03-aeron-udp-gso-gro).  Linux does GSO/GRO;
# other platforms compile a no-op (create() returns NULL -> stock Aeron).
if(aeron_FOUND)
    enable_language(C)
    add_library(rune_gso STATIC ${RUNE_PROTOCOL_INCLUDE}/../src/net/rune_gso_bindings.c)
    set_target_properties(rune_gso PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON POSITION_INDEPENDENT_CODE ON)
    target_include_directories(rune_gso PUBLIC $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>)
    target_link_libraries(rune_gso PUBLIC aeron::aeron)
    if(UNIX AND NOT APPLE)
        # Aeron's internal driver headers include siblings relative to <root>, <root>/aeron, <root>/aeronmd.
        foreach(_rune_aeron_inc IN LISTS aeron_INCLUDE_DIRS)
            target_include_directories(rune_gso PRIVATE
                "${_rune_aeron_inc}" "${_rune_aeron_inc}/aeron" "${_rune_aeron_inc}/aeronmd")
        endforeach()
    endif()
endif()

# ── rune_client_driver_wake — A7 park-until-work + epoll-first polling ──
# ONE copy shared with rune-broker (review L4): the rune-protocol sibling C source, like rune_gso.  Aeron's internal
# driver headers are C (same include probe as rune_gso).  Non-Linux: a stub (0 fds, stock polling).
if(aeron_FOUND)
    add_library(rune_client_driver_wake STATIC ${RUNE_PROTOCOL_INCLUDE}/../src/net/rune_driver_wake.c)
    set_target_properties(rune_client_driver_wake PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON POSITION_INDEPENDENT_CODE ON)
    target_include_directories(rune_client_driver_wake PUBLIC $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>)
    if(UNIX AND NOT APPLE)
        target_link_libraries(rune_client_driver_wake PRIVATE aeron::aeron)
        foreach(_rune_aeron_inc IN LISTS aeron_INCLUDE_DIRS)
            target_include_directories(rune_client_driver_wake PRIVATE
                "${_rune_aeron_inc}" "${_rune_aeron_inc}/aeron" "${_rune_aeron_inc}/aeronmd")
        endforeach()
    endif()
endif()

# ── rune_client_transport — AeronTransport (compiled, links aeronmd C driver) ─
if(aeron_FOUND)
    add_library(rune_client_transport STATIC
        src/transport/AeronTransport.cpp
    )
    target_include_directories(rune_client_transport PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>  # FrameCrc.h (B1): Crc32c / RecordBatch layout
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(rune_client_transport PUBLIC aeron::aeron spdlog::spdlog rune_fr rune_gso rune_client_driver_wake)
    # client<->broker TLS/AEAD (Task 5): SessionMachine.h is a PUBLIC header that
    # #includes rune-crypto's headers and holds a TlsSession/CryptoState member, so
    # every downstream target compiling a TU that includes ClientAgent.h/Client.h
    # (rune_client_api, rune_client_producer, test/benchmark targets, ...) needs
    # rune_crypto's include dir too — PUBLIC, same as rune_fr right above (fix
    # round 1: an earlier PRIVATE here only satisfied THIS target's own link,
    # not the transitive header dependency every downstream target has).
    target_link_libraries(rune_client_transport PUBLIC rune_crypto OpenSSL::SSL OpenSSL::Crypto)

    # Linux CI portability: libuuid is needed on Linux but not macOS (no-op there).
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_transport PUBLIC ${UUID_LIB})
            # The embedded driver archive (libaeron_driver_static.a) needs uuid_generate:
            # append -luuid to aeron::aeron's interface so it is always linked AFTER
            # the Aeron archives.  With --as-needed (Ubuntu) a libuuid placed before
            # them is dropped, and CMake de-duplicates a repeated ${UUID_LIB} path
            # (keeping the early one) — hence the -l form (T15b, k3s gcc-13).
            if(TARGET aeron::aeron)
                set_property(TARGET aeron::aeron APPEND PROPERTY INTERFACE_LINK_LIBRARIES "-luuid")
            endif()
        endif()
    endif()
endif()

# ── rune_client_agent — ClientAgent (CC-5, compiled, links transport) ─────────
if(aeron_FOUND)
    add_library(rune_client_agent STATIC
        src/agent/ClientAgent.cpp
    )
    target_include_directories(rune_client_agent PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(rune_client_agent PUBLIC
        rune_client_transport
        aeron::aeron
        spdlog::spdlog
        rune_codec
    )
    # client<->broker TLS/AEAD (Task 5): PUBLIC, same transitive-header reason as
    # rune_client_transport above — ClientAgent.h is itself a public header
    # downstream targets (rune_client_producer, rune_client_subscriber,
    # rune_client_api, ...) compile against.
    target_link_libraries(rune_client_agent PUBLIC rune_crypto OpenSSL::SSL OpenSSL::Crypto)
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_agent PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── rune_client_producer — Producer (CB-2 + TX-5, compiled, links agent) ──────
if(aeron_FOUND)
    add_library(rune_client_producer STATIC
        src/producer/ProducerImpl.cpp
        src/producer/Producer.cpp
        src/producer/ProducerHub.cpp
        src/producer/BatchBuilder.cpp
        src/producer/TransactionalProducerImpl.cpp
        src/producer/TransactionalProducer.cpp
    )
    target_include_directories(rune_client_producer PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(rune_client_producer PUBLIC
        rune_client_agent
        aeron::aeron
        spdlog::spdlog
        rune_fr
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_producer PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── rune_client_subscriber — Subscription + SubscriberHub (CB-4) ──────────────
if(aeron_FOUND)
    add_library(rune_client_subscriber STATIC
        src/subscriber/Subscription.cpp
        src/subscriber/SubscriberHub.cpp
    )
    target_include_directories(rune_client_subscriber PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(rune_client_subscriber PUBLIC
        rune_client_agent
        aeron::aeron
        spdlog::spdlog
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_subscriber PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── rune_client_group — GroupConsumer + GroupConsumerHub (CG-5) ───────────────
add_library(rune_client_group STATIC
    src/group/GroupConsumer.cpp
    src/group/GroupConsumerHub.cpp
)
target_include_directories(rune_client_group PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(rune_client_group PUBLIC
    spdlog::spdlog
    rune_codec
)
if(aeron_FOUND)
    target_link_libraries(rune_client_group PUBLIC aeron::aeron)
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_group PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── rune_client_api — Client (CC-6, compiled, links agent) ────────────────────
if(aeron_FOUND)
    add_library(rune_client_api STATIC
        src/Client.cpp
        src/Completions.cpp
        src/detail/CpuAffinity.cpp
        src/shard/ShardConfig.cpp
        src/shard/ClientShard.cpp
    )
    target_include_directories(rune_client_api PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RUNE_PROTOCOL_INCLUDE}>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(rune_client_api PUBLIC
        rune_client_group
        rune_client_subscriber
        rune_client_producer
        rune_client_agent
        aeron::aeron
        spdlog::spdlog
    )

    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_api PUBLIC ${UUID_LIB})
        endif()
    endif()
endif()

# ── Examples ──────────────────────────────────────────────────────────────────
option(BUILD_EXAMPLES "Build example programs" OFF)
if(BUILD_EXAMPLES AND aeron_FOUND)
    add_executable(rune_client_ping
        examples/rune_client_ping.cpp
    )
    target_include_directories(rune_client_ping PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_client_ping PRIVATE
        rune_client_api
    )
    # Linux: link libuuid (aeron embedded driver).
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_ping PRIVATE ${UUID_LIB})
        endif()
    endif()

    # CB-5 C — E2E produce/subscribe example vs 3-node cluster
    add_executable(rune_produce_subscribe
        examples/rune_produce_subscribe.cpp
    )
    target_include_directories(rune_produce_subscribe PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_produce_subscribe PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_produce_subscribe PRIVATE ${UUID_LIB})
        endif()
    endif()

    # Network-hop throughput load client (batched/pipelined produce over Aeron).
    add_executable(rune_produce_throughput
        examples/rune_produce_throughput.cpp
        examples/bench/InvokerBench.cpp
    )
    target_include_directories(rune_produce_throughput PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        ${CMAKE_CURRENT_SOURCE_DIR}/examples/bench
    )
    target_link_libraries(rune_produce_throughput PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
        rune_fr
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_produce_throughput PRIVATE ${UUID_LIB})
        endif()
    endif()

    # Network-hop READ throughput load client (subscribe/catch-up/tail over Aeron).
    add_executable(rune_consume_throughput
        examples/rune_consume_throughput.cpp
    )
    target_include_directories(rune_consume_throughput PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_consume_throughput PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB_CONSUME uuid)
        if(UUID_LIB_CONSUME)
            target_link_libraries(rune_consume_throughput PRIVATE ${UUID_LIB_CONSUME})
        endif()
    endif()

    # DR-1 — configurable disaster-recovery driver (rune-broker/dr-e2e harness)
    # T15b gates 2-5: real-broker driver for every client arm (see its header).
    add_executable(shard_gate examples/shard_gate.cpp)
    target_include_directories(shard_gate PRIVATE ${RUNE_PROTOCOL_INCLUDE} ${CMAKE_CURRENT_SOURCE_DIR}/examples/bench)
    target_link_libraries(shard_gate PRIVATE rune_client_api rune_client_producer rune_client_subscriber rune_fr)
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB_SHARD_GATE uuid)
        if(UUID_LIB_SHARD_GATE)
            target_link_libraries(shard_gate PRIVATE ${UUID_LIB_SHARD_GATE})
        endif()
    endif()

    add_executable(dr_client
        examples/dr_client.cpp
    )
    target_include_directories(dr_client PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(dr_client PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(dr_client PRIVATE ${UUID_LIB})
        endif()
    endif()

    # ── Task 13 — headline typed round-trip probe (define→open→send→consume) ──
    #
    # Build-time schema codegen (mirrors rune-codec's generate_characters_header):
    #   rune_codec_gen <blob> <out.h>  reads the CHECKED-IN descriptor blob
    #   tests/generated/characters.blob and emits characters.gen.h into the build
    #   tree.  The SAME blob is used by the probe at runtime to defineTopic(), so
    #   the generated struct's fingerprint matches the broker's stored schema by
    #   construction and openTopic<Characters>'s exact-fingerprint gate passes.
    #
    # The rune_codec_gen CLI source lives in the rune-codec sibling (0a, frozen);
    # we compile it here against the rune_codec static lib already wired above.
    add_executable(rune_codec_gen "${RUNE_CODEC_ROOT}/tools/rune_codec_gen_main.cpp")
    target_link_libraries(rune_codec_gen PRIVATE rune_codec)

    set(CHARACTERS_BLOB  "${CMAKE_CURRENT_SOURCE_DIR}/tests/generated/characters.blob")
    set(CHARACTERS_GEN_H "${CMAKE_CURRENT_BINARY_DIR}/gen/characters.gen.h")
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/gen")
    add_custom_command(
        OUTPUT  "${CHARACTERS_GEN_H}"
        COMMAND $<TARGET_FILE:rune_codec_gen> "${CHARACTERS_BLOB}" "${CHARACTERS_GEN_H}"
        DEPENDS rune_codec_gen "${CHARACTERS_BLOB}"
        COMMENT "Task 13: generating characters.gen.h from characters.blob"
    )
    add_custom_target(generate_characters_header DEPENDS "${CHARACTERS_GEN_H}")

    add_executable(rune_schema_probe
        examples/rune_schema_probe.cpp
    )
    add_dependencies(rune_schema_probe generate_characters_header)
    target_include_directories(rune_schema_probe PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        "${CMAKE_CURRENT_BINARY_DIR}/gen"   # for characters.gen.h
    )
    target_link_libraries(rune_schema_probe PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
        rune_codec
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB_PROBE uuid)
        if(UUID_LIB_PROBE)
            target_link_libraries(rune_schema_probe PRIVATE ${UUID_LIB_PROBE})
        endif()
    endif()

    # ── Task 10 (0c #11 gate) — CLI-driven rune-ql typed probe ────────────────
    #
    # tests/e2e/rune_ql_probe.cpp is the 0c single-node real-Aeron probe.  Unlike
    # the 0b schema-probe above (build-time codegen from a checked-in blob), this
    # probe's header comes from `rune schema generate` at RUNTIME.  The e2e script
    # (docker/rune-ql-e2e.sh) runs, inside the client container:
    #     rune schema apply    tests/e2e/characters.rql --broker <node> --validate
    #     rune schema generate characters --out ${RUNE_GEN_DIR} --broker <node>
    #     cmake --build . --target rune_ql_probe    (compiles THIS against the header)
    #     ./rune_ql_probe
    #
    # RUNE_GEN_DIR is where `rune schema generate` writes characters.gen.h; the
    # probe includes it from there.  Default: ${CMAKE_BINARY_DIR}/e2e-gen.  The
    # probe target is configured at cmake time but only COMPILES once the header
    # exists (post-generate), so it is NOT part of the default `all` build.
    set(RUNE_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/e2e-gen"
        CACHE PATH "Directory where `rune schema generate` writes characters.gen.h for rune_ql_probe")
    file(MAKE_DIRECTORY "${RUNE_GEN_DIR}")

    add_executable(rune_ql_probe EXCLUDE_FROM_ALL
        tests/e2e/rune_ql_probe.cpp
    )
    target_include_directories(rune_ql_probe PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        "${RUNE_GEN_DIR}"   # for the runtime-generated characters.gen.h
    )
    target_link_libraries(rune_ql_probe PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
        rune_codec
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB_QL_PROBE uuid)
        if(UUID_LIB_QL_PROBE)
            target_link_libraries(rune_ql_probe PRIVATE ${UUID_LIB_QL_PROBE})
        endif()
    endif()
endif()

# ── Benchmarks ────────────────────────────────────────────────────────────────
if(BUILD_BENCHMARKS AND aeron_FOUND)
    add_executable(rune_client_bench
        bench/ClientPingBench.cpp
    )
    target_include_directories(rune_client_bench PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_client_bench PRIVATE
        rune_client_api
        benchmark::benchmark
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_bench PRIVATE ${UUID_LIB})
        endif()
    endif()

    # CB-5 D — Produce RTT + subscribe delivery latency benchmark
    add_executable(rune_client_produce_bench
        bench/ClientProduceBench.cpp
    )
    target_include_directories(rune_client_produce_bench PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_client_produce_bench PRIVATE
        rune_client_api
        rune_client_producer
        rune_client_subscriber
        benchmark::benchmark
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_produce_bench PRIVATE ${UUID_LIB})
        endif()
    endif()

    # Accumulator engine micro-bench (in-process, no Aeron) — client batching throughput
    add_executable(rune_producer_accum_bench
        bench/ProducerAccumBench.cpp
    )
    target_include_directories(rune_producer_accum_bench PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_producer_accum_bench PRIVATE
        rune_client_api
        rune_client_producer
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_producer_accum_bench PRIVATE ${UUID_LIB})
        endif()
    endif()

    # F8c C6 — acked producer path (token acks) micro-bench (in-process, no Aeron)
    add_executable(rune_producer_acked_bench bench/ProducerAckedBench.cpp)
    target_include_directories(rune_producer_acked_bench PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_producer_acked_bench PRIVATE rune_client_api rune_client_producer)

    # F8c C7 — the producer hot path THROUGH THE HANDLE (Producer::trySend, void + acked)
    add_executable(rune_shard_route_bench bench/ShardRouteBench.cpp)
    target_include_directories(rune_shard_route_bench PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_shard_route_bench PRIVATE rune_client_api)

    add_executable(rune_producer_handle_bench bench/ProducerHandleBench.cpp)
    target_include_directories(rune_producer_handle_bench PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_producer_handle_bench PRIVATE rune_client_api rune_client_producer)

    # LAT-1 h/i/j (backlog B) — producer encode + CRC cost under back-pressure
    add_executable(rune_producer_encode_bench bench/ProducerEncodeBench.cpp)
    target_include_directories(rune_producer_encode_bench PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_link_libraries(rune_producer_encode_bench PRIVATE
        rune_client_api rune_client_producer benchmark::benchmark)

    # F8c C1(a) — M2ORingBuffer tryClaim/commit micro-bench (producer hot path)
    add_executable(rune_m2o_claim_bench
        bench/M2OClaimBench.cpp
    )
    target_link_libraries(rune_m2o_claim_bench PRIVATE rune_client_producer)

    # F12 — per-duty-cycle producer cost vs partition count (in-process, no Aeron)
    add_executable(rune_producer_flush_scaling_bench
        bench/ProducerFlushScalingBench.cpp
    )
    target_include_directories(rune_producer_flush_scaling_bench PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_producer_flush_scaling_bench PRIVATE
        rune_client_api
        rune_client_producer
        benchmark::benchmark
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_producer_flush_scaling_bench PRIVATE ${UUID_LIB})
        endif()
    endif()

    # CG-7 — GroupConsumer hot-path benchmarks (in-process, no Aeron)
    add_executable(rune_client_group_bench
        bench/GroupConsumerBench.cpp
    )
    target_include_directories(rune_client_group_bench PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_client_group_bench PRIVATE
        rune_client_group
        benchmark::benchmark
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB uuid)
        if(UUID_LIB)
            target_link_libraries(rune_client_group_bench PRIVATE ${UUID_LIB})
        endif()
    endif()
endif()

# ── Tests ─────────────────────────────────────────────────────────────────────
if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    include(GoogleTest)

    # Conformance vector directory — the sibling rune-protocol repo.
    # D6: the golden vectors come from the SAME rune-protocol checkout as the
    # headers (RUNE_PROTOCOL_INCLUDE) — a header change and its vectors move together.
    set(RUNE_CONFORMANCE_DIR "${RUNE_PROTOCOL_INCLUDE}/../conformance")
    if(NOT EXISTS "${RUNE_CONFORMANCE_DIR}/vectors")
        message(FATAL_ERROR
            "CC-2: conformance vector directory not found: ${RUNE_CONFORMANCE_DIR}/vectors\n"
            "  The sibling rune-protocol repo must be present at ${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol")
    endif()

    add_executable(rune_client_tests
        tests/SmokeTest.cpp
        tests/ConformanceTest.cpp
        tests/DataConformanceTest.cpp
    )
    target_link_libraries(rune_client_tests PRIVATE rune_client GTest::gtest_main)
    target_include_directories(rune_client_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_compile_definitions(rune_client_tests PRIVATE
        RUNE_CONFORMANCE_DIR="${RUNE_CONFORMANCE_DIR}"
    )

    gtest_discover_tests(rune_client_tests)

    # ── Schema codec smoke test (0b Task 9) — no Aeron dependency ───────────────
    add_executable(rune_schema_codec_smoke_tests
        tests/SchemaCodecSmokeTest.cpp
    )
    target_link_libraries(rune_schema_codec_smoke_tests PRIVATE rune_codec GTest::gtest_main)
    gtest_discover_tests(rune_schema_codec_smoke_tests)

    # ── Task 11: TypedTopic unit tests (schemaMatches) — no Aeron dependency ───
    # Tests schemaMatches() / fingerprint gate logic without a live broker.
    add_executable(rune_open_topic_tests
        tests/client/OpenTopicTest.cpp
    )
    target_include_directories(rune_open_topic_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${RUNE_CODEC_ROOT}/include
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_open_topic_tests PRIVATE rune_codec GTest::gtest_main)
    gtest_discover_tests(rune_open_topic_tests)

    # ── Task 11: openTopic<T> gate tests (loopback Aeron gateway) ────────────
    if(aeron_FOUND)
        add_executable(rune_open_topic_gate_tests
            tests/client/OpenTopicGateTest.cpp
        )
        target_include_directories(rune_open_topic_gate_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_open_topic_gate_tests PRIVATE
            rune_client_api
            GTest::gtest_main
        )

        gtest_discover_tests(rune_open_topic_gate_tests)
    endif()

    # ── Session state machine tests (CC-4) — no Aeron dependency ──────────────
    add_executable(rune_session_tests
        tests/session/SessionMachineTest.cpp
    )
    # client<->broker TLS/AEAD (Task 5): SessionMachine.h now drives a real
    # TlsSession/AeadCipher directly (header-only) — this target links rune_crypto
    # itself since it never goes through rune_client_agent/transport.
    target_link_libraries(rune_session_tests PRIVATE rune_client rune_crypto GTest::gtest_main)
    target_include_directories(rune_session_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})

    gtest_discover_tests(rune_session_tests)

    # ── client<->broker TLS/AEAD (Task 5): handshake drive round-trip ─────────
    # No Aeron dependency — drives a real SessionMachine (fake SessionIo) against
    # a real TlsSession::makeServer loopback, over rune-crypto's own test certs.
    add_executable(rune_client_crypto_tests
        tests/crypto/ClientCryptoRoundTripTest.cpp
    )
    target_link_libraries(rune_client_crypto_tests PRIVATE rune_client rune_crypto GTest::gtest_main)
    target_include_directories(rune_client_crypto_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_compile_definitions(rune_client_crypto_tests PRIVATE
        RUNE_CRYPTO_TESTDATA_DIR="${RUNE_CRYPTO_ROOT}/tests/testdata")

    gtest_discover_tests(rune_client_crypto_tests)

    # ── Task 15: SECURITY REGRESSION SUITE (named `*Regression*` so it is
    # selected as the run-first priority gate per CLAUDE.md rule 6/7).
    # Consolidates the tamper/AAD/replay/cross-session/per-sender/downgrade/
    # handshake security properties already proven piecemeal in
    # rune_client_crypto_tests, plus the gaps
    # ClientCryptoRegressionTest.cpp's own file header documents. Same
    # aeron-free build shape as rune_client_crypto_tests above. ──
    add_executable(rune_client_crypto_regression_tests
        tests/crypto/ClientCryptoRegressionTest.cpp
    )
    target_link_libraries(rune_client_crypto_regression_tests PRIVATE rune_client rune_crypto GTest::gtest_main)
    target_include_directories(rune_client_crypto_regression_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})
    target_compile_definitions(rune_client_crypto_regression_tests PRIVATE
        RUNE_CRYPTO_TESTDATA_DIR="${RUNE_CRYPTO_ROOT}/tests/testdata")

    gtest_discover_tests(rune_client_crypto_regression_tests)

    # ── Transport tests (CC-3) — embedded Aeron driver + ipc loopback echo ──
    if(aeron_FOUND)
        add_executable(rune_transport_tests
            tests/transport/AeronTransportTest.cpp
        )
        target_link_libraries(rune_transport_tests PRIVATE
            rune_client_transport
            GTest::gtest_main
        )

        gtest_discover_tests(rune_transport_tests)

        if(UNIX AND NOT APPLE)
            # UDP GSO/GRO bindings in the client's embedded driver (Linux kernels only).
            add_executable(rune_transport_gso_tests tests/transport/AeronTransportGsoTest.cpp)
            target_link_libraries(rune_transport_gso_tests PRIVATE rune_client_transport GTest::gtest_main)
            gtest_discover_tests(rune_transport_gso_tests)
        endif()
    endif()

    # ── Agent tests (CC-5) — single-writer duty-cycle + ipc gateway loop ──────
    if(aeron_FOUND)
        add_executable(rune_agent_tests
            tests/agent/ClientAgentTest.cpp
            tests/agent/SessionLivenessRegressionTest.cpp
        )
        target_include_directories(rune_agent_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_agent_tests PRIVATE
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_agent_tests)
    endif()

    # ── Task 1.3: Multi-node partition routing + multi-session registry ───────
    # Pure unit test: agent routing table (setRoute/routeEndpointFor) + session
    # registry (sessionForEndpoint/sessionCount) + MetadataCache.  No live driver.
    if(aeron_FOUND)
        add_executable(rune_multinode_routing_tests
            tests/agent/MultiNodeRoutingTest.cpp
        )
        target_include_directories(rune_multinode_routing_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_multinode_routing_tests PRIVATE
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_multinode_routing_tests)
    endif()

    # ── Client API tests (CC-6) — connect/ping futures + ipc gateway ──────────
    if(aeron_FOUND)
        add_executable(rune_client_api_tests
            tests/client/ClientTest.cpp
        )
        target_include_directories(rune_client_api_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_client_api_tests PRIVATE
            rune_client_api
            GTest::gtest_main
        )

        gtest_discover_tests(rune_client_api_tests)
    endif()

    # ── Task 10: Schema DDL round-trip tests (defineTopic / getTopicSchema) ─────
    # Uses the embedded Aeron IPC loopback gateway (same pattern as ClientTest).
    if(aeron_FOUND)
        add_executable(rune_schema_client_tests
            tests/client/ClientSchemaTest.cpp
        )
        target_include_directories(rune_schema_client_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_schema_client_tests PRIVATE
            rune_client_api
            GTest::gtest_main
        )

        gtest_discover_tests(rune_schema_client_tests)
    endif()

    # ── Redirect-follow tests (CB-5 B) — data-op redirect follow ──────────────
    if(aeron_FOUND)
        add_executable(rune_redirect_tests
            tests/client/RedirectTest.cpp
        )
        target_include_directories(rune_redirect_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_redirect_tests PRIVATE
            rune_client_api
            rune_client_producer
            rune_client_subscriber
            GTest::gtest_main
        )

        gtest_discover_tests(rune_redirect_tests)

        # ── Milestone D Phase-2 Task 14 (Note A) — bare NotLeader is retriable ──
        add_executable(rune_notleader_retry_tests
            tests/client/ClientNotLeaderRetryTest.cpp
        )
        target_include_directories(rune_notleader_retry_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_notleader_retry_tests PRIVATE
            rune_client_api
            rune_client_producer
            rune_client_subscriber
            GTest::gtest_main
        )

        gtest_discover_tests(rune_notleader_retry_tests)

        # ── MD1 Task 10 review — metadata routing against a real gateway ──────
        add_executable(rune_metadata_routing_tests
            tests/client/MetadataRoutingTest.cpp
        )
        target_include_directories(rune_metadata_routing_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_metadata_routing_tests PRIVATE
            rune_client_api
            rune_client_producer
            rune_client_subscriber
            GTest::gtest_main
        )
        gtest_discover_tests(rune_metadata_routing_tests)
    endif()

    # ── Producer tests (CB-2) — auto-batch, partitioning, acks, idempotence ──
    if(aeron_FOUND)
        add_executable(rune_producer_tests
            tests/producer/ProducerTest.cpp
        )
        target_include_directories(rune_producer_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_producer_tests PRIVATE
            rune_client_producer
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_producer_tests)
    endif()

    # ── Producer race regression tests (DR-BUG-1) ─────────────────────────────
    # Exercises the connect → immediate first-produce path with NO settle delay.
    # Must pass cleanly and TSan-clean after the lock-free snapshot fix.
    if(aeron_FOUND)
        add_executable(rune_producer_race_tests
            tests/producer/ProducerRaceTest.cpp
        )
        target_include_directories(rune_producer_race_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_producer_race_tests PRIVATE
            rune_client_api
            rune_client_producer
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_producer_race_tests)
    endif()

    # ── TX-5 Transactional producer + read_committed consumer tests ─────────────
    if(aeron_FOUND)
        add_executable(rune_txn_producer_tests
            tests/producer/TransactionalProducerTest.cpp
        )
        target_include_directories(rune_txn_producer_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_txn_producer_tests PRIVATE
            rune_client_producer
            rune_client_group
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_txn_producer_tests)
    endif()

    # ── BatchBuilder tests (CB-3) — Tier-2 explicit atomic batch ──────────────
    if(aeron_FOUND)
        add_executable(rune_batch_builder_tests
            tests/producer/BatchBuilderTest.cpp
        )
        target_include_directories(rune_batch_builder_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_batch_builder_tests PRIVATE
            rune_client_producer
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_batch_builder_tests)
    endif()

    # ── E5.1: Per-partition ingress-stream routing tests ──────────────────────
    # Pure unit test: ProducerImpl::updateIngressRoute + MetadataCache round-trip.
    # No real Aeron transport — sendFn is a fake lambda that records streamId calls.
    if(aeron_FOUND)
        add_executable(rune_per_worker_routing_tests
            tests/producer/ClientPerWorkerRoutingTest.cpp
        )
        target_include_directories(rune_per_worker_routing_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_per_worker_routing_tests PRIVATE
            rune_client_producer
            GTest::gtest_main
        )

        gtest_discover_tests(rune_per_worker_routing_tests)
    endif()

    # ── Producer batching instrumentation tests (Gate-2) ─────────────────────
    # Tests: framesSent/recordsSent counters + RUNE_LINGER_US/BATCH_BYTES/BATCH_RECORDS env knobs.
    if(aeron_FOUND)
        add_executable(rune_producer_batching_tests
            tests/producer/ProducerBatchingTest.cpp
        )
        target_include_directories(rune_producer_batching_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_producer_batching_tests PRIVATE
            rune_client_producer
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_producer_batching_tests)
    endif()

    # ── Subscriber tests (CB-4) — push RecordHandler, offset tracking, unsub ──
    if(aeron_FOUND)
        add_executable(rune_subscriber_tests
            tests/subscriber/SubscriberTest.cpp
            tests/subscriber/SubscriberHubCommandTest.cpp
        )
        target_include_directories(rune_subscriber_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_subscriber_tests PRIVATE
            rune_client_api
            rune_client_subscriber
            rune_client_producer
            rune_client_agent
            GTest::gtest_main
        )

        gtest_discover_tests(rune_subscriber_tests)
    endif()

    # ── MD1 Task 10: Metadata cache + partition router tests ──────────────────
    # No Aeron dependency: MetadataCache and PartitionRouter are header-only
    # data structures; MetadataIntegrationTest uses the protocol codec directly.
    add_executable(rune_metadata_client_tests
        tests/metadata/MetadataCacheTest.cpp
        tests/metadata/PartitionRouterTest.cpp
        tests/metadata/MetadataIntegrationTest.cpp
    )
    target_include_directories(rune_metadata_client_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_metadata_client_tests PRIVATE
        rune_client
        GTest::gtest_main
    )
    gtest_discover_tests(rune_metadata_client_tests)

    # ── Group consumer tests (CG-5) — state machine, records routing, commit ──
    # No Aeron dependency: GroupConsumerHub is self-contained (no transport).
    add_executable(rune_group_tests
        tests/group/GroupConsumerTest.cpp
    )
    target_include_directories(rune_group_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_group_tests PRIVATE
        rune_client_group
        GTest::gtest_main
    )

    gtest_discover_tests(rune_group_tests)

    # ── Group consumer TSan tests (CG-5) ─────────────────────────────────────
    # Targets: commit() wait-free path vs runner doWork (hot path).
    # Build with -fsanitize=thread if a TSan build dir is configured.
    # This target is only added when ENABLE_TSAN is explicitly set.
    option(ENABLE_TSAN "Enable ThreadSanitizer for group consumer tests" OFF)
    if(ENABLE_TSAN)
        add_executable(rune_group_tsan_tests
            tests/group/GroupConsumerTest.cpp
        )
        target_include_directories(rune_group_tsan_tests PRIVATE
            ${RUNE_PROTOCOL_INCLUDE}
        )
        target_link_libraries(rune_group_tsan_tests PRIVATE
            rune_client_group
            GTest::gtest_main
        )
        target_compile_options(rune_group_tsan_tests PRIVATE -fsanitize=thread -g -O1)
        target_link_options(rune_group_tsan_tests PRIVATE -fsanitize=thread)

        gtest_discover_tests(rune_group_tsan_tests)
    endif()

    # ── Task 12: rune_schema_gen tool unit tests (--from-file, no Aeron) ─────
    add_executable(rune_schema_gen_tests
        tests/SchemaGenToolTest.cpp
    )
    target_include_directories(rune_schema_gen_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${RUNE_CODEC_ROOT}/include
    )
    target_link_libraries(rune_schema_gen_tests PRIVATE rune_codec GTest::gtest_main)
    gtest_discover_tests(rune_schema_gen_tests)

    # ── Sharded invoker client: pure (Aeron-free) unit tests ─────────────────
    add_executable(rune_pure_tests
        tests/transport/TransportConfigTest.cpp
        tests/transport/ResolutionTest.cpp
        tests/detail/FlatIndexMapTest.cpp
        tests/producer/InFlightRingTest.cpp
        tests/shard/ShardMapTest.cpp
        tests/detail/Crc32cPatchTest.cpp
        tests/detail/Crc32cFastTest.cpp
        tests/producer/ProduceFrameBuilderTest.cpp
    )
    target_include_directories(rune_pure_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_pure_tests PRIVATE GTest::gtest_main)
    gtest_discover_tests(rune_pure_tests)

    # ── Sharded invoker client: Aeron-backed tests (single-threaded where possible) ──
    if(aeron_FOUND)
        add_executable(rune_invoker_tests
            tests/transport/InvokerTransportTest.cpp
            tests/agent/InvokerAgentTest.cpp
            tests/producer/ProducerSinkTest.cpp
            tests/producer/ProducerAllocRegressionTest.cpp
            tests/producer/ProducerAckPathTest.cpp
            tests/producer/ProducerSendTest.cpp
            tests/producer/DirectIngressTest.cpp
            tests/producer/ProducerFlushDirtyTest.cpp
            tests/producer/ProducerAckTokenTest.cpp
            tests/producer/ProducerCloseTest.cpp
            tests/detail/M2ORingBufferTest.cpp
            tests/detail/OneToOneRingBufferTest.cpp
            tests/detail/CompletionsTest.cpp
            tests/client/InvokerClientTest.cpp
            tests/client/IngressEndpointRoutingTest.cpp
            tests/producer/ProducerRouteParkTest.cpp
            tests/shard/ShardConfigTest.cpp
            tests/shard/ClientShardTest.cpp
            tests/shard/ShardGroupTest.cpp
            tests/shard/ShardStateTest.cpp
            tests/client/HubCommandClientTest.cpp
            tests/client/CompletionsClientTest.cpp
            tests/client/HandleLifetimeTest.cpp
            tests/client/GroupRoutingRegressionTest.cpp
            tests/support/AllocCounter.cpp
        )
        target_include_directories(rune_invoker_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_invoker_tests PRIVATE rune_client_api GTest::gtest_main)
        if(UNIX AND NOT APPLE)
            find_library(UUID_LIB_INVOKER uuid)
            if(UUID_LIB_INVOKER)
                target_link_libraries(rune_invoker_tests PRIVATE ${UUID_LIB_INVOKER})
            endif()
        endif()
        gtest_discover_tests(rune_invoker_tests)

        # EOS-OOS / EOS-DUP follow-ups: idempotence end to end against the
        # InvokerGateway's broker idempotence model + fault injection (own
        # binary: public API + bare ProducerImpl only).
        add_executable(rune_eos_tests tests/producer/ProducerEosTest.cpp)
        target_include_directories(rune_eos_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_eos_tests PRIVATE rune_client_api GTest::gtest_main)
        if(UNIX AND NOT APPLE AND UUID_LIB_INVOKER)
            target_link_libraries(rune_eos_tests PRIVATE ${UUID_LIB_INVOKER})
        endif()
        gtest_discover_tests(rune_eos_tests)

        # D6: UnknownProducerId — rule U on a bare ProducerImpl + end to end
        # (retention + failover) against the InvokerGateway broker model.
        add_executable(rune_unknown_pid_tests tests/producer/ProducerUnknownProducerIdTest.cpp)
        target_include_directories(rune_unknown_pid_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_unknown_pid_tests PRIVATE rune_client_api GTest::gtest_main)
        if(UNIX AND NOT APPLE AND UUID_LIB_INVOKER)
            target_link_libraries(rune_unknown_pid_tests PRIVATE ${UUID_LIB_INVOKER})
        endif()
        gtest_discover_tests(rune_unknown_pid_tests)

        # T15b Gate 4: SIGSTOP DR gate (fork: a client process under SIGSTOP /
        # SIGCONT; own binary so the fork happens in a single-threaded process).
        add_executable(rune_shard_dr_gate_tests tests/shard/ShardSigstopDrGateTest.cpp)
        target_include_directories(rune_shard_dr_gate_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_shard_dr_gate_tests PRIVATE rune_client_api GTest::gtest_main)
        if(UNIX AND NOT APPLE AND UUID_LIB_INVOKER)
            target_link_libraries(rune_shard_dr_gate_tests PRIVATE ${UUID_LIB_INVOKER})
        endif()
        gtest_discover_tests(rune_shard_dr_gate_tests PROPERTIES TIMEOUT 180)
    endif()

    # ── Benchmark building blocks (header-only, examples/bench) ──────────────
    add_executable(rune_bench_util_tests tests/bench/BenchUtilTest.cpp)
    target_include_directories(rune_bench_util_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/examples/bench)
    target_link_libraries(rune_bench_util_tests PRIVATE GTest::gtest_main)
    gtest_discover_tests(rune_bench_util_tests)
endif()

# ── Task 12: rune_schema_gen executable ───────────────────────────────────────
# The offline (--from-file) path links only rune_codec + filesystem.
# The live (generate <topic>) path requires Aeron and is gated by
# RUNE_SCHEMA_GEN_LIVE_ENABLED.  For Task 13 e2e the live path is enabled;
# for local/on-host builds only the offline path is compiled in.
add_executable(rune_schema_gen
    tools/rune_schema_gen.cpp
)
target_include_directories(rune_schema_gen PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${RUNE_CODEC_ROOT}/include
)
target_link_libraries(rune_schema_gen PRIVATE rune_codec)
target_compile_features(rune_schema_gen PRIVATE cxx_std_23)

# Enable the live broker path when Aeron is available.
if(aeron_FOUND)
    target_compile_definitions(rune_schema_gen PRIVATE RUNE_SCHEMA_GEN_LIVE_ENABLED=1)
    target_include_directories(rune_schema_gen PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_schema_gen PRIVATE
        rune_client_api
    )
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB_SCHEMAGEN uuid)
        if(UUID_LIB_SCHEMAGEN)
            target_link_libraries(rune_schema_gen PRIVATE ${UUID_LIB_SCHEMAGEN})
        endif()
    endif()
endif()

# ── Task 8: rune CLI shared implementation library ────────────────────────────
# src/cli/Cli.cpp contains rune::cli::run() with no main().
# Compiled into a static lib so both the `rune` executable and the offline
# tests can link it without a duplicate-main conflict with gtest_main.
add_library(rune_cli_lib STATIC
    src/cli/Cli.cpp
)
target_include_directories(rune_cli_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${RUNE_QL_ROOT}/include
    ${RUNE_CODEC_ROOT}/include
)
target_link_libraries(rune_cli_lib PUBLIC rune_ql rune_codec)
target_compile_features(rune_cli_lib PUBLIC cxx_std_23)

# Enable the live broker path when Aeron is available.
if(aeron_FOUND)
    target_compile_definitions(rune_cli_lib PUBLIC RUNE_CLI_LIVE_ENABLED=1)
    target_include_directories(rune_cli_lib PUBLIC
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_cli_lib PUBLIC rune_client_api)
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB_RUNE uuid)
        if(UUID_LIB_RUNE)
            target_link_libraries(rune_cli_lib PUBLIC ${UUID_LIB_RUNE})
        endif()
    endif()
endif()

# ── Task 8: rune CLI executable ───────────────────────────────────────────────
# tools/rune_cli.cpp is just main() — all logic is in rune_cli_lib.
add_executable(rune
    tools/rune_cli.cpp
)
target_link_libraries(rune PRIVATE rune_cli_lib)
target_compile_features(rune PRIVATE cxx_std_23)

# ── Task 8: rune CLI offline tests ────────────────────────────────────────────
# Tests schema apply --dry-run (and generate --from-file) without Aeron.
if(BUILD_TESTING)
    add_executable(rune_cli_offline_tests
        tests/RuneCliOfflineTest.cpp
    )
    target_link_libraries(rune_cli_offline_tests PRIVATE rune_cli_lib GTest::gtest_main)
    target_compile_features(rune_cli_offline_tests PRIVATE cxx_std_23)
    gtest_discover_tests(rune_cli_offline_tests)
endif()

# ── Task 9: rune CLI live tests (schema apply/get/list via loopback Aeron) ─────
# Requires Aeron (embedded driver + ipc loopback gateway).
if(BUILD_TESTING AND aeron_FOUND)
    add_executable(rune_cli_live_tests
        tests/RuneCliLiveTest.cpp
    )
    target_include_directories(rune_cli_live_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        ${RUNE_CODEC_ROOT}/include
        ${RUNE_QL_ROOT}/include
    )
    target_link_libraries(rune_cli_live_tests PRIVATE
        rune_cli_lib
        rune_client_api
        rune_codec
        rune_ql
        GTest::gtest_main
    )
    target_compile_features(rune_cli_live_tests PRIVATE cxx_std_23)
    if(UNIX AND NOT APPLE)
        find_library(UUID_LIB_CLI_LIVE uuid)
        if(UUID_LIB_CLI_LIVE)
            target_link_libraries(rune_cli_live_tests PRIVATE ${UUID_LIB_CLI_LIVE})
        endif()
    endif()
    gtest_discover_tests(rune_cli_live_tests)
endif()
