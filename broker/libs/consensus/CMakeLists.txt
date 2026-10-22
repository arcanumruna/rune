add_library(rune_consensus
        src/NodeStateFile.cpp
        src/TermLog.cpp
        src/Election.cpp
        src/SbeConsensusPublisher.cpp
        src/ConsensusAgent.cpp)
target_include_directories(rune_consensus PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_PROTOCOL_INCLUDE})
# Authentication plan Task 9: inter-broker authentication (cluster keys; Tasks 15/16 add the handshake + frame crypto).
target_link_libraries(rune_consensus PUBLIC rune_peerlink spdlog::spdlog)
# Aeron headers needed for IngressRing, IdleStrategy, AgentRunner
if(aeron_FOUND)
    target_link_libraries(rune_consensus PUBLIC aeron::aeron rune_aeron_util)
endif()

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_consensus_tests
        tests/consensus/TypesTest.cpp
        tests/consensus/ElectionStateTest.cpp
        tests/consensus/ClusterMemberTest.cpp
        tests/consensus/NodeStateFileTest.cpp
        tests/consensus/TermLogTest.cpp
        tests/consensus/ConsensusMessagesTest.cpp
        tests/consensus/ElectionBallotTest.cpp
        tests/consensus/ElectionLogTest.cpp
        tests/consensus/FollowerLivenessTest.cpp
        tests/consensus/ConsensusPublisherAdapterTest.cpp
        tests/consensus/ConsensusAgentTest.cpp
        tests/consensus/IdleStrategyTest.cpp
        tests/consensus/AgentRunnerIngressRingTest.cpp
        tests/consensus/SlowTickTest.cpp
        tests/consensus/IngressRingDrainTest.cpp
        tests/consensus/ConsensusDevirtTest.cpp
        tests/consensus/AgentRunnerStopRingTest.cpp)
    target_link_libraries(rune_consensus_tests PRIVATE rune_consensus GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_consensus_tests
        PROPERTIES ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
    # D29b review MEDIUM-1: the PreVote / D29b sims again with seeded delivery
    # jitter (late + reordered responses), so a jitter-only regression is RED in ctest.
    foreach(jit 5 10)
        add_test(NAME ConsensusSimsUnderJitter${jit}Regression
                 COMMAND rune_consensus_tests --gtest_filter=*PreVoteRegression*:*D29bRegression*)
        set_tests_properties(ConsensusSimsUnderJitter${jit}Regression PROPERTIES ENVIRONMENT "SIM_JITTER=${jit}")
    endforeach()
endif()

# Authentication Task 16: three consensus nodes over real UDP, every frame sealed (needs an embedded media driver).
if(BUILD_TESTING AND aeron_FOUND)
    add_executable(rune_peer_sealed_consensus_tests tests/consensus/PeerSealedConsensusTest.cpp
                   tests/consensus/AeronIngressOversizeTest.cpp)   # #254
    target_link_libraries(rune_peer_sealed_consensus_tests PRIVATE rune_consensus aeron::aeron GTest::gtest_main)
    gtest_discover_tests(rune_peer_sealed_consensus_tests
        PROPERTIES TIMEOUT 120 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
endif()

option(BUILD_INTEGRATION_TESTING "Build integration tests" ON)
if(BUILD_INTEGRATION_TESTING)
    if(NOT BUILD_TESTING)
        find_package(GTest REQUIRED)
        include(GoogleTest)
    endif()
    set(CLUSTER_INTEGRATION_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../tests/integration)
    add_executable(rune_consensus_integration_tests
        ${CLUSTER_INTEGRATION_TEST_DIR}/ClusterDisasterRecoveryIntegrationTest.cpp
        ${CLUSTER_INTEGRATION_TEST_DIR}/TempDirHygieneRegressionTest.cpp)
    target_include_directories(rune_consensus_integration_tests PRIVATE
        ${CLUSTER_INTEGRATION_TEST_DIR})
    target_link_libraries(rune_consensus_integration_tests PRIVATE
        rune_consensus
        GTest::gtest_main)
    if(aeron_FOUND)
        target_link_libraries(rune_consensus_integration_tests PRIVATE aeron::aeron)
    endif()
    gtest_discover_tests(rune_consensus_integration_tests
        PROPERTIES TIMEOUT 40 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
endif()

# ---------------------------------------------------------------------------
# Aeron transport tests (P2P-8) — gated behind BUILD_AERON_TRANSPORT_TESTS.
# OFF by default so normal CI does not require a live media driver.
# Requires aeron to be found (aeron::aeron target) and GTest.
# Build+run:
#   cmake -DBUILD_AERON_TRANSPORT_TESTS=ON ..
#   cmake --build . -j2
#   ctest -R AeronTransport --timeout 60
# ---------------------------------------------------------------------------
option(BUILD_AERON_TRANSPORT_TESTS
    "Build real-Aeron IPC transport integration tests (requires embedded media driver)" OFF)
if(BUILD_AERON_TRANSPORT_TESTS)
    if(NOT aeron_FOUND)
        message(FATAL_ERROR "BUILD_AERON_TRANSPORT_TESTS requires the aeron conan package")
    endif()
    if(NOT BUILD_TESTING AND NOT BUILD_INTEGRATION_TESTING)
        find_package(GTest REQUIRED)
        include(GoogleTest)
    endif()
    set(AERON_TRANSPORT_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../tests/integration)
    add_executable(rune_aeron_transport_tests
        ${AERON_TRANSPORT_TEST_DIR}/AeronTransportLoopbackTest.cpp)
    target_include_directories(rune_aeron_transport_tests PRIVATE
        ${AERON_TRANSPORT_TEST_DIR})
    target_link_libraries(rune_aeron_transport_tests PRIVATE
        rune_consensus
        aeron::aeron
        GTest::gtest_main)
    # 60 s real-time timeout: embedded driver + election over real IPC
    gtest_discover_tests(rune_aeron_transport_tests
        PROPERTIES TIMEOUT 60 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
endif()
