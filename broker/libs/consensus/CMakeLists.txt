add_library(rune_consensus
        src/PersistentState.cpp
        src/RaftLog.cpp
        src/RaftNode.cpp
        src/NodeStateFile.cpp
        src/TermLog.cpp
        src/Election.cpp
        src/SbeConsensusPublisher.cpp
        src/ConsensusAdapter.cpp
        src/ConsensusAgent.cpp)
target_include_directories(rune_consensus PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        ${RUNE_PROTOCOL_INCLUDE})
# Aeron headers needed for IngressRing, IdleStrategy, AgentRunner
if(aeron_FOUND)
    target_link_libraries(rune_consensus PUBLIC aeron::aeron)
endif()

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_consensus_tests
        tests/consensus/TypesTest.cpp
        tests/consensus/PersistentStateTest.cpp
        tests/consensus/RaftLogTest.cpp
        tests/consensus/RaftNodeTest.cpp
        tests/consensus/ElectionTest.cpp
        tests/consensus/ReplicationTest.cpp
        tests/consensus/ElectionStateTest.cpp
        tests/consensus/ClusterMemberTest.cpp
        tests/consensus/NodeStateFileTest.cpp
        tests/consensus/TermLogTest.cpp
        tests/consensus/ConsensusMessagesTest.cpp
        tests/consensus/ElectionBallotTest.cpp
        tests/consensus/ElectionLogTest.cpp
        tests/consensus/ConsensusPublisherAdapterTest.cpp
        tests/consensus/ConsensusAgentTest.cpp
        tests/consensus/IdleStrategyTest.cpp
        tests/consensus/AgentRunnerIngressRingTest.cpp)
    target_link_libraries(rune_consensus_tests PRIVATE rune_consensus GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_consensus_tests)
endif()
