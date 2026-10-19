# rune_peerlink — inter-broker authentication (authentication plan D32-D35).  Task 9: the cluster key files
# (ClusterKeys); Task 15 adds the handshake, Task 16 the frame crypto.  Depends on rune_crypto only, so
# consensus / replication / gateway / node can all link it without a cycle.
add_library(rune_peerlink STATIC src/ClusterKeys.cpp src/PeerLinkKeys.cpp src/PeerHandshake.cpp)
target_include_directories(rune_peerlink PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_link_libraries(rune_peerlink PUBLIC rune_crypto)
target_compile_features(rune_peerlink PUBLIC cxx_std_23)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_peerlink_tests tests/ClusterKeysTest.cpp tests/PeerLinkKeysTest.cpp tests/PeerHandshakeTest.cpp tests/PeerLinkKeyStoreTest.cpp)
    target_link_libraries(rune_peerlink_tests PRIVATE rune_peerlink GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_peerlink_tests PROPERTIES TIMEOUT 120)
endif()
