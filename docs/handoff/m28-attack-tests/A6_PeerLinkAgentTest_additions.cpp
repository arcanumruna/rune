// TARGET FILE:   broker/libs/node/tests/integration/PeerLinkAgentTest.cpp   (append in Part 1; uses AgentRig, keysOf,
//                kPeerAuthStream)
// TARGET BINARY: rune_peer_link_agent_tests   (ctest -R PeerLinkAgentRegression)
// NOTE: real Aeron UDP, so the agents' clock is AgentRig::nowNs() like every Part-1 test; the loop yields (no sleeps) and
// is capped by iterations AND a 10 s deadline (fails, never hangs).

// ---- A6 Resources/DoS + liveness (finding F2): a SUSTAINED Hello flood from several attacker images (each a separate ----
// Aeron session), interleaved with the real initiator's Hellos, never starves the real handshake: Subscription::poll
// rotates its starting image every cycle, so the real image leads within (K+1) cycles and finds a fresh budget.  The
// junk mixes forged-but-plausible HelloV2s (real ids and key id: full crypto) with zero-cost ones (an unknown key id:
// refused without crypto) - both are charged to the shared budget today.
// Mutation: removing the per-cycle reset `helloBudget_ = kHelloAnswersPerCycle` from doWork (the budget, once spent,
// is never refilled) -> the real Hello is shed forever -> red.  Mutation 2 (documents F2): poll fragment limit 8 -> 64
// with the attacker images first keeps the result green - the guarantee rests on the image rotation, not the budget.
TEST(PeerLinkAgentRegression, ARealHelloInterleavedWithASustainedJunkHelloFloodStillLinksAdversarialRegression) {
    AgentRig rig;
    rig.start(2, keysOf('k'), 102);                                  // the responder of the pair (1, 2)
    PeerLinkAgent& b = *rig.agents[2];
    const std::string bChannel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rig.ports[1]);
    constexpr int kAttackers = 4;
    std::vector<int64_t> regIds;
    std::vector<std::shared_ptr<aeron::ExclusivePublication>> pubs(kAttackers);
    for (int i = 0; i < kAttackers; ++i) regIds.push_back(rig.aeron->addExclusivePublication(bChannel, kPeerAuthStream));
    const auto connectBy = std::chrono::steady_clock::now() + 10s;
    auto allConnected = [&] {
        for (auto& p : pubs) if (!p || !p->isConnected()) return false;
        return true;
    };
    for (int i = 0; i < 10'000'000 && !allConnected() && std::chrono::steady_clock::now() < connectBy; ++i) {
        for (int k = 0; k < kAttackers; ++k) if (!pubs[k]) pubs[k] = rig.aeron->findExclusivePublication(regIds[k]);
        (void)b.doWork(AgentRig::nowNs());
        std::this_thread::yield();
    }
    ASSERT_TRUE(allConnected());

    std::mt19937_64 rng(6196);
    const uint64_t keyId = keysOf('k').current.keyId;
    std::array<uint8_t, pl::PeerHelloV2::kSize> wire{};
    aeron::concurrent::AtomicBuffer ab(wire.data(), wire.size());
    auto junk = [&](int k) {                                         // one Hello per attacker image, non-blocking
        pl::PeerHelloV2 h; h.from = 1; h.to = 2; h.incarnation = rng();
        h.keyId = (rng() & 1) ? keyId : keyId ^ 0x5A5A5A5Aull;      // half plausible, half zero-cost refusals
        for (auto& x : h.nonceA) x = static_cast<uint8_t>(rng());
        for (auto& x : h.shareA) x = static_cast<uint8_t>(rng());
        h.encode(wire.data());
        (void)pubs[k]->offer(ab, 0, static_cast<aeron::util::index_t>(wire.size()));
    };
    rig.start(1, keysOf('k'), 101);                                  // the real initiator joins DURING the flood
    bool linked = false;
    const auto by = std::chrono::steady_clock::now() + 10s;
    for (int i = 0; i < 10'000'000 && std::chrono::steady_clock::now() < by; ++i) {
        for (int burst = 0; burst < 4; ++burst) for (int k = 0; k < kAttackers; ++k) junk(k);   // > budget every cycle
        for (auto& [id, a] : rig.agents) (void)a->doWork(AgentRig::nowNs());
        if (rig.agents[1]->establishedPeers() >= 1 && b.establishedPeers() >= 1) { linked = true; break; }
        std::this_thread::yield();
    }
    EXPECT_TRUE(linked) << "the real handshake was starved: answered " << b.helloAnswered() << " shed " << b.helloShed();
    EXPECT_GT(b.helloShed(), 0u) << "the flood really exceeded the budget";
}
