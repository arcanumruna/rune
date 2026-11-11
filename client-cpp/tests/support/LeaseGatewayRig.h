// tests/support/LeaseGatewayRig.h - the pieces of the lease-session rig shared by LeaseSessionTest and ProducerTxnGuardTest: the lease
// row schema, the stub broker's lease-topic configuration, the connect handshake and the lease-topic open (the stub is pumped from a
// helper thread for the one call that blocks the owner in openLeaseTopicAndWait).  The rest of each rig (clock, session, completions)
// stays with its test.
#pragma once

#include "InvokerGateway.h"
#include "TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/client/LeaseMessages.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <optional>
#include <poll.h>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

namespace test_support {

inline rune::codec::Descriptor leaseDescriptor() {
    return rune::codec::DescriptorBuilder("lease_t", 1).add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true).build();
}
inline const uint64_t& leaseFingerprint() {
    static const uint64_t fp = rune::codec::fingerprint(leaseDescriptor());
    return fp;
}

struct LeaseRow {
    static inline const uint64_t      kFingerprint = leaseFingerprint();
    static constexpr std::string_view kSchemaName  = "lease_t";
    uint64_t id = 0;
    [[nodiscard]] uint64_t    key() const noexcept { return id; }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + 32 + 8; }
    std::size_t encode(uint8_t* b) const noexcept {
        std::memset(b, 0, 44);
        b[0] = 40;
        b[2] = 1;
        std::memcpy(b + 36, &id, 8);
        return 44;
    }
};

inline const rune::protocol::LeaseSpec& leaseSpec() {
    static const rune::protocol::LeaseSpec s{.ttlMs = 6000, .minTtlMs = 1000, .maxTtlMs = 60000, .maxKeys = 100000,
                                             .expiryBatch = 64, .checkpoint = 0, .checkpointIntervalMs = 1000, .checkpointRate = 1};
    return s;
}

/// The stub broker serves the lease topic "lease_t" (topic id 1, view topic 9) with a lease trailer on produce answers.
inline void configureLeaseGateway(InvokerGateway& gw, uint32_t partitions = 1) {
    gw.idempotence            = true;
    gw.replySchemas           = true;
    gw.schemaBlob             = rune::codec::serialize(leaseDescriptor());
    gw.schemaTopicId          = 1;
    gw.schemaPartitionCount   = static_cast<int32_t>(partitions);
    gw.schemaLeaseViewTopicId = 9;
    rune::protocol::TopicKeyFeatures f;
    f.keyIndex = true;
    gw.schemaKeyFeatures = f;
    gw.schemaLease       = leaseSpec();
    gw.produceLeaseTrailer = [] {
        std::vector<uint8_t> t(16);
        const int64_t  v   = 77;
        const uint64_t ttl = 5000;
        std::memcpy(t.data(), &v, 8);
        std::memcpy(t.data() + 8, &ttl, 8);
        return t;
    }();
}

/// The Aeron handshake and client.connect() against the stub, each loop capped.
inline void connectGateway(rune::client::Client& client, InvokerGateway& gw) {
    for (int i = 0; i < 4000 && !gw.ready(); ++i) { client.doWork(); gw.pump(); }
    ASSERT_TRUE(gw.ready());
    TestCq cq(client);
    auto f = cq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client.connect(q, t); });
    for (int i = 0; i < 4000 && !f.ready(); ++i) { client.doWork(); gw.pump(); }
    ASSERT_TRUE(f.ok()) << f.describe();
    for (int i = 0; i < 4000 && gw.metadataRequests == 0; ++i) { client.doWork(); gw.pump(); }
}

/// Runs `fn` (which blocks the owner, e.g. openLeaseTopicAndWait) while a helper thread pumps the stub.  The helper stops when `fn`
/// returns; the wall deadline is only a safety net so a hung `fn` cannot leave the pump spinning (nothing in a passing run waits on it).
template <class Fn>
void whilePumping(InvokerGateway& gw, Fn&& fn) {
    int stop[2];
    ASSERT_EQ(::pipe(stop), 0);
    std::thread t([&] {
        pollfd pfd{stop[0], POLLIN, 0};
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (::poll(&pfd, 1, 0) == 0 && std::chrono::steady_clock::now() < deadline) gw.pump();
    });
    const auto release = [&] {
        const char b = 0;
        (void)!::write(stop[1], &b, 1);
        t.join();
        ::close(stop[0]);
        ::close(stop[1]);
    };
    try { fn(); } catch (...) { release(); throw; }
    release();
}

inline rune::client::LeaseTopic<LeaseRow> openLeaseTopic(rune::client::Client& client, InvokerGateway& gw) {
    std::optional<rune::client::LeaseTopic<LeaseRow>> h;
    whilePumping(gw, [&] { h.emplace(client.openLeaseTopicAndWait<LeaseRow>("lease_t", {})); });
    EXPECT_TRUE(h.has_value());
    return std::move(*h);
}

}  // namespace test_support
