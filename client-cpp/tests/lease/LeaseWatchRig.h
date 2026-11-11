// tests/lease/LeaseWatchRig.h - the rig shared by the LeaseWatch test files: the stub broker, the model-based ScanLeases server, a fake clock,
// the event recorder and the lease row.  Included by LeaseWatchTest.cpp and LeaseWatchLifeTest.cpp.
#pragma once

#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"
#include "../support/LeaseScanModel.h"
#include "../support/TestCompletions.h"
#include <rune/client/Client.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/client/lease/LeaseWatch.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseScanHash.h>
#include <rune/protocol/client/LeaseMessages.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <poll.h>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
using test_support::Bytes;
using test_support::LeaseModel;
using test_support::LeaseStub;
using Kind = rc::LeaseEventKind;
using Op   = rune::protocol::record::LeaseOp;

namespace lease_watch_rig {

inline constexpr int64_t kMs = 1'000'000;
inline int64_t g_now = 0;
inline int64_t fakeNow() noexcept { return g_now; }
inline int64_t realNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// lease_version, lease_ttl_ms, lease_holder, lease_remaining_ms, then the key: view row = bitmap(1) + 5 * 8 + 2 * 8 = 57, log value = 44.
inline rune::codec::Descriptor watchDescriptor() {
    using rune::codec::PrimType;
    return rune::codec::DescriptorBuilder("lease_t", 1)
        .add("lease_version", PrimType::Int64, 1, false)
        .add("lease_ttl_ms", PrimType::UInt64, 1, false)
        .add("lease_holder", PrimType::UInt64, 1, false)
        .add("lease_remaining_ms", PrimType::UInt64, 1, false)
        .add("id", PrimType::UInt64, 1, /*isKey=*/true)
        .build();
}
inline const uint64_t kFp = rune::codec::fingerprint(watchDescriptor());

struct Row {
    static const uint64_t             kFingerprint;
    static constexpr std::string_view kSchemaName = "lease_t";
    uint64_t id = 0;
    [[nodiscard]] uint64_t    key() const noexcept { return id; }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 44; }
    std::size_t encode(uint8_t* b) const noexcept {
        std::memset(b, 0, 44);
        b[0] = 40;
        b[2] = 1;
        std::memcpy(b + 36, &id, 8);
        return 44;
    }
};
inline const uint64_t Row::kFingerprint = kFp;

struct DataDirs {
    std::vector<std::string> dirs;
    ~DataDirs() {
        std::error_code ec;
        for (const std::string& d : dirs) std::filesystem::remove_all(d, ec);
    }
    std::string next() {
        dirs.push_back((std::filesystem::temp_directory_path() /
                        ("lease_watch_" + std::to_string(::getpid()) + "_" + std::to_string(dirs.size()))).string());
        return dirs.back();
    }
};
inline DataDirs g_dataDirs;

inline rc::ClientConfig invokerCfg(int32_t in, int32_t eg, int32_t partitions) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir         = g_dataDirs.next();
    c.runnerMode      = rc::RunnerMode::Invoker;
    c.nanoClock       = &fakeNow;
    g_now             = realNow();
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = partitions;
    c.topics.push_back(ts);
    return c;
}

inline const rune::protocol::LeaseSpec kSpec{.ttlMs = 6000, .minTtlMs = 1000, .maxTtlMs = 60000, .maxKeys = 100000,
                                      .expiryBatch = 64, .checkpoint = 0, .checkpointIntervalMs = 1000, .checkpointRate = 1};

struct Cb {
    struct Lost { rc::LeaseRef ref; rc::LostReason why; };
    std::vector<Lost> lost;
};
struct OnLostF {
    Cb* cb;
    void operator()(rc::LeaseRef r, rc::LostReason w) const { cb->lost.push_back({r, w}); }
};
struct OnRiskF {
    void operator()(rc::LeaseRef, rc::AtRiskEdge) const {}
};

using Held = std::set<std::pair<Bytes, int64_t>>;

/// Records the watch's events and folds them: a Granted for a held (key, version), or an Updated / Revoked / Expired / Vanished / Confirmed for
/// an unheld one, or a RerunBegin in an open bracket / a RerunEnd outside one, is a protocol violation (`bad`).
struct Recorder {
    struct Ev { Kind kind; int32_t part; int64_t off; Bytes key; int64_t ver; bool fromScan; Bytes bytes; };
    std::vector<Ev> evs;
    Held            held;
    std::set<int32_t> open;   ///< the partitions with an open RerunBegin..RerunEnd bracket
    int             bad = 0, readies = 0, failures = 0;
    static void fn(void* c, const rc::LeaseEvent& e) { static_cast<Recorder*>(c)->on(e); }
    void on(const rc::LeaseEvent& e) {
        Bytes k(e.key.begin(), e.key.end());
        evs.push_back({e.kind, e.partition, e.offset, k, e.version, e.fromScan, Bytes(e.bytes.begin(), e.bytes.end())});
        const auto id = std::make_pair(k, e.version);
        switch (e.kind) {
        case Kind::Granted:  if (!held.insert(id).second) ++bad; break;
        case Kind::Updated:  if (held.count(id) == 0) ++bad; break;
        case Kind::Revoked:
        case Kind::Expired:
        case Kind::Vanished: if (held.erase(id) == 0) ++bad; break;
        case Kind::Ready:    ++readies; break;
        case Kind::Failed:   ++failures; break;
        case Kind::Confirmed:  if (held.count(id) == 0) ++bad; break;
        case Kind::RerunBegin: if (!open.insert(e.partition).second) ++bad; break;
        case Kind::RerunEnd:   if (open.erase(e.partition) == 0) ++bad; break;
        }
    }
    [[nodiscard]] int count(Kind kind, const Bytes* key = nullptr) const {
        int n = 0;
        for (const Ev& e : evs) n += e.kind == kind && (key == nullptr || e.key == *key);
        return n;
    }
};

inline Bytes kb(int i) {   // 8 bytes: byte 0 is the group (i % 4), byte 1 the index
    Bytes k(8, 0);
    k[0] = static_cast<uint8_t>(i % 4);
    k[1] = static_cast<uint8_t>(i);
    return k;
}

struct WRig {
    using Session = rc::LeaseSession<4, OnLostF, OnRiskF>;
    std::unique_ptr<rc::Client>        client;
    test_support::InvokerGateway       gw;
    std::optional<rc::LeaseTopic<Row>> topic;
    std::unique_ptr<LeaseStub>         stub;
    Cb                                 cb;
    std::unique_ptr<Session>           session;
    uint32_t                           parts;

    explicit WRig(const rc::ClientConfig& c, uint32_t partitions = 2)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId),
          parts(partitions) {
        gw.idempotence            = true;
        gw.replySchemas           = true;
        gw.schemaBlob             = rune::codec::serialize(watchDescriptor());
        gw.schemaTopicId          = 1;
        gw.schemaPartitionCount   = static_cast<int32_t>(partitions);
        gw.schemaLeaseViewTopicId = 9;
        rune::protocol::TopicKeyFeatures f;
        f.keyIndex = true;
        gw.schemaKeyFeatures = f;
        gw.schemaLease       = kSpec;
        gw.kaAutoOk          = true;
        stub = std::make_unique<LeaseStub>(gw, 1, partitions);
        stub->install();
        connect();
        topic.emplace(openTopic());
    }
    ~WRig() {
        session.reset();
        topic.reset();
    }
    void makeSession() { session = std::make_unique<Session>(*client, OnLostF{&cb}, OnRiskF{}, rc::LeaseSessionOptions{}); }
    void connect() {
        for (int i = 0; i < 4000 && !gw.ready(); ++i) { client->doWork(); gw.pump(); }
        ASSERT_TRUE(gw.ready());
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
        for (int i = 0; i < 4000 && !f.ready(); ++i) { client->doWork(); gw.pump(); }
        ASSERT_TRUE(f.ok()) << f.describe();
        for (int i = 0; i < 4000 && gw.metadataRequests == 0; ++i) { client->doWork(); gw.pump(); }
    }
    template <class Fn>
    void whilePumping(Fn&& fn) {
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
    rc::LeaseTopic<Row> openTopic() {
        std::optional<rc::LeaseTopic<Row>> h;
        whilePumping([&] { h.emplace(client->openLeaseTopicAndWait<Row>("lease_t", {})); });
        EXPECT_TRUE(h.has_value());
        return std::move(*h);
    }
    /// One hop of everything: the feed of the log to the latest subscriptions, the client, the stub broker, the watch (and the session).
    void round(rc::LeaseWatchBase& w) {
        for (uint32_t p = 0; p < parts; ++p) (void)stub->feed(static_cast<int32_t>(p));
        client->doWork();
        gw.pump();
        w.doWork(g_now);
        if (session) session->doWork(g_now);
    }
    template <class P>
    bool drive(rc::LeaseWatchBase& w, P&& pred, int cap = 3000) {
        for (int i = 0; i < cap; ++i) {
            round(w);
            if (pred()) return true;
        }
        return false;
    }
    void rounds(rc::LeaseWatchBase& w, int n) {
        for (int i = 0; i < n; ++i) round(w);
    }
};

inline rc::OpResult startWith(rc::LeaseWatchBase& w, const std::vector<Bytes>& prefixes) {
    std::vector<std::span<const uint8_t>> sp;
    for (const Bytes& p : prefixes) sp.emplace_back(p.data(), p.size());
    return w.startFromNow(sp);
}

}  // namespace lease_watch_rig

using namespace lease_watch_rig;
