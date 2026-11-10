#pragma once
// LeaseBrokerRig.h - lease topics Task 17 (L18, decision 3): ONE real, authenticating, shard-mode BrokerNode with the injected lease clock,
// driven by REAL clients.  Namespaces `a` and `b` (users a/ua, b/ub), in `a`: the lease topic `t` (2 partitions) with a user view `u` over it
// (keyed on field a), a second lease topic `t2`, a non-lease topic `n`; in `b`: a lease topic `t`.  A v17 session (testAnnounceClientVersion)
// for the version-gate cells.  advance(ns) moves the lease clock of every worker (single-node only: the view read lease is unbounded).
#include "support/AuthRig.h"
#include "support/LeaseTestClock.h"

#include "gen/lease_claim.gen.h"

#include <rune/client/Client.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/view/ViewPlan.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace rune::node::test {

namespace lbr {
namespace rc = rune::client;
using rune::gen::LeaseClaim;

constexpr const char* kAdminPw = "lease-admin-password-123";
constexpr const char* kUserPw  = "lease-user-password-1234";

inline std::vector<uint8_t> claimBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("lease_claim")
                                      .addFixedStr("claim", 32, 1, true)
                                      .add("a", rune::codec::PrimType::UInt64)
                                      .addFixedStr("b", 8)
                                      .build());
}
inline rune::protocol::LeaseSpec spec(uint32_t ttlMs = 60'000, uint32_t minMs = 1000, uint32_t maxMs = 600'000) {
    rune::protocol::LeaseSpec s{};
    s.ttlMs = ttlMs;
    s.minTtlMs = minMs;
    s.maxTtlMs = maxMs;
    s.maxKeys = 1000;
    s.expiryBatch = 64;
    s.checkpoint = 0;
    s.checkpointIntervalMs = 1000;
    s.checkpointRate = 1;
    return s;
}
inline LeaseClaim claimRow(std::string_view key, uint64_t a = 0, std::string_view b = {}) {
    LeaseClaim r;
    if (!key.empty()) std::memcpy(r.claim.data(), key.data(), std::min(key.size(), r.claim.size()));
    r.a = a;
    if (!b.empty()) std::memcpy(r.b.data(), b.data(), std::min(b.size(), r.b.size()));
    return r;
}
}  // namespace lbr

using namespace std::chrono_literals;
namespace rc = rune::client;

class LeaseBrokerRig {
public:
    using Client = rune::client::Client;
    struct Topic { int32_t id = -1; int32_t partitions = 0; };

    explicit LeaseBrokerRig(const std::string& tag, uint32_t ttlMs = 60'000, uint32_t minMs = 1000, uint32_t maxMs = 600'000)
        : b_(tag, opts_()) {
        b_.beforeStartForTest = [this](BrokerNode&) {};
        b_.bootstrapAdmin(lbr::kAdminPw);
        admin_ = connect_("admin", lbr::kAdminPw);
        for (const char* ns : {"a", "b"}) {
            auto r = admin_->createNamespace(ns);
            if (!r) throw std::runtime_error(std::string("createNamespace ") + ns + ": " + r.error().detail);
        }
        using namespace grants;
        for (const char* u : {"a/ua", "b/ub"})
            b_.createUser(u, lbr::kUserPw,
                          {prefix(SO::Create, ST::Topic, ""), prefix(SO::Alter, ST::Topic, ""), prefix(SO::Drop, ST::Topic, ""),
                           prefix(SO::Describe, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
                           prefix(SO::Read, ST::Topic, ""), prefix(SO::Create, ST::View, ""), prefix(SO::Describe, ST::View, ""),
                           prefix(SO::Read, ST::View, "")});
        if (!clock_.nowNs()) throw std::runtime_error("clock");
        clock_.install(b_.node());   // before the first lease topic
        ua_ = connect_("a/ua", lbr::kUserPw);
        ub_ = connect_("b/ub", lbr::kUserPw);
        ua17_ = connect_("a/ua", lbr::kUserPw, 17);
        t_  = defineLease_(*ua_, "t", 2, ttlMs, minMs, maxMs);
        t2_ = defineLease_(*ua_, "t2", 2, ttlMs, minMs, maxMs);
        bt_ = defineLease_(*ub_, "t", 2, ttlMs, minMs, maxMs);
        const auto n = ua_->defineTopicAndWait("n", 1, "", {}, 20s);
        if (n.errorCode != 0) throw std::runtime_error("define n: " + n.detail);
        n_ = Topic{n.topicId, 1};
        createUserView_();
    }
    ~LeaseBrokerRig() {
        for (auto* c : {&ua17_, &ua_, &ub_, &admin_})
            if (*c) (*c)->close();
    }

    /// A further lease topic in namespace a (the DropTopic cells drop their own).
    Topic defineLease(const std::string& name, uint32_t partitions = 2) { return defineLease_(*ua_, name, partitions, 60'000, 1000, 600'000); }
    AuthBroker&       broker() noexcept { return b_; }
    Client&           ua() { return *ua_; }
    Client&           ub() { return *ub_; }
    Client&           ua17() { return *ua17_; }
    Client&           admin() { return *admin_; }
    const Topic&      t() const noexcept { return t_; }
    const Topic&      t2() const noexcept { return t2_; }
    const Topic&      nonLease() const noexcept { return n_; }
    const Topic&      bt() const noexcept { return bt_; }
    lbr::LeaseClaim   row(std::string_view key, uint64_t a = 0, std::string_view b = {}) const { return lbr::claimRow(key, a, b); }

    /// The injected lease clock's current value (what every worker's lease clock reads).
    int64_t leaseNowNs() const noexcept { return clock_.nowNs(); }

    /// Moves the injected lease clock of every worker; the broker's due Expire / Checkpoint work runs on its next doWork turns.
    void advance(std::chrono::milliseconds ms) { clock_.advance(&b_.node(), std::chrono::duration_cast<std::chrono::nanoseconds>(ms).count()); }

    /// A clean stop and start over the same data dir; the clients are reconnected (fresh sessions), the lease clock resumes on the real
    /// clock (LeaseTestClock::resync).
    void restart() {
        for (auto* c : {&ua17_, &ua_, &ub_, &admin_})
            if (*c) { (*c)->close(); c->reset(); }
        b_.restart(false);
        clock_.resync();
        clock_.install(b_.node());
        admin_ = connect_("admin", lbr::kAdminPw);
        ua_    = connect_("a/ua", lbr::kUserPw);
        ub_    = connect_("b/ub", lbr::kUserPw);
        ua17_  = connect_("a/ua", lbr::kUserPw, 17);
    }

    /// Like restart(), but the rig's clients stay open and reconnect on their own (a watch built on one survives the restart).  The
    /// lease clock resumes on the real clock; the caller waits for the clients' sessions (e.g. by polling a request).
    void restartKeepingClients() {
        b_.restart(false);
        clock_.resync();
        clock_.install(b_.node());
    }

    /// The user view's row bytes for `a` (the projected lease row), or nullopt when the view has none.
    std::optional<std::vector<uint8_t>> userViewRow(const lbr::LeaseClaim& r0) {
        const std::span<const uint8_t> key(reinterpret_cast<const uint8_t*>(r0.claim.data()), r0.claim.size());
        lastViewStatus_ = 0;
        for (int32_t part = 0; part < t_.partitions; ++part) {   // the view has the source's partitions: the row is on one of them
            rc::ViewReadOptions ro;
            ro.partition = part;
            const auto r = ua_->getViewRowAndWait(viewId_, key, ro, 10s);
            lastViewStatus_ = uint16_t(lastViewStatus_ * 100 + r.status);
            if (r.status == rc::ViewRowResult::kOk) return std::vector<uint8_t>(r.rowBytes.begin(), r.rowBytes.end());
        }
        return std::nullopt;
    }
    uint16_t lastViewStatus() const noexcept { return lastViewStatus_; }
    int32_t viewId() const noexcept { return viewId_; }

private:
    static AuthBroker::Options opts_() {
        AuthBroker::Options o;
        o.shard = true;
        return o;
    }
    std::unique_ptr<Client> connect_(const std::string& user, const std::string& pw, int32_t announce = 0) {
        rc::ClientConfig c;
        c.gatewayChannel  = b_.channel();
        c.gatewayStreamId = 1;
        c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        c.egressStreamId  = 2;
        c.dataDir         = b_.clientDir().string();
        c.username        = user;
        c.password        = pw;
        c.offerCrypto     = true;
        c.tlsTrustPem     = AuthBroker::tlsCaPem();
        c.testAnnounceClientVersion = announce;
        auto cl = std::make_unique<Client>(c);
        cl->connectAndWait(20s);
        return cl;
    }
    Topic defineLease_(Client& c, const std::string& name, uint32_t partitions, uint32_t ttlMs, uint32_t minMs, uint32_t maxMs) {
        rc::TopicOptions to;
        to.lease = lbr::spec(ttlMs, minMs, maxMs);
        to.config.cleanupPolicy = 1;
        const auto r = c.defineTopicAndWait(name, partitions, "claim", lbr::claimBlob(), rc::Persistence::Disk, to, {}, 60s);
        if (r.errorCode != 0) throw std::runtime_error("define lease " + name + ": " + r.detail);
        return Topic{r.topicId, static_cast<int32_t>(partitions)};
    }
    void createUserView_() {
        namespace rv = rune::view;
        rv::ViewPlan p;
        p.viewName = "u";
        p.key      = "claim";
        p.persistence = 1;
        p.sources.push_back(rv::SourceSpec{t_.id, "claim"});
        p.output.push_back(rv::AuthoredField{"claim", rune::codec::PrimType::FixedStr, 0, "claim", rv::MergeRule::Replace});
        p.output.push_back(rv::AuthoredField{"a", rune::codec::PrimType::UInt64, 0, "a", rv::MergeRule::Replace});
        const auto blob = rv::serializeAuthored(p);
        const auto r = ua_->createViewAndWait("u", "claim", 1, blob, {}, 30s);
        if (r.errorCode != 0) throw std::runtime_error("create view u: " + r.detail);
        viewId_ = r.viewTopicId;
    }

    LeaseTestClock          clock_;   // before b_: destroyed after it, so no worker reads a freed cell (TSan, broker teardown)
    AuthBroker              b_;
    std::unique_ptr<Client> admin_, ua_, ub_, ua17_;
    Topic                   t_, t2_, bt_, n_;
    int32_t                 viewId_ = -1;
    uint16_t                lastViewStatus_ = 0;
};

}  // namespace rune::node::test
