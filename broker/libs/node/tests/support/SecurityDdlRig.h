#pragma once
// SecurityDdlRig.h — authentication Task 10: one SecurityAdmin driving a real `__security` leader replica and its
// SecurityApplier, with recording answers (CatalogDdlRig.h's counterpart).
//
//   * a real disk Log under a temp dir (`<tmp>/__security`) and a PartitionReplica over it, replicas {1..brokers}, self 1;
//   * becomeServingLeader(): the role (leader, ISR) pumped until the admin serves; every ISR follower that is not
//     SILENT fetches at the leader's LEO on each pump (an append commits on the next pass);
//   * a deterministic clock; every drive loop is capped and fails loudly rather than hanging;
//   * answers by correlation id and recordedEvents() — the events the log holds.
// Single-threaded: plain members.

#include <rune/node/AdminChannels.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/SecurityAdmin.h>
#include <rune/node/SecurityApplier.h>
#include <rune/node/SecurityPartition.h>

#include <rune/crypto/Scram.h>
#include <algorithm>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/SecurityMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/security/SecurityEvent.h>
#include <rune/storage/Log.h>

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

namespace rune::node::test {

namespace sec_ddl_detail { inline int gSeq = 0; }

class SecurityDdlRig {
public:
    static constexpr int32_t kSelf    = 1;
    static constexpr int64_t kSession = 5252;
    static constexpr int kCap = 4096;

    struct Answer {
        int64_t     corr = 0;
        uint16_t    code = 0;
        uint64_t    objectId = 0;
        uint32_t    version = 0;
        int64_t     gateOffset = -1;
        std::string detail;
    };

    explicit SecurityDdlRig(const char* tag, int32_t brokers = 3)
        : root_(std::filesystem::temp_directory_path() /
                (std::string("rune_secddl_") + tag + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(sec_ddl_detail::gSeq++))),
          brokers_(brokers), applier_(securityDir_()) {
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(root_ / "__security");
        open_();
    }
    ~SecurityDdlRig() {
        admin_.reset();
        replica_.reset();
        log_.reset();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }
    SecurityDdlRig(const SecurityDdlRig&)            = delete;
    SecurityDdlRig& operator=(const SecurityDdlRig&) = delete;

    // ── roles ─────────────────────────────────────────────────────────────────
    void becomeServingLeader(std::vector<int32_t> isr = {}) {
        if (isr.empty()) silent_.clear();
        else for (int32_t f : isr) if (f != kSelf) silent_.insert(f);
        assumeLeadership(std::move(isr));
        for (int i = 0; i < kCap && admin_->roleName() != "LeaderServing"; ++i) pump();
        if (admin_->roleName() != "LeaderServing")
            throw std::logic_error("SecurityDdlRig: the leader never served (" + std::string(admin_->roleName()) + ")");
    }
    void assumeLeadership(std::vector<int32_t> isr = {}) {
        if (isr.empty()) for (int32_t b = 1; b <= brokers_; ++b) isr.push_back(b);
        isr_ = isr;
        AdminRoleMsg r;
        r.leader = true; r.epoch = ++epoch_; r.leaderId = kSelf; r.partitionEpoch = 0;
        r.isrCount = static_cast<int32_t>(isr.size());
        for (std::size_t i = 0; i < isr.size(); ++i) r.isr[i] = isr[i];
        admin_->onRole(r);
    }
    void becomeFollower(int32_t leaderId, bool drive = true) {
        AdminRoleMsg r;
        r.leader = false; r.epoch = ++epoch_; r.leaderId = leaderId;
        admin_->onRole(r);
        if (drive) pump();
    }
    void silence(int32_t follower) { silent_.insert(follower); }
    void unsilence(int32_t follower) { silent_.erase(follower); }

    // ── requests ──────────────────────────────────────────────────────────────
    /// Submit and pump until answered (capped).
    Answer ddl(SecurityDdlMsg m) { return await_(submitNoWait(std::move(m))); }
    Answer bootstrap(SecurityBootstrapMsg m) { return await_(submitNoWait(std::move(m))); }
    int64_t submitNoWait(SecurityDdlMsg m) {
        m.session = kSession; if (m.corr == 0) m.corr = ++corr_;
        const int64_t corr = m.corr;
        admin_->submit(SecurityRequest{std::move(m)});
        for (int i = 0; i < 4; ++i) pump();
        return corr;
    }
    int64_t submitNoWait(SecurityBootstrapMsg m) {
        m.session = kSession; if (m.corr == 0) m.corr = ++corr_;
        const int64_t corr = m.corr;
        admin_->submit(SecurityRequest{std::move(m)});
        for (int i = 0; i < 4; ++i) pump();
        return corr;
    }
    Answer await_(int64_t corr) {
        for (int i = 0; i < kCap && !answers_.contains(corr); ++i) pump();
        const auto it = answers_.find(corr);
        if (it == answers_.end()) throw std::logic_error("SecurityDdlRig: request " + std::to_string(corr) + " was never answered");
        return it->second;
    }
    [[nodiscard]] std::optional<Answer> answerFor(int64_t corr) const {
        const auto it = answers_.find(corr);
        if (it == answers_.end()) return std::nullopt;
        return it->second;
    }
    [[nodiscard]] std::size_t answerCount() const noexcept { return answers_.size(); }
    [[nodiscard]] int64_t nextCorr() noexcept { return ++corr_; }

    // ── request builders ──────────────────────────────────────────────────────
    /// The meta every rig request carries (a fresh requestId unless `rid` is set).
    DdlMeta meta(const std::string& statement, const std::string& authenticated = "admin",
                 rune::protocol::RequestId rid = {}) {
        DdlMeta m;
        m.statement = statement;
        m.clientName = "rig";
        m.sourceAddress = "aeron:udp?endpoint=rig-client:40123";
        m.authenticated = authenticated;
        m.sessionId = kSession;
        m.requestId = rid.none() ? rune::protocol::RequestId{0x5EC1, ++ridCounter_} : rid;
        return m;
    }
    static rune::security::ScramVerifier verifierFor(const std::string& password) {
        auto v = rune::crypto::scram::makeVerifier(password, 4096);
        if (!v) throw std::logic_error("SecurityDdlRig: makeVerifier failed");
        rune::security::ScramVerifier out;
        out.salt = v->salt;
        out.iterations = v->iterations;
        std::copy_n(v->storedKey.bytes.begin(), 64, out.storedKey.begin());
        std::copy_n(v->serverKey.bytes.begin(), 64, out.serverKey.begin());
        return out;
    }
    SecurityDdlMsg createUser(const std::string& name, const std::string& password, const std::string& principal = "admin") {
        SecurityDdlMsg m;
        m.kind = static_cast<uint8_t>(rune::protocol::client::SecurityOpKind::CreateUser);
        m.name = name;
        m.verifier = verifierFor(password);
        m.meta = meta("CREATE USER " + name + " WITH PASSWORD '<redacted>';", principal);
        return m;
    }
    SecurityDdlMsg createRole(const std::string& name) {
        SecurityDdlMsg m;
        m.kind = static_cast<uint8_t>(rune::protocol::client::SecurityOpKind::CreateRole);
        m.name = name;
        m.meta = meta("CREATE ROLE " + name + ";");
        return m;
    }
    SecurityDdlMsg simple(rune::protocol::client::SecurityOpKind kind, const std::string& name, const std::string& stmt) {
        SecurityDdlMsg m;
        m.kind = static_cast<uint8_t>(kind);
        m.name = name;
        m.meta = meta(stmt);
        return m;
    }
    SecurityDdlMsg alterUser(const std::string& name, rune::protocol::client::UserChangeKind change) {
        SecurityDdlMsg m = simple(rune::protocol::client::SecurityOpKind::AlterUser, name, "ALTER USER " + name + ";");
        m.change = static_cast<uint8_t>(change);
        return m;
    }
    SecurityDdlMsg grant(bool revoke, rune::security::GranteeKind who, const std::string& name, rune::security::Operation op,
                         rune::security::ResourceType type, rune::security::PatternKind pk, const std::string& pattern) {
        SecurityDdlMsg m = simple(revoke ? rune::protocol::client::SecurityOpKind::Revoke
                                         : rune::protocol::client::SecurityOpKind::Grant, name, revoke ? "REVOKE ..." : "GRANT ...");
        m.granteeKind = static_cast<uint8_t>(who);
        m.op = static_cast<uint8_t>(op);
        m.resourceType = static_cast<uint8_t>(type);
        m.patternKind = static_cast<uint8_t>(pk);
        m.pattern = pattern;
        return m;
    }
    SecurityDdlMsg grantRole(bool revoke, const std::string& user, const std::string& role) {
        SecurityDdlMsg m = simple(revoke ? rune::protocol::client::SecurityOpKind::RevokeRole
                                         : rune::protocol::client::SecurityOpKind::GrantRole, user,
                                  std::string(revoke ? "REVOKE ROLE " : "GRANT ROLE ") + role + ";");
        m.roleName = role;
        return m;
    }
    SecurityBootstrapMsg bootstrapMsg(const std::string& admin, const std::string& password, uint8_t nonceSeed = 1) {
        SecurityBootstrapMsg m;
        m.adminName = admin;
        m.verifier = verifierFor(password);
        for (std::size_t i = 0; i < m.fakeSaltSeed.size(); ++i) m.fakeSaltSeed[i] = static_cast<uint8_t>(0x70 + i);
        for (std::size_t i = 0; i < m.nonce.size(); ++i) m.nonce[i] = static_cast<uint8_t>(nonceSeed + i);
        m.meta.clientName = "rune-cli";
        m.meta.sourceAddress = "aeron:udp?endpoint=rig-client:40123";
        m.meta.sessionId = kSession;
        return m;
    }

    // ── time / driving ────────────────────────────────────────────────────────
    void advanceClockPast(int64_t ns) { now_ += ns + 1; pump(); }
    void pump() {
        now_ += 1'000'000;
        if (replica_->role() == rune::replication::Role::Leader)
            for (int32_t f : isr_) if (f != kSelf && !silent_.contains(f)) fetch_(f);
        (void)replica_->doWork(now_);
        (void)applier_.applyCommitted(*replica_);
        (void)admin_->doWork(now_);
    }

    // ── observation ───────────────────────────────────────────────────────────
    struct Recorded { int64_t offset; rune::security::SecurityEvent event; std::vector<uint8_t> bytes; };
    [[nodiscard]] std::vector<Recorded> recordedEvents() const {
        std::vector<Recorded> out;
        int64_t next = 0;
        const int64_t leo = log_->logEndOffset();
        while (next < leo) {
            const std::vector<uint8_t> data = log_->read(next, 1 << 22);
            if (data.empty()) break;
            std::size_t pos = 0;
            const int64_t before = next;
            while (pos < data.size()) {
                rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                if (!reader.ensureValid()) break;
                const auto tl = static_cast<std::size_t>(reader.totalLength());
                reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                    if (off < next) return;
                    std::vector<uint8_t> bytes(reinterpret_cast<const uint8_t*>(val.data()),
                                               reinterpret_cast<const uint8_t*>(val.data()) + val.size());
                    auto ev = rune::security::decode(bytes);
                    if (!ev) throw std::logic_error("SecurityDdlRig: a logged event does not decode");
                    out.push_back(Recorded{off, std::move(*ev), std::move(bytes)});
                    next = off + 1;
                });
                if (tl == 0) break;
                pos += tl;
            }
            if (next == before) break;
        }
        return out;
    }
    [[nodiscard]] SecurityAdmin& admin() { return *admin_; }
    [[nodiscard]] SecurityApplier& applier() { return applier_; }
    [[nodiscard]] rune::replication::PartitionReplica& replica() { return *replica_; }
    [[nodiscard]] const rune::security::SecurityState& state() const { return applier_.state(); }

private:
    [[nodiscard]] std::string securityDir_() const { return (root_ / "__security").string(); }

    void open_() {
        log_ = std::make_unique<rune::storage::Log>(securityDir_());
        log_->recover();
        NodeConfig nc;
        nc.nodeId = kSelf;
        std::vector<rune::replication::NodeId> all;
        for (int32_t b = 1; b <= brokers_; ++b) all.push_back(b);
        replica_ = std::make_unique<rune::replication::PartitionReplica>(
            makeSecurityPartitionConfig(nc, static_cast<std::size_t>(brokers_)), rune::replication::Role::Follower, 0,
            all, all, log_.get(), &transport_);
        (void)applier_.replayLocal(*log_, SecurityBootEffects{});
        applier_.goLive(SecurityLiveEffects{});
        admin_ = std::make_unique<SecurityAdmin>(
            applier_, replica_.get(),
            [this](int64_t, int64_t corr, uint16_t code, uint64_t objectId, uint32_t version, int64_t gateOffset,
                   std::string_view detail) {
                answers_[corr] = Answer{corr, code, objectId, version, gateOffset, std::string{detail}};
            },
            kSelf);
    }
    void fetch_(int32_t follower) {
        rune::replication::FetchRequestMsg f{};
        f.leaderEpoch = epoch_;
        f.partition = kSecurityPartition;
        f.followerId = follower;
        f.fetchOffset = replica_->logEndOffset();
        f.maxBytes = 1 << 20;
        f.topicId = kSecurityTopicId;
        f.lastFetchedEpoch = -1;
        replica_->onFetchRequest(f, now_);
    }

    std::filesystem::path root_;
    int32_t brokers_;
    SecurityApplier applier_;
    std::unique_ptr<rune::storage::Log> log_;
    rune::replication::NullPartitionTransport transport_;
    std::unique_ptr<rune::replication::PartitionReplica> replica_;
    std::unique_ptr<SecurityAdmin> admin_;
    std::vector<int32_t> isr_;
    std::set<int32_t> silent_;
    std::map<int64_t, Answer> answers_;
    int32_t epoch_ = 0;
    int64_t now_ = 1'000'000, corr_ = 0;
    uint64_t ridCounter_ = 0;
};

} // namespace rune::node::test
