#pragma once
// AuthTrio.h - authentication Task 19: three BrokerNodes (ids 1..3, real Aeron UDP) with authentication ENABLED, sealed
// peer links keyed by per-node key files (the same by default; `setKey` for the wrong-key / rotation cases), a
// bootstrapped admin, and one raw-frame wire client per broker.  Built on CatalogBrokerCluster.h's ports / tmp dirs.
//
//   start()                    - all three up, a catalog and a __security leader settled
//   restart(i, cur, next)      - node i stopped and started again over the same data dir with those key files
//   bootstrapAdmin()           - `rune security bootstrap` against whichever broker leads __security, then the admin's
//                                one-time password change; afterwards adminOn(i) is an admin session on broker i
//   ddl(prefer, fn)            - a security DDL as the admin, sent to `prefer` first and re-sent to the others on
//                                Redirect / NotLeader (it is the __security leader that answers)
//   login(i, user, pw)         - a fresh session on broker i
// Every wait is bounded (the deadline is the failure, never a hang).
#include "support/AuthBroker.h"
#include "support/SecurityBrokerCluster.h"

#include <rune/node/PeerLinkAgent.h>
#include <rune/node/RoleCounter.h>

#include <CncFileReader.h>
#include <concurrent/CountersReader.h>

#include <array>
#include <fstream>
#include <iterator>
#include <functional>
#include <memory>
#include <optional>

namespace rune::node::test {

using namespace rune::node::test::security_cluster;
using Clock3 = std::chrono::steady_clock;

class AuthTrio {
public:
    static constexpr const char* kPw = "user-password-12345";
    static constexpr const char* kBootstrapPassword = AuthBroker::kBootstrapPassword;
    static constexpr const char* kAdminPassword = AuthBroker::kAdminPassword;

    explicit AuthTrio(const std::string& tag) : keys_("authtrio_keys_" + tag), c_(tag, Mode::Shard) {
        for (int i = 0; i < 3; ++i) {
            keyContents_[i] = std::string(48, 'K') + "\n";
            keyPath_[i] = writeKey_("k" + std::to_string(i + 1) + ".key", keyContents_[i]);
        }
        c_.configTweak = [this](NodeConfig& cfg) {
            const auto i = static_cast<std::size_t>(cfg.nodeId - 1);
            cfg.authMode = authMode_;
            cfg.clientTransport = ClientTransport::PlaintextForTest;   // issue #10: the raw-frame client is plaintext
            cfg.tlsCertFile = std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/server.pem";   // ... and TLS clients get TLS
            cfg.tlsKeyFile  = std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/server.key";
            cfg.clientEndpoint.streamId = AuthWireClient::kClientStream;   // the raw-frame client speaks stream 1
            cfg.clusterKeyFile = keyPath_[i];
            cfg.clusterKeyFileNext = nextPath_[i];
        };
    }

    /// The CA of the brokers' test certificate (issue #14: a credentialed client needs TLS).
    [[nodiscard]] static std::string tlsCaPem() {
        std::ifstream f(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/ca.pem", std::ios::binary);
        return std::string((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    }

    ThreeNodes& cluster() noexcept { return c_; }
    BrokerNode& node(int id) { return c_.node(id); }
    [[nodiscard]] std::string channel(int id) const {
        return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(c_.ports.client[static_cast<std::size_t>(id - 1)]);
    }
    [[nodiscard]] const std::string& keyContents(int id) const { return keyContents_[static_cast<std::size_t>(id - 1)]; }

    /// A key file of `fill` bytes for node `id` (current); "" path = no key configured.  Takes effect at the next start.
    std::string makeKeyFile(const std::string& name, char fill) {
        return writeKey_(name, std::string(48, fill) + "\n");
    }
    void setKey(int id, const std::string& current, const std::string& next = {}) {
        keyPath_[static_cast<std::size_t>(id - 1)] = current;
        nextPath_[static_cast<std::size_t>(id - 1)] = next;
    }

    /// Start all three and wait for the catalog and the __security leaders.
    void start() {
        for (int id : {1, 2, 3}) c_.startNode(id);
        if (!c_.awaitCatalogLeader(std::chrono::seconds(60))) throw std::runtime_error("AuthTrio: no catalog leader");
        if (!awaitSecurityLeader(c_, std::chrono::seconds(60))) throw std::runtime_error("AuthTrio: no __security leader");
    }
    /// Stop node i and start it again over the same dir with the given key files; its wires/sessions are dropped.
    void restart(int id, const std::string& current, const std::string& next = {}) {
        dropSessions_(id);
        setKey(id, current, next);
        c_.stop(id);
        c_.startNode(id);
    }
    [[nodiscard]] const std::string& keyContentsPath(int id) const { return keyPath_[static_cast<std::size_t>(id - 1)]; }
    void setAuthMode(AuthMode m) noexcept { authMode_ = m; }
    void stopNode(int id) { dropSessions_(id); c_.stop(id); }

    // ── wires and sessions ───────────────────────────────────────────────────────────────────────────────────
    /// The ONE embedded Aeron driver of this trio (a client with sessions on every broker, for every user): it lives as
    /// long as the trio, so a test with many cells does not pile up drivers.  Sessions, not wires, are per user / broker;
    /// a session whose broker restarted is dead, the wire itself stays valid (no dangling pointers).
    AuthWireClient& wire() {
        if (!wire_) wire_ = std::make_unique<AuthWireClient>(c_.clientsDir->path / "trio_wire");
        return *wire_;
    }
    AuthWireClient& newWire(int /*id*/) { return wire(); }
    /// A logged-in session on broker `id` (on the trio wire).  Throws if the login is refused.
    struct Session { AuthWireClient* w = nullptr; int64_t sid = 0; int node = 0; };
    Session login(int id, const std::string& user, const std::string& pw = kPw) {
        return loginOn(wire(), id, user, pw);
    }

    /// Log `user` in on broker `id` through an EXISTING wire (one wire can hold sessions on all three brokers).
    Session loginOn(AuthWireClient& w, int id, const std::string& user, const std::string& pw = kPw) {
        const int64_t sid = w.connect(channel(id));
        if (sid <= 0) throw std::runtime_error("AuthTrio: connect failed on node " + std::to_string(id));
        const auto l = w.login(channel(id), sid, user, {pw, pw});
        if (!l.ok()) throw std::runtime_error("AuthTrio: login refused for " + user + " on node " + std::to_string(id));
        return {&w, sid, id};
    }

    void bootstrapAdmin() {
        const auto deadline = Clock3::now() + std::chrono::seconds(60);
        bool done = false;
        std::string attempts;   // every attempt (node, answer, when): the failure names the hop that did not answer
        auto note = [&](int id, const std::string& what) {
            attempts += " [+" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                    Clock3::now() - (deadline - std::chrono::seconds(60))).count()) +
                        " ms node " + std::to_string(id) + ": " + what + "]";
        };
        while (!done && Clock3::now() < deadline) {
            for (int id : {1, 2, 3}) {
                AuthWireClient& w = newWire(id);
                const int64_t s0 = w.connect(channel(id));
                if (s0 <= 0) { note(id, "no connect"); continue; }
                // the cluster-key MAC is under node `id`'s key contents (all equal at bootstrap)
                const auto b = w.bootstrap(channel(id), s0, keyContents(id), "admin", kBootstrapPassword);
                // The bootstrap session never logs in: end it, or its authentication deadline counts a failed login.
                (void)w.raw().disconnect(channel(id), AuthWireClient::kClientStream, s0);
                if (b.errorCode == 0) { done = true; break; }
                note(id, "code " + std::to_string(b.errorCode) + " " + b.detail);
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
        if (!done) throw std::runtime_error("AuthTrio: bootstrap never accepted:" + attempts);
        // first login (one-time password) on whichever broker applied it first, then set the real password
        const auto lg = Clock3::now() + std::chrono::seconds(30);
        bool changed = false;
        while (!changed && Clock3::now() < lg) {
            for (int id : {1, 2, 3}) {
                AuthWireClient& w = newWire(id);
                const int64_t s1 = w.connect(channel(id));
                if (s1 <= 0) continue;
                const auto l = w.login(channel(id), s1, "admin", {kBootstrapPassword, kBootstrapPassword});
                if (!l.ok() || !l.mustChangePassword()) continue;
                const auto set = w.alterPassword(channel(id), s1, "admin", pc::UserChangeKind::SetPassword, kAdminPassword);
                if (set.errorCode == 0) { changed = true; break; }
            }
            if (!changed) std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        if (!changed) throw std::runtime_error("AuthTrio: admin password change never accepted");
        // an admin session on every broker (the new password reaches the others asynchronously)
        for (int id : {1, 2, 3}) {
            admin_[static_cast<std::size_t>(id - 1)] = loginRetry_(id, "admin", kAdminPassword);
            adminUsedAt_[static_cast<std::size_t>(id - 1)] = Clock3::now();
        }
    }
    /// The admin session on broker `id`.  The raw wire client sends no keepalives, so a broker reaps an idle session
    /// after its session timeout (ClientGateway::kDefaultSessionTimeoutNs, 10 s) and a frame on it is dropped
    /// unanswered - each such attempt cost a whole answer deadline (30 s), so a test idle for 10 s+ before a DDL ran
    /// out of time (ClientAuthClusterRegression.WorldServerClientProducesAndConsumesThroughEveryBroker).  A session
    /// idle for half the timeout is replaced by a fresh login before it is used.
    Session adminOn(int id) {
        auto& s = admin_[static_cast<std::size_t>(id - 1)];
        auto& used = adminUsedAt_[static_cast<std::size_t>(id - 1)];
        if (s.w && Clock3::now() - used > std::chrono::seconds(5)) s = {};
        if (!s.w) s = loginRetry_(id, "admin", kAdminPassword);
        used = Clock3::now();
        return s;
    }

    /// A security DDL as the admin: `fn(wire, channel, adminSid)`; sent to `prefer` first, then the other brokers on a
    /// Redirect (3) / NotLeader (2) answer.  Bounded.
    AuthWireClient::DdlAnswer ddl(int prefer, const std::function<AuthWireClient::DdlAnswer(AuthWireClient&, const std::string&, int64_t)>& fn,
                                  std::chrono::milliseconds within = std::chrono::seconds(30)) {
        const auto deadline = Clock3::now() + within;
        AuthWireClient::DdlAnswer last{0xFFFF, 0, 0, -1, "no attempt"};
        for (;;) {
            for (int k = 0; k < 3; ++k) {
                const int id = (prefer - 1 + k) % 3 + 1;
                if (!c_.running(id)) continue;
                const Session a = adminOn(id);
                last = fn(*a.w, channel(id), a.sid);
                // 0xFFFF (no answer: the session idle-expired) / 44 / 46 (revoked): log in again before the next attempt
                if (last.errorCode == 0xFFFF || last.errorCode == 44 || last.errorCode == 46) { admin_[static_cast<std::size_t>(id - 1)] = {}; continue; }
                // 48 (issues #15 / #111): that broker's security state is not current right now - try another.
                if (last.errorCode != 2 && last.errorCode != 3 && last.errorCode != 48) return last;
            }
            if (Clock3::now() >= deadline) return last;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    void mustOk(const AuthWireClient::DdlAnswer& a, const std::string& what) {
        if (a.errorCode != 0) throw std::runtime_error("AuthTrio: " + what + " failed: " + std::to_string(a.errorCode) + " " + a.detail);
    }
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    using SP = rune::protocol::SecurityPatternKind;
    void createUser(int prefer, const std::string& name, const std::string& pw = kPw) {
        mustOk(ddl(prefer, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.createUser(ch, sid, name, pw); }), "CREATE USER " + name);
    }
    void grant(int prefer, const std::string& who, SO op, ST t, SP k, const std::string& pattern, bool role = false) {
        mustOk(ddl(prefer, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.grant(ch, sid, who, role, op, t, k, pattern); }), "GRANT " + who);
    }
    AuthWireClient::DdlAnswer revoke(int prefer, const std::string& who, SO op, ST t, SP k, const std::string& pattern, bool role = false) {
        return ddl(prefer, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.revoke(ch, sid, who, role, op, t, k, pattern); });
    }

    // ── observation ──────────────────────────────────────────────────────────────────────────────────────────
    /// The aeron CnC counter of broker `id` (type id), nullopt = absent.
    [[nodiscard]] std::optional<int64_t> counter(int id, std::int32_t typeId) const {
        try {
            const std::string dir = (c_.dirs[static_cast<std::size_t>(id - 1)]->path / "aeron").string();
            aeron::CncFileReader reader = aeron::CncFileReader::mapExisting(dir.c_str());
            aeron::concurrent::CountersReader counters = reader.countersReader();
            std::optional<int64_t> found;
            counters.forEach([&](std::int32_t cid, std::int32_t t, const aeron::concurrent::AtomicBuffer&, const std::string&) {
                if (t == typeId && !found) found = counters.getCounterValue(cid);
            });
            return found;
        } catch (...) { return std::nullopt; }
    }
    /// Every running broker's SecurityView is at the same applied offset (>= `atLeast`) with the same digest.
    bool awaitDigestsAgree(int64_t atLeast, std::chrono::milliseconds within, uint64_t* digestOut = nullptr) {
        const auto deadline = Clock3::now() + within;
        while (Clock3::now() < deadline) {
            bool ok = true;
            int64_t off = -2; uint64_t dg = 0;
            for (BrokerNode* n : c_.live()) {
                const auto v = n->securityViewForTest();
                if (!v.present || v.halted || v.appliedOffset < atLeast) { ok = false; break; }
                if (off == -2) { off = v.appliedOffset; dg = v.digest; }
                else if (v.appliedOffset != off || v.digest != dg) { ok = false; break; }
            }
            if (ok) { if (digestOut) *digestOut = dg; return true; }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }
    template <class Pred> static bool until(std::chrono::milliseconds within, Pred&& p) {
        const auto deadline = Clock3::now() + within;
        while (Clock3::now() < deadline) {
            if (p()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return p();
    }

private:
    std::string writeKey_(const std::string& name, const std::string& contents) {
        const std::filesystem::path p = keys_.path / name;
        { std::ofstream f(p, std::ios::binary); f << contents; }
        std::filesystem::permissions(p, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
        return p.string();
    }
    Session loginRetry_(int id, const std::string& user, const std::string& pw) {
        const auto deadline = Clock3::now() + std::chrono::seconds(30);
        for (;;) {
            try { return login(id, user, pw); }
            catch (const std::exception&) { if (Clock3::now() >= deadline) throw; }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    void dropSessions_(int id) { admin_[static_cast<std::size_t>(id - 1)] = {}; }   // the broker's sessions died with it

    catalog_cluster::TmpDir keys_;
    ThreeNodes c_;
    std::array<std::string, 3> keyContents_, keyPath_, nextPath_;
    std::unique_ptr<AuthWireClient> wire_;
    std::array<Session, 3> admin_{};
    std::array<Clock3::time_point, 3> adminUsedAt_{};   ///< last hand-out of admin_[i] (adminOn)
    AuthMode authMode_ = AuthMode::Enabled;
};

} // namespace rune::node::test
