#pragma once
// AuthBroker.h - ONE real BrokerNode with authentication ENABLED (single node, real Aeron UDP), a generated cluster key
// file, and the helpers every authentication test needs (authentication Task 11; used by Tasks 11-19):
//
//   bootstrapAdmin()          - `rune security bootstrap` the way the CLI does it over the wire (cluster-key MAC), the
//                               one-time-password login, and the admin's own password change; afterwards an admin
//                               session is kept for the helpers below
//   createUser(name, pw, grants...) / createRole / grant / grantRole / alter*  - security DDL as that admin
//   wire()                    - a fresh AuthWireClient (its own embedded Aeron driver) for raw-frame work
//   counter(typeId)           - a metric of this broker (its metrics file) (1'000'229-231 = logins ok / failed / rate-limited)
//   LogCapture                - the broker's control-thread log lines (reason=..., "authenticated over plaintext")
//
// Every wait is bounded (the deadline is the failure, never a hang).  Options: DevDisabled mode, a short auth deadline.

#include "support/AuthWireClient.h"
#include "support/SecurityBrokerCluster.h"
#include "support/TestNodeConfig.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/RuneMetrics.h>

#include "MetricsTestReader.h"

#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

namespace rune::node::test {

/// RAII: captures the default logger (every broker thread logs through it) and restores it.  Install BEFORE the broker.
struct LogCapture {
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(4096);
    std::shared_ptr<spdlog::logger> previous = spdlog::default_logger();
    LogCapture() {
        auto capture = std::make_shared<spdlog::logger>("auth-log-capture", sink);
        capture->set_level(spdlog::level::info);
        spdlog::set_default_logger(capture);
    }
    ~LogCapture() { spdlog::set_default_logger(previous); }
    LogCapture(const LogCapture&) = delete;
    LogCapture& operator=(const LogCapture&) = delete;
    [[nodiscard]] int count(const std::string& text) const {
        int n = 0;
        for (const std::string& l : sink->last_formatted()) n += l.find(text) != std::string::npos ? 1 : 0;
        return n;
    }
    [[nodiscard]] std::string all() const {
        std::string out;
        for (const std::string& l : sink->last_formatted()) out += l;
        return out;
    }
};

struct AuthGrant {
    rune::protocol::SecurityOperation op = rune::protocol::SecurityOperation::Read;
    rune::protocol::SecurityResourceType type = rune::protocol::SecurityResourceType::Topic;
    rune::protocol::SecurityPatternKind kind = rune::protocol::SecurityPatternKind::Exact;
    std::string pattern;
};

class AuthBroker {
public:
    struct Options {
        AuthMode mode = AuthMode::Enabled;
        int64_t  authTimeoutMs = 10'000;
        std::string keyContents = std::string(48, 'K') + "\n";
        bool shard = false;   ///< shard worker mode (clients can produce straight to the worker ingress; Task 13)
        /// Issues #10 / #137: the broker always serves the generated test certificate (a client offering TLS gets an
        /// encrypted session).  tlsOnly = the DEFAULT transport of an authenticating broker: every client session TLS,
        /// a plaintext Connect HandshakeFailed.  false = ClientTransport::PlaintextForTest, so the raw-frame helpers
        /// (bootstrapAdmin, wire()) still drive plaintext sessions.
        bool tlsOnly = false;
        /// Applied to the node's config last, before the start (a test's own knobs, e.g. the provisioner cap).
        std::function<void(NodeConfig&)> configure;
    };
    static constexpr const char* kBootstrapPassword = "bootstrap-otp-123456";
    static constexpr const char* kAdminPassword     = "admin-password-1234";

    explicit AuthBroker(const std::string& tag) : AuthBroker(tag, Options{}) {}
    AuthBroker(const std::string& tag, Options o)
        : opts_(std::move(o)),
          dir_(std::filesystem::temp_directory_path() /
               ("rune_authbroker_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq_()++))) {
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
        NodeConfig cfg;
        boundTestSegments(cfg);
        if (!opts_.shard) pinLegacyRuntime(cfg);
        cfg.nodeId = 0;
        cfg.dataDir = (dir_ / "broker").string();
        cfg.aeronDir = (dir_ / "broker" / "aeron").string();
        cfg.consensusStreamId = 10;
        PeerConfig self;
        self.id = 0;
        self.host = "127.0.0.1";
        self.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();
        cfg.peers = {self};
        clientPort_ = rune::aeronutil::testing::allocFreeUdpPort();
        cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort_);
        cfg.clientEndpoint.streamId = 1;
        cfg.workerCount = 1;
        if (opts_.shard) {   // like ShardModeTest::makeShardConfig: explicit collision-free ports, one worker
            const auto blocks = freeUdpPortBlocks(3, 1);
            cfg.workerMode = NodeConfig::WorkerMode::Shard;
            cfg.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
            cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = true;
            cfg.workerIngressBasePort = blocks[0];
            cfg.workerReplBasePort = blocks[1];
            cfg.workerEgressControlHost = "127.0.0.1";
            cfg.workerEgressControlBasePort = blocks[2];
            cfg.workerIngressBaseStreamId = 111;
            cfg.workerCoreBase = -1;
            cfg.aeronTermBufferBytes = 1 << 20;
            cfg.replicationStreamId = 100;
            cfg.replBaseStreamId = 200;
            cfg.peers[0].clientPort = clientPort_;
            cfg.peers[0].replicationPort = rune::aeronutil::testing::allocFreeUdpPort();
            cfg.peers[0].replBasePort = blocks[1];
            cfg.peers[0].ingressBasePort = blocks[0];
            cfg.peers[0].workerCount = 1;
            cfg.peers[0].egressControlBasePort = blocks[2];
            ingressChannel_ = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(blocks[0]);
        }
        cfg.authMode = opts_.mode;
        cfg.authTimeoutMs = opts_.authTimeoutMs;
        cfg.tlsCertFile = tlsCaFile().parent_path().string() + "/server.pem";
        cfg.tlsKeyFile  = tlsCaFile().parent_path().string() + "/server.key";
        if (!opts_.tlsOnly) cfg.clientTransport = ClientTransport::PlaintextForTest;
        if (opts_.mode == AuthMode::Enabled) {
            keyFile_ = dir_ / "cluster.key";
            { std::ofstream f(keyFile_, std::ios::binary); f << opts_.keyContents; }
            std::filesystem::permissions(keyFile_, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
            cfg.clusterKeyFile = keyFile_.string();
        }
        if (opts_.configure) opts_.configure(cfg);
        channel_ = cfg.clientEndpoint.channel;
        cfg_ = cfg;
        startNode_();
    }
    /// Stop the broker and start it again over the SAME data dir (the catalog and the security log replay at boot);
    /// the admin session is logged in again (the bootstrap and the password change persisted).  Every wire() obtained
    /// before is gone (its sessions died with the broker).
    /// `reloginAdmin` false: the admin may no longer log in (disabled / dropped) - nobody is logged in afterwards.
    void restart(bool reloginAdmin = true) {
        const bool hadAdmin = admin_ != nullptr && reloginAdmin;
        admin_ = nullptr;
        adminSid_ = 0;
        wires_.clear();
        node_->stop();
        node_.reset();
        startNode_();
        loginsOkDone_ = 0;   // a fresh node: its logins-ok counter starts again at 0
        if (hadAdmin) {
            AuthWireClient& w = wire();
            const int64_t sid = w.connect(channel_);
            require(sid > 0, "admin reconnect");
            require(w.login(channel_, sid, "admin", {kAdminPassword, kAdminPassword}).ok(), "admin login after restart");
            ++loginsOkDone_;
            admin_ = &w;
            adminSid_ = sid;
        }
    }
    ~AuthBroker() {
        admin_ = nullptr;
        wires_.clear();
        if (node_) node_->stop();
        node_.reset();
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }
    AuthBroker(const AuthBroker&) = delete;
    AuthBroker& operator=(const AuthBroker&) = delete;

    [[nodiscard]] BrokerNode& node() noexcept { return *node_; }
    [[nodiscard]] const std::string& channel() const noexcept { return channel_; }
    /// Shard mode: worker 0's produce ingress endpoint (clients dial it with the client stream id).
    [[nodiscard]] const std::string& workerIngressChannel() const noexcept { return ingressChannel_; }
    [[nodiscard]] std::string endpoint() const { return "127.0.0.1:" + std::to_string(clientPort_); }
    [[nodiscard]] const std::string& keyContents() const noexcept { return opts_.keyContents; }
    [[nodiscard]] const std::filesystem::path& keyFile() const noexcept { return keyFile_; }
    /// The CA that signed the broker's test certificate (names localhost and 127.0.0.1): the CLI's --tls-ca, a
    /// client's tlsTrustPem.
    [[nodiscard]] static std::filesystem::path tlsCaFile() { return std::filesystem::path(RUNE_CRYPTO_TESTDATA_DIR) / "ca.pem"; }
    [[nodiscard]] static std::string tlsCaPem() {
        std::ifstream f(tlsCaFile(), std::ios::binary);
        return std::string((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    }
    [[nodiscard]] const std::filesystem::path& dir() const noexcept { return dir_; }
    /// A fresh directory for a client's own Aeron driver / data.
    [[nodiscard]] std::filesystem::path clientDir() { return dir_ / ("client" + std::to_string(clientDirs_++)); }

    /// A fresh raw-frame client (its own embedded driver).  Owned here: it never outlives the broker.
    AuthWireClient& wire() {
        wires_.push_back(std::make_unique<AuthWireClient>(clientDir()));
        return *wires_.back();
    }

    /// Bootstrap (cluster-key MAC), log in with the one-time password, change the admin's password; keeps that admin
    /// session.  Throws on any step that does not behave (a test precondition).
    void bootstrapAdmin(const std::string& adminPassword = kAdminPassword) {
        AuthWireClient& w = wire();
        const int64_t s0 = w.connect(channel_);
        require(s0 > 0, "bootstrap connect");
        require(w.lastConnect() && w.lastConnect()->authState == pc::AuthState::Anonymous, "the bootstrap window is offered");
        auto b = w.bootstrap(channel_, s0, opts_.keyContents, "admin", kBootstrapPassword);
        require(b.errorCode == 0, "bootstrap: " + b.detail);
        // The bootstrap connection never logs in: end it, or its authentication deadline would count it as one failed login
        // (logins-failed counter) 10 s later, in the middle of whatever a test asserts about that counter.
        (void)w.raw().disconnect(channel_, AuthWireClient::kClientStream, s0);
        const int64_t s1 = w.connect(channel_);
        require(s1 > 0, "admin connect");
        const auto login = w.login(channel_, s1, "admin", {kBootstrapPassword, kBootstrapPassword});
        require(login.ok() && login.mustChangePassword(), "the one-time password logs in and must be changed");
        ++loginsOkDone_;
        const auto set = w.alterPassword(channel_, s1, "admin", pc::UserChangeKind::SetPassword, adminPassword);
        require(set.errorCode == 0, "admin SET PASSWORD: " + set.detail);
        admin_ = &w;
        adminSid_ = s1;
    }

    void createUser(const std::string& name, const std::string& password, const std::vector<AuthGrant>& grants = {}) {
        auto a = admin().createUser(channel_, adminSid_, name, password);
        require(a.errorCode == 0, "CREATE USER " + name + ": " + a.detail);
        for (const AuthGrant& g : grants) {
            auto r = admin().grant(channel_, adminSid_, name, false, g.op, g.type, g.kind, g.pattern);
            require(r.errorCode == 0, "GRANT to " + name + ": " + r.detail);
        }
    }
    void disableUser(const std::string& name) {
        auto a = admin().alterPassword(channel_, adminSid_, name, pc::UserChangeKind::Disable);
        require(a.errorCode == 0, "DISABLE " + name + ": " + a.detail);
    }
    void addPassword(const std::string& name, const std::string& password) {
        auto a = admin().alterPassword(channel_, adminSid_, name, pc::UserChangeKind::AddPassword, password);
        require(a.errorCode == 0, "ADD PASSWORD " + name + ": " + a.detail);
    }
    void dropOldestPassword(const std::string& name) {
        auto a = admin().alterPassword(channel_, adminSid_, name, pc::UserChangeKind::DropPassword, {}, false, 1);
        require(a.errorCode == 0, "DROP PASSWORD 1 " + name + ": " + a.detail);
    }
    AuthWireClient& admin() {
        require(admin_ != nullptr, "bootstrapAdmin() first");
        return *admin_;
    }
    [[nodiscard]] int64_t adminSession() const noexcept { return adminSid_; }
    /// Successful logins the test rig itself performed on THIS node start (admin + AuthRig::login via noteLoginOk).
    /// The metrics-file logins-ok counter is published asynchronously, so a rig that needs an exact baseline counts its own.
    [[nodiscard]] int64_t loginsOkDone() const noexcept { return loginsOkDone_; }
    void noteLoginOk() noexcept { ++loginsOkDone_; }

    // ── metrics (arcanum-metrics plan Task 8: the node's metrics file, no longer cnc.dat) ─────────────────────
    [[nodiscard]] std::optional<int64_t> counter(std::int32_t typeId) const {
        return rune::node::test::readRuneMetricAt(cfg_.effectiveMetricsFile(), typeId, cfg_.nodeId);
    }
    /// The counter reaches `want` (bounded); the last value read otherwise.
    [[nodiscard]] bool awaitCounter(std::int32_t typeId, int64_t want, std::chrono::milliseconds within = std::chrono::seconds(10)) const {
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline) {
            if (const auto v = counter(typeId); v && *v == want) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

private:
    void startNode_() {
        node_ = std::make_unique<BrokerNode>(cfg_);
        node_->start();
        if (!node_->awaitLeader(std::chrono::seconds(30))) throw std::runtime_error("AuthBroker: no leader");
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (security_cluster::securityOf(*node_).role != rune::replication::Role::Leader) {
            if (std::chrono::steady_clock::now() > deadline) throw std::runtime_error("AuthBroker: __security never led");
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
    static void require(bool ok, const std::string& what) {
        if (!ok) throw std::runtime_error("AuthBroker: " + what);
    }
    static int& seq_() { static int s = 0; return s; }

    Options                                      opts_;
    NodeConfig                                   cfg_;
    std::filesystem::path                        dir_;
    int64_t                                      loginsOkDone_ = 0;
    std::filesystem::path                        keyFile_;
    uint16_t                                     clientPort_ = 0;
    std::string                                  channel_, ingressChannel_;
    std::unique_ptr<BrokerNode>                  node_;
    std::vector<std::unique_ptr<AuthWireClient>> wires_;
    AuthWireClient*                              admin_ = nullptr;
    int64_t                                      adminSid_ = 0;
    int                                          clientDirs_ = 0;
};

} // namespace rune::node::test
