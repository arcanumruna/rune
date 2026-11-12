// AuthLogNamesGatewayTest.cpp - issue #666: client-supplied names with control bytes never reach a log line raw on any
// frame that carries a name (the SecurityBootstrap admin name, security DDL names and grant targets, topic / view /
// namespace names), only the login path was pinned end to end (AuthLoginGatewayTest.cpp).  Gateway only, no Aeron.
#include "support/ScramTestClient.h"

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/client/NamespaceMessages.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <gtest/gtest.h>

#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <functional>
#include <random>

using namespace rune::gateway;
using namespace rune::node::test;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;

namespace {

// A sink that accepts and records nothing: the answer is irrelevant here, only the log lines are judged.
struct NullAdminSink final : IAdminRequestSink {
    AdminSubmit submitDefineTopic(int64_t, int64_t, std::string_view, uint32_t, std::string_view, bool, const uint8_t*,
                                  uint32_t, uint8_t, int16_t, const rune::protocol::TopicConfigView&,
                                  const DdlMetaView&, std::string_view, const rune::protocol::TopicKeyFeatures&,
                                  const std::optional<rune::protocol::LeaseSpec>&) override {
        return AdminSubmit::Accepted;
    }
    bool submitGetSchema(int64_t, int64_t, std::string_view, uint64_t, uint32_t) override { return true; }
    AdminSubmit submitAlterTopic(int64_t, int64_t, std::string_view, uint16_t, const uint8_t*, uint32_t,
                                 const DdlMetaView&) override { return AdminSubmit::Accepted; }
    AdminSubmit submitCreateView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, const uint8_t*,
                                 uint32_t, const DdlMetaView&) override { return AdminSubmit::Accepted; }
    AdminSubmit submitDropTopic(int64_t, int64_t, std::string_view, const DdlMetaView&) override {
        return AdminSubmit::Accepted;
    }
    AdminSubmit submitAlterTopicConfig(int64_t, int64_t, std::string_view, uint32_t, std::span<const uint8_t>,
                                       const DdlMetaView&) override { return AdminSubmit::Accepted; }
    AdminSubmit submitDropView(int64_t, int64_t, std::string_view, const DdlMetaView&) override {
        return AdminSubmit::Accepted;
    }
    AdminSubmit submitAlterView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, uint32_t, const uint8_t*,
                                uint32_t, const DdlMetaView&) override { return AdminSubmit::Accepted; }
    bool submitCatalogSubscribe(int64_t, int64_t, int64_t) override { return true; }
    bool submitCatalogUnsubscribe(int64_t, int64_t) override { return true; }
    AdminSubmit submitSecurityDdl(const pc::SecurityDdl&, const DdlMetaView&, std::string_view, bool) override {
        return AdminSubmit::Accepted;
    }
    AdminSubmit submitNamespaceDdl(const pc::NamespaceDdlRequest&, const DdlMetaView&, std::string_view,
                                   uint16_t) override { return AdminSubmit::Accepted; }
    AdminSubmit submitListNamespaces(int64_t, int64_t, uint32_t, uint16_t, uint32_t, bool) override {
        return AdminSubmit::Accepted;
    }
    AdminSubmit submitSecurityBootstrap(const pc::SecurityBootstrap&, const DdlMetaView&) override {
        return AdminSubmit::Accepted;
    }
    bool submitSecuritySubscribe(int64_t, int64_t, int64_t) override { return true; }
    bool submitSecurityUnsubscribe(int64_t, int64_t) override { return true; }
};

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-auth-names-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

struct Rig {
    TempDir dir;
    std::unique_ptr<TopicRegistry> registry;
    SecurityEventChain chain;
    SecurityView view;
    ClientGateway gw;
    NullAdminSink admin;
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> logSink;
    std::shared_ptr<spdlog::logger> previous;

    explicit Rig(SecurityEventChain c)
        : chain(std::move(c)), view(chain.state()),
          gw(ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1)) {
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        gw.setNodeIdForTest(1);
        gw.setOnEgressFrame([](int64_t, const uint8_t*, std::size_t) {});
        gw.setAdminSink(&admin);
        gw.setAuthMode(GatewayAuthMode::Required, &view);
        gw.setAuthTimeoutNs(1'000'000'000);
        logSink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(4096);
        previous = spdlog::default_logger();
        auto capture = std::make_shared<spdlog::logger>("auth-names-capture", logSink);
        capture->set_level(spdlog::level::trace);
        spdlog::set_default_logger(capture);
    }
    ~Rig() { spdlog::set_default_logger(previous); }
    void send(const std::vector<uint8_t>& f) { gw.applyClientFrameForTest(f.data(), f.size()); }
    /// A session that is logged in as an administrator who may do everything.
    int64_t adminSession() {
        const int64_t sid = gw.injectTestSession(0, pc::kClientVersionAuth);
        gw.setSessionAuthForTest(sid, AuthAuthenticated{7, "admin", PermissionTable::allowAll()});
        return sid;
    }
    /// A session in the bootstrap window (nobody is logged in).
    int64_t anonymousSession() {
        const int64_t sid = gw.injectTestSession(0, pc::kClientVersionAuth);
        gw.setSessionAuthForTest(sid, AuthAnonymous{});
        return sid;
    }
};

template <typename M>
std::vector<uint8_t> encoded(const M& m) {
    std::vector<uint8_t> b(m.encodedSize() + 64);
    b.resize(m.encode(b.data()));
    return b;
}

pc::VerifierWire verifierFor(const rune::security::ScramVerifier& v) {
    return pc::VerifierWire{std::string_view(reinterpret_cast<const char*>(v.salt.data()), v.salt.size()), v.iterations,
                            std::string_view(reinterpret_cast<const char*>(v.storedKey.data()), 64),
                            std::string_view(reinterpret_cast<const char*>(v.serverKey.data()), 64)};
}

struct FrameKind {
    const char* what;
    bool needsBootstrapWindow;
    std::function<std::vector<uint8_t>(int64_t sid, const std::string& name)> build;
};

const rune::security::ScramVerifier& someVerifier() {
    static const rune::security::ScramVerifier v = scramVerifierOf("a password of some length", 5);
    return v;
}

std::vector<uint8_t> securityDdl(int64_t sid, pc::SecurityOpKind kind, const std::string& name,
                                 const std::string& pattern, const std::string& role, uint8_t granteeKind = 0) {
    pc::SecurityDdl m;
    m.sessionId = sid; m.correlationId = 900;
    m.ext.clientName = "t"; m.ext.requestId = {1, 900};
    m.ext.statement = "GRANT ...;";
    m.kind = kind; m.name = name;
    m.granteeKind = granteeKind; m.op = 1; m.resourceType = 1; m.patternKind = 0;
    m.pattern = pattern; m.roleName = role;
    if (kind == pc::SecurityOpKind::CreateUser) m.verifier = verifierFor(someVerifier());
    return encoded(m);
}

std::vector<FrameKind> frameKinds() {
    std::vector<FrameKind> v;
    v.push_back({"SecurityBootstrap.adminName", true, [](int64_t sid, const std::string& n) {
        pc::SecurityBootstrap m;
        m.sessionId = sid; m.correlationId = 901; m.adminName = n; m.verifier = verifierFor(someVerifier());
        return encoded(m);
    }});
    v.push_back({"SecurityDdl CREATE USER name", false, [](int64_t sid, const std::string& n) {
        return securityDdl(sid, pc::SecurityOpKind::CreateUser, n, "", "");
    }});
    v.push_back({"SecurityDdl GRANT target user", false, [](int64_t sid, const std::string& n) {
        return securityDdl(sid, pc::SecurityOpKind::Grant, n, "t", "");
    }});
    v.push_back({"SecurityDdl GRANT pattern", false, [](int64_t sid, const std::string& n) {
        return securityDdl(sid, pc::SecurityOpKind::Grant, "u", n, "");
    }});
    v.push_back({"SecurityDdl role name", false, [](int64_t sid, const std::string& n) {
        return securityDdl(sid, pc::SecurityOpKind::CreateRole, n, "", "");
    }});
    v.push_back({"SecurityDdl GRANT ROLE role name", false, [](int64_t sid, const std::string& n) {
        return securityDdl(sid, pc::SecurityOpKind::GrantRole, "u", "", n);
    }});
    v.push_back({"DefineTopic name", false, [](int64_t sid, const std::string& n) {
        pc::DefineTopic m;
        m.sessionId = sid; m.correlationId = 902; m.topicName = n; m.partitionCount = 1;
        return encoded(m);
    }});
    v.push_back({"CreateView name", false, [](int64_t sid, const std::string& n) {
        pc::CreateView m;
        m.sessionId = sid; m.correlationId = 903; m.viewName = n; m.keyField = "id";
        return encoded(m);
    }});
    v.push_back({"DropTopic name", false, [](int64_t sid, const std::string& n) {
        pc::DropTopic m;
        m.sessionId = sid; m.correlationId = 904; m.topicName = n;
        return encoded(m);
    }});
    v.push_back({"AlterTopicConfig name", false, [](int64_t sid, const std::string& n) {
        pc::AlterTopicConfig m;
        static const uint8_t kNoChanges[2] = {0, 0};
        m.sessionId = sid; m.correlationId = 905; m.topicName = n; m.changes = std::span<const uint8_t>(kNoChanges, 2);
        return encoded(m);
    }});
    v.push_back({"NamespaceDdl name", false, [](int64_t sid, const std::string& n) {
        pc::NamespaceDdlRequest m;
        m.sessionId = sid; m.correlationId = 906; m.kind = pc::NamespaceDdlKind::Create; m.name = n;
        return encoded(m);
    }});
    v.push_back({"GetTopicSchema name", false, [](int64_t sid, const std::string& n) {
        pc::GetTopicSchema m;
        m.sessionId = sid; m.correlationId = 907; m.topicName = n;
        return encoded(m);
    }});
    return v;
}

}  // namespace

// RED: log a client-supplied name with a plain `{}` at any of these sites (the line then holds the raw byte, or the
// forged line starts a physical line of its own).
TEST(AuthLogRegression, NamesWithControlBytesNeverReachADdlOrBootstrapLogLineRegression) {
    const std::vector<std::string> names = {
        "x\nauth: login ok user=admin", "a\rb", "a\x1b[31mb", std::string("a\0b", 3), "a\x7f", "a\x85" "b",
        "a\xE2\x80\xA8" "b"};
    const std::string forged = "auth: login ok user=admin";
    int linesJudged = 0;
    for (const FrameKind& fk : frameKinds()) {
        for (const std::string& name : names) {
            // Bootstrap needs a cluster that was never bootstrapped; every other frame an authenticated admin.
            Rig rig(fk.needsBootstrapWindow ? SecurityEventChain{} : makeAuthTestChain());
            const int64_t sid = fk.needsBootstrapWindow ? rig.anonymousSession() : rig.adminSession();
            rig.send(fk.build(sid, name));
            for (std::string line : rig.logSink->last_formatted()) {
                ++linesJudged;
                if (!line.empty() && line.back() == '\n') line.pop_back();   // the logger's own terminator
                for (std::size_t i = 0; i < line.size(); ++i) {
                    const auto c = static_cast<unsigned char>(line[i]);
                    EXPECT_FALSE(c < 0x20 || c == 0x7f)
                        << fk.what << ": raw control byte " << int(c) << " at " << i << " in: " << line;
                }
                EXPECT_NE(line.rfind(forged, 0), 0u) << fk.what << ": a forged line: " << line;
                for (const char* raw : {"\x85", "\xE2\x80\xA8"})
                    EXPECT_EQ(line.find(std::string("a") + raw + "b"), std::string::npos)
                        << fk.what << ": the raw name reached a log line: " << line;
            }
        }
    }
    EXPECT_GT(linesJudged, 0) << "no log line was captured at all: the capture is not live";
}
