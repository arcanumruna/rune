// Authentication Task 8 (spec §4/§7, D4, D9, D10, D36): the client's security DDL — the VERIFIER is built
// client-side, the statement that travels is the REDACTED one, every plan kind maps to its wire op, the bootstrap
// MAC verifies under the cluster key file, NotLeader is retried with the same request id.  The fake broker decodes
// every frame with the protocol's strict decoder (bounds-checked salt / iterations / enums), like the real gateway.
#include <filesystem>
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/crypto/Kdf.h>
#include <rune/crypto/Scram.h>
#include <rune/ql/Ast.h>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>

#include <gtest/gtest.h>

#include <unistd.h>

namespace pc  = rune::protocol::client;
namespace rc  = rune::client;
namespace rcr = rune::crypto;

namespace {
int64_t g_now = 1'000'000'000;
int64_t fakeNow() noexcept { return g_now; }
int g_ctr = 0;

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = eg;
    c.dataDir = std::filesystem::temp_directory_path().string() + "/security_ddl_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    c.runnerMode = rc::RunnerMode::Invoker;
    c.nanoClock = &fakeNow;
    c.ddlTimeoutMs = 3'000;
    return c;
}

struct Rig {
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    template <class P> bool drive(P&& pred, int64_t stepNs = 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            g_now += stepNs; client.doWork(); gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void passes(int n) { for (int i = 0; i < n; ++i) { client.doWork(); gw.pump(); } }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};

rune::ql::SecurityPlan planOf(const std::string& text) {
    auto st = rune::ql::parse(text);
    EXPECT_TRUE(st.has_value()) << text;
    auto plan = rune::ql::applySecurityPlan(*st);
    EXPECT_TRUE(plan.has_value()) << text;
    return *plan;
}

/// What the fake broker saw of one SecurityDdl (decoded strictly) plus the raw bytes.
struct Seen {
    std::vector<uint8_t> raw;
    std::string statement, name, pattern, roleName;
    pc::SecurityOpKind kind{};
    pc::UserChangeKind change{};
    uint8_t passwordIndex = 0, terminate = 0, granteeKind = 0, op = 0, resourceType = 0, patternKind = 0;
    std::string salt, storedKey, serverKey;
    uint32_t iterations = 0;
    rune::protocol::RequestId requestId{};
};

void answerOk(Rig& r, int64_t sid, int64_t corr, uint64_t objectId = 7, uint32_t version = 1) {
    pc::DdlResponse resp; resp.sessionId = sid; resp.correlationId = corr; resp.objectId = objectId; resp.newVersion = version;
    r.gw.sendDdlResponse(0, resp);
}

bool containsWindow(const std::vector<uint8_t>& hay, const std::string& secret, std::size_t window) {
    for (std::size_t i = 0; i + window <= secret.size(); ++i) {
        const std::string w = secret.substr(i, window);
        if (std::search(hay.begin(), hay.end(), w.begin(), w.end()) != hay.end()) return true;
    }
    return false;
}
} // namespace

TEST(SecurityDdlRegression, CreateUserSendsAVerifierAndARedactedStatementRegression) {
    Rig r(invokerCfg(3810, 3811));
    r.connect();
    const std::string password = "long enough pw 42";
    std::vector<Seen> seen;
    r.gw.templateHandlers[pc::SecurityDdl::TEMPLATE_ID] = [&](int, const uint8_t* f, std::size_t len) {
        auto m = pc::SecurityDdl::decode(f, len);   // strict: salt 16..64, iterations 4096..1e6, 64-byte keys
        ASSERT_TRUE(m.has_value()) << "the broker's decoder refused the frame";
        Seen s; s.raw.assign(f, f + len); s.statement = std::string(m->ext.statement); s.kind = m->kind;
        s.name = std::string(m->name); s.salt = std::string(m->verifier.salt); s.iterations = m->verifier.iterations;
        s.storedKey = std::string(m->verifier.storedKey); s.serverKey = std::string(m->verifier.serverKey);
        seen.push_back(std::move(s));
        answerOk(r, m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    rc::DdlOptions ddl; ddl.statement = "CREATE USER ws1 WITH PASSWORD 'SHOULD-NEVER-TRAVEL';";   // ignored: only the plan's redacted text
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.securityDdl(planOf("create user ws1 with password '" + password + "';"), ddl, q, t);
    });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();
    ASSERT_EQ(seen.size(), 1u);
    const Seen& s = seen[0];
    EXPECT_EQ(s.kind, pc::SecurityOpKind::CreateUser);
    EXPECT_EQ(s.name, "ws1");
    EXPECT_EQ(s.statement, "CREATE USER ws1 WITH PASSWORD '<redacted>';");
    EXPECT_EQ(s.statement.find("SHOULD-NEVER"), std::string::npos);
    EXPECT_FALSE(containsWindow(s.raw, password, 5)) << "no substring of the password is on the wire";
    EXPECT_FALSE(containsWindow(s.raw, "SHOULD-NEVER-TRAVEL", 6));
    EXPECT_EQ(s.iterations, 4096u);
    // The verifier verifies the password with rune-crypto (and only it).
    const std::vector<uint8_t> salt(s.salt.begin(), s.salt.end());
    auto sp = rcr::scram::saltPassword(rcr::Digest::Sha512, password, salt, s.iterations);
    ASSERT_TRUE(sp.has_value());
    const std::string am = "n=ws1,r=abc,r=abcdef,s=x,i=4096,c=biws,r=abcdef";
    auto proof = rcr::scram::clientProof(rcr::Digest::Sha512, *sp, am);
    ASSERT_TRUE(proof.has_value());
    rcr::DigestBytes stored; stored.size = 64; std::memcpy(stored.bytes.data(), s.storedKey.data(), 64);
    EXPECT_TRUE(rcr::scram::verifyClientProof(rcr::Digest::Sha512, stored, am, proof->view()));
    auto bad = rcr::scram::saltPassword(rcr::Digest::Sha512, "another long password", salt, s.iterations);
    auto badProof = rcr::scram::clientProof(rcr::Digest::Sha512, *bad, am);
    EXPECT_FALSE(rcr::scram::verifyClientProof(rcr::Digest::Sha512, stored, am, badProof->view()));
}

TEST(SecurityDdlRegression, ShortPasswordsAreRefusedLocallyBeforeSending) {
    Rig r(invokerCfg(3812, 3813));
    r.connect();
    int arrived = 0;
    r.gw.templateHandlers[pc::SecurityDdl::TEMPLATE_ID] = [&](int, const uint8_t*, std::size_t) { ++arrived; };
    test_support::TestCq cq(r.client);
    const auto plan = planOf("create user ws1 with password 'elevenchars';");   // 11 characters
    ASSERT_EQ(plan.password.size(), 11u);
    try {
        (void)cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.securityDdl(plan, {}, q, t); });
        FAIL() << "a short password must throw";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("password must be at least 12 characters"), std::string::npos) << e.what();
    }
    r.passes(50);
    EXPECT_EQ(arrived, 0) << "nothing was sent";
    // The minimum is the client's rule (D9): configurable.
    rc::ClientConfig lax = invokerCfg(3814, 3815);
    lax.minPasswordLength = 8;
    Rig r2(lax);
    r2.connect();
    test_support::TestCq cq2(r2.client);
    EXPECT_NO_THROW((void)cq2.issue([&](rc::Completions& q, rc::Token t) { return r2.client.securityDdl(plan, {}, q, t); }));
}

TEST(SecurityDdlRegression, EveryPlanKindMapsToItsWireOp) {
    Rig r(invokerCfg(3816, 3817));
    r.connect();
    std::vector<Seen> seen;
    r.gw.templateHandlers[pc::SecurityDdl::TEMPLATE_ID] = [&](int, const uint8_t* f, std::size_t len) {
        auto m = pc::SecurityDdl::decode(f, len);
        ASSERT_TRUE(m.has_value());
        Seen s; s.kind = m->kind; s.name = std::string(m->name); s.change = m->change; s.passwordIndex = m->passwordIndex;
        s.terminate = m->terminateSessions; s.granteeKind = m->granteeKind; s.op = m->op; s.resourceType = m->resourceType;
        s.patternKind = m->patternKind; s.pattern = std::string(m->pattern); s.roleName = std::string(m->roleName);
        s.statement = std::string(m->ext.statement); s.requestId = m->ext.requestId; s.iterations = m->verifier.iterations;
        seen.push_back(std::move(s));
        answerOk(r, m->sessionId, m->correlationId);
    };
    struct Case { std::string text; pc::SecurityOpKind kind; };
    const std::vector<Case> cases = {
        {"CREATE USER ws1 WITH PASSWORD 'correct horse battery';", pc::SecurityOpKind::CreateUser},
        {"ALTER USER ws1 ADD PASSWORD 'another long password';", pc::SecurityOpKind::AlterUser},
        {"ALTER USER ws1 SET PASSWORD 'yet another password' TERMINATE SESSIONS;", pc::SecurityOpKind::AlterUser},
        {"ALTER USER ws1 DROP PASSWORD 2;", pc::SecurityOpKind::AlterUser},
        {"ALTER USER ws1 DISABLE;", pc::SecurityOpKind::AlterUser},
        {"ALTER USER ws1 ENABLE;", pc::SecurityOpKind::AlterUser},
        {"DROP USER ws1;", pc::SecurityOpKind::DropUser},
        {"CREATE ROLE world_server;", pc::SecurityOpKind::CreateRole},
        {"DROP ROLE world_server;", pc::SecurityOpKind::DropRole},
        {"GRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE world_server;", pc::SecurityOpKind::Grant},
        {"REVOKE READ ON TOPIC player_events FROM USER ws1;", pc::SecurityOpKind::Revoke},
        {"GRANT ROLE world_server TO USER ws1;", pc::SecurityOpKind::GrantRole},
        {"REVOKE ROLE world_server FROM USER ws1;", pc::SecurityOpKind::RevokeRole},
    };
    test_support::TestCq cq(r.client);
    for (const auto& c : cases) {
        SCOPED_TRACE(c.text);
        const std::size_t before = seen.size();
        auto plan = planOf(c.text);
        const std::string redacted = plan.statement;
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.securityDdl(plan, {}, q, t); });
        ASSERT_TRUE(f.accepted());
        ASSERT_TRUE(r.drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
        ASSERT_EQ(seen.size(), before + 1);
        const Seen& s = seen.back();
        EXPECT_EQ(s.kind, c.kind);
        EXPECT_EQ(s.statement, redacted) << "the plan's redacted statement is what travels";
        EXPECT_FALSE(s.requestId.none()) << "a request id is stamped (F14: dedupe on retry)";
        if (c.text.find("SET PASSWORD") != std::string::npos) {
            EXPECT_EQ(s.change, pc::UserChangeKind::SetPassword);
            EXPECT_EQ(s.terminate, 1);
        }
        if (c.text.find("DROP PASSWORD") != std::string::npos) {
            EXPECT_EQ(s.change, pc::UserChangeKind::DropPassword);
            EXPECT_EQ(s.passwordIndex, 2);
        }
        if (c.kind == pc::SecurityOpKind::Grant) {
            EXPECT_EQ(s.name, "world_server");
            EXPECT_EQ(s.pattern, "world.");
            EXPECT_EQ(s.patternKind, plan.patternKind);
            EXPECT_EQ(s.op, plan.op);
            EXPECT_EQ(s.resourceType, plan.resourceType);
            EXPECT_EQ(s.granteeKind, plan.granteeKind);
        }
        if (c.kind == pc::SecurityOpKind::GrantRole || c.kind == pc::SecurityOpKind::RevokeRole) {
            EXPECT_EQ(s.roleName, "world_server");
            EXPECT_EQ(s.name, "ws1");
        }
    }
}

TEST(SecurityDdlRegression, BootstrapMacVerifiesUnderTheKeyFile) {
    Rig r(invokerCfg(3818, 3819));
    r.connect();
    const std::string keyFile = std::string(64, 'a') + "\n";   // D32: >= 32 bytes after trimming
    const std::vector<uint8_t> keyBytes(keyFile.begin(), keyFile.end());
    std::vector<std::array<uint8_t, 16>> nonces;
    r.gw.templateHandlers[pc::SecurityBootstrap::TEMPLATE_ID] = [&](int, const uint8_t* f, std::size_t len) {
        auto m = pc::SecurityBootstrap::decode(f, len);
        ASSERT_TRUE(m.has_value());
        EXPECT_EQ(m->adminName, "admin");
        std::vector<uint8_t> input;
        pc::bootstrapMacInput(*m, input);
        ASSERT_FALSE(input.empty());
        const auto key = rcr::deriveClusterKey(keyBytes);
        ASSERT_TRUE(key.has_value());
        rcr::DigestBytes mac;
        ASSERT_TRUE(rcr::hmac(rcr::Digest::Sha512, key->master, input, mac));
        EXPECT_EQ(std::memcmp(mac.bytes.data(), m->mac.data(), 64), 0) << "the MAC under the cluster key verifies";
        const std::array<uint8_t, 32> zero{};
        EXPECT_NE(m->fakeSaltSeed, zero) << "D8: a random fake-salt seed";
        nonces.push_back(m->nonce);
        // A different key must not verify.
        const std::vector<uint8_t> other(64, 'b');
        const auto k2 = rcr::deriveClusterKey(other);
        rcr::DigestBytes mac2;
        ASSERT_TRUE(rcr::hmac(rcr::Digest::Sha512, k2->master, input, mac2));
        EXPECT_NE(std::memcmp(mac2.bytes.data(), m->mac.data(), 64), 0);
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId; resp.objectId = 1;
        r.gw.sendDdlResponse(0, resp);
    };
    test_support::TestCq cq(r.client);
    for (int i = 0; i < 2; ++i) {
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
            return r.client.bootstrapSecurity("admin", "a one-time password 24", keyBytes, q, t);
        });
        ASSERT_TRUE(f.accepted());
        ASSERT_TRUE(r.drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
    ASSERT_EQ(nonces.size(), 2u);
    EXPECT_NE(nonces[0], nonces[1]) << "a fresh nonce per bootstrap";
    // A key file under 32 bytes is refused locally.
    const std::vector<uint8_t> shortKey(10, 'k');
    EXPECT_THROW((void)cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.bootstrapSecurity("admin", "a one-time password 24", shortKey, q, t);
    }), std::invalid_argument);
}

TEST(SecurityDdlRegression, NotLeaderIsRetriedWithTheSameRequestId) {
    Rig r(invokerCfg(3820, 3821));
    r.connect();
    std::vector<rune::protocol::RequestId> ids;
    r.gw.templateHandlers[pc::SecurityDdl::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::SecurityDdl::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ids.push_back(m->ext.requestId);
        if (ids.size() < 3) { r.gw.sendError(ep, static_cast<int16_t>(pc::ErrorCode::NotLeader), "", m->sessionId, m->correlationId); return; }
        answerOk(r, m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.securityDdl(planOf("CREATE ROLE world_server;"), {}, q, t);
    });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, 50'000'000));   // walks the 100 ms -> 200 ms backoff
    ASSERT_TRUE(f.ok()) << f.describe();
    ASSERT_EQ(ids.size(), 3u);
    EXPECT_FALSE(ids[0].none());
    EXPECT_EQ(ids[0], ids[1]);
    EXPECT_EQ(ids[1], ids[2]) << "the same request id on every resend (broker-side dedupe)";
}

TEST(SecurityDdlRegression, ABrokerRefusalIsTheResultNotAnException) {
    Rig r(invokerCfg(3822, 3823));
    r.connect();
    r.gw.templateHandlers[pc::SecurityDdl::TEMPLATE_ID] = [&](int, const uint8_t* f, std::size_t len) {
        auto m = pc::SecurityDdl::decode(f, len);
        ASSERT_TRUE(m.has_value());
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId; resp.errorCode = 39;
        resp.detail = "user 'ws1' already exists";
        r.gw.sendDdlResponse(0, resp);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.securityDdl(planOf("CREATE USER ws1 WITH PASSWORD 'correct horse battery';"), {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.outcome().c.errorCode, 39);
    const auto b = f.bytes();
    EXPECT_EQ(std::string(b.begin(), b.end()), "user 'ws1' already exists");
}
