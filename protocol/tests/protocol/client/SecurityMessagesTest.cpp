#include <rune/protocol/client/SecurityMessages.h>
#include <gtest/gtest.h>
#include <string>
#include <vector>
namespace pc = rune::protocol::client;

namespace {
const std::string kSalt(16, 's'), kStored(64, 'a'), kServer(64, 'b');
pc::VerifierWire verifier() { return pc::VerifierWire{kSalt, 4096, kStored, kServer}; }
pc::DdlExtension ext() {
    pc::DdlExtension e;
    e.statement = "CREATE USER ws1 PASSWORD '<redacted>'";
    e.clientName = "cli";
    e.requestId = {0xC11E, 77};
    return e;
}
pc::SecurityDdl make(pc::SecurityOpKind k, std::string_view name) {
    pc::SecurityDdl m; m.sessionId = 7; m.correlationId = 9; m.ext = ext(); m.kind = k; m.name = name; return m;
}
// Decoded string_views point into the wire buffer, so it must outlive the checks.
std::vector<uint8_t> gWire;
void roundTrip(const pc::SecurityDdl& m, std::optional<pc::SecurityDdl>& out) {
    std::vector<uint8_t>& buf = gWire;
    buf.assign(m.encodedSize() + 8, 0);
    const std::size_t n = m.encode(buf.data());
    EXPECT_EQ(n, m.encodedSize());
    buf.resize(n);
    out = pc::SecurityDdl::decode(buf.data(), buf.size());
}
} // namespace

TEST(SecurityMessagesRegression, EveryOpKindRoundTrips) {
    std::optional<pc::SecurityDdl> d;
    auto common = [&](const pc::SecurityDdl& m) {
        ASSERT_TRUE(d) << static_cast<int>(m.kind);
        EXPECT_EQ(d->sessionId, 7); EXPECT_EQ(d->correlationId, 9);
        EXPECT_EQ(d->ext, m.ext); EXPECT_EQ(d->ext.requestId.hi, 0xC11Eu); EXPECT_EQ(d->ext.requestId.lo, 77u);
        EXPECT_EQ(d->kind, m.kind); EXPECT_EQ(d->name, m.name);
    };
    {   auto m = make(pc::SecurityOpKind::CreateUser, "ws1"); m.verifier = verifier();
        roundTrip(m, d); common(m); EXPECT_EQ(d->verifier, verifier()); }
    {   auto m = make(pc::SecurityOpKind::AlterUser, "ws1"); m.change = pc::UserChangeKind::AddPassword; m.verifier = verifier();
        roundTrip(m, d); common(m); EXPECT_EQ(d->change, pc::UserChangeKind::AddPassword); EXPECT_EQ(d->verifier, verifier()); }
    {   auto m = make(pc::SecurityOpKind::AlterUser, "ws1"); m.change = pc::UserChangeKind::DropPassword; m.passwordIndex = 2;
        roundTrip(m, d); common(m); EXPECT_EQ(d->passwordIndex, 2); }
    {   auto m = make(pc::SecurityOpKind::AlterUser, "ws1"); m.change = pc::UserChangeKind::SetPassword;
        m.terminateSessions = 1; m.verifier = verifier();
        roundTrip(m, d); common(m); EXPECT_EQ(d->terminateSessions, 1); EXPECT_EQ(d->verifier, verifier()); }
    for (auto c : {pc::UserChangeKind::Disable, pc::UserChangeKind::Enable}) {
        auto m = make(pc::SecurityOpKind::AlterUser, "ws1"); m.change = c;
        roundTrip(m, d); common(m); EXPECT_EQ(d->change, c);
    }
    for (auto k : {pc::SecurityOpKind::DropUser, pc::SecurityOpKind::CreateRole, pc::SecurityOpKind::DropRole}) {
        auto m = make(k, "r1"); roundTrip(m, d); common(m);
    }
    for (auto k : {pc::SecurityOpKind::Grant, pc::SecurityOpKind::Revoke}) {
        auto m = make(k, "ws1"); m.granteeKind = 1; m.op = 2; m.resourceType = 3; m.patternKind = 1; m.pattern = "world.";
        roundTrip(m, d); common(m);
        EXPECT_EQ(d->granteeKind, 1); EXPECT_EQ(d->op, 2); EXPECT_EQ(d->resourceType, 3); EXPECT_EQ(d->patternKind, 1);
        EXPECT_EQ(d->pattern, "world.");
    }
    for (auto k : {pc::SecurityOpKind::GrantRole, pc::SecurityOpKind::RevokeRole}) {
        auto m = make(k, "ws1"); m.roleName = "game-server";
        roundTrip(m, d); common(m); EXPECT_EQ(d->roleName, "game-server");
    }
}

TEST(SecurityMessagesRegression, VerifierKeysMustBe64Bytes) {
    uint8_t buf[2048];
    const std::string k63(63, 'a'), k65(65, 'a');
    auto m = make(pc::SecurityOpKind::CreateUser, "ws1");
    m.verifier = {kSalt, 4096, k63, kServer};
    EXPECT_EQ(m.encode(buf), 0u) << "storedKey 63";
    m.verifier = {kSalt, 4096, kStored, k65};
    EXPECT_EQ(m.encode(buf), 0u) << "serverKey 65";
    pc::SecurityBootstrap b; b.adminName = "admin"; b.verifier = {kSalt, 4096, k63, kServer};
    EXPECT_EQ(b.encode(buf), 0u);
    // a verifier with out-of-range salt / iterations encodes but never decodes
    m.verifier = {std::string(15, 's'), 4096, kStored, kServer};
    std::size_t n = m.encode(buf); ASSERT_GT(n, 0u);
    EXPECT_FALSE(pc::SecurityDdl::decode(buf, n)) << "salt 15";
    m.verifier = {kSalt, 4095, kStored, kServer};
    n = m.encode(buf); EXPECT_FALSE(pc::SecurityDdl::decode(buf, n)) << "iterations 4095";
    // a kind that carries no verifier ignores a malformed one
    auto drop = make(pc::SecurityOpKind::DropUser, "ws1"); drop.verifier = {kSalt, 4096, k63, kServer};
    EXPECT_GT(drop.encode(buf), 0u);
    // forged kind / change bytes
    auto ok = make(pc::SecurityOpKind::DropUser, "ws1");
    auto bad = ok; bad.kind = static_cast<pc::SecurityOpKind>(10);
    EXPECT_EQ(bad.encode(buf), 0u);
    bad.kind = static_cast<pc::SecurityOpKind>(0);
    EXPECT_EQ(bad.encode(buf), 0u);
    auto badChange = make(pc::SecurityOpKind::AlterUser, "ws1"); badChange.change = static_cast<pc::UserChangeKind>(5);
    EXPECT_EQ(badChange.encode(buf), 0u);
    auto emptyName = make(pc::SecurityOpKind::DropUser, "");
    n = emptyName.encode(buf); EXPECT_FALSE(pc::SecurityDdl::decode(buf, n)) << "empty name";
}

TEST(SecurityMessagesRegression, BootstrapRoundTripsAndMacInputIsStable) {
    pc::SecurityBootstrap b;
    b.sessionId = 7; b.correlationId = 9; b.adminName = "admin";
    b.verifier = {kSalt, 4096, kStored, kServer};
    for (std::size_t i = 0; i < b.nonce.size(); ++i) b.nonce[i] = static_cast<uint8_t>(0x10 + i);
    for (std::size_t i = 0; i < b.fakeSaltSeed.size(); ++i) b.fakeSaltSeed[i] = static_cast<uint8_t>(0x40 + i);
    b.mac.fill(0xEE);
    uint8_t buf[512];
    const std::size_t n = b.encode(buf);
    ASSERT_EQ(n, b.encodedSize());
    auto d = pc::SecurityBootstrap::decode(buf, n);
    ASSERT_TRUE(d);
    EXPECT_EQ(d->adminName, "admin"); EXPECT_EQ(d->verifier, b.verifier); EXPECT_EQ(d->nonce, b.nonce); EXPECT_EQ(d->mac, b.mac); EXPECT_EQ(d->fakeSaltSeed, b.fakeSaltSeed);

    std::vector<uint8_t> in;
    pc::bootstrapMacInput(b, in);
    // "rune-bootstrap-v1" | 05 00 "admin" | 10 00 salt | 00 10 00 00 | 64x'a' | 64x'b' | nonce 16
    std::vector<uint8_t> want;
    auto app = [&](std::string_view s) { want.insert(want.end(), s.begin(), s.end()); };
    app("rune-bootstrap-v1");
    want.push_back(5); want.push_back(0); app("admin");
    want.push_back(16); want.push_back(0); app(kSalt);
    want.push_back(0x00); want.push_back(0x10); want.push_back(0); want.push_back(0);   // 4096 LE
    app(kStored); app(kServer);
    for (std::size_t i = 0; i < 32; ++i) want.push_back(static_cast<uint8_t>(0x40 + i));   // fakeSaltSeed (D8)
    for (std::size_t i = 0; i < 16; ++i) want.push_back(static_cast<uint8_t>(0x10 + i));
    EXPECT_EQ(in, want);
    EXPECT_EQ(in.size(), 17u + 2 + 5 + 2 + 16 + 4 + 64 + 64 + 32 + 16);

    b.mac.fill(0);   // the MAC is not part of its own input
    std::vector<uint8_t> in2; pc::bootstrapMacInput(b, in2);
    EXPECT_EQ(in, in2);
    b.nonce[0] ^= 1;
    pc::bootstrapMacInput(b, in2);
    EXPECT_NE(in, in2) << "the nonce is covered";
    // reuse keeps capacity (zero steady-state allocation)
    const auto cap = in.capacity();
    pc::bootstrapMacInput(b, in);
    EXPECT_EQ(in.capacity(), cap);
}

TEST(SecurityMessagesRegression, TruncationNeverDecodes) {
    std::vector<pc::SecurityDdl> all;
    { auto m = make(pc::SecurityOpKind::CreateUser, "ws1"); m.verifier = verifier(); all.push_back(m); }
    { auto m = make(pc::SecurityOpKind::AlterUser, "ws1"); m.change = pc::UserChangeKind::SetPassword; m.verifier = verifier(); all.push_back(m); }
    { auto m = make(pc::SecurityOpKind::AlterUser, "ws1"); m.change = pc::UserChangeKind::DropPassword; m.passwordIndex = 1; all.push_back(m); }
    { auto m = make(pc::SecurityOpKind::Grant, "ws1"); m.pattern = "world."; all.push_back(m); }
    { auto m = make(pc::SecurityOpKind::GrantRole, "ws1"); m.roleName = "r"; all.push_back(m); }
    { auto m = make(pc::SecurityOpKind::DropRole, "r"); all.push_back(m); }
    for (const auto& m : all) {
        std::vector<uint8_t> buf(m.encodedSize());
        ASSERT_EQ(m.encode(buf.data()), buf.size());
        for (std::size_t cut = 0; cut < buf.size(); ++cut)
            EXPECT_FALSE(pc::SecurityDdl::decode(buf.data(), cut)) << static_cast<int>(m.kind) << " cut " << cut;
    }
    pc::SecurityBootstrap b; b.adminName = "admin"; b.verifier = verifier();
    std::vector<uint8_t> buf(b.encodedSize());
    ASSERT_EQ(b.encode(buf.data()), buf.size());
    for (std::size_t cut = 0; cut < buf.size(); ++cut)
        EXPECT_FALSE(pc::SecurityBootstrap::decode(buf.data(), cut)) << "bootstrap cut " << cut;
}

TEST(SecurityMessagesRegression, BootstrapMacCoversTheFakeSaltSeedRegression) {
    pc::SecurityBootstrap b; b.adminName = "admin"; b.verifier = verifier();
    b.fakeSaltSeed.fill(0x33);
    std::vector<uint8_t> a, c;
    pc::bootstrapMacInput(b, a);
    for (std::size_t i = 0; i < b.fakeSaltSeed.size(); ++i) {   // every seed byte is covered
        auto b2 = b; b2.fakeSaltSeed[i] ^= 1;
        pc::bootstrapMacInput(b2, c);
        ASSERT_EQ(a.size(), c.size());
        EXPECT_NE(a, c) << "seed byte " << i;
    }
    // and travels on the wire: a bootstrap shorter than seed+nonce+mac never decodes
    std::vector<uint8_t> buf(b.encodedSize());
    ASSERT_EQ(b.encode(buf.data()), buf.size());
    EXPECT_EQ(pc::SecurityBootstrap::decode(buf.data(), buf.size())->fakeSaltSeed, b.fakeSaltSeed);
}

TEST(SecurityMessagesRegression, OutOfRangeGrantEnumBytesDoNotDecodeRegression) {
    uint8_t buf[512];
    for (int field = 0; field < 4; ++field) {
        const uint8_t bad[4] = {2, 7, 5, 2};   // first out-of-range value of grantee, op, type, pattern kind
        auto m = make(pc::SecurityOpKind::Grant, "ws1"); m.pattern = "world.";
        m.granteeKind = 1; m.op = 1; m.resourceType = 1; m.patternKind = 1;
        std::size_t n = m.encode(buf); ASSERT_GT(n, 0u);
        EXPECT_TRUE(pc::SecurityDdl::decode(buf, n)) << "the in-range baseline decodes";
        uint8_t* f[4] = {&m.granteeKind, &m.op, &m.resourceType, &m.patternKind};
        *f[field] = bad[field];
        n = m.encode(buf); ASSERT_GT(n, 0u);
        EXPECT_FALSE(pc::SecurityDdl::decode(buf, n)) << "field " << field;
    }
    // the largest valid values still decode
    auto top = make(pc::SecurityOpKind::Revoke, "r"); top.pattern = "x";
    top.granteeKind = 1; top.op = 6; top.resourceType = 4; top.patternKind = 1;
    std::size_t n = top.encode(buf);
    EXPECT_TRUE(pc::SecurityDdl::decode(buf, n));
}
