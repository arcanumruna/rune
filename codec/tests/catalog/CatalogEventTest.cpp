// Catalog history (spec §4): the frozen formatVer-1 event encoding.
#include <rune/catalog/CatalogEvent.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <functional>
#include <new>
#include <string>
#include <string_view>
#include <utility>

// ── allocation probe (test binary only, single-threaded tests) ────────────────
// While armed it records the largest single allocation; while failing it throws.
namespace {
bool        gProbeArmed = false;
bool        gProbeFail  = false;
std::size_t gProbePeak  = 0;
} // namespace
void* operator new(std::size_t n) {
    if (gProbeFail) throw std::bad_alloc();
    if (gProbeArmed && n > gProbePeak) gProbePeak = n;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
// The nothrow forms route through the replaced throwing forms. Otherwise a nothrow allocation (libstdc++'s
// get_temporary_buffer under std::stable_sort, e.g. in gtest) comes from the default/ASan allocator but is released
// by the replaced delete: ASan alloc-dealloc-mismatch, and the allocation goes uncounted.
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    try { return ::operator new(n); } catch (...) { return nullptr; }
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    try { return ::operator new[](n); } catch (...) { return nullptr; }
}
void operator delete(void* p, const std::nothrow_t&) noexcept { ::operator delete(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { ::operator delete[](p); }

namespace rc = rune::catalog;
using rune::codec::AlterOpType;
using rune::codec::CompatMode;
using rune::codec::PrimType;

namespace {
rc::CatalogEvent baseEvent(rc::Ops ops) {
    rc::CatalogEvent e;
    e.formatVer = rc::kFormatV1;   // this file pins the FROZEN v1 layout; NamespaceCatalogV2Test.cpp pins v2
    e.kind = rc::kindOf(ops);
    e.objectId = 7;
    e.objectName = "players";
    e.prevVersion = 0;
    e.newVersion = 1;
    e.statement = "topic players { id int64 } partitions 3 key id;";
    e.ops = std::move(ops);
    e.principal = rc::Principal{.sessionId = 42, .clientName = "cli", .sourceAddress = "aeron:udp?endpoint=10.0.0.5:40500",
                                .authenticated = "", .requestId = {.hi = 0x0123456789ABCDEFull, .lo = 0xABCDEFull},
                                .requestFingerprint = 0x5151515151515151ull};
    e.timestampMs = 1'760'000'000'000;
    e.fingerprint = 0x1122334455667788ull;
    return e;
}
/// Every TopicConfig field off its default (catches a reordered putConfig/getConfig pair).
rc::TopicConfig nonDefaultConfig() {
    rc::TopicConfig c;
    c.cleanupPolicy = 2; c.retentionMs = 1001; c.retentionBytes = 1002; c.deleteRetentionMs = 1003;
    c.minCleanableDirtyRatioPpm = 1004; c.segmentRollBytes = 1005; c.minCompactionLagMs = 1006;
    c.maxCompactionLagMs = 1007; c.retentionCheckMs = 1008; c.fileDeleteDelayMs = 1009;
    c.durability = 1; c.durabilityTier = 2; c.coPartitionGroup = "grp"; c.partitionKey = "pk";
    return c;
}
rc::CreateTopicOps typedCreate() {
    rc::CreateTopicOps o;
    o.topicId = 3; o.partitions = 6; o.replicationFactor = -2; o.persistence = rc::Persistence::Transient;
    o.config = nonDefaultConfig();
    o.schema = rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = {1, 2, 3, 4}, .nextTag = 5};
    return o;
}
rc::ViewDef viewDef() {
    rc::ViewDef v;
    v.viewTopicId = 9; v.sinkPartitions = 2; v.persistence = rc::Persistence::Transient; v.keyField = "id";
    v.sourceNames = {"players", "guilds"};
    v.normalizedPlanBlob = {9, 9}; v.compiledPlanBlob = {8, 8, 8}; v.outputDescriptorBlob = {7};
    return v;
}
/// One richly-populated Ops per kind, in kind order 1..9.
std::vector<rc::Ops> everyKind() {
    rc::ViewDef altered = viewDef();
    altered.viewTopicId = 11; altered.persistence = rc::Persistence::Disk; altered.sourceNames = {"players"};
    return {typedCreate(), rc::DropTopicOps{.topicId = 3},
            rc::AlterTopicConfigOps{.changes = {{rc::ConfigKey::RetentionMs, 1000}, {rc::ConfigKey::RetentionBytes, -1},
                                                {rc::ConfigKey::DeleteRetentionMs, 2}, {rc::ConfigKey::MinCleanableDirtyRatioPpm, 3},
                                                {rc::ConfigKey::MinCompactionLagMs, 4}, {rc::ConfigKey::MaxCompactionLagMs, 5}}},
            rc::CreateSchemaOps{.schema = {.keyField = "id", .validate = false, .descriptorBlob = {5, 6}, .nextTag = 2}},
            rc::AlterSchemaOps{.ops = {{.type = AlterOpType::Add, .tag = 2, .name = "tag16", .primType = PrimType::FixedStr,
                                        .fixedLen = 16},
                                       {.type = AlterOpType::Rename, .tag = 1, .name = "health"},
                                       {.type = AlterOpType::Drop, .tag = 0},
                                       {.type = AlterOpType::SetCompatibility, .mode = CompatMode::Forward}}},
            rc::SetCompatibilityOps{.mode = CompatMode::Forward},
            rc::CreateViewOps{.view = viewDef()}, rc::AlterViewOps{.view = altered}, rc::DropViewOps{.viewTopicId = 9}};
}
rc::ReplayedFrom replay() { return {.clusterId = "src", .offset = 12, .principal = "cli@h"}; }

/// The golden event of a kind: compact non-default header, principal and replayedFrom.
rc::CatalogEvent goldenEvent(std::size_t i) {
    rc::CatalogEvent e;
    e.formatVer = rc::kFormatV1;
    e.ops = everyKind().at(i);
    e.kind = rc::kindOf(e.ops);
    e.objectId = 0x0102030405060708ull;
    e.objectName = "t";
    e.prevVersion = 0x0A0B0C0D;
    e.newVersion = 0x0A0B0C0E;
    e.statement = "s;";
    e.principal = rc::Principal{.sessionId = -2, .clientName = "c", .sourceAddress = "a", .authenticated = "u",
                                .requestId = {.hi = 0x0123456789ABCDEFull, .lo = 0xFEDCBA9876543210ull},
                                .requestFingerprint = 0x5151515151515151ull};
    e.replayedFrom = rc::ReplayedFrom{.clusterId = "x", .offset = -3, .principal = "p"};
    e.timestampMs = -5;
    e.fingerprint = 0xF0E1D2C3B4A59687ull;
    return e;
}

// ── an INDEPENDENT spelling of the formatVer-1 layout ────────────────────────
// Written field by field from the header's layout comment, byte-wise little-endian,
// with named offsets.  It never calls the library's encoder: the golden vectors below
// pin both it and the encoder, and the refusal tests patch bytes by NAME, not magic offset.
struct Layout {
    std::vector<uint8_t>                             bytes;
    std::vector<std::pair<std::string, std::size_t>> marks;

    void mark(std::string name) { marks.emplace_back(std::move(name), bytes.size()); }
    void le(uint64_t v, int n) { for (int i = 0; i < n; ++i) bytes.push_back(static_cast<uint8_t>(v >> (8 * i))); }
    void u8(uint64_t v) { le(v, 1); }
    void u16(uint64_t v) { le(v, 2); }
    void u32(uint64_t v) { le(v, 4); }
    void u64(uint64_t v) { le(v, 8); }
    void s16(std::string_view s) { u16(s.size()); bytes.insert(bytes.end(), s.begin(), s.end()); }
    void s32(std::string_view s) { u32(s.size()); bytes.insert(bytes.end(), s.begin(), s.end()); }
    void b32(const std::vector<uint8_t>& b) { u32(b.size()); bytes.insert(bytes.end(), b.begin(), b.end()); }
    [[nodiscard]] std::size_t at(std::string_view name) const {
        for (const auto& [n, o] : marks) if (n == name) return o;
        ADD_FAILURE() << "layout has no mark " << name;
        return 0;
    }
};
void layoutSchema(Layout& l, const rc::SchemaDef& s) {
    l.s16(s.keyField); l.mark("schema.validate"); l.u8(s.validate); l.b32(s.descriptorBlob); l.u16(s.nextTag);
}
void layoutView(Layout& l, const rc::ViewDef& v) {
    l.u32(static_cast<uint32_t>(v.viewTopicId)); l.u32(static_cast<uint32_t>(v.sinkPartitions));
    l.mark("view.persistence"); l.u8(static_cast<uint8_t>(v.persistence));
    l.s16(v.keyField);
    l.mark("view.sourceNames.count"); l.u16(v.sourceNames.size());
    for (const auto& n : v.sourceNames) l.s16(n);
    l.b32(v.normalizedPlanBlob); l.b32(v.compiledPlanBlob); l.b32(v.outputDescriptorBlob);
}
Layout layoutOf(const rc::CatalogEvent& e) {
    Layout l;
    l.mark("formatVer"); l.u16(e.formatVer);
    l.mark("kind"); l.u16(static_cast<uint16_t>(e.kind));
    l.mark("objectId"); l.u64(e.objectId);
    l.u32(e.prevVersion); l.u32(e.newVersion);
    l.u64(static_cast<uint64_t>(e.timestampMs)); l.u64(e.fingerprint);
    l.s16(e.objectName); l.s32(e.statement);
    l.u64(static_cast<uint64_t>(e.principal.sessionId));
    l.s16(e.principal.clientName); l.s16(e.principal.sourceAddress); l.s16(e.principal.authenticated);
    l.mark("requestIdHi"); l.u64(e.principal.requestId.hi); l.u64(e.principal.requestId.lo);
    l.u64(e.principal.requestFingerprint);
    l.mark("hasReplayedFrom"); l.u8(e.replayedFrom ? 1 : 0);
    if (e.replayedFrom) {
        l.mark("replayedFrom"); l.s16(e.replayedFrom->clusterId);
        l.u64(static_cast<uint64_t>(e.replayedFrom->offset)); l.s16(e.replayedFrom->principal);
    }
    l.mark("opsLen"); l.u32(0);
    const std::size_t opsStart = l.bytes.size();
    l.mark("opsTag"); l.u8(e.ops.index() + 1);
    if (const auto* o = std::get_if<rc::CreateTopicOps>(&e.ops)) {
        l.u32(static_cast<uint32_t>(o->topicId)); l.u32(static_cast<uint32_t>(o->partitions));
        l.u16(static_cast<uint16_t>(o->replicationFactor));
        l.mark("persistence"); l.u8(static_cast<uint8_t>(o->persistence));
        const rc::TopicConfig& c = o->config;
        l.mark("cleanupPolicy"); l.u8(c.cleanupPolicy);
        l.u64(static_cast<uint64_t>(c.retentionMs)); l.u64(static_cast<uint64_t>(c.retentionBytes));
        l.u64(static_cast<uint64_t>(c.deleteRetentionMs)); l.u32(static_cast<uint32_t>(c.minCleanableDirtyRatioPpm));
        l.u64(static_cast<uint64_t>(c.segmentRollBytes)); l.u64(static_cast<uint64_t>(c.minCompactionLagMs));
        l.u64(static_cast<uint64_t>(c.maxCompactionLagMs)); l.u64(static_cast<uint64_t>(c.retentionCheckMs));
        l.u64(static_cast<uint64_t>(c.fileDeleteDelayMs));
        l.mark("durability"); l.u8(c.durability);
        l.mark("durabilityTier"); l.u8(c.durabilityTier);
        l.s16(c.coPartitionGroup); l.s16(c.partitionKey);
        l.mark("hasSchema"); l.u8(o->schema ? 1 : 0);
        if (o->schema) layoutSchema(l, *o->schema);
    } else if (const auto* o = std::get_if<rc::DropTopicOps>(&e.ops)) {
        l.u32(static_cast<uint32_t>(o->topicId));
    } else if (const auto* o = std::get_if<rc::AlterTopicConfigOps>(&e.ops)) {
        l.mark("changes.count"); l.u16(o->changes.size());
        for (std::size_t i = 0; i < o->changes.size(); ++i) {
            l.mark("change" + std::to_string(i) + ".key"); l.u8(static_cast<uint8_t>(o->changes[i].key));
            l.u64(static_cast<uint64_t>(o->changes[i].value));
        }
    } else if (const auto* o = std::get_if<rc::CreateSchemaOps>(&e.ops)) {
        layoutSchema(l, o->schema);
    } else if (const auto* o = std::get_if<rc::AlterSchemaOps>(&e.ops)) {
        l.mark("alterOps.count"); l.u16(o->ops.size());
        for (std::size_t i = 0; i < o->ops.size(); ++i) {
            const auto& a = o->ops[i];
            const std::string p = "alterOp" + std::to_string(i);
            l.mark(p + ".type"); l.u8(static_cast<uint8_t>(a.type)); l.u16(a.tag); l.s16(a.name);
            l.mark(p + ".primType"); l.u8(static_cast<uint8_t>(a.primType)); l.u16(a.fixedLen);
            l.mark(p + ".mode"); l.u8(static_cast<uint8_t>(a.mode));
        }
    } else if (const auto* o = std::get_if<rc::SetCompatibilityOps>(&e.ops)) {
        l.mark("setCompat.mode"); l.u8(static_cast<uint8_t>(o->mode));
    } else if (const auto* o = std::get_if<rc::CreateViewOps>(&e.ops)) {
        layoutView(l, o->view);
    } else if (const auto* o = std::get_if<rc::AlterViewOps>(&e.ops)) {
        layoutView(l, o->view);
    } else if (const auto* o = std::get_if<rc::DropViewOps>(&e.ops)) {
        l.u32(static_cast<uint32_t>(o->viewTopicId));
    }
    const auto n = static_cast<uint32_t>(l.bytes.size() - opsStart);
    for (int i = 0; i < 4; ++i) l.bytes[l.at("opsLen") + i] = static_cast<uint8_t>(n >> (8 * i));
    return l;
}

std::vector<uint8_t> fromHex(std::string_view h) {
    std::vector<uint8_t> b;
    auto nib = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; };
    for (std::size_t i = 0; i + 1 < h.size(); i += 2) b.push_back(static_cast<uint8_t>(nib(h[i]) << 4 | nib(h[i + 1])));
    return b;
}
std::string toHex(const std::vector<uint8_t>& b) {
    static constexpr char d[] = "0123456789abcdef";
    std::string s;
    for (uint8_t x : b) { s += d[x >> 4]; s += d[x & 15]; }
    return s;
}
void putU16(std::vector<uint8_t>& b, std::size_t at, uint16_t v) { b[at] = v & 0xff; b[at + 1] = v >> 8; }

// ── GOLDEN formatVer-1 vectors: one full event (header + principal + replayedFrom + body)
//    per kind.  FROZEN FOREVER — a mismatch means the formatVer-1 layout changed.
const char* const kGolden[9] = {
    // CreateTopic
    "0100010008070605040302010d0c0b0a0e0c0b0afbffffffffffffff8796a5b4c3d2e1f001007402000000733bfeffff"
    "ffffffffff010063010061010075efcdab89674523011032547698badcfe515151515151515101010078fdffffffffff"
    "ffff0100706c000000010300000006000000feff0102e903000000000000ea03000000000000eb03000000000000ec03"
    "0000ed03000000000000ee03000000000000ef03000000000000f003000000000000f103000000000000010203006772"
    "700200706b01020069640104000000010203040500",
    // DropTopic
    "0100020008070605040302010d0c0b0a0e0c0b0afbffffffffffffff8796a5b4c3d2e1f001007402000000733bfeffff"
    "ffffffffff010063010061010075efcdab89674523011032547698badcfe515151515151515101010078fdffffffffff"
    "ffff010070050000000203000000",
    // AlterTopicConfig
    "0100030008070605040302010d0c0b0a0e0c0b0afbffffffffffffff8796a5b4c3d2e1f001007402000000733bfeffff"
    "ffffffffff010063010061010075efcdab89674523011032547698badcfe515151515151515101010078fdffffffffff"
    "ffff0100703900000003060001e80300000000000002ffffffffffffffff030200000000000000040300000000000000"
    "050400000000000000060500000000000000",
    // CreateSchema
    "0100040008070605040302010d0c0b0a0e0c0b0afbffffffffffffff8796a5b4c3d2e1f001007402000000733bfeffff"
    "ffffffffff010063010061010075efcdab89674523011032547698badcfe515151515151515101010078fdffffffffff"
    "ffff0100700e0000000402006964000200000005060200",
    // AlterSchema
    "0100050008070605040302010d0c0b0a0e0c0b0afbffffffffffffff8796a5b4c3d2e1f001007402000000733bfeffff"
    "ffffffffff010063010061010075efcdab89674523011032547698badcfe515151515151515101010078fdffffffffff"
    "ffff01007032000000050400010200050074616731360a10000003010006006865616c74680000000002000000000000"
    "0000040000000000000001",
    // SetCompatibility
    "0100060008070605040302010d0c0b0a0e0c0b0afbffffffffffffff8796a5b4c3d2e1f001007402000000733bfeffff"
    "ffffffffff010063010061010075efcdab89674523011032547698badcfe515151515151515101010078fdffffffffff"
    "ffff010070020000000601",
    // CreateView
    "0100070008070605040302010d0c0b0a0e0c0b0afbffffffffffffff8796a5b4c3d2e1f001007402000000733bfeffff"
    "ffffffffff010063010061010075efcdab89674523011032547698badcfe515151515151515101010078fdffffffffff"
    "ffff01007033000000070900000002000000010200696402000700706c617965727306006775696c6473020000000909"
    "030000000808080100000007",
    // AlterView
    "0100080008070605040302010d0c0b0a0e0c0b0afbffffffffffffff8796a5b4c3d2e1f001007402000000733bfeffff"
    "ffffffffff010063010061010075efcdab89674523011032547698badcfe515151515151515101010078fdffffffffff"
    "ffff0100702b000000080b00000002000000000200696401000700706c61796572730200000009090300000008080801"
    "00000007",
    // DropView
    "0100090008070605040302010d0c0b0a0e0c0b0afbffffffffffffff8796a5b4c3d2e1f001007402000000733bfeffff"
    "ffffffffff010063010061010075efcdab89674523011032547698badcfe515151515151515101010078fdffffffffff"
    "ffff010070050000000909000000",
};
} // namespace

// ── layout pins ───────────────────────────────────────────────────────────────
TEST(CatalogEventRegression, GoldenBytesPinEveryKindsFullLayout) {
    ASSERT_EQ(std::size(kGolden), 9u);
    for (std::size_t i = 0; i < 9; ++i) {
        const rc::CatalogEvent e = goldenEvent(i);
        const std::vector<uint8_t> golden = fromHex(kGolden[i]);
        EXPECT_EQ(toHex(layoutOf(e).bytes), kGolden[i]) << rc::kindName(e.kind) << ": the test's layout spelling drifted";
        auto enc = rc::encode(e);
        ASSERT_TRUE(enc.has_value()) << rc::kindName(e.kind);
        EXPECT_EQ(toHex(*enc), kGolden[i]) << rc::kindName(e.kind) << ": the encoder's formatVer-1 bytes changed";
        auto d = rc::decode(golden);
        ASSERT_TRUE(d.has_value()) << rc::kindName(e.kind) << ": " << rc::decodeErrorName(d.error());
        EXPECT_TRUE(*d == e) << rc::kindName(e.kind) << ": the golden bytes no longer decode to the golden event";
    }
}

TEST(CatalogEventRegression, HeaderLayoutIsFrozen) {
    // formatVer u16 LE, kind u16 LE, objectId u64 LE, prevVersion u32, newVersion u32,
    // timestampMs i64, fingerprint u64 — the first 36 bytes, in this order, forever.
    const auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 3})).value();
    ASSERT_GE(b.size(), 36u);
    EXPECT_EQ(b[0], 1); EXPECT_EQ(b[1], 0);                 // formatVer 1
    EXPECT_EQ(b[2], 2); EXPECT_EQ(b[3], 0);                 // DropTopic
    EXPECT_EQ(b[4], 7);                                      // objectId low byte
    EXPECT_EQ(b[12], 0); EXPECT_EQ(b[16], 1);                // prev 0, new 1
    EXPECT_EQ(b[28], 0x88); EXPECT_EQ(b[35], 0x11);          // fingerprint LE
}

TEST(CatalogEventRegression, MultiByteIntegersAreLittleEndianByteWise) {
    const rc::CatalogEvent e = goldenEvent(1);
    const auto b = rc::encode(e).value();
    const Layout l = layoutOf(e);
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(b[l.at("objectId") + i], 8 - i) << "objectId byte " << i;   // 0x0102030405060708 → 08 07 .. 01
        EXPECT_EQ(b[l.at("requestIdHi") + i], static_cast<uint8_t>(0x0123456789ABCDEFull >> (8 * i))) << "requestId.hi byte " << i;
    }
}

// ── round trips compare the DECODED EVENT, not re-encoded bytes ───────────────
TEST(CatalogEventRegression, EveryKindRoundTripsToAnEqualEvent) {
    for (const rc::Ops& ops : everyKind()) {
        for (const bool withReplay : {false, true}) {
            rc::CatalogEvent e = baseEvent(ops);
            if (withReplay) e.replayedFrom = replay();
            auto d = rc::decode(rc::encode(e).value());
            ASSERT_TRUE(d.has_value()) << rc::kindName(e.kind) << ": " << rc::decodeErrorName(d.error());
            EXPECT_TRUE(*d == e) << rc::kindName(e.kind) << " replay=" << withReplay;
            EXPECT_EQ(d->principal.requestId, (rc::RequestId{0x0123456789ABCDEFull, 0xABCDEFull})) << "128-bit request id (F14)";
            if (const auto* a = std::get_if<rc::AlterSchemaOps>(&d->ops)) {   // F20: durable tags, never names
                ASSERT_EQ(a->ops.size(), 4u);
                EXPECT_EQ(a->ops[0].tag, 2); EXPECT_EQ(a->ops[0].fixedLen, 16); EXPECT_EQ(a->ops[1].name, "health");
                EXPECT_EQ(a->ops[2].type, AlterOpType::Drop); EXPECT_TRUE(a->ops[2].name.empty());
                EXPECT_EQ(a->ops[3].mode, CompatMode::Forward);
            }
        }
    }
}

// ── byte-level refusals ───────────────────────────────────────────────────────
TEST(CatalogEventRegression, EveryTruncationIsRefusedNeverMisread) {
    for (const rc::Ops& ops : everyKind()) {
        for (const bool withReplay : {false, true}) {   // includes every cut inside the replayedFrom block
            rc::CatalogEvent e = baseEvent(ops);
            if (withReplay) e.replayedFrom = replay();
            const auto b = rc::encode(e).value();
            for (std::size_t n = 0; n < b.size(); ++n) {
                auto d = rc::decode(std::span<const uint8_t>(b.data(), n));
                ASSERT_FALSE(d.has_value()) << "prefix " << n << " of " << b.size() << " decoded";
                EXPECT_EQ(d.error(), rc::DecodeError::Truncated) << rc::kindName(e.kind) << " prefix " << n;
            }
        }
    }
}

TEST(CatalogEventRegression, TrailingBytesAreRefused) {
    auto b = rc::encode(baseEvent(rc::DropViewOps{.viewTopicId = 1})).value();
    b.push_back(0);
    auto d = rc::decode(b);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), rc::DecodeError::TrailingBytes);
}

TEST(CatalogEventRegression, OpsLengthDisagreeingWithItsBodyIsRefused) {
    for (std::size_t i = 0; i < 9; ++i) {
        const rc::CatalogEvent e = goldenEvent(i);
        const Layout l = layoutOf(e);
        const std::size_t at = l.at("opsLen");
        const auto opsLen = static_cast<uint32_t>(l.bytes.size() - at - 4);
        {   // opsLen claims one byte more than the body, and that byte is present: inner trailing bytes
            auto b = rc::encode(e).value();
            b.push_back(0);
            const uint32_t v = opsLen + 1;
            for (int k = 0; k < 4; ++k) b[at + k] = static_cast<uint8_t>(v >> (8 * k));
            auto d = rc::decode(b);
            ASSERT_FALSE(d.has_value()) << rc::kindName(e.kind);
            EXPECT_EQ(d.error(), rc::DecodeError::TrailingBytes) << rc::kindName(e.kind);
        }
        {   // opsLen one byte short: the body is cut inside the ops blob
            auto b = rc::encode(e).value();
            const uint32_t v = opsLen - 1;
            for (int k = 0; k < 4; ++k) b[at + k] = static_cast<uint8_t>(v >> (8 * k));
            auto d = rc::decode(b);
            ASSERT_FALSE(d.has_value()) << rc::kindName(e.kind);
            EXPECT_EQ(d.error(), rc::DecodeError::Truncated) << rc::kindName(e.kind);
        }
    }
}

// ── value refusals: encode refuses EXACTLY what decode refuses (one validation) ─
TEST(CatalogEventRegression, UnknownFormatVersionIsRefusedByEncodeAndDecode) {
    for (const uint16_t v : {uint16_t{0}, uint16_t{3}, uint16_t{0xFFFF}}) {   // 1 and 2 are the known formats
        auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1})).value();
        putU16(b, 0, v);
        auto d = rc::decode(b);
        ASSERT_FALSE(d.has_value()) << v;
        EXPECT_EQ(d.error(), rc::DecodeError::UnknownFormatVer) << v;

        rc::CatalogEvent e = baseEvent(rc::DropTopicOps{.topicId = 1});
        e.formatVer = v;
        std::vector<uint8_t> buf{1};
        auto r = rc::encodeInto(e, buf);
        ASSERT_FALSE(r.has_value()) << v;
        EXPECT_EQ(r.error(), rc::EncodeError::UnknownFormatVer) << v;
        EXPECT_TRUE(buf.empty());
    }
}

TEST(CatalogEventRegression, ReservedAndUnknownKindBoundariesAreRefusedByEncodeAndDecode) {
    std::vector<std::pair<uint16_t, bool>> kinds;   // (kind, reserved?)
    for (uint16_t k = 32; k <= 63; ++k) kinds.emplace_back(k, true);   // the whole reserved range
    for (uint16_t k : {0, 10, 11, 31, 64, 65, 0x100, 0xFFFF}) kinds.emplace_back(k, false);
    for (const auto& [k, reserved] : kinds) {
        const auto expectDecode = reserved ? rc::DecodeError::ReservedKind : rc::DecodeError::UnknownKind;
        const auto expectEncode = reserved ? rc::EncodeError::ReservedKind : rc::EncodeError::UnknownKind;
        auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1})).value();
        putU16(b, 2, k);
        auto d = rc::decode(b);
        ASSERT_FALSE(d.has_value()) << k;
        EXPECT_EQ(d.error(), expectDecode) << k;

        rc::CatalogEvent e = baseEvent(rc::DropTopicOps{.topicId = 1});
        e.kind = static_cast<rc::EventKind>(k);
        std::vector<uint8_t> buf{1};
        auto r = rc::encodeInto(e, buf);
        ASSERT_FALSE(r.has_value()) << k;
        EXPECT_EQ(r.error(), expectEncode) << k;
        EXPECT_TRUE(buf.empty()) << k;
    }
    for (uint16_t k = 1; k <= 9; ++k) {   // the valid boundaries still decode
        rc::CatalogEvent e = baseEvent(everyKind()[k - 1]);
        EXPECT_TRUE(rc::decode(rc::encode(e).value()).has_value()) << k;
    }
}

TEST(CatalogEventRegression, KindDisagreeingWithOpsIsRefusedByEncodeAndDecode) {
    const auto ops = everyKind();
    for (uint16_t k = 1; k <= 9; ++k) {
        for (std::size_t j = 0; j < 9; ++j) {
            if (j + 1 == k) continue;
            const rc::CatalogEvent good = baseEvent(ops[j]);
            const Layout l = layoutOf(good);
            {   // header kind rewritten
                auto b = rc::encode(good).value();
                putU16(b, l.at("kind"), k);
                EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::KindOpsMismatch) << k << " vs ops " << j;
            }
            {   // ops tag rewritten
                auto b = rc::encode(good).value();
                b[l.at("opsTag")] = static_cast<uint8_t>(k);
                EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::KindOpsMismatch) << "tag " << k << " vs kind " << j + 1;
            }
            {   // the encoder refuses to write the mix-up at all
                rc::CatalogEvent bad = good;
                bad.kind = static_cast<rc::EventKind>(k);
                std::vector<uint8_t> buf{1};
                auto r = rc::encodeInto(bad, buf);
                ASSERT_FALSE(r.has_value()) << k << " vs ops " << j;
                EXPECT_EQ(r.error(), rc::EncodeError::KindOpsMismatch) << k << " vs ops " << j;
                EXPECT_TRUE(buf.empty());
            }
        }
    }
}

namespace {
struct BadValueCase {
    const char*                             name;
    std::size_t                             kindIndex;   // everyKind() index
    const char*                             mark;        // layout mark of the checked byte
    uint8_t                                 badByte;
    std::function<void(rc::CatalogEvent&)>  corrupt;     // the same bad value in the model; empty for bools
};
template <class T> T& opsOf(rc::CatalogEvent& e) { return std::get<T>(e.ops); }
std::vector<BadValueCase> badValueCases() {
    using CT = rc::CreateTopicOps;
    auto cv = [](rc::CatalogEvent& e) -> rc::ViewDef& { return opsOf<rc::CreateViewOps>(e).view; };
    return {
        {"CreateTopic.persistence=2", 0, "persistence", 2, [](auto& e) { opsOf<CT>(e).persistence = rc::Persistence{2}; }},
        {"CreateTopic.persistence=255", 0, "persistence", 255, [](auto& e) { opsOf<CT>(e).persistence = rc::Persistence{255}; }},
        {"config.cleanupPolicy=3", 0, "cleanupPolicy", 3, [](auto& e) { opsOf<CT>(e).config.cleanupPolicy = 3; }},
        {"config.durability=2", 0, "durability", 2, [](auto& e) { opsOf<CT>(e).config.durability = 2; }},
        {"config.durabilityTier=3", 0, "durabilityTier", 3, [](auto& e) { opsOf<CT>(e).config.durabilityTier = 3; }},
        {"CreateTopic.hasSchema=2 (bool)", 0, "hasSchema", 2, nullptr},
        {"CreateTopic.schema.validate=2 (bool)", 0, "schema.validate", 2, nullptr},
        {"CreateSchema.schema.validate=2 (bool)", 3, "schema.validate", 2, nullptr},
        {"hasReplayedFrom=2 (bool)", 1, "hasReplayedFrom", 2, nullptr},
        {"ConfigKey=0", 2, "change0.key", 0, [](auto& e) { opsOf<rc::AlterTopicConfigOps>(e).changes[0].key = rc::ConfigKey{0}; }},
        {"ConfigKey=7", 2, "change5.key", 7, [](auto& e) { opsOf<rc::AlterTopicConfigOps>(e).changes[5].key = rc::ConfigKey{7}; }},
        {"AlterOpType=0", 4, "alterOp0.type", 0, [](auto& e) { opsOf<rc::AlterSchemaOps>(e).ops[0].type = AlterOpType{0}; }},
        {"AlterOpType=5", 4, "alterOp1.type", 5, [](auto& e) { opsOf<rc::AlterSchemaOps>(e).ops[1].type = AlterOpType{5}; }},
        {"PrimType=11", 4, "alterOp0.primType", 11, [](auto& e) { opsOf<rc::AlterSchemaOps>(e).ops[0].primType = PrimType{11}; }},
        {"PrimType=255", 4, "alterOp2.primType", 255, [](auto& e) { opsOf<rc::AlterSchemaOps>(e).ops[2].primType = PrimType{255}; }},
        {"AlterOp.mode=2", 4, "alterOp3.mode", 2, [](auto& e) { opsOf<rc::AlterSchemaOps>(e).ops[3].mode = CompatMode{2}; }},
        {"SetCompatibility.mode=2", 5, "setCompat.mode", 2, [](auto& e) { opsOf<rc::SetCompatibilityOps>(e).mode = CompatMode{2}; }},
        {"CreateView.persistence=2", 6, "view.persistence", 2, [=](auto& e) { cv(e).persistence = rc::Persistence{2}; }},
        {"AlterView.persistence=2", 7, "view.persistence", 2, [](auto& e) { opsOf<rc::AlterViewOps>(e).view.persistence = rc::Persistence{2}; }},
    };
}
} // namespace

// Every checked enum / bool byte: decode refuses it as BadEnum; for every enum the encoder
// refuses the same value in the model as BadEnum (a bool cannot hold a bad value in the model).
TEST(CatalogEventRegression, EveryOutOfRangeEnumOrBoolIsRefusedByEncodeAndDecode) {
    for (const BadValueCase& c : badValueCases()) {
        const rc::CatalogEvent good = goldenEvent(c.kindIndex);   // golden events carry replayedFrom
        const Layout l = layoutOf(good);
        auto b = rc::encode(good).value();
        b[l.at(c.mark)] = c.badByte;
        auto d = rc::decode(b);
        ASSERT_FALSE(d.has_value()) << c.name;
        EXPECT_EQ(d.error(), rc::DecodeError::BadEnum) << c.name << ": " << rc::decodeErrorName(d.error());
        if (!c.corrupt) continue;
        rc::CatalogEvent bad = good;
        c.corrupt(bad);
        std::vector<uint8_t> buf{1, 2};
        auto r = rc::encodeInto(bad, buf);
        ASSERT_FALSE(r.has_value()) << c.name << ": encode wrote an event decode refuses";
        EXPECT_EQ(r.error(), rc::EncodeError::BadEnum) << c.name << ": " << rc::encodeErrorName(r.error());
        EXPECT_TRUE(buf.empty()) << c.name;
    }
}

TEST(CatalogEventRegression, AllocationFailureIsOutOfMemoryNotTruncated) {
    const auto b = rc::encode(goldenEvent(0)).value();   // CreateTopic: its schema blob must allocate
    gProbeFail = true;
    auto d = rc::decode(b);
    gProbeFail = false;
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), rc::DecodeError::OutOfMemory) << rc::decodeErrorName(d.error());
}

TEST(CatalogEventRegression, EncodeIntoReusesCapacity) {
    std::vector<uint8_t> buf;
    buf.reserve(4096);
    const auto* data = buf.data();
    ASSERT_TRUE(rc::encodeInto(baseEvent(typedCreate()), buf).has_value());
    EXPECT_EQ(buf.data(), data) << "encodeInto must not reallocate a buffer that fits";
}

// ── Fix round 0: encoding never truncates a length or a count ────────────────
namespace {
struct OversizeCase {
    const char*                                   name;
    rc::EncodeError                               expected;
    std::size_t                                   max;
    std::function<rc::CatalogEvent(std::size_t)>  make;   // an event whose field has length / count n
};
rc::CatalogEvent withReplay(rc::Ops ops) {
    rc::CatalogEvent e = baseEvent(std::move(ops));
    e.replayedFrom = rc::ReplayedFrom{.clusterId = "c", .offset = 1, .principal = "p"};
    return e;
}
std::vector<OversizeCase> oversizeCases() {
    using E = rc::EncodeError;
    const std::size_t S = rc::kMaxStr16Bytes, L = rc::kMaxListCount;
    auto str = [](std::size_t n) { return std::string(n, 'x'); };
    return {
        {"objectName", E::FieldTooLong, S, [=](std::size_t n) { auto e = baseEvent(rc::DropTopicOps{1}); e.objectName = str(n); return e; }},
        {"principal.clientName", E::FieldTooLong, S, [=](std::size_t n) { auto e = baseEvent(rc::DropTopicOps{1}); e.principal.clientName = str(n); return e; }},
        {"principal.sourceAddress", E::FieldTooLong, S, [=](std::size_t n) { auto e = baseEvent(rc::DropTopicOps{1}); e.principal.sourceAddress = str(n); return e; }},
        {"principal.authenticated", E::FieldTooLong, S, [=](std::size_t n) { auto e = baseEvent(rc::DropTopicOps{1}); e.principal.authenticated = str(n); return e; }},
        {"replayedFrom.clusterId", E::FieldTooLong, S, [=](std::size_t n) { auto e = withReplay(rc::DropTopicOps{1}); e.replayedFrom->clusterId = str(n); return e; }},
        {"replayedFrom.principal", E::FieldTooLong, S, [=](std::size_t n) { auto e = withReplay(rc::DropTopicOps{1}); e.replayedFrom->principal = str(n); return e; }},
        {"config.coPartitionGroup", E::FieldTooLong, S, [=](std::size_t n) { auto o = typedCreate(); o.config.coPartitionGroup = str(n); return baseEvent(o); }},
        {"config.partitionKey", E::FieldTooLong, S, [=](std::size_t n) { auto o = typedCreate(); o.config.partitionKey = str(n); return baseEvent(o); }},
        {"createTopic.schema.keyField", E::FieldTooLong, S, [=](std::size_t n) { auto o = typedCreate(); o.schema->keyField = str(n); return baseEvent(o); }},
        {"createSchema.keyField", E::FieldTooLong, S, [=](std::size_t n) { rc::CreateSchemaOps o; o.schema.keyField = str(n); return baseEvent(o); }},
        {"alterSchema.op.name", E::FieldTooLong, S, [=](std::size_t n) {
             return baseEvent(rc::AlterSchemaOps{.ops = {{.type = rune::codec::AlterOpType::Rename, .tag = 1, .name = str(n)}}}); }},
        {"view.keyField", E::FieldTooLong, S, [=](std::size_t n) { auto v = viewDef(); v.keyField = str(n); return baseEvent(rc::CreateViewOps{v}); }},
        {"view.sourceName", E::FieldTooLong, S, [=](std::size_t n) { auto v = viewDef(); v.sourceNames = {str(n)}; return baseEvent(rc::AlterViewOps{v}); }},
        {"alterTopicConfig.changes", E::TooManyElements, L, [=](std::size_t n) {
             return baseEvent(rc::AlterTopicConfigOps{.changes = std::vector<rc::ConfigChange>(n, {rc::ConfigKey::RetentionMs, 1})}); }},
        {"alterSchema.ops", E::TooManyElements, L, [=](std::size_t n) {
             return baseEvent(rc::AlterSchemaOps{.ops = std::vector<rc::ResolvedAlterOp>(n, rc::ResolvedAlterOp{.tag = 1})}); }},
        {"view.sourceNames", E::TooManyElements, L, [=](std::size_t n) {
             auto v = viewDef(); v.sourceNames.assign(n, "s"); return baseEvent(rc::CreateViewOps{v}); }},
    };
}
} // namespace

// Every str16 field and every u16-counted list: the wire maximum round-trips; one more is an
// explicit EncodeError and leaves the buffer empty — never a silently truncated prefix.
// (str32 / bytes32 fields go through the same length check at 4 GiB; not materialized here.)
TEST(CatalogEventRegression, OversizeFieldOrListIsAnEncodeErrorNeverAnUndecodableEvent) {
    for (const OversizeCase& c : oversizeCases()) {
        const rc::CatalogEvent atMax = c.make(c.max);
        auto ok = rc::encode(atMax);
        ASSERT_TRUE(ok.has_value()) << c.name << " at its wire maximum must encode";
        auto d = rc::decode(*ok);
        ASSERT_TRUE(d.has_value()) << c.name << ": " << rc::decodeErrorName(d.error());
        EXPECT_TRUE(*d == atMax) << c.name << ": decoded event differs from the encoded one";

        std::vector<uint8_t> buf{1, 2, 3};
        auto bad = rc::encodeInto(c.make(c.max + 1), buf);
        ASSERT_FALSE(bad.has_value()) << c.name << " one over its wire width encoded";
        EXPECT_EQ(bad.error(), c.expected) << c.name << ": " << rc::encodeErrorName(bad.error());
        EXPECT_TRUE(buf.empty()) << c.name << ": a refused event leaves no bytes behind";
        EXPECT_FALSE(rc::encode(c.make(c.max + 1)).has_value()) << c.name;
    }
}

// ── Fix round 0: a corrupt list count never sizes an allocation ──────────────
TEST(CatalogEventRegression, HugeListCountInAShortBufferIsTruncatedWithoutAllocating) {
    struct Case { const char* name; std::size_t kindIndex; const char* mark; };
    const Case cases[] = {
        {"alterTopicConfig.changes", 2, "changes.count"},
        {"alterSchema.ops", 4, "alterOps.count"},
        {"view.sourceNames", 6, "view.sourceNames.count"},
    };
    for (const Case& c : cases) {
        const rc::CatalogEvent e = goldenEvent(c.kindIndex);
        auto b = rc::encode(e).value();
        putU16(b, layoutOf(e).at(c.mark), 0xFFFF);   // 65535 elements claimed, a handful of bytes present
        gProbePeak = 0; gProbeArmed = true;
        auto d = rc::decode(b);
        gProbeArmed = false;
        ASSERT_FALSE(d.has_value()) << c.name;
        EXPECT_EQ(d.error(), rc::DecodeError::Truncated) << c.name;
        EXPECT_LT(gProbePeak, 4096u) << c.name << ": decode sized an allocation from an untrusted count";
    }
}

