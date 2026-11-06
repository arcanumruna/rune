// CheckedWritesKeyIndexWideTest.cpp - checked writes Part 07 Task 16: the key index is keyed by the wire key at the topic's width (2 to 16 bytes).
#include "CheckedWritesRig.h"

#include <rune/protocol/RecordBatch.h>

#include <string>

using namespace rune::node;
using namespace rune::node::cw;
using rune::protocol::client::ErrorCode;
using rune::codec::PrimType;

namespace {
constexpr uint8_t kAcksViewAll = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;
constexpr uint16_t kKeyExists = static_cast<uint16_t>(ErrorCode::KeyExists);
constexpr uint16_t kKeyNotFound = static_cast<uint16_t>(ErrorCode::KeyNotFound);
constexpr uint8_t kUpsert = 0, kInsert = 1, kExisting = 2;
using V = std::vector<std::tuple<uint16_t, uint16_t, uint32_t>>;

/// A key-index topic whose key is `shape`, with acks and an idempotent producer.
struct Wide {
    VerdictRig r;
    int32_t    seq = 0;
    explicit Wide(KeyShape shape) {
        r.createKeyIndex(shape);
        r.enableAcks();
        r.passes(60);
    }
    std::optional<VerdictRig::Ack> send(const std::vector<Rec>& recs) {
        const auto a = r.ack(r.produceFrame(recs, kAcksViewAll, 42, seq));
        seq += static_cast<int32_t>(recs.size());
        return a;
    }
    /// One record keyed by the 16 bytes of `name` (zero padded); the source is string(16).
    std::optional<VerdictRig::Ack> sendName(const std::string& name, uint8_t intent, bool remove = false) {
        std::string key = name;
        key.resize(16, '\0');
        std::vector<uint8_t> val;
        const auto le16 = [&](uint16_t x) { val.push_back(uint8_t(x)); val.push_back(uint8_t(x >> 8)); };
        le16(26); le16(1);
        val.insert(val.end(), key.begin(), key.end());
        val.insert(val.end(), 10, uint8_t{0});
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.setProducerId(42); b.setProducerEpoch(0); b.setBaseSequence(seq);
        b.append(key, remove ? std::string_view{} : std::string_view(reinterpret_cast<const char*>(val.data()), val.size()), 1000, intent);
        const auto a = r.ack(r.produceBytes(b.build(), kAcksViewAll, 42, seq));
        ++seq;
        return a;
    }
};

V expectCode(const std::optional<VerdictRig::Ack>& a) {
    EXPECT_TRUE(a.has_value());
    return a ? a->verdicts : V{};
}
const V kOk{};
const V kExists{{0, kKeyExists, 0}};
const V kNotFound{{0, kKeyNotFound, 0}};
}  // namespace

TEST(CheckedWritesKeyIndexWide, SixteenByteStringKeys) {
    Wide w({PrimType::FixedStr, 16});
    EXPECT_EQ(expectCode(w.sendName("alice", kInsert)), kOk);
    EXPECT_EQ(expectCode(w.sendName("alice", kInsert)), kExists);
    EXPECT_EQ(expectCode(w.sendName("bob", kExisting)), kNotFound);
    EXPECT_EQ(expectCode(w.sendName("alice", kExisting, true)), kOk);
    EXPECT_EQ(expectCode(w.sendName("alice", kExisting)), kNotFound);
}

TEST(CheckedWritesKeyIndexWide, EveryKeyWidth) {
    const KeyShape shapes[] = {{PrimType::UInt16}, {PrimType::UInt32}, {PrimType::UInt64}, {PrimType::Int64}, {PrimType::FixedStr, 9}, {PrimType::FixedStr, 16}};
    for (const KeyShape& s : shapes) {
        SCOPED_TRACE(keyWidthOf(s));
        Wide w(s);
        EXPECT_EQ(expectCode(w.send({Rec{7, 1, 0, false, kInsert}})), kOk);
        EXPECT_EQ(expectCode(w.send({Rec{7, 1, 0, false, kInsert}})), kExists);
        EXPECT_EQ(expectCode(w.send({Rec{7, 2, 0, false, kExisting}})), kOk);
        EXPECT_EQ(expectCode(w.send({Rec{7, 0, 0, true, kExisting}})), kOk);
        EXPECT_EQ(expectCode(w.send({Rec{7, 2, 0, false, kExisting}})), kNotFound);
    }
}

TEST(CheckedWritesKeyIndexWide, KeysDifferingInTheLastByteAreDistinct) {
    Wide w({PrimType::FixedStr, 16});
    const std::string base(15, 'k');
    EXPECT_EQ(expectCode(w.sendName(base + 'a', kInsert)), kOk);
    EXPECT_EQ(expectCode(w.sendName(base + 'b', kInsert)), kOk);
    EXPECT_EQ(expectCode(w.sendName(base + 'b', kInsert)), kExists);
}
