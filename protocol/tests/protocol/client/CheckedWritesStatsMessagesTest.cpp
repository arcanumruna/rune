#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/StatsMessages.h>
#include <gtest/gtest.h>
#include <cstdio>
#include <string>
#include <vector>

namespace pc = rune::protocol::client;

namespace {

constexpr int64_t kSession = 0x1122334455667788LL;
constexpr int64_t kCorr    = 0x0A0B0C0D0E0F1011LL;

template <class T>
std::vector<uint8_t> encodeVec(const T& m) {
    std::vector<uint8_t> b(m.encodedSize());
    EXPECT_EQ(m.encode(b.data()), b.size());
    return b;
}

std::vector<uint8_t> readVector(const char* name) {
    const std::string path = std::string(RUNE_CONFORMANCE_DIR) + "/vectors/" + name;
    FILE* f = std::fopen(path.c_str(), "rb");
    EXPECT_NE(f, nullptr) << path;
    if (!f) return {};
    std::vector<uint8_t> b;
    uint8_t chunk[256];
    std::size_t got;
    while ((got = std::fread(chunk, 1, sizeof chunk, f)) > 0) b.insert(b.end(), chunk, chunk + got);
    std::fclose(f);
    return b;
}

void putLE(std::vector<uint8_t>& b, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i)));
}

// stats {id, value} and constraints {id, rejections} as they sit in a response
std::vector<uint8_t> statBytes(const std::vector<uint16_t>& ids) {
    std::vector<uint8_t> b;
    for (const uint16_t id : ids) { putLE(b, id, 2); putLE(b, 1000u + id, 8); }
    return b;
}

std::vector<uint8_t> constraintBytes(uint16_t n) {
    std::vector<uint8_t> b;
    for (uint16_t i = 0; i < n; ++i) { putLE(b, 100u + i, 4); putLE(b, 5000u + i, 8); }
    return b;
}

pc::StatsResponse makeResponse(const std::vector<uint8_t>& stats, uint16_t count,
                               const std::vector<uint8_t>& cons, uint16_t nCons) {
    pc::StatsResponse m;
    m.sessionId = kSession; m.correlationId = kCorr;
    m.status = pc::StatsResponse::kOk; m.count = count; m.statBytes = stats;
    m.constraintCount = nCons; m.constraintBytes = cons;
    return m;
}

std::vector<uint8_t> raw(uint16_t count, const std::vector<uint8_t>& stats, uint16_t nCons, const std::vector<uint8_t>& cons) {
    auto b = encodeVec(makeResponse({}, 0, {}, 0));
    b.resize(pc::MessageHeader::ENCODED_SIZE);
    putLE(b, 0, 2); putLE(b, count, 2);
    b.insert(b.end(), stats.begin(), stats.end());
    putLE(b, nCons, 2);
    b.insert(b.end(), cons.begin(), cons.end());
    return b;
}

} // namespace

TEST(CheckedWritesStatsMessages, GetStatsRoundTrip) {
    for (const auto scope : {pc::StatsScope::TopicPartition, pc::StatsScope::ViewPartition}) {
        pc::GetStats m;
        m.sessionId = kSession; m.correlationId = kCorr; m.scope = scope; m.objectId = 7; m.partition = 3;
        EXPECT_EQ(m.encodedSize(), 33u);
        const auto wire = encodeVec(m);
        ASSERT_EQ(wire.size(), 33u);
        const auto d = pc::GetStats::decode(wire.data(), wire.size());
        ASSERT_TRUE(d);
        EXPECT_EQ(d->sessionId, kSession);
        EXPECT_EQ(d->correlationId, kCorr);
        EXPECT_EQ(d->scope, scope);
        EXPECT_EQ(d->objectId, 7);
        EXPECT_EQ(d->partition, 3);
    }
}

TEST(CheckedWritesStatsMessages, ResponseRoundTrip) {
    const auto stats = statBytes({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    const auto cons = constraintBytes(2);
    const auto m = makeResponse(stats, 10, cons, 2);
    EXPECT_EQ(m.encodedSize(), 24u + 4 + 100 + 2 + 24);
    const auto wire = encodeVec(m);
    const auto d = pc::StatsResponse::decode(wire.data(), wire.size());
    ASSERT_TRUE(d);
    EXPECT_EQ(d->status, 0);
    EXPECT_EQ(d->count, 10);
    EXPECT_EQ(d->constraintCount, 2);
    uint16_t next = 1;
    d->forEachStat([&](uint16_t id, int64_t value) {
        EXPECT_EQ(id, next);
        EXPECT_EQ(value, 1000 + next);
        ++next;
    });
    EXPECT_EQ(next, 11);
    uint32_t c = 0;
    d->forEachConstraint([&](uint32_t id, uint64_t rejections) {
        EXPECT_EQ(id, 100 + c);
        EXPECT_EQ(rejections, 5000u + c);
        ++c;
    });
    EXPECT_EQ(c, 2u);
    EXPECT_EQ(encodeVec(*d), wire);

    const auto empty = makeResponse({}, 0, {}, 0);
    EXPECT_EQ(empty.encodedSize(), 30u);
    const auto ew = encodeVec(empty);
    const auto ed = pc::StatsResponse::decode(ew.data(), ew.size());
    ASSERT_TRUE(ed);
    EXPECT_EQ(ed->count, 0);
    EXPECT_EQ(ed->constraintCount, 0);
}

TEST(CheckedWritesStatsMessagesRegression, BoundsAreRefusedRegression) {
    for (const uint8_t scope : {uint8_t{0}, uint8_t{3}}) {
        pc::GetStats m;
        m.sessionId = kSession; m.correlationId = kCorr; m.scope = pc::StatsScope::TopicPartition;
        auto wire = encodeVec(m);
        wire[pc::MessageHeader::ENCODED_SIZE] = scope;
        EXPECT_FALSE(pc::GetStats::decode(wire.data(), wire.size())) << int(scope);
    }
    {
        pc::GetStats m;
        m.scope = pc::StatsScope::ViewPartition;
        const auto wire = encodeVec(m);
        for (std::size_t n = 0; n < wire.size(); ++n) EXPECT_FALSE(pc::GetStats::decode(wire.data(), n)) << n;
        auto extra = wire;
        extra.push_back(0);
        EXPECT_FALSE(pc::GetStats::decode(extra.data(), extra.size()));
    }

    std::vector<uint16_t> many;
    for (uint16_t i = 1; i <= 65; ++i) many.push_back(i);
    const auto cons = constraintBytes(0);
    auto w = raw(65, statBytes(many), 0, cons);
    EXPECT_FALSE(pc::StatsResponse::decode(w.data(), w.size()));
    many.pop_back();
    w = raw(64, statBytes(many), 0, cons);
    EXPECT_TRUE(pc::StatsResponse::decode(w.data(), w.size()));

    w = raw(2, statBytes({5, 5}), 0, cons);
    EXPECT_FALSE(pc::StatsResponse::decode(w.data(), w.size()));
    w = raw(2, statBytes({6, 5}), 0, cons);
    EXPECT_FALSE(pc::StatsResponse::decode(w.data(), w.size()));

    w = raw(0, {}, 257, constraintBytes(257));
    EXPECT_FALSE(pc::StatsResponse::decode(w.data(), w.size()));
    w = raw(0, {}, 256, constraintBytes(256));
    EXPECT_TRUE(pc::StatsResponse::decode(w.data(), w.size()));

    const auto good = encodeVec(makeResponse(statBytes({1, 2, 3}), 3, constraintBytes(2), 2));
    ASSERT_TRUE(pc::StatsResponse::decode(good.data(), good.size()));
    for (std::size_t n = 0; n < good.size(); ++n) EXPECT_FALSE(pc::StatsResponse::decode(good.data(), n)) << n;
    auto extra = good;
    extra.push_back(0);
    EXPECT_FALSE(pc::StatsResponse::decode(extra.data(), extra.size()));
}

TEST(CheckedWritesStatsMessages, IdsAndNamesArePinned) {
    EXPECT_EQ(static_cast<uint16_t>(pc::StatId::verdictLag), 4);
    EXPECT_EQ(static_cast<uint16_t>(pc::StatId::autoIncrementIdsBurned), 10);
    EXPECT_EQ(static_cast<uint16_t>(pc::StatId::txnMarkerWithoutVote), 20);
    EXPECT_EQ(static_cast<uint16_t>(pc::StatId::uniqueConflicts), 32);
    EXPECT_EQ(static_cast<uint16_t>(pc::StatId::transientSourceResets), 36);
    EXPECT_EQ(pc::statName(pc::StatId::scanPages), "scanPages");
    EXPECT_EQ(pc::statName(pc::StatId::rejectionsKeyExists), "rejectionsKeyExists");
    EXPECT_EQ(pc::statName(pc::StatId::txnVoteLatencyNsTotal), "txnVoteLatencyNsTotal");
    EXPECT_EQ(pc::statName(static_cast<pc::StatId>(99)), "unknown");
    EXPECT_EQ(pc::statName(pc::StatId::schemaRetiredBelow), "schemaRetiredBelow");   // #419: id 21
    EXPECT_EQ(pc::statName(static_cast<pc::StatId>(22)), "unknown");
}

TEST(CheckedWritesStatsMessages, KnownTemplates) {
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::GetStats), 91);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::StatsResponse), 92);
    EXPECT_TRUE(pc::isKnownTemplate(91));
    EXPECT_TRUE(pc::isKnownTemplate(92));
    EXPECT_FALSE(pc::isKnownTemplate(93));
}

TEST(CheckedWritesStatsMessages, ConformanceVectors) {
    const auto getWire = readVector("get_stats.bin");
    ASSERT_EQ(getWire.size(), 33u);
    const auto g = pc::GetStats::decode(getWire.data(), getWire.size());
    ASSERT_TRUE(g);
    EXPECT_EQ(g->sessionId, kSession);
    EXPECT_EQ(g->correlationId, kCorr);
    EXPECT_EQ(g->scope, pc::StatsScope::ViewPartition);
    EXPECT_EQ(g->objectId, 7);
    EXPECT_EQ(g->partition, 3);
    EXPECT_EQ(encodeVec(*g), getWire);

    const auto respWire = readVector("stats_response.bin");
    ASSERT_EQ(respWire.size(), 24u + 4 + 3 * 10 + 2 + 2 * 12);
    const auto r = pc::StatsResponse::decode(respWire.data(), respWire.size());
    ASSERT_TRUE(r);
    EXPECT_EQ(r->status, 0);
    EXPECT_EQ(r->count, 3);
    EXPECT_EQ(r->constraintCount, 2);
    const uint16_t ids[] = {4, 32, 36};
    std::size_t i = 0;
    r->forEachStat([&](uint16_t id, int64_t value) {
        ASSERT_LT(i, 3u);
        EXPECT_EQ(id, ids[i]);
        EXPECT_EQ(value, 1000 + int64_t(i));
        ++i;
    });
    EXPECT_EQ(i, 3u);
    uint32_t c = 0;
    r->forEachConstraint([&](uint32_t id, uint64_t rejections) {
        EXPECT_EQ(id, 100 + c);
        EXPECT_EQ(rejections, 5000u + c);
        ++c;
    });
    EXPECT_EQ(c, 2u);
    EXPECT_EQ(encodeVec(*r), respWire);
}
