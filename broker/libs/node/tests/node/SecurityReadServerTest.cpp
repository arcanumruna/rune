// SecurityReadServerTest.cpp — authentication Task 10: `__security` reads are the catalog read contract (backlog up to
// the HW, ONE caught-up marker, live events, history epoch) served by the admin thread's SECOND CatalogReadServer;
// the server stamps the topic it was built for (fix-on-sight: it hard-coded kCatalogTopicId twice).
#include "support/CatalogReadRig.h"

#include <rune/node/SecurityPartition.h>

#include <gtest/gtest.h>

#include <filesystem>

using rune::node::test::ReadRig;
namespace pc = rune::protocol::client;

TEST(SecurityReadServerRegression, ServesSecurityUpToTheHighWatermarkThenTheMarker) {
    ReadRig r("sec_rd_basic", false, 64, rune::node::kSecurityTopicId);
    r.commitEvents(4);
    r.server.subscribe(/*session=*/7, /*corr=*/1, /*from=*/-2);
    r.server.pump();
    ASSERT_TRUE(r.subscribeAnswered(7, 1));
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{0, 1, 2, 3}));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{4})) << "the caught-up marker at the HW";
    EXPECT_EQ(r.notes(7).size(), 1u) << "the history epoch note opens the stream";
    r.commitEvents(1);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7).back(), 4) << "live events follow";
    EXPECT_EQ(r.markers(7).size(), 1u) << "one marker per subscription";
    // The subscription id lives in the security id space (bit 62 + bit 61): one id names one server.
    const int64_t sid = r.subscriptionIdOf(7, 1);
    EXPECT_TRUE(rune::gateway::isCatalogSubscriptionId(sid));
    EXPECT_TRUE(rune::gateway::isSecuritySubscriptionId(sid));
    // The history epoch is persisted in the security log's own directory.
    EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(r.replica().log()->dir()) / "history_epoch"));
    EXPECT_NE(r.replica().log()->dir().find("__security"), std::string::npos);
}

TEST(CatalogReadServerRegression, ServesTheTopicItWasBuiltForRegression) {
    // A server built for -7 stamps -7 on EVERY frame (note, data, marker); the -6 server's frames are unchanged.
    for (const int32_t topic : {rune::node::kSecurityTopicId, rune::node::kCatalogTopicId}) {
        ReadRig r(topic == rune::node::kSecurityTopicId ? "topic_sec" : "topic_cat", false, 64, topic);
        r.commitEvents(2);
        r.server.subscribe(9, 1, -2);
        r.server.pump();
        const auto recs = r.records(9);
        ASSERT_GE(recs.size(), 3u) << "note + data + marker";
        for (const auto& rec : recs) {
            EXPECT_EQ(rec.topicId, topic) << "topic " << topic;
            EXPECT_EQ(rec.partition, 0);
        }
        EXPECT_FALSE(rune::gateway::isSecuritySubscriptionId(r.subscriptionIdOf(9, 1)) == (topic == rune::node::kCatalogTopicId));
    }
}

namespace {
// The frame layouts, from the encoders in protocol/client/DataMessages.h (header = MessageHeader::ENCODED_SIZE = 24):
//   SubscribeResponse: errorCode(2) partition(4) subscriptionId(8) at header+0 / +2 / +6
//   Records:           subscriptionId(8) topicId(4) partition(4) baseOffset(8) at header+0 / +8 / +12 / +16, then a u32
//                      length prefix and the record batch.
constexpr std::size_t kHdr = pc::MessageHeader::ENCODED_SIZE;
constexpr std::size_t kSubRespSubIdAt = kHdr + 6;
constexpr std::size_t kRecSubIdAt     = kHdr + 0;
constexpr std::size_t kRecTopicAt     = kHdr + 8;
constexpr std::size_t kRecBodyAt      = kHdr + pc::Records::BODY_FIXED + 4;

void zero(std::vector<uint8_t>& b, std::size_t at, std::size_t n) {
    ASSERT_LE(at + n, b.size());
    for (std::size_t i = 0; i < n; ++i) b[at + i] = 0;
}

// The frame with exactly the fields that legitimately differ between the two servers zeroed.
std::vector<uint8_t> masked(const std::vector<uint8_t>& frame) {
    std::vector<uint8_t> b = frame;
    const auto h = pc::MessageHeader::decode(b.data(), b.size());
    EXPECT_TRUE(h.has_value());
    if (!h) return b;
    if (h->templateId == pc::SubscribeResponse::TEMPLATE_ID) {
        zero(b, kSubRespSubIdAt, 8);
    } else if (h->templateId == pc::Records::TEMPLATE_ID) {
        zero(b, kRecSubIdAt, 8);
        zero(b, kRecTopicAt, 4);
        // The 8-byte payload of a note / marker is the history epoch, minted per log directory.
        const auto r = pc::Records::decode(frame.data(), frame.size());
        EXPECT_TRUE(r.has_value());
        if (r && r->recordBatch.size() == 8) zero(b, kRecBodyAt, 8);
    }
    return b;
}
}  // namespace

TEST(SecurityReadServerRegression, SecurityFramesAreByteIdenticalInFramingToCatalogFramesRegression) {
    // The client's SecurityReader assumes the -7 frames are framed exactly like the -6 frames: pin it byte for byte.
    ReadRig cat("frames_cat", false, 64, rune::node::kCatalogTopicId);
    ReadRig sec("frames_sec", false, 64, rune::node::kSecurityTopicId);
    for (ReadRig* r : {&cat, &sec}) {
        r->commitEvents(4);
        r->server.subscribe(/*session=*/7, /*corr=*/1, /*from=*/-2);
        r->server.pump();
        r->commitEvents(2);   // live events after the marker
        r->server.pump();
    }
    std::vector<const decltype(cat.frames)::value_type*> a, b;
    for (const auto& f : cat.frames) if (f.session == 7) a.push_back(&f);
    for (const auto& f : sec.frames) if (f.session == 7) b.push_back(&f);
    ASSERT_GE(a.size(), 4u) << "SubscribeResponse + note + records + marker at least";
    ASSERT_EQ(a.size(), b.size());
    bool sawSubscribeResponse = false, sawNote = false, sawMarker = false, sawData = false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        const auto& fa = a[i]->bytes;
        const auto& fb = b[i]->bytes;
        ASSERT_EQ(fa.size(), fb.size()) << "frame " << i << ": length";
        const auto ha = pc::MessageHeader::decode(fa.data(), fa.size());
        ASSERT_TRUE(ha.has_value());
        if (ha->templateId == pc::SubscribeResponse::TEMPLATE_ID) sawSubscribeResponse = true;
        if (const auto rec = pc::Records::decode(fa.data(), fa.size())) {
            if (rec->baseOffset < 0) sawNote = true;
            else if (rec->recordBatch.size() == 8) sawMarker = true;
            else sawData = true;
        }
        EXPECT_EQ(masked(fa), masked(fb)) << "frame " << i << " (template " << ha->templateId
                                          << "): bytes differ beyond the topic id / subscription id";
    }
    EXPECT_TRUE(sawSubscribeResponse && sawNote && sawMarker && sawData);
    // The masks only hide what they should: the ids really do differ.
    EXPECT_NE(cat.subscriptionIdOf(7, 1), sec.subscriptionIdOf(7, 1));
}
