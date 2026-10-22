// AuthVisibilityTest.cpp - authentication Task 12 (spec §2 visibility, D22 / D23): NotAuthorized never reveals whether
// an object exists, and Metadata lists only what the session may DESCRIBE.
#include "support/AuthRig.h"

#include <algorithm>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;

namespace {
constexpr std::size_t kHeader = pc::MessageHeader::ENCODED_SIZE;

/// The frame with its header ids (session, correlation) dropped and every occurrence of `name` blanked: what two
/// answers must agree on byte for byte.
std::vector<uint8_t> body(std::vector<uint8_t> f, const std::string& name = {}) {
    if (f.size() <= kHeader) return {};
    f.erase(f.begin(), f.begin() + kHeader);
    if (!name.empty())
        for (auto it = std::search(f.begin(), f.end(), name.begin(), name.end()); it != f.end();
             it = std::search(it, f.end(), name.begin(), name.end()))
            std::fill(it, it + static_cast<std::ptrdiff_t>(name.size()), uint8_t{'#'});
    return f;
}
}  // namespace

TEST(AuthVisibilityRegression, MissingAndHiddenObjectsAnswerIdenticallyRegression) {
    AuthRig r;
    const int64_t u = r.newUser({exact(SO::Read, ST::Topic, "orders")});   // sees nothing of "secret" / "board"
    constexpr int32_t kMissing = 98'765;
    ASSERT_NE(r.secret, kMissing);

    // Subscribe by id.
    {
        const auto hidden  = r.rawAnswer(r.c().sendSubscribe(r.ch(), r.stream(), u, r.secret, 0));
        const auto missing = r.rawAnswer(r.c().sendSubscribe(r.ch(), r.stream(), u, kMissing, 0));
        ASSERT_FALSE(hidden.empty());
        EXPECT_EQ(body(hidden), body(missing)) << "Subscribe: a hidden topic answers like a missing one";
        EXPECT_EQ(pc::SubscribeResponse::decode(hidden.data(), hidden.size())->errorCode, kNotAuthorized);
    }
    // Produce by id.
    {
        pc::Produce m1, m2;
        const auto batch = makeBatch(1);
        for (auto* m : {&m1, &m2}) {
            m->sessionId = u; m->partition = 0; m->partitionKnown = 1; m->acks = 1; m->producerId = -1; m->baseSequence = -1;
            m->recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        }
        m1.topicId = r.secret; m1.correlationId = r.c().nextCorrelationId();
        m2.topicId = kMissing; m2.correlationId = r.c().nextCorrelationId();
        std::vector<uint8_t> b1(m1.encodedSize()), b2(m2.encodedSize());
        ASSERT_TRUE(r.c().send(r.ch(), r.stream(), b1.data(), m1.encode(b1.data())));
        ASSERT_TRUE(r.c().send(r.ch(), r.stream(), b2.data(), m2.encode(b2.data())));
        const auto hidden = r.rawAnswer(m1.correlationId), missing = r.rawAnswer(m2.correlationId);
        ASSERT_FALSE(hidden.empty());
        EXPECT_EQ(body(hidden), body(missing)) << "Produce";
    }
    // GetTopicSchema by name ("secret" and "ghosts" have the same length).
    {
        const auto hidden  = r.rawAnswer(r.c().sendGetTopicSchema(r.ch(), r.stream(), u, "secret"));
        const auto missing = r.rawAnswer(r.c().sendGetTopicSchema(r.ch(), r.stream(), u, "ghosts"));
        ASSERT_FALSE(hidden.empty());
        EXPECT_EQ(body(hidden), body(missing)) << "GetTopicSchema";
        EXPECT_EQ(pc::TopicSchemaResponse::decode(hidden.data(), hidden.size())->errorCode, kNotAuthorized);
    }
    // DropTopic by name: the DdlResponse's detail is built from the request alone.
    {
        const auto hidden  = r.rawAnswer(r.c().sendDropTopic(r.ch(), r.stream(), u, "secret"));
        const auto missing = r.rawAnswer(r.c().sendDropTopic(r.ch(), r.stream(), u, "ghosts"));
        ASSERT_FALSE(hidden.empty());
        EXPECT_EQ(body(hidden, "secret"), body(missing, "ghosts")) << "DropTopic";
        const auto d = pc::DdlResponse::decode(hidden.data(), hidden.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->errorCode, kNotAuthorized);
        EXPECT_EQ(std::string(d->detail), "not authorized: DROP on TOPIC secret");
    }
    // Open a view by id (GetViewRow).
    {
        const auto hidden  = r.rawAnswer(r.c().sendGetViewRow(r.ch(), r.stream(), u, r.board, 1));
        const auto missing = r.rawAnswer(r.c().sendGetViewRow(r.ch(), r.stream(), u, kMissing, 1));
        ASSERT_FALSE(hidden.empty());
        EXPECT_EQ(body(hidden), body(missing)) << "GetViewRow";
        EXPECT_EQ(pc::ViewRowResponse::decode(hidden.data(), hidden.size())->status, kNotAuthorized);
    }
}

TEST(AuthVisibilityRegression, MetadataListsOnlyDescribableTopics) {
    AuthRig r;
    ASSERT_TRUE(r.awaitListed(r.orders));
    ASSERT_TRUE(r.awaitListed(r.secret));
    const int64_t reader = r.newUser({exact(SO::Read, ST::Topic, "orders")});    // READ implies DESCRIBE (D15)
    const int64_t describer = r.newUser({exact(SO::Describe, ST::Topic, "secret")});
    const int64_t none = r.newUser({});
    const int64_t cluster_ = r.newUser({cluster(SO::Describe)});

    const auto readerSees = r.metadataTopics(reader);
    EXPECT_NE(std::find(readerSees.begin(), readerSees.end(), r.orders), readerSees.end());
    EXPECT_EQ(std::find(readerSees.begin(), readerSees.end(), r.secret), readerSees.end());
    const auto describerSees = r.metadataTopics(describer);
    EXPECT_NE(std::find(describerSees.begin(), describerSees.end(), r.secret), describerSees.end());
    EXPECT_EQ(std::find(describerSees.begin(), describerSees.end(), r.orders), describerSees.end());
    EXPECT_TRUE(r.metadataTopics(none).empty());
    const auto all = r.metadataTopics(cluster_);
    EXPECT_NE(std::find(all.begin(), all.end(), r.orders), all.end());
    EXPECT_NE(std::find(all.begin(), all.end(), r.secret), all.end());
}

// #126: a fetch-all lists only the committed offsets of topics the caller may READ (a listed entry already needs READ).
namespace {
std::vector<int32_t> fetchAllTopics(AuthRig& r, int64_t sid, const std::string& group, bool* ok = nullptr) {
    pc::OffsetFetch m;
    m.sessionId = sid; m.correlationId = r.c().nextCorrelationId();
    m.groupId = {reinterpret_cast<const uint8_t*>(group.data()), group.size()};
    std::vector<uint8_t> buf(512);
    const std::size_t len = m.encode(buf.data(), std::span<const pc::OffsetFetchEntry>{});
    std::vector<int32_t> ids;
    if (ok) *ok = false;
    if (len == 0 || !r.c().send(r.ch(), r.stream(), buf.data(), len)) return ids;
    const auto a = r.rawAnswer(m.correlationId);
    const auto resp = a.empty() ? std::nullopt : pc::OffsetFetchResponse::decode(a.data(), a.size());
    if (!resp) return ids;
    if (ok) *ok = true;
    for (const auto res : resp->results) {
        EXPECT_EQ(res.errorCode, 0) << "topic " << res.topicId;
        ids.push_back(res.topicId);
    }
    return ids;
}
}  // namespace

TEST(AuthVisibilityRegression, OffsetFetchAllHidesTopicsTheCallerCannotReadRegression) {
    AuthRig r;
    const int64_t a = r.newUser({exact(SO::Read, ST::Group, "g126"), exact(SO::Read, ST::Topic, "orders")});
    const int64_t b = r.newUser({exact(SO::Read, ST::Group, "g126"), exact(SO::Read, ST::Topic, "orders"),
                                 exact(SO::Read, ST::Topic, "secret")});
    const int64_t all = r.newUser({exact(SO::Read, ST::Group, "g126"), prefix(SO::Read, ST::Topic, "")});
    const auto [member, gen] = r.joinStable(b, "g126", {r.orders, r.secret});
    ASSERT_FALSE(member.empty());
    ASSERT_EQ(r.commitAs(b, "g126", member, gen, r.orders, 11), 0);
    ASSERT_EQ(r.commitAs(b, "g126", member, gen, r.secret, 22), 0);

    bool ok = false;
    auto seenByB = fetchAllTopics(r, b, "g126", &ok);
    ASSERT_TRUE(ok);
    std::sort(seenByB.begin(), seenByB.end());
    EXPECT_EQ(seenByB, (std::vector<int32_t>{std::min(r.orders, r.secret), std::max(r.orders, r.secret)}));

    const auto seenByA = fetchAllTopics(r, a, "g126", &ok);
    ASSERT_TRUE(ok);
    EXPECT_EQ(seenByA, std::vector<int32_t>{r.orders}) << "a topic the caller cannot READ was listed";

    auto seenByAll = fetchAllTopics(r, all, "g126", &ok);
    ASSERT_TRUE(ok);
    EXPECT_EQ(seenByAll.size(), 2u);
}

// ── Adversarial review of PR #497 (#126) ─────────────────────────────────────────────────────────────────────────
// A caller with the group but no READ on any committed topic - including a DESCRIBE-ON-CLUSTER holder (the D22
// pass-through covers only ids the gateway has not indexed) - gets an empty fetch-all, and a listed fetch of the
// hidden topic is still refused; a grant revoked mid-session hides the topic on the next fetch-all.
TEST(AuthVisibilityRegression, OffsetFetchAllHidesEveryUnreadableTopicAndFollowsARevokeAdversarialRegression) {
    AuthRig r;
    std::string revokedName;
    const int64_t writer = r.newUser({exact(SO::Read, ST::Group, "g126x"), exact(SO::Read, ST::Topic, "orders"),
                                      exact(SO::Read, ST::Topic, "secret")}, &revokedName);
    const int64_t groupOnly = r.newUser({exact(SO::Read, ST::Group, "g126x")});
    const int64_t describer = r.newUser({exact(SO::Read, ST::Group, "g126x"), cluster(SO::Describe)});
    const auto [member, gen] = r.joinStable(writer, "g126x", {r.orders, r.secret});
    ASSERT_FALSE(member.empty());
    ASSERT_EQ(r.commitAs(writer, "g126x", member, gen, r.orders, 11), 0);
    ASSERT_EQ(r.commitAs(writer, "g126x", member, gen, r.secret, 22), 0);

    bool ok = false;
    for (const int64_t sid : {groupOnly, describer}) {
        SCOPED_TRACE(::testing::Message() << "session " << sid);
        EXPECT_TRUE(fetchAllTopics(r, sid, "g126x", &ok).empty()) << "an unreadable topic's offset was listed";
        EXPECT_TRUE(ok) << "the filtered fetch-all was not answered";
        EXPECT_EQ(r.offsetFetch(sid, "g126x", r.secret), kNotAuthorized);
    }
    // The writer can see both, then loses READ on "secret" while its session stays open.
    auto both = fetchAllTopics(r, writer, "g126x", &ok);
    ASSERT_TRUE(ok);
    EXPECT_EQ(both.size(), 2u);
    const auto rv = r.broker().admin().revoke(r.ch(), r.broker().adminSession(), revokedName, false, SO::Read,
                                              ST::Topic, SP::Exact, "secret");
    ASSERT_EQ(rv.errorCode, 0) << rv.detail;
    const auto after = fetchAllTopics(r, writer, "g126x", &ok);
    ASSERT_TRUE(ok);
    EXPECT_EQ(after, std::vector<int32_t>{r.orders}) << "a revoked topic stayed visible to an open session";
}
