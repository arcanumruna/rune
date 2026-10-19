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
