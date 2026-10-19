// CatalogPrincipalTest.cpp - authentication Task 12 (fix-on-sight): the catalog event's principal.authenticated was
// always empty; it is now the session's user, and a security event records the user who ran it.
#include "support/AuthRig.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/security/SecurityEvent.h>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;

namespace {
/// Every event of a replica's log, in offset order (one batch per read; `read(offset)` runs on the admin thread).
template <class Event, class Read, class Decode>
std::vector<Event> readAll(Read read, Decode decode) {
    std::vector<Event> out;
    int64_t offset = 0;
    for (;;) {
        const std::vector<uint8_t> data = read(offset);
        if (data.empty()) break;
        rune::protocol::RecordBatchReader reader(data.data(), data.size());
        if (!reader.ensureValid()) break;
        int64_t last = -1;
        reader.forEachRecord([&](std::string_view, std::string_view v, int64_t off, int64_t) {
            last = off;
            auto d = decode(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()));
            if (d) out.push_back(std::move(*d));
        });
        if (last < offset) break;
        offset = last + 1;
    }
    return out;
}
}  // namespace

TEST(CatalogPrincipalRegression, DdlEventsRecordTheAuthenticatedUserRegression) {
    AuthRig r;
    std::string ws1;
    const int64_t sid = r.newUser({exact(SO::Create, ST::Topic, "pt1")}, &ws1);
    ASSERT_EQ(r.defineTopicCode(sid, "pt1"), 0);

    BrokerNode& n = r.broker().node();
    const auto events = readAll<rune::catalog::CatalogEvent>(
        [&n](int64_t off) { return n.onAdminThreadForTest([&n, off] { return n.catalogReplicaForTest()->read(off, 1 << 20); }); },
        [](std::span<const uint8_t> b) {
            auto d = rune::catalog::decode(b);
            return d ? std::optional(std::move(*d)) : std::nullopt;
        });
    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().principal.authenticated, ws1) << "the last catalog event is the DefineTopic by the user";
    // The builder's own DDL (made during the rig's setup) records "builder".
    bool sawBuilder = false;
    for (const auto& e : events) sawBuilder = sawBuilder || e.principal.authenticated == "builder";
    EXPECT_TRUE(sawBuilder);
}

TEST(CatalogPrincipalRegression, SecurityEventsRecordTheUserWhoRanThemRegression) {
    AuthRig r;
    std::string self;
    const int64_t sid = r.newUser({}, &self);
    ASSERT_EQ(r.wire().alterPassword(r.ch(), sid, self, pc::UserChangeKind::AddPassword, "second-password-123").errorCode, 0u);

    BrokerNode& n = r.broker().node();
    const auto events = readAll<rune::security::SecurityEvent>(
        [&n](int64_t off) { return n.onAdminThreadForTest([&n, off] { return n.securityReplicaForTest()->read(off, 1 << 20); }); },
        [](std::span<const uint8_t> b) {
            auto d = rune::security::decode(b);
            return d ? std::optional(std::move(*d)) : std::nullopt;
        });
    ASSERT_GE(events.size(), 3u);
    EXPECT_EQ(events.front().principal.authenticated, "bootstrap");
    EXPECT_EQ(events.back().principal.authenticated, self) << "a self password change is recorded as the user";
    bool sawAdmin = false;
    for (const auto& e : events) sawAdmin = sawAdmin || e.principal.authenticated == "admin";
    EXPECT_TRUE(sawAdmin) << "CREATE USER by admin records admin";
}
