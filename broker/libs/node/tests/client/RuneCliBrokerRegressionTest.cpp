// RuneCliBrokerRegressionTest.cpp — catalog history Task 15 (F7): the REAL `rune` CLI
// (rune::cli::run, which builds its own Client and blocks) against REAL in-process
// BrokerNodes (OneBroker over Aeron UDP).  `rune ql` (DDL + reads, -f scripts, --request-id)
// and `rune catalog export | replay | diff`.  Every wait is bounded by the CLI's own client
// timeouts; every expected value is read back from the broker's own __catalog.
#include "client/OneBroker.h"
#include "support/CatalogBrokerCluster.h"

#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogQueries.h>
#include <rune/cli/CatalogCommands.h>
#include <rune/cli/Cli.h>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using rune::node::test::OneBroker;
namespace rcat = rune::catalog;
namespace fs   = std::filesystem;

namespace {

fs::path tmpDir() {
    static const fs::path d = [] {
        fs::path p = fs::temp_directory_path() / ("rune_cli_broker_test_" + std::to_string(::getpid()));
        fs::create_directories(p);
        return p;
    }();
    return d;
}

std::string writeTemp(const std::string& name, const std::string& content) {
    const fs::path p = tmpDir() / name;
    std::ofstream o(p, std::ios::binary);
    o << content;
    return p.string();
}

int runCli(std::vector<std::string> args, std::string* out = nullptr, std::string* err = nullptr) {
    std::stringstream o, e;
    const int rc = rune::cli::run(std::move(args), o, e);
    if (out) *out = o.str();
    if (err) *err = e.str();
    return rc;
}

bool hasEventNamed(OneBroker& b, const std::string& name) {
    for (const auto& r : b.catalogEvents())
        if (r.event.objectName == name) return true;
    return false;
}

constexpr const char* kUnreachable = "127.0.0.1:1";

} // namespace

// ── export ──────────────────────────────────────────────────────────────────

TEST(RuneCliBrokerRegression, ExportWritesEveryEventAndUpToStops) {
    OneBroker b("cli_export");
    b.runEveryKind();   // all 9 kinds through the real client (OneBroker helper)
    std::stringstream out, err;
    ASSERT_EQ(rune::cli::run({"catalog", "export", "--up-to", "5", "--broker", b.endpoint()}, out, err), 0) << err.str();
    auto recs = rune::catalog::readRcat(out);
    ASSERT_TRUE(recs.has_value());
    EXPECT_EQ(recs->size(), 6u);
}

TEST(RuneCliBrokerRegression, ExportIsAValidV2RcatWithPerRecordCrcAndBeyondTheEndExportsToTheEnd) {
    OneBroker b("cli_export2");
    b.runEveryKind();
    const auto log = b.catalogEvents();
    ASSERT_EQ(log.size(), 10u);
    std::string out, err;
    ASSERT_EQ(runCli({"catalog", "export", "--up-to", "99999", "--broker", b.endpoint()}, &out, &err), 0) << err;
    ASSERT_GE(out.size(), 6u);
    EXPECT_EQ(out.substr(0, 4), "RCAT");
    EXPECT_EQ(static_cast<uint8_t>(out[4]), 2) << "file version 2 (little-endian u16)";
    EXPECT_EQ(static_cast<uint8_t>(out[5]), 0);
    std::istringstream in(out);
    auto recs = rcat::readRcat(in);   // verifies every record's CRC32C
    ASSERT_TRUE(recs.has_value()) << recs.error();
    ASSERT_EQ(recs->size(), log.size());
    for (std::size_t i = 0; i < log.size(); ++i) {
        EXPECT_EQ((*recs)[i].first, log[i].offset);
        auto ev = rcat::decode(std::span<const uint8_t>((*recs)[i].second.data(), (*recs)[i].second.size()));
        ASSERT_TRUE(ev.has_value());
        EXPECT_EQ(*ev, log[i].event) << "record " << i << " is the event the broker wrote";
    }
    // no --up-to: the same file; --up-to 0: exactly the first event
    std::string all, one;
    ASSERT_EQ(runCli({"catalog", "export", "--broker", b.endpoint()}, &all, &err), 0) << err;
    EXPECT_EQ(all, out);
    ASSERT_EQ(runCli({"catalog", "export", "--up-to", "0", "--broker", b.endpoint()}, &one, &err), 0) << err;
    std::istringstream oneIn(one);
    ASSERT_EQ(rcat::readRcat(oneIn)->size(), 1u);
}

TEST(RuneCliBrokerRegression, ExportOfAnEmptyCatalogIsAValidHeaderOnlyFile) {
    OneBroker b("cli_export_empty");
    std::string out, err;
    ASSERT_EQ(runCli({"catalog", "export", "--broker", b.endpoint()}, &out, &err), 0) << err;
    std::istringstream in(out);
    auto recs = rcat::readRcat(in);
    ASSERT_TRUE(recs.has_value());
    EXPECT_TRUE(recs->empty());
}

TEST(RuneCliBrokerRegression, UnreachableBrokerIsAnExplicitErrorAndExportClaimsNoFile) {
    std::string out, err;
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(runCli({"catalog", "export", "--broker", kUnreachable}, &out, &err), 1);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 60s) << "bounded by the client's connect timeout";
    EXPECT_FALSE(err.empty()) << "an explicit message";
    EXPECT_TRUE(out.empty()) << "no partial file is claimed as complete: " << out.size() << " bytes written";
    EXPECT_EQ(runCli({"ql", "-e", "topic t partitions 1;", "--broker", kUnreachable}, &out, &err), 1);
    EXPECT_FALSE(err.empty());
    EXPECT_EQ(runCli({"catalog", "diff", "--from", kUnreachable, "--to", kUnreachable}, &out, &err), 1);
    EXPECT_FALSE(err.empty());
}

// ── replay ──────────────────────────────────────────────────────────────────

TEST(RuneCliBrokerRegression, ReplayReissuesEveryKindAndVerifiesFingerprints) {
    OneBroker src("cli_src"), dst("cli_dst");
    src.runEveryKind();
    std::stringstream out, err;
    ASSERT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint(),
                              "--cluster-id", "src"}, out, err), 0) << err.str();
    const auto s = src.catalogEvents(), d = dst.catalogEvents();
    ASSERT_EQ(d.size(), s.size());
    for (std::size_t i = 0; i < d.size(); ++i) {
        ASSERT_TRUE(d[i].event.replayedFrom.has_value());
        EXPECT_EQ(d[i].event.replayedFrom->clusterId, "src");
        EXPECT_EQ(d[i].event.fingerprint, s[i].event.fingerprint) << "offset " << i;
    }
    std::stringstream o2, e2;
    EXPECT_EQ(rune::cli::run({"catalog", "diff", "--from", src.endpoint(), "--to", dst.endpoint()}, o2, e2), 0) << o2.str();
    std::stringstream h, e3;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "show history;", "--broker", dst.endpoint()}, h, e3), 0);
    EXPECT_NE(h.str().find("src@0"), std::string::npos) << "F18: replayedFrom in the history rows";
}

TEST(RuneCliBrokerRegression, ReplayAbortsLoudlyAtTheFirstFailingEvent) {
    OneBroker src("cli_src2"), dst("cli_dst2");
    src.runEveryKind();
    ASSERT_EQ(dst.connectClient()->defineTopicAndWait(src.catalogEvents()[1].event.objectName, 1, "", {}, false,
                                                     rune::client::Persistence::Disk, {}, {}, 10s).errorCode, 0);
    const std::size_t before = dst.catalogEvents().size();
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint()}, out, err), 1);
    EXPECT_NE(err.str().find("replay aborted at source offset 1"), std::string::npos) << err.str();
    EXPECT_NE(err.str().find("TopicExists (25)"), std::string::npos) << "the broker's code, named: " << err.str();
    EXPECT_EQ(dst.catalogEvents().size(), before + 1) << "offset 0 replayed, nothing issued past the failure";
}

TEST(RuneCliBrokerRegression, ReplayIsIdempotentAfterACrash) {
    OneBroker src("cli_src3"), dst("cli_dst3");
    src.runEveryKind();
    std::stringstream o, e;
    ASSERT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint(),
                              "--up-to", "4"}, o, e), 0) << e.str();
    ASSERT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint()}, o, e), 0) << e.str();
    EXPECT_EQ(dst.catalogEvents().size(), src.catalogEvents().size()) << "same request ids: the first 5 are deduplicated";
}

TEST(RuneCliBrokerRegression, ReplayingTheSameSourceTwiceReturnsTheOriginalResultsAndAddsNothing) {
    OneBroker src("cli_src4"), dst("cli_dst4");
    src.runEveryKind();
    std::string o1, e1, o2, e2;
    ASSERT_EQ(runCli({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint(), "--cluster-id", "c"}, &o1, &e1), 0)
        << e1;
    const auto first = dst.catalogEvents();
    ASSERT_EQ(first.size(), 10u);
    ASSERT_EQ(runCli({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint(), "--cluster-id", "c"}, &o2, &e2), 0)
        << "the second run is a success, not a conflict: " << e2;
    const auto second = dst.catalogEvents();
    ASSERT_EQ(second.size(), first.size()) << "nothing was applied twice";
    for (std::size_t i = 0; i < first.size(); ++i) EXPECT_EQ(second[i].event, first[i].event) << i;
    EXPECT_NE(o2.find("0 issued"), std::string::npos) << o2;
    EXPECT_NE(o2.find("10 already applied"), std::string::npos) << o2;
}

TEST(RuneCliBrokerRegression, ReplayUpToStopsExactlyAfterThatOffset) {
    OneBroker src("cli_src5"), dst("cli_dst5");
    src.runEveryKind();
    std::string out, err;
    ASSERT_EQ(runCli({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint(), "--up-to", "3"}, &out, &err), 0)
        << err;
    const auto d = dst.catalogEvents();
    ASSERT_EQ(d.size(), 4u) << "offsets 0..3 and not one more";
    EXPECT_EQ(d.back().event.replayedFrom->offset, 3);
    // --up-to beyond the end replays everything that is there
    OneBroker dst2("cli_dst5b");
    ASSERT_EQ(runCli({"catalog", "replay", "--from", src.endpoint(), "--into", dst2.endpoint(), "--up-to", "100000"}, &out, &err), 0)
        << err;
    EXPECT_EQ(dst2.catalogEvents().size(), src.catalogEvents().size());
    // --up-to 0: exactly one
    OneBroker dst3("cli_dst5c");
    ASSERT_EQ(runCli({"catalog", "replay", "--from", src.endpoint(), "--into", dst3.endpoint(), "--up-to", "0"}, &out, &err), 0)
        << err;
    EXPECT_EQ(dst3.catalogEvents().size(), 1u);
}

TEST(RuneCliBrokerRegression, ReplayClusterIdDefaultsToTheFromArgumentVerbatim) {
    OneBroker src("cli_src6"), dst("cli_dst6");
    src.runEveryKind();
    std::string out, err;
    ASSERT_EQ(runCli({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint(), "--up-to", "1"}, &out, &err), 0)
        << err;
    const auto d = dst.catalogEvents();
    ASSERT_EQ(d.size(), 2u);
    EXPECT_EQ(d[0].event.replayedFrom->clusterId, src.endpoint());
    EXPECT_EQ(d[1].event.replayedFrom->clusterId, src.endpoint());
    EXPECT_EQ(d[0].event.principal.requestId, rune::cli::replayRequestId(src.endpoint(), 0));
    EXPECT_EQ(d[1].event.principal.requestId, rune::cli::replayRequestId(src.endpoint(), 1));
}

TEST(RuneCliBrokerRegression, ReplayRemapsViewSourcesOntoTheTargetsOwnTopicIds) {
    OneBroker src("cli_src7"), dst("cli_dst7");
    src.runEveryKind();
    // The target already holds another topic, so every topic id there is shifted against the source's.
    ASSERT_EQ(dst.connectClient()->defineTopicAndWait("zzz", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s)
                  .errorCode, 0);
    std::string out, err;
    ASSERT_EQ(runCli({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint()}, &out, &err), 0) << err;
    const auto s = src.catalogEvents(), d = dst.catalogEvents();
    ASSERT_EQ(d.size(), s.size() + 1);
    for (std::size_t i = 0; i < s.size(); ++i)
        EXPECT_EQ(d[i + 1].event.fingerprint, s[i].event.fingerprint) << "source offset " << i;
    const auto& srcView = std::get<rcat::CreateViewOps>(s[6].event.ops).view;
    const auto& dstView = std::get<rcat::CreateViewOps>(d[7].event.ops).view;
    EXPECT_NE(srcView.viewTopicId, dstView.viewTopicId) << "ids differ across clusters; fingerprints do not cover them";
    EXPECT_EQ(srcView.sourceNames, dstView.sourceNames);
    // the target's final state: its own ids (zzz = 0), the replayed topic under a shifted id
    rcat::CatalogReplayer sr, dr;
    for (const auto& e : s) ASSERT_TRUE(sr.feedDecoded(e.offset, e.event).has_value());
    for (const auto& e : d) ASSERT_TRUE(dr.feedDecoded(e.offset, e.event).has_value()) << "the target's log verifies";
    EXPECT_EQ(dr.state().topicByName(rune::catalog::kDefaultNsId, "zzz")->topicId, 0);
    EXPECT_EQ(dr.state().topicByName(rune::catalog::kDefaultNsId, "events")->topicId, sr.state().topicByName(rune::catalog::kDefaultNsId, "events")->topicId + 1);
}

TEST(RuneCliBrokerRegression, ReplayFromAnExportedFileAndATamperedFileIssuesNothing) {
    OneBroker src("cli_src8"), dst("cli_dst8"), dst2("cli_dst8b");
    src.runEveryKind();
    std::string out, err;
    ASSERT_EQ(runCli({"catalog", "export", "--broker", src.endpoint()}, &out, &err), 0) << err;
    const std::string file = writeTemp("exported.rcat", out);
    ASSERT_EQ(runCli({"catalog", "replay", "--from", file, "--into", dst.endpoint()}, &out, &err), 0) << err;
    EXPECT_EQ(dst.catalogEvents().size(), src.catalogEvents().size());
    EXPECT_EQ(dst.catalogEvents()[0].event.replayedFrom->clusterId, file) << "--cluster-id defaults to the --from argument verbatim";
    EXPECT_EQ(runCli({"catalog", "diff", "--from", file, "--to", dst.endpoint()}, &out, &err), 0)
        << "a file against a cluster: " << out;
    EXPECT_TRUE(out.empty());

    // A source whose event was altered after its fingerprint was stamped: verified BEFORE anything is issued.
    auto log = src.catalogEvents();
    std::get<rcat::CreateTopicOps>(log[1].event.ops).partitions += 1;
    std::ostringstream tampered(std::ios::binary);
    rcat::writeRcatHeader(tampered);
    for (const auto& r : log) {
        const auto bytes = rcat::encode(r.event);
        rcat::writeRcatRecord(tampered, r.offset, std::span<const uint8_t>(bytes->data(), bytes->size()));
    }
    const std::string bad = writeTemp("tampered.rcat", tampered.str());
    EXPECT_EQ(runCli({"catalog", "replay", "--from", bad, "--into", dst2.endpoint()}, &out, &err), 1);
    EXPECT_NE(err.find("offset 1"), std::string::npos) << err;
    EXPECT_TRUE(dst2.catalogEvents().empty()) << "not even offset 0 was issued";
}

// ── ql ──────────────────────────────────────────────────────────────────────

TEST(RuneCliBrokerRegression, QlExecutesDdlWithItsStatementAndPrintsHistory) {
    OneBroker b("cli_ql");
    std::stringstream out, err;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "topic events partitions 2 replicas 1;", "--broker", b.endpoint()}, out, err), 0)
        << err.str();
    EXPECT_EQ(b.catalogEvents().back().event.statement, "topic events partitions 2 replicas 1;");
    std::stringstream h;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "show history topic events;", "--broker", b.endpoint()}, h, err), 0);
    EXPECT_NE(h.str().find("CreateTopic"), std::string::npos);
}

TEST(RuneCliBrokerRegression, QlFileRunsEveryStatementAndStopsAtTheFirstErrorRegression) {
    // F8: e2e.rql-style scripts create every topic, in order; a failure stops the script, named.
    OneBroker b("cli_script");
    const auto f = writeTemp("s.rql", "topic a partitions 1;\ntopic b partitions 3;\ntopic a partitions 1;\ntopic c partitions 1;\n");
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "-f", f, "--broker", b.endpoint()}, out, err), 1);
    EXPECT_NE(err.str().find("statement 3"), std::string::npos) << err.str();
    EXPECT_EQ(b.catalogEvents().size(), 2u) << "a and b created; c never issued";
}

TEST(RuneCliBrokerRegression, QlFileStatementNPlusOneIsNeverIssuedAndErrorsAreExact) {
    OneBroker b("cli_script2");
    const auto f = writeTemp("s2.rql", "topic a partitions 1;\ntopic a partitions 1;\ntopic never partitions 1;\n");
    std::string out, err;
    EXPECT_EQ(runCli({"ql", "-f", f, "--broker", b.endpoint()}, &out, &err), 1);
    EXPECT_EQ(err.rfind("statement 2: TopicExists (25)", 0), 0u) << "'statement N: <error>' exactly: " << err;
    EXPECT_FALSE(hasEventNamed(b, "never"));
    EXPECT_EQ(b.catalogEvents().size(), 1u);

    // a broker error in statement 1: nothing after it
    OneBroker b2("cli_script3");
    const auto g = writeTemp("s3.rql", "drop topic ghost;\ntopic after partitions 1;\n");
    EXPECT_EQ(runCli({"ql", "-f", g, "--broker", b2.endpoint()}, &out, &err), 1);
    EXPECT_EQ(err.rfind("statement 1: UnknownTopic (7)", 0), 0u) << err;
    EXPECT_TRUE(b2.catalogEvents().empty());

    // a parse error is named by its index too, and nothing at all is issued (every statement is parsed first)
    const auto h = writeTemp("s4.rql", "topic p partitions 1;\ntopic q partitions;\n");
    EXPECT_EQ(runCli({"ql", "-f", h, "--broker", b2.endpoint()}, &out, &err), 1);
    EXPECT_EQ(err.rfind("statement 2: ", 0), 0u) << err;
    EXPECT_TRUE(b2.catalogEvents().empty());
}

TEST(RuneCliBrokerRegression, QlFileRecordsEachStatementsOwnTextAndHandlesCommentsAndStrings) {
    OneBroker b("cli_script4");
    const auto f = writeTemp("s5.rql",
                             "-- create the topics\n"
                             "topic a partitions 1;   -- trailing comment\n"
                             "\n"
                             "topic b\n  partitions 2\n  with (copartition.group = 'g;1');\n"
                             "alter topic a set retention.ms = 60000;\n");
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "-f", f, "--broker", b.endpoint()}, &out, &err), 0) << err;
    const auto log = b.catalogEvents();
    ASSERT_EQ(log.size(), 3u);
    EXPECT_EQ(log[0].event.statement, "topic a partitions 1;");
    EXPECT_EQ(log[1].event.statement, "topic b\n  partitions 2\n  with (copartition.group = 'g;1');");
    EXPECT_EQ(log[2].event.statement, "alter topic a set retention.ms = 60000;");
    EXPECT_EQ(std::get<rcat::AlterTopicConfigOps>(log[2].event.ops).changes.size(), 1u);
}

TEST(RuneCliBrokerRegression, QlFileReadsStdin) {
    OneBroker b("cli_stdin");
    std::istringstream in("topic fromstdin partitions 1;\n");
    std::streambuf* old = std::cin.rdbuf(in.rdbuf());
    std::string out, err;
    const int rc = runCli({"ql", "-f", "-", "--broker", b.endpoint()}, &out, &err);
    std::cin.rdbuf(old);
    ASSERT_EQ(rc, 0) << err;
    EXPECT_TRUE(hasEventNamed(b, "fromstdin"));
}

TEST(RuneCliBrokerRegression, QlEmptyFileIssuesNothing) {
    OneBroker b("cli_empty");
    const auto f = writeTemp("empty.rql", "  \n-- only a comment\n");
    std::string out, err;
    EXPECT_EQ(runCli({"ql", "-f", f, "--broker", b.endpoint()}, &out, &err), 0) << err;
    EXPECT_TRUE(b.catalogEvents().empty());
}

TEST(RuneCliBrokerRegression, QlRequestIdMakesARerunReturnTheOriginalResultRegression) {
    // F17: a re-run of a DDL whose first attempt may have committed returns its original success.
    OneBroker b("cli_rid");
    const std::string rid = "00000000000000c1000000000000002a";
    std::stringstream o1, e1, o2, e2;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "topic midkill partitions 2;", "--request-id", rid, "--broker", b.endpoint()},
                             o1, e1), 0) << e1.str();
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic midkill partitions 2;", "--request-id", rid, "--broker", b.endpoint()},
                             o2, e2), 0) << "not TopicExists: " << e2.str();
    EXPECT_EQ(b.catalogEvents().size(), 1u);
    std::stringstream o3, e3;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic other partitions 2;", "--request-id", rid, "--broker", b.endpoint()},
                             o3, e3), 1);
    EXPECT_NE(e3.str().find("requestId reused for a different statement"), std::string::npos) << e3.str();
}

TEST(RuneCliBrokerRegression, QlRequestIdWithAScriptGivesStatementNTheIdLoPlusNMinusOne) {
    OneBroker b("cli_rid2");
    const auto f = writeTemp("rid.rql", "topic r1 partitions 1;\ntopic r2 partitions 1;\ntopic r3 partitions 1;\n");
    const std::string rid = "00000000000000d1ffffffffffffff00";
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "-f", f, "--request-id", rid, "--broker", b.endpoint()}, &out, &err), 0) << err;
    auto log = b.catalogEvents();
    ASSERT_EQ(log.size(), 3u);
    for (std::size_t i = 0; i < 3; ++i)
        EXPECT_EQ(log[i].event.principal.requestId, (rcat::RequestId{0xd1, 0xffffffffffffff00ull + i})) << i;
    // the whole script again with the same ids: every statement returns its original result, nothing is added
    ASSERT_EQ(runCli({"ql", "-f", f, "--request-id", rid, "--broker", b.endpoint()}, &out, &err), 0) << err;
    EXPECT_EQ(b.catalogEvents().size(), 3u);
    // lo overflow: refused before anything is issued
    OneBroker b2("cli_rid3");
    EXPECT_EQ(runCli({"ql", "-f", f, "--request-id", "00000000000000d1fffffffffffffffe", "--broker", b2.endpoint()}, &out, &err), 1);
    EXPECT_NE(err.find("overflow"), std::string::npos) << err;
    EXPECT_TRUE(b2.catalogEvents().empty());
}

TEST(RuneCliBrokerRegression, QlReadsShowHistoryAsOfAndDiff) {
    OneBroker b("cli_reads");
    b.runEveryKind();
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "-e", "show history;", "--broker", b.endpoint()}, &out, &err), 0) << err;
    for (const char* kind : {"CreateTopic", "CreateSchema", "AlterSchema", "SetCompatibility", "AlterTopicConfig", "CreateView",
                             "AlterView", "DropView", "DropTopic"})
        EXPECT_NE(out.find(kind), std::string::npos) << kind << "\n" << out;
    EXPECT_NE(out.find("offset"), std::string::npos) << "a header row";
    EXPECT_NE(out.find("topic players { id int64, hp int64 } partitions 1 key id;"), std::string::npos) << "the recorded statement";
    ASSERT_EQ(runCli({"ql", "-e", "show history schema events;", "--broker", b.endpoint()}, &out, &err), 0) << err;
    EXPECT_NE(out.find("CreateSchema"), std::string::npos);
    EXPECT_EQ(out.find("DropView"), std::string::npos);

    ASSERT_EQ(runCli({"ql", "-e", "show topic players as of offset 0;", "--broker", b.endpoint()}, &out, &err), 0) << err;
    EXPECT_NE(out.find("topic players"), std::string::npos) << out;
    ASSERT_EQ(runCli({"ql", "-e", "show schema events as of version 2;", "--broker", b.endpoint()}, &out, &err), 0) << err;
    EXPECT_NE(out.find("schema events version 2"), std::string::npos) << out;
    EXPECT_NE(out.find("mana"), std::string::npos) << "the ADD is in version 2: " << out;
    ASSERT_EQ(runCli({"ql", "-e", "show view v as of offset 6;", "--broker", b.endpoint()}, &out, &err), 0) << err;
    EXPECT_NE(out.find("view v"), std::string::npos) << out;
    EXPECT_NE(out.find("sources players"), std::string::npos) << out;
    // players was dropped at offset 9
    EXPECT_EQ(runCli({"ql", "-e", "show topic players as of offset 9;", "--broker", b.endpoint()}, &out, &err), 1);
    EXPECT_NE(err.find("no topic 'players'"), std::string::npos) << err;
    EXPECT_EQ(runCli({"ql", "-e", "show schema events as of version 9;", "--broker", b.endpoint()}, &out, &err), 1) << "no such version";
    EXPECT_FALSE(err.empty());

    ASSERT_EQ(runCli({"ql", "-e", "diff catalog offset 0 to 9;", "--broker", b.endpoint()}, &out, &err), 0) << err;
    EXPECT_NE(out.find("+ topic events"), std::string::npos) << out;
    EXPECT_NE(out.find("- topic players"), std::string::npos) << out;
    ASSERT_EQ(runCli({"ql", "-e", "diff schema events version 1 to 2;", "--broker", b.endpoint()}, &out, &err), 0) << err;
    EXPECT_NE(out.find("~ topic events:"), std::string::npos) << out;
    EXPECT_EQ(out.find("players"), std::string::npos) << "restricted to the schema's topic";

    // versions above 65535 are refused explicitly, before any broker contact
    EXPECT_EQ(runCli({"ql", "-e", "show schema events as of version 65536;", "--broker", b.endpoint()}, &out, &err), 1);
    EXPECT_NE(err.find("65536"), std::string::npos) << err;
    EXPECT_EQ(runCli({"ql", "-e", "diff schema events version 1 to 99999;", "--broker", b.endpoint()}, &out, &err), 1);
}

TEST(RuneCliBrokerRegression, QlVerbosePrintsNothingExtraWhenNothingRedirectsOrRetries) {
    OneBroker b("cli_verbose");
    std::string out, err, out2, err2;
    ASSERT_EQ(runCli({"ql", "-e", "topic v1 partitions 1;", "--verbose", "--broker", b.endpoint()}, &out, &err), 0) << err;
    ASSERT_EQ(runCli({"ql", "-e", "topic v2 partitions 1;", "--broker", b.endpoint()}, &out2, &err2), 0) << err2;
    EXPECT_TRUE(err.empty()) << "a leader answers directly: no Redirect, no NotLeader: " << err;
    EXPECT_TRUE(err2.empty()) << err2;
    // an explicit failure is reported once, with its code name
    EXPECT_EQ(runCli({"ql", "-e", "topic v1 partitions 1;", "--verbose", "--broker", b.endpoint()}, &out, &err), 1);
    EXPECT_EQ(err.rfind("rune: TopicExists (25)", 0), 0u) << err;
}

TEST(RuneCliBrokerRegression, QlDdlOverEveryStatementKind) {
    OneBroker b("cli_kinds");
    const auto f = writeTemp("kinds.rql",
                             "topic t { id int64, hp int64 } partitions 2 key id with (retention.ms = 1000);\n"
                             "alter topic t add column mana int64, rename column hp to health;\n"
                             "alter topic t set compatibility forward;\n"
                             "alter topic t set retention.ms = 60000, retention.bytes = 1048576;\n"
                             "transient topic scratch partitions 1;\n"
                             "drop topic scratch;\n");
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "-f", f, "--broker", b.endpoint()}, &out, &err), 0) << err;
    const auto log = b.catalogEvents();
    ASSERT_EQ(log.size(), 6u);
    EXPECT_EQ(log[0].event.kind, rcat::EventKind::CreateTopic);
    EXPECT_EQ(std::get<rcat::CreateTopicOps>(log[0].event.ops).config.retentionMs, 1000) << "the with(...) config was applied";
    EXPECT_EQ(log[1].event.kind, rcat::EventKind::AlterSchema);
    EXPECT_EQ(log[2].event.kind, rcat::EventKind::SetCompatibility);
    EXPECT_EQ(log[3].event.kind, rcat::EventKind::AlterTopicConfig);
    EXPECT_EQ(log[4].event.kind, rcat::EventKind::CreateTopic);
    EXPECT_EQ(std::get<rcat::CreateTopicOps>(log[4].event.ops).persistence, rcat::Persistence::Transient);
    EXPECT_EQ(log[5].event.kind, rcat::EventKind::DropTopic);
    for (const auto& r : log) EXPECT_FALSE(r.event.statement.empty()) << r.offset;
}

// ── diff ────────────────────────────────────────────────────────────────────

TEST(RuneCliBrokerRegression, DiffOfTwoBrokersReportsDifferencesWithExit3) {
    OneBroker a("cli_da"), b("cli_db");
    a.runEveryKind();
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"catalog", "diff", "--from", a.endpoint(), "--to", b.endpoint()}, out, err), 3);
}

TEST(RuneCliBrokerRegression, DiffClusterVsClusterAndFileVsClusterPrintEveryDifference) {
    OneBroker a("cli_dc"), b("cli_dd");
    a.runEveryKind();
    ASSERT_EQ(b.connectClient()->defineTopicAndWait("events", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s).errorCode, 0);
    ASSERT_EQ(b.connectClient()->defineTopicAndWait("only_b", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s).errorCode, 0);
    std::string out, err;
    // cluster vs cluster: a has {events (typed)}, b has {events (untyped), only_b}
    ASSERT_EQ(runCli({"catalog", "diff", "--from", a.endpoint(), "--to", b.endpoint()}, &out, &err), 3) << err;
    EXPECT_NE(out.find("~ topic events:"), std::string::npos) << out;
    EXPECT_NE(out.find("+ topic only_b"), std::string::npos) << out;
    // the same differences, file vs cluster
    std::string file;
    ASSERT_EQ(runCli({"catalog", "export", "--broker", a.endpoint()}, &file, &err), 0) << err;
    const std::string path = writeTemp("a_export.rcat", file);
    std::string out2;
    ASSERT_EQ(runCli({"catalog", "diff", "--from", path, "--to", b.endpoint()}, &out2, &err), 3) << err;
    EXPECT_EQ(out2, out);
    // and the other way around: cluster vs file
    std::string out3;
    ASSERT_EQ(runCli({"catalog", "diff", "--from", b.endpoint(), "--to", path}, &out3, &err), 3) << err;
    EXPECT_NE(out3.find("- topic only_b"), std::string::npos) << out3;
    // identical: a against its own export — exit 0, no output at all
    std::string out4;
    EXPECT_EQ(runCli({"catalog", "diff", "--from", path, "--to", a.endpoint()}, &out4, &err), 0) << out4;
    EXPECT_TRUE(out4.empty()) << out4;
    EXPECT_TRUE(err.empty()) << err;
}

// ── fix round 1, R1: a follower seed lags the leader that answered the DDL ──

TEST(RuneCliBrokerRegression, ReplayAndQlIntoAFollowerSeeTheirOwnWritesRegression) {
    // Intermittent by nature (replication lag); the deterministic gate for the wait is the offline
    // RuneCliCatalogRegression.AwaitRequestVisibleRetriesUntilVisibleAndIsBoundedRegression.
    using rune::node::test::catalog_cluster::ThreeNodes;
    using rune::node::test::catalog_cluster::kClientStream;
    OneBroker src("cli_src_f");
    src.runEveryKind();
    std::string file, out, err;
    ASSERT_EQ(runCli({"catalog", "export", "--broker", src.endpoint()}, &file, &err), 0) << err;
    const std::string path = writeTemp("f.rcat", file);

    ThreeNodes c = ThreeNodes::start("cli_follow");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = c.settledLeader();
    // The seed is a catalog FOLLOWER (the recipe): it hosts the CLI's session itself, its DDL is redirected to
    // the leader, and its reads lag the leader's commit — the read-your-writes wait covers that.  (A follower
    // used to Redirect the Connect itself and the client never followed: follower-connect-hang.md.)
    const int32_t follower = leader == 1 ? 2 : 1;
    const std::string ep = "127.0.0.1:" + std::to_string(c.ports.client[static_cast<std::size_t>(follower - 1)]);

    ASSERT_EQ(runCli({"catalog", "replay", "--from", path, "--into", ep, "--cluster-id", "f", "--cli-stream-in",
                      std::to_string(kClientStream)}, &out, &err), 0) << err;
    EXPECT_NE(out.find("10 issued"), std::string::npos) << out;

    const std::string rw = writeTemp("rw.rql", "topic rw1 partitions 1;\nshow history topic rw1;\n");
    ASSERT_EQ(runCli({"ql", "-f", rw, "--broker", ep, "--cli-stream-in", std::to_string(kClientStream)}, &out, &err), 0) << err;
    EXPECT_NE(out.find("CreateTopic"), std::string::npos) << out;
}

// ── fix round 1, R3: a rerun must not accept a target that changed after the replay ──

TEST(RuneCliBrokerRegression, ReplayRerunRefusesATargetChangedAfterTheReplayRegression) {
    OneBroker src("cli_src9"), dst("cli_dst9");
    src.runEveryKind();
    std::string o, e;
    ASSERT_EQ(runCli({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint(), "--cluster-id", "c"}, &o, &e), 0) << e;
    const rune::protocol::client::ConfigChangeWire ch{1, 120'000};
    ASSERT_EQ(dst.connectClient()->alterTopicConfigAndWait("events", 0, std::span(&ch, 1), {}, 10s).errorCode, 0);
    const auto n = dst.catalogEvents().size();
    EXPECT_EQ(runCli({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint(), "--cluster-id", "c"}, &o, &e), 1);
    EXPECT_NE(e.find("differs from the source"), std::string::npos) << e;
    EXPECT_NE(e.find("~ topic events"), std::string::npos) << e;
    EXPECT_TRUE(o.empty()) << o;
    EXPECT_EQ(dst.catalogEvents().size(), n) << "nothing was issued";
}

// ── fix round 1, R4: the missing replay tests ───────────────────────────────

TEST(RuneCliBrokerRegression, ReplayOfADifferentSourceUnderTheSameClusterIdAbortsAtTheFirstHeldMismatchRegression) {
    OneBroker a("cli_sa"), b("cli_sb"), dst("cli_sd");
    a.runEveryKind();
    std::string o, e;
    ASSERT_EQ(runCli({"ql", "-e", "topic players partitions 2;", "--broker", b.endpoint()}, &o, &e), 0) << e;
    ASSERT_EQ(runCli({"catalog", "replay", "--from", a.endpoint(), "--into", dst.endpoint(), "--cluster-id", "same"}, &o, &e), 0) << e;
    const auto n = dst.catalogEvents().size();
    EXPECT_EQ(runCli({"catalog", "replay", "--from", b.endpoint(), "--into", dst.endpoint(), "--cluster-id", "same"}, &o, &e), 1);
    EXPECT_NE(e.find("replay aborted at source offset 0"), std::string::npos) << e;
    EXPECT_NE(e.find("target fingerprint 0x"), std::string::npos) << e;
    EXPECT_EQ(dst.catalogEvents().size(), n);
}

TEST(RuneCliBrokerRegression, ReplayMapsRenamesDropsReAddsConfigAndTransientByNameRegression) {
    OneBroker src("cli_sr"), dst("cli_dr");
    const auto f = writeTemp("maps.rql",
                             "topic t { id int64, hp int64, mp int64 } partitions 2 key id with (retention.ms = 1000);\n"
                             "alter topic t add column mana int64, rename column hp to health;\n"
                             "alter topic t rename column health to vitality;\n"
                             "alter topic t drop column mp;\n"
                             "alter topic t add column mp int64;\n"
                             "transient topic scratch partitions 1;\n"
                             "alter topic t set compatibility forward;\n");
    std::string o, e;
    ASSERT_EQ(runCli({"ql", "-f", f, "--broker", src.endpoint()}, &o, &e), 0) << e;
    ASSERT_EQ(src.catalogEvents().size(), 7u);
    ASSERT_EQ(runCli({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint()}, &o, &e), 0) << e;
    const auto s = src.catalogEvents(), d = dst.catalogEvents();
    ASSERT_EQ(d.size(), s.size());
    for (std::size_t i = 0; i < s.size(); ++i) EXPECT_EQ(d[i].event.fingerprint, s[i].event.fingerprint) << "offset " << i;
    EXPECT_EQ(std::get<rcat::CreateTopicOps>(d[0].event.ops).config.retentionMs, 1000);
    EXPECT_EQ(std::get<rcat::CreateTopicOps>(d[5].event.ops).persistence, rcat::Persistence::Transient);
    EXPECT_EQ(runCli({"catalog", "diff", "--from", src.endpoint(), "--to", dst.endpoint()}, &o, &e), 0) << o;
}

TEST(RuneCliBrokerRegression, QlExpectedVersionConflictIsTheBrokersSchemaConflictRegression) {
    OneBroker b("cli_ev");
    std::stringstream o, e;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "topic ev { id int64 } partitions 1 key id;", "--broker", b.endpoint()}, o, e), 0) << e.str();
    std::stringstream o2, e2;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "alter topic ev add column a int32;", "--expected-version", "1",
                              "--broker", b.endpoint()}, o2, e2), 0) << e2.str();
    std::stringstream o3, e3;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "alter topic ev add column b int32;", "--expected-version", "1",
                              "--broker", b.endpoint()}, o3, e3), 1);
    EXPECT_NE(e3.str().find("SchemaConflict"), std::string::npos) << e3.str();
}

// ── schema generate (#418) ────────────────────────────────────────────────────

// `rune schema generate` builds the type from the topic's LINEAGE: after a mid-block DROP the generated v2 type has
// only v2's members, and its kLineage still places a v1 record's fields (score at 12 in v1, at 8 from v2 on).
TEST(RuneCliBrokerRegression, SchemaGenerateEmbedsTheLineageOfEveryVersionRegression) {
    OneBroker b("cli_gen_lineage");
    rune::client::Client& c = *b.connectClient();
    const auto blob = rune::codec::serialize(rune::codec::DescriptorBuilder("evo")
                                                 .add("id", rune::codec::PrimType::Int64, 1, true)
                                                 .add("age", rune::codec::PrimType::Int32)
                                                 .add("score", rune::codec::PrimType::Int32)
                                                 .build());
    ASSERT_EQ(c.defineTopicAndWait("evo", 1, "id", blob, false, rune::client::Persistence::Disk, {}, {}, 10s).errorCode, 0);
    rune::codec::AlterOp drop{}; drop.type = rune::codec::AlterOpType::Drop; drop.name = "age";
    const auto altered = c.alterTopicAndWait("evo", 0, rune::codec::encodeAlterOps({drop}), {}, 10s);
    ASSERT_EQ(altered.errorCode, 0) << altered.detail;

    const fs::path out = tmpDir() / "gen_lineage";
    std::string o, e;
    // The `schema` commands take a full Aeron channel (as the docker e2e passes it) and a reachable egress.
    const std::string egress = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    ASSERT_EQ(runCli({"schema", "generate", "evo", "--out", out.string(), "--broker", "aeron:udp?endpoint=" + b.endpoint(),
                      "--cli-egress", egress, "--cli-data-dir", (tmpDir() / "gen_lineage_cli").string()}, &o, &e), 0) << e;
    std::ifstream in(out / "evo.gen.h");
    ASSERT_TRUE(in);
    const std::string hdr((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_NE(hdr.find("kVersion     = 2u"), std::string::npos);
    EXPECT_NE(hdr.find("int32_t score = {};"), std::string::npos);
    EXPECT_EQ(hdr.find("int32_t age = {};"), std::string::npos) << "age is retired at v2";
    EXPECT_NE(hdr.find("{0u, 12u},  // v1"), std::string::npos) << "a v1 record's score is at 12\n" << hdr;
    EXPECT_NE(hdr.find("{0u, 8u},  // v2"), std::string::npos) << "a v2 record's score is at 8";
}
