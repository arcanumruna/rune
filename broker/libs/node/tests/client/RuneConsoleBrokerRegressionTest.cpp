// Phase 2b Part D: the real `rune console` against a real in-process broker (catalog ruling F7).
#include "client/OneBroker.h"
#include "support/AuthRig.h"

#include <rune/cli/Cli.h>
#include <rune/cli/Console.h>   // runLiveOnFdForTest
#include <rune/client/session/SessionMachine.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <sstream>
#include <streambuf>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

using namespace std::chrono_literals;
using rune::node::test::AuthBroker;
using rune::node::test::OneBroker;
namespace fs = std::filesystem;

namespace {
struct Run { int rc; std::string out, err; };

template <class Broker>
Run consoleLive(Broker& b, std::vector<std::string> extra, const std::string& input) {
    // Issue #13: a UDP broker is reached over TLS (credentials only over it); trust the auth rig's test CA.
    std::vector<std::string> args{"console", "--broker", b.endpoint(), "--tls-ca", AuthBroker::tlsCaFile().string()};
    args.insert(args.end(), extra.begin(), extra.end());
    std::istringstream in(input);
    std::ostringstream out, err;
    const int rc = rune::cli::run(args, in, out, err);
    return {rc, out.str(), err.str()};
}

std::string writeFile(const std::string& name, const std::string& content) {
    static const fs::path dir = [] {
        fs::path p = fs::temp_directory_path() / ("rune_console_test_" + std::to_string(::getpid()));
        fs::create_directories(p);
        return p;
    }();
    const fs::path p = dir / name;
    std::ofstream o(p, std::ios::binary);
    o << content;
    fs::permissions(p, fs::perms::owner_read | fs::perms::owner_write);
    return p.string();
}

constexpr const char* kUserPw = "console-user-pw-1234";

std::size_t countOf(const std::string& hay, const std::string& needle) {
    std::size_t n = 0;
    for (auto pos = hay.find(needle); pos != std::string::npos; pos = hay.find(needle, pos + needle.size())) ++n;
    return n;
}

/// How many CreateTopic events the node's __catalog holds for `name`.
std::size_t createTopicCount(const std::vector<rune::catalog::RecordedEvent>& ev, const std::string& name) {
    std::size_t n = 0;
    for (const auto& e : ev)
        if (e.event.objectName == name && std::holds_alternative<rune::catalog::CreateTopicOps>(e.event.ops)) ++n;
    return n;
}

/// The AlterSchema events for `name`, in catalog order (copies: the caller's event vector may be a temporary).
std::vector<rune::catalog::CatalogEvent> alterSchemaEvents(const std::vector<rune::catalog::RecordedEvent>& ev,
                                                                  const std::string& name) {
    std::vector<rune::catalog::CatalogEvent> out;
    for (const auto& e : ev)
        if (e.event.objectName == name && std::holds_alternative<rune::catalog::AlterSchemaOps>(e.event.ops)) out.push_back(e.event);
    return out;
}

/// A thread-safe output sink: the console thread writes, the test thread polls it (no shared std::ostringstream).
class SyncBuf : public std::streambuf {
public:
    [[nodiscard]] std::string str() {
        std::lock_guard<std::mutex> g(m_);
        return s_;
    }

protected:
    int_type overflow(int_type c) override {
        if (c != traits_type::eof()) {
            std::lock_guard<std::mutex> g(m_);
            s_.push_back(static_cast<char>(c));
        }
        return c;
    }
    std::streamsize xsputn(const char* p, std::streamsize n) override {
        std::lock_guard<std::mutex> g(m_);
        s_.append(p, static_cast<std::size_t>(n));
        return n;
    }

private:
    std::mutex  m_;
    std::string s_;
};

/// An ostream onto a SyncBuf: written by a console thread, read (str()) by the test thread.
struct Sink : SyncBuf, std::ostream {
    Sink() : std::ostream(static_cast<SyncBuf*>(this)) {}
};

/// Polls `cond` (yielding) until it holds; false when the hard cap is hit - a failure, never a hang.
template <class Cond>
bool waitUntil(Cond&& cond, std::chrono::milliseconds cap = 20s) {
    const auto end = std::chrono::steady_clock::now() + cap;
    for (std::uint64_t i = 0; i < 400'000'000ull && std::chrono::steady_clock::now() < end; ++i) {
        if (cond()) return true;
        std::this_thread::yield();
    }
    return cond();
}

/// A console on a pipe in its own thread (the pipe plays the terminal).
struct PipeConsole {
    int fds[2]{-1, -1};
    Sink out, err;   // read by the test thread while the console thread writes
    uint64_t pumps = 0;
    int rc = -1;
    std::thread t;
    explicit PipeConsole(std::vector<std::string> args) {
        if (::pipe(fds) != 0) throw std::runtime_error("pipe");
        t = std::thread([this, a = std::move(args)] { rc = rune::cli::console::runLiveOnFdForTest(a, fds[0], out, err, &pumps); });
    }
    void send(const std::string& s) { ASSERT_EQ(::write(fds[1], s.data(), s.size()), static_cast<ssize_t>(s.size())); }
    void finish() {
        if (fds[1] >= 0) { ::close(fds[1]); fds[1] = -1; }
        if (t.joinable()) t.join();
        if (fds[0] >= 0) { ::close(fds[0]); fds[0] = -1; }
    }
    ~PipeConsole() { finish(); }
};
} // namespace

TEST(RuneConsoleBrokerRegression, InteractiveDefinesAltersAndReadsInOneSessionRegression) {
    OneBroker b("console_live");
    const auto r = consoleLive(b, {},
        "topic player { id int64, name string } partitions 8 key id;\n"
        "alter topic player add column level int32;\n"
        "transient topic ticks { id int64 } partitions 1 key id;\n"
        "show history topic player;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok topic player v1 (8 partitions)"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok topic player v2"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok transient topic ticks v1 (1 partition)"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("CreateTopic"), std::string::npos) << "show history ran in the same session: " << r.out;
    const auto ev = b.catalogEvents();
    ASSERT_GE(ev.size(), 3u);
    EXPECT_EQ(ev[0].event.statement, "topic player { id int64, name string } partitions 8 key id;")
        << "each DDL records its own statement text";
    // Migrated from CliLive.SchemaApplyLive / ApplyTransientTopicSendsTransientPersistence: what reaches the broker.
    bool sawPlayer = false, sawTicks = false, sawLevel = false;
    for (const auto& e : ev) {
        if (const auto* c = std::get_if<rune::catalog::CreateTopicOps>(&e.event.ops)) {
            if (e.event.objectName == "player") {
                sawPlayer = true;
                EXPECT_EQ(c->partitions, 8);
                ASSERT_TRUE(c->schema.has_value());
                EXPECT_EQ(c->schema->keyField, "id");
                EXPECT_EQ(c->persistence, rune::catalog::Persistence::Disk);
                ASSERT_FALSE(c->schema->descriptorBlob.empty()) << "descriptor blob must be non-empty";
                const auto d = rune::codec::deserialize(std::span<const uint8_t>(c->schema->descriptorBlob));
                EXPECT_EQ(d.name, "player");
                // #313: the stored descriptor is the script's - field names AND types, in order, not just a count.
                ASSERT_EQ(d.fields.size(), 2u);
                EXPECT_EQ(d.fields[0].name, "id");
                EXPECT_EQ(d.fields[0].type, rune::codec::PrimType::Int64);
                EXPECT_EQ(d.fields[1].name, "name");
                EXPECT_EQ(d.fields[1].type, rune::codec::PrimType::String);
            }
            if (e.event.objectName == "ticks") {
                sawTicks = true;
                EXPECT_EQ(c->persistence, rune::catalog::Persistence::Transient) << "a transient topic is never created on disk";
            }
        }
        if (const auto* a = std::get_if<rune::catalog::AlterSchemaOps>(&e.event.ops); a != nullptr && e.event.objectName == "player") {
            for (const auto& op : a->ops)
                if (op.type == rune::codec::AlterOpType::Add && op.name == "level") sawLevel = true;
        }
    }
    EXPECT_TRUE(sawPlayer && sawTicks && sawLevel) << "player create, transient ticks create and the level ADD were all issued";
    // #313: exactly one CreateTopic per topic the script defines (a retry or a double issue would add a second).
    EXPECT_EQ(createTopicCount(ev, "player"), 1u);
    EXPECT_EQ(createTopicCount(ev, "ticks"), 1u);
    const auto alters = alterSchemaEvents(ev, "player");
    ASSERT_EQ(alters.size(), 1u) << "one ALTER statement, one AlterSchema event";
    ASSERT_EQ(std::get<rune::catalog::AlterSchemaOps>(alters[0].ops).ops.size(), 1u);
    const auto& add = std::get<rune::catalog::AlterSchemaOps>(alters[0].ops).ops[0];
    EXPECT_EQ(add.name, "level");
    EXPECT_EQ(add.primType, rune::codec::PrimType::Int32) << "the added column's type reached the broker";
}

TEST(RuneConsoleBrokerRegression, OneSessionForEveryStatementRegression) {
    OneBroker b("console_one");
    const uint64_t before = b.node().testConnectsAccepted();
    const auto r = consoleLive(b, {}, "topic a partitions 1;\ntopic b partitions 1;\ntopic c partitions 1;\n");
    ASSERT_EQ(r.rc, 0) << r.err;
    EXPECT_EQ(b.node().testConnectsAccepted() - before, 1u) << "one connected client for the whole session";
}

TEST(RuneConsoleBrokerRegression, BrokerErrorKeepsTheSessionRegression) {
    OneBroker b("console_err");
    const auto r = consoleLive(b, {}, "topic t partitions 1;\ntopic t partitions 1;\ntopic u partitions 1;\n");
    EXPECT_EQ(r.rc, 0);
    EXPECT_NE(r.err.find("TopicExists (25)"), std::string::npos) << r.err;   // was CliLive.SchemaApplyTopicExists ("already exists", rc 0; DD8: now an error line)
    EXPECT_EQ(r.out.find("ok topic t "), r.out.rfind("ok topic t ")) << "the duplicate is not reported ok: " << r.out;
    EXPECT_NE(r.out.find("ok topic u (1 partition)"), std::string::npos) << "the session continues: " << r.out;
}

TEST(RuneConsoleBrokerRegression, ScriptStopsAtTheFirstBrokerErrorWithItsCodeRegression) {
    OneBroker b("console_script");
    const std::string f = writeFile("script.rql",
        "topic ev { id int64, a int32 } partitions 1 key id;\n"
        "alter topic ev set compatibility forward;\n"
        "alter topic ev drop column a;\n"          // a DROP under FORWARD: SchemaIncompatible
        "topic never partitions 1;\n");
    const auto r = consoleLive(b, {"-f", f}, "");
    EXPECT_EQ(r.rc, static_cast<int>(rune::protocol::client::ErrorCode::SchemaIncompatible)) << r.err;
    EXPECT_NE(r.err.find(f + ":statement 3: "), std::string::npos) << r.err;
    EXPECT_NE(r.err.find("SchemaIncompatible (30)"), std::string::npos) << "the broker code is named: " << r.err;   // was CliLive.AlterTopicSchemaIncompatible
    for (const auto& e : b.catalogEvents()) EXPECT_NE(e.event.objectName, "never") << "nothing after the failure is issued";
}

TEST(RuneConsoleBrokerRegression, ValidateAndExpectedVersionReachTheBrokerRegression) {
    OneBroker b("console_flags");
    const std::string f1 = writeFile("flags1.rql", "topic fv { id int64 } partitions 1 key id;\nalter topic fv add column a int32;\n");
    ASSERT_EQ(consoleLive(b, {"-f", f1, "--validate", "--expected-version", "1"}, "").rc, 0);
    {   // --validate reached the broker: the CreateTopic carries validate = true (and a plain run does not)
        const std::string f3 = writeFile("flags3.rql", "topic plain { id int64 } partitions 1 key id;\n");
        ASSERT_EQ(consoleLive(b, {"-f", f3}, "").rc, 0);
        bool sawFv = false, sawPlain = false;
        for (const auto& e : b.catalogEvents()) {
            const auto* c = std::get_if<rune::catalog::CreateTopicOps>(&e.event.ops);
            if (c == nullptr || !c->schema) continue;
            if (e.event.objectName == "fv") { sawFv = true; EXPECT_TRUE(c->schema->validate); }
            if (e.event.objectName == "plain") { sawPlain = true; EXPECT_FALSE(c->schema->validate); }
        }
        EXPECT_TRUE(sawFv && sawPlain);
    }
    const std::string f2 = writeFile("flags2.rql", "alter topic fv add column b int32;\n");
    const auto r = consoleLive(b, {"-f", f2, "--expected-version", "1"}, "");   // the topic is at v2 now
    EXPECT_EQ(r.rc, static_cast<int>(rune::protocol::client::ErrorCode::SchemaConflict)) << r.err;
    EXPECT_NE(r.err.find("SchemaConflict (31)"), std::string::npos) << "the stale expected version is named: " << r.err;   // was CliLive.AlterTopicSchemaConflict
}

TEST(RuneConsoleBrokerRegression, ScriptDefinesThenAltersWithExpectedVersionRegression) {
    OneBroker b("console_evo");
    const std::string f = writeFile("evo.rql",
        "topic characters { id int64, userId int64 } partitions 4 key userId;\n"
        "alter topic characters add column nickname string, set compatibility backward;\n");
    const auto r = consoleLive(b, {"-f", f, "--expected-version", "1"}, "");
    ASSERT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok topic characters v1 (4 partitions)"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok topic characters v2"), std::string::npos) << "the new version is printed: " << r.out;
    // The ops that reach the broker are the ones parsed (was CliLive.AlterTopicLive's opsBlob decode).
    bool sawAdd = false, sawCompat = false;
    for (const auto& e : b.catalogEvents()) {
        if (e.event.objectName != "characters") continue;
        if (const auto* a = std::get_if<rune::catalog::AlterSchemaOps>(&e.event.ops))
            for (const auto& op : a->ops) {
                if (op.type == rune::codec::AlterOpType::Add && op.name == "nickname") sawAdd = true;
                if (op.type == rune::codec::AlterOpType::SetCompatibility && op.mode == rune::codec::CompatMode::Backward) sawCompat = true;
            }
        if (const auto* sc = std::get_if<rune::catalog::SetCompatibilityOps>(&e.event.ops); sc != nullptr && sc->mode == rune::codec::CompatMode::Backward) sawCompat = true;
    }
    EXPECT_TRUE(sawAdd) << "ADD nickname reached the broker";
    EXPECT_TRUE(sawCompat) << "SET COMPATIBILITY backward reached the broker";
    // #313: exactly one define, and the alter was checked against the version the define produced.  The broker accepts a
    // non-zero expectedVersion only when it equals the stored version, so the accepted event's prevVersion IS the
    // value the gate matched (the event itself does not carry the request field).
    const auto ev = b.catalogEvents();
    EXPECT_EQ(createTopicCount(ev, "characters"), 1u) << "exactly one CreateTopic for the one `topic` statement";
    uint32_t createdVersion = 0;
    for (const auto& e : ev)
        if (e.event.objectName == "characters" && std::holds_alternative<rune::catalog::CreateTopicOps>(e.event.ops)) createdVersion = e.event.newVersion;
    const auto alters = alterSchemaEvents(ev, "characters");
    ASSERT_EQ(alters.size(), 1u);
    EXPECT_EQ(alters[0].prevVersion, createdVersion) << "the alter applied on top of the version the define created";
    EXPECT_EQ(alters[0].prevVersion, 1u) << "the version the test passed as --expected-version";
    EXPECT_EQ(alters[0].newVersion, createdVersion + 1);
}

TEST(RuneConsoleBrokerRegression, ExpectedVersionIsTheOneTheBrokerChecksRegression) {
    // #313: the value after --expected-version is what the broker compares - proven with values read back from the
    // catalog events the test captured (never a literal the product could also hard-code), in both directions.
    OneBroker b("console_evcas");
    ASSERT_EQ(consoleLive(b, {"-f", writeFile("cas1.rql", "topic cas { id int64 } partitions 1 key id;\n")}, "").rc, 0);
    auto stored = [&] {   // the topic's stored schema version = the newest event's newVersion
        uint32_t v = 0;
        for (const auto& e : b.catalogEvents())
            if (e.event.objectName == "cas") v = e.event.newVersion;
        return v;
    };
    uint32_t v = stored();
    ASSERT_GE(v, 1u);
    for (int round = 0; round < 3; ++round) {   // the gate follows the stored version: v, v+1, v+2 all pass
        const std::string f = writeFile("cas_ok" + std::to_string(round) + ".rql",
                                        "alter topic cas add column c" + std::to_string(round) + " int32;\n");
        const auto r = consoleLive(b, {"-f", f, "--expected-version", std::to_string(v)}, "");
        ASSERT_EQ(r.rc, 0) << "expected version " << v << ": " << r.err;
        const auto alters = alterSchemaEvents(b.catalogEvents(), "cas");
        ASSERT_EQ(alters.size(), static_cast<std::size_t>(round) + 1);
        EXPECT_EQ(alters.back().prevVersion, v) << "the gate matched the version the test passed";
        EXPECT_EQ(alters.back().newVersion, v + 1);
        v = stored();
        EXPECT_EQ(v, alters.back().newVersion);
    }
    const auto before = b.catalogEvents().size();
    for (const uint32_t wrong : {v - 1, v + 1, v + 4}) {   // any other value is a conflict, and nothing is issued
        const std::string f = writeFile("cas_bad.rql", "alter topic cas add column bad" + std::to_string(wrong) + " int32;\n");
        const auto r = consoleLive(b, {"-f", f, "--expected-version", std::to_string(wrong)}, "");
        EXPECT_EQ(r.rc, static_cast<int>(rune::protocol::client::ErrorCode::SchemaConflict)) << wrong << ": " << r.err;
    }
    EXPECT_EQ(b.catalogEvents().size(), before) << "a refused alter appends nothing";
    // 0 / absent = unconditional.
    const auto u = consoleLive(b, {"-f", writeFile("cas_any.rql", "alter topic cas add column any1 int32;\n")}, "");
    EXPECT_EQ(u.rc, 0) << u.err;
}

TEST(RuneConsoleBrokerRegression, AlterOnlyScriptIssuesNoCreateTopicRegression) {
    // #313: a script with only ALTER statements never defines a topic.
    OneBroker b("console_alteronly");
    ASSERT_EQ(consoleLive(b, {"-f", writeFile("ao1.rql", "topic ao { id int64 } partitions 1 key id;\n")}, "").rc, 0);
    const auto before = b.catalogEvents();
    ASSERT_EQ(createTopicCount(before, "ao"), 1u);
    const auto r = consoleLive(b, {"-f", writeFile("ao2.rql",
        "alter topic ao add column a int32;\nalter topic ao add column b int32;\n")}, "");
    ASSERT_EQ(r.rc, 0) << r.err;
    const auto after = b.catalogEvents();
    EXPECT_EQ(createTopicCount(after, "ao"), 1u) << "still the one CreateTopic from the first script";
    std::size_t creates = 0;
    for (std::size_t i = before.size(); i < after.size(); ++i)
        if (std::holds_alternative<rune::catalog::CreateTopicOps>(after[i].event.ops)) ++creates;
    EXPECT_EQ(creates, 0u) << "the alter-only script appended no CreateTopic";
    EXPECT_EQ(alterSchemaEvents(after, "ao").size(), 2u) << "one AlterSchema per ALTER statement";
}

TEST(RuneConsoleBrokerRegression, IdlePromptKeepsTheSessionAliveRegression) {
    // Review Focus #6 / DD9: an interactive console idle past the session's stale-session window stays connected.
    OneBroker b("console_idle");
    PipeConsole c({"console", "--broker", b.endpoint()});
    c.send("topic idle1 partitions 1;\n");
    std::this_thread::sleep_for(std::chrono::nanoseconds(rune::client::session::SessionConfig{}.responseTimeoutNs) + 2s);
    c.send("topic idle2 partitions 1;\n");
    c.finish();   // EOF: the console leaves
    EXPECT_EQ(c.rc, 0) << c.err.str();
    EXPECT_NE(c.out.str().find("ok topic idle2"), std::string::npos) << c.err.str();
    EXPECT_GT(c.pumps, 10u) << "the client was pumped while the prompt waited";
}

TEST(RuneConsoleBrokerRegression, LoginWithAPasswordFileAndNotAuthorizedKeepsTheSessionRegression) {
    using namespace rune::node::test::grants;
    AuthBroker b("console_login");
    b.bootstrapAdmin();
    b.createUser("u", kUserPw, {prefix(SO::Create, ST::Topic, "ok_")});
    const std::string pw = writeFile("u.pw", std::string(kUserPw) + "\n");
    const auto r = consoleLive(b, {"--user", "u", "--password-file", pw},
                               "topic ok_a partitions 1;\ntopic other partitions 1;\ntopic ok_b partitions 1;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok topic ok_a"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok topic ok_b"), std::string::npos) << "the session continues after NotAuthorized: " << r.out;
    EXPECT_NE(r.err.find("NotAuthorized (45)"), std::string::npos) << r.err;
    EXPECT_EQ(r.out.find("ok topic other"), std::string::npos) << r.out;

    // A user without a password file and without a terminal is a usage error, and nothing connects.
    const uint64_t before = b.node().testConnectsAccepted();
    const auto u = consoleLive(b, {"--user", "u"}, "topic ok_c partitions 1;\n");
    EXPECT_EQ(u.rc, 2);
    EXPECT_NE(u.err.find("--password-file"), std::string::npos) << u.err;
    EXPECT_EQ(b.node().testConnectsAccepted(), before) << "no connect";
}

TEST(RuneConsoleBrokerRegression, CatalogReadRefusalSetsTheScriptExitCodeRegression) {
    // Carried finding (Task 2 review): a catalog READ refused by the broker carries its code to QlRun::lastErrorCode, so
    // `-f` exits with it (here NotAuthorized, 45) instead of the generic 1.
    AuthBroker b("console_readcode");
    b.bootstrapAdmin();
    b.createUser("nobody", kUserPw, {});
    const std::string pw = writeFile("nobody.pw", std::string(kUserPw) + "\n");
    const std::string f = writeFile("history.rql", "show history;\n");
    const auto r = consoleLive(b, {"-f", f, "--user", "nobody", "--password-file", pw}, "");
    EXPECT_EQ(r.rc, static_cast<int>(rune::protocol::client::ErrorCode::NotAuthorized)) << r.err;
}

TEST(RuneConsoleBrokerRegression, SessionRevokedEndsTheConsoleRegression) {
    // Auth D26 / DD11: the admin disables the user between two input lines; the next statement fails SessionRevoked.
    using namespace rune::node::test::grants;
    AuthBroker b("console_revoked");
    b.bootstrapAdmin();
    b.createUser("u", kUserPw, {prefix(SO::Create, ST::Topic, "ok_")});
    const std::string pw = writeFile("revoked.pw", std::string(kUserPw) + "\n");
    PipeConsole c({"console", "--broker", b.endpoint(), "--user", "u", "--password-file", pw, "--tls-ca",
                   AuthBroker::tlsCaFile().string()});   // issue #13: credentials only over TLS
    c.send("topic ok_a partitions 1;\n");
    // The login and the first statement must be done before the admin disables the user (a fixed 500 ms is not enough
    // under load: the disable then lands before the connect and the login itself fails).
    ASSERT_TRUE(waitUntil([&] { return c.out.str().find("ok topic ok_a") != std::string::npos; }))
        << "out: " << c.out.str() << "\nerr: " << c.err.str();
    b.disableUser("u");
    std::this_thread::sleep_for(1500ms);   // the console pumps while idle and learns the session ended
    c.send("topic ok_b partitions 1;\ntopic ok_c partitions 1;\n");
    c.finish();
    EXPECT_EQ(c.rc, 46) << c.err.str();
    EXPECT_NE(c.err.str().find("the session ended"), std::string::npos) << c.err.str();
    EXPECT_NE(c.out.str().find("ok topic ok_a"), std::string::npos) << c.out.str();
    EXPECT_EQ(c.out.str().find("ok topic ok_c"), std::string::npos) << "no statement after the revoke ran";
    // #313: the console stops READING at the revoke.  Prompts are printed before each read: one before `ok_a`, one
    // before `ok_b` (the statement that learned the session ended) and none after it - not "no ok_c", which a dead
    // client would also give.
    EXPECT_EQ(countOf(c.out.str(), "rune> "), 2u) << c.out.str();
    EXPECT_EQ(countOf(c.err.str(), "the session ended"), 1u) << "reported once: " << c.err.str();
}

namespace {
// The injected duty clock of the piped-stdin test: the real steady clock plus an offset only the test advances.
std::atomic<int64_t>  gClockOffsetNs{0};
std::atomic<uint64_t> gClockSamples{0};
int64_t pipedTestClock() noexcept {
    gClockSamples.fetch_add(1, std::memory_order_relaxed);
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count() +
           gClockOffsetNs.load(std::memory_order_relaxed);
}

/// Process stdin (fd 0) replaced by the read end of a pipe for the scope; restored after.
struct StdinPipe {
    int fds[2]{-1, -1};
    int saved = -1;
    StdinPipe() {
        if (::pipe(fds) != 0) throw std::runtime_error("pipe");
        saved = ::dup(STDIN_FILENO);
        if (saved < 0 || ::dup2(fds[0], STDIN_FILENO) < 0) throw std::runtime_error("dup2");
    }
    void send(const std::string& s) { ASSERT_EQ(::write(fds[1], s.data(), s.size()), static_cast<ssize_t>(s.size())); }
    void closeWriter() {
        if (fds[1] >= 0) { ::close(fds[1]); fds[1] = -1; }
    }
    ~StdinPipe() {
        closeWriter();
        if (saved >= 0) { ::dup2(saved, STDIN_FILENO); ::close(saved); }
        if (fds[0] >= 0) ::close(fds[0]);
    }
};

} // namespace

TEST(RuneConsoleBrokerRegression, PipedStdinGapKeepsTheSessionAliveRegression) {
    // #311: the console reads the process stdin through the polling source whether or not it is a terminal, so a gap on
    // a pipe (`slow-producer | rune console`) pumps the client and keeps the session.  The duty clock is injected and
    // advanced one second per step, past the stale-session window (responseTimeoutNs), each step waiting for the
    // console to have sampled the clock again (= to have pumped the client while blocked for input).
    OneBroker b("console_piped");
    const uint64_t connectsBefore = b.node().testConnectsAccepted();
    StdinPipe in;
    Sink outBuf, errBuf;
    std::ostream& out = outBuf;
    std::ostream& err = errBuf;
    gClockOffsetNs = 0;
    gClockSamples = 0;
    uint64_t pumps = 0;
    int rc = -1;
    std::thread t([&] {
        rc = rune::cli::console::runLiveOnStdinForTest({"console", "--broker", b.endpoint()}, &pipedTestClock, out, err, &pumps);
    });
    auto finish = [&] {
        in.closeWriter();
        if (t.joinable()) t.join();
    };
    in.send("topic piped1 partitions 1;\n");
    const bool first = waitUntil([&] { return outBuf.str().find("ok topic piped1") != std::string::npos; });
    if (!first) { finish(); FAIL() << "the first statement never completed: " << errBuf.str(); }

    const int64_t windowNs = rune::client::session::SessionConfig::kDefaultResponseTimeoutNs;
    const int steps = static_cast<int>(windowNs / 1'000'000'000LL) + 4;   // 4 s past the window
    for (int i = 0; i < steps; ++i) {
        gClockOffsetNs.fetch_add(1'000'000'000LL);
        const uint64_t target = gClockSamples.load() + 2;
        if (!waitUntil([&] { return gClockSamples.load() >= target; }, 5s)) {
            finish();
            FAIL() << "step " << i << ": the console blocked on the idle pipe and never pumped the client (clock sampled "
                   << gClockSamples.load() << " times)";
        }
    }
    in.send("topic piped2 partitions 1;\n");   // a second statement, after a gap longer than the stale window
    const bool second = waitUntil([&] { return outBuf.str().find("ok topic piped2") != std::string::npos; });
    finish();
    EXPECT_TRUE(second) << "out: " << outBuf.str() << "\nerr: " << errBuf.str();
    EXPECT_EQ(rc, 0) << errBuf.str();
    EXPECT_EQ(countOf(outBuf.str(), "ok topic piped"), 2u) << "both statements ran on the one session: " << outBuf.str();
    EXPECT_EQ(errBuf.str().find("closed"), std::string::npos) << errBuf.str();
    EXPECT_EQ(errBuf.str().find("session"), std::string::npos) << errBuf.str();
    EXPECT_GT(pumps, static_cast<uint64_t>(steps)) << "the idle pump ran during the gap";
    EXPECT_EQ(b.node().testConnectsAccepted() - connectsBefore, 1u) << "no reconnect: one connected client for the whole session";
}
