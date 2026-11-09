// examples/checked_writes_probe.cpp - Checked writes Part 05: write intents and the key index on 3 authenticated
// brokers (docker/checked-writes-e2e.sh).
//
// One binary, RUNE_MODE (or argv[1]) selects the leg.  Every write is a TypedTopic<rune::gen::KiAccount> *AndWait call
// on a Producer with acks All | View; the Client runs its own thread and the legs use a few threads of their own, one
// Producer and one TypedTopic each.  Every leg writes one result line `<id> <op> <code>` per write to
// <KI_E2E_SHARED>/<tag>-w<worker>.txt (code 0 = Ok, a broker error code otherwise, 10000 + OpStatus for a client-side
// status, 20000 for an exception); verify rebuilds the expected log from those files.
//
//   setup      defines ki_account (4 partitions, rf 3, compact, key.index on); prints KI_TOPIC_ID and
//              CHECKED-WRITES-SETUP: OK.
//   race       --worker <i> --ids <a>-<b> [--tag <t>]: insert every id of the range (KI_E2E_THREADS threads, each its
//              own residue class of the range).  Writes <KI_E2E_SHARED>/<tag>-progress-w<i> (completed writes) while it
//              runs; tag defaults to "race".  The restart leg re-runs it with --tag restart.
//   lifecycle  ids a-b (KI_E2E_LIFECYCLE_IDS, default 2000): update ok; update of an absent id 57; removeExisting ok, again 57;
//              remove ok; insert ok; every 4th id then remove ok (the removed set the restart leg re-inserts).
//   verify     subscribes from 0 on every partition and compares the delivered (id, op) multiset to the accepted writes in
//              the result files.  Prints CHECKED-WRITES-VERIFY: OK.
//   refusals   key.index on a transient topic, an untyped topic, a keyless topic and a string(9) key: InvalidRequest with
//              the exact detail of each.  Prints CHECKED-WRITES-REFUSALS: OK.
//
// The autoincrement legs (Part 06) use ai_user {userId key, provider, subject, createdAt} and write `<id> <op> <code>` where id is
// the id the write was answered with (a key-0 create) or sent (an import); the user comes from RUNE_USER.
//   ai-setup     defines ai_user (1 partition, rf 3, compact, autoincrement); prints AI_TOPIC_ID and CHECKED-WRITES-AI-SETUP: OK.
//   ai-backfill  --ids <a>-<b> [--step <s>] [--tag <t>]: import every s-th explicit id of the range (tag defaults to ai-backfill).
//   ai-create    --worker <i> --count <n> [--tag <t>]: n key-0 creates; writes <tag>-progress-w<i> (tag defaults to ai-create).
//   ai-remove    --ids <id>-<id>: removeExisting of that id.
//   ai-verify    a subscription from 0 holds exactly the ids of the ai-backfill and ai-create result files, one insert each.
//   ai-refusals  autoincrement on a cleanup.policy = delete topic: InvalidRequest with the exact detail.
//
// The RegressorsPath legs (Part 09) run against the golden script's `user` and `character` topics and its views.  Every write
// leaves one result line `<op> <key> <code> <constraintName> <offset> <name> <id>` in <KI_E2E_SHARED>/<tag>-w<worker>.txt: op is
// login (user topic) or create / remove (character topic), key the subject number (login) or the user, code as above (55 =
// ConstraintViolation, 57 = KeyNotFound), constraintName `view.constraint` of a 55 (- otherwise), offset the record's offset (-1
// none), name the subject / character name, id the assigned id (login: a loser's id is what its minOffset read of user_identity returned).
//   rp-setup       prints RP_USER_TOPIC_ID, RP_CHARACTER_TOPIC_ID and CHECKED-WRITES-RP-SETUP: OK once the golden views exist.
//   rp-login       --worker <i> --subjects <a>-<b>: first logins of "steam:<n>"; writes <tag> rp-login and rp-login-progress-w<i>.
//   rp-characters  --worker <i> --users <a>-<b>: 13 creates per user (names unique per worker); workers 0 and 1 also race one
//                  Foo<u> / foo<u> pair per odd user u on the users kRpPairBase + u and + u + 1.  Tag rp-characters.
//   rp-restart     --users <a>-<b>: the account_stats row of each user, then a 13th create.  Tag rp-restart.
//   rp-lifecycle   --users <a>-<b>: removeExisting one character, count, reuse its name for another user, removeExisting again.
//   rp-verify      a subscription from 0 on user and character holds no offset a write was refused at.
//   rp-compact     accepted updates on user and character until both leaders report rejectedNotCompacted 0.
//
// The transaction legs (Part 11) run against the golden script's `character` topic and a `ledger` topic {entryId key, account, amount,
// memo} with the view `balances` (balance >= 0).  Each transaction writes two characters and two ledger entries of 1 through a
// transactional producer (acks All); the result line is `txn <user> <code> <constraintName> <failingOffset> <u<user>k<k>> <first entryId> <start ms> <end ms> <character ids>`.
//   txn-setup      prints TXN_CHARACTER_TOPIC_ID, TXN_LEDGER_TOPIC_ID and CHECKED-WRITES-TXN-SETUP: OK once the topics and views exist.
//   txn-run        --worker <i> --users <a>-<b>: per user six transactions that fit account_stats, a seventh the cap refuses, and one
//                  that overdraws the user's account (the workers share the users and the names u<user>k<k>a / b: per user six
//                  transactions fit in all, a worker that loses the name race answers 55).  Writes the tag txn-run and txn-run-progress-w<i>;
//                  a line ends with the transaction's start and end in epoch ms (the host's clock) and its two assigned character ids.
//   txn-verify     a read_committed subscription from 0 holds every committed transaction whole, no refused one, and a transaction
//                  whose answer was lost whole or not at all; the account_stats and balances rows agree with it.
//
// Environment: RUNE_TARGET, RUNE_EGRESS_HOST + RUNE_EGRESS_PORT, RUNE_DATA_DIR, RUNE_USER / RUNE_PASSWORD_FILE /
// RUNE_TLS_CA_FILE, RUNE_TIMEOUT_MS, KI_E2E_SHARED (a writable directory shared between legs), KI_E2E_THREADS.

#include "ExampleAuth.h"
#include "ai_user.gen.h"
#include "ki_account.gen.h"
#include "rp_character.gen.h"
#include "rp_user.gen.h"

#include <rune/catalog/CatalogQueries.h>
#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/view/ViewKeyBuilder.h>
#include <rune/client/view/ViewRowCodec.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <expected>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace rune::client;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Account = rune::gen::KiAccount;
using Typed   = TypedTopic<Account, ProducerRef>;
using AiUser  = rune::gen::AiUser;
using AiTyped = TypedTopic<AiUser, ProducerRef>;
namespace rcv = rune::client::view;
using RpUser      = rune::gen::User;        // the golden script's topics (rp_user.gen.h, rp_character.gen.h)
using RpCharacter = rune::gen::Character;
using RpUserTyped = TypedTopic<RpUser, ProducerRef>;
using RpCharTyped = TypedTopic<RpCharacter, ProducerRef>;

namespace {

constexpr const char* kTopicName = "ki_account";
constexpr const char* kAiTopicName = "ai_user";
constexpr uint32_t    kParts     = 4;
constexpr uint16_t    kInvalidRequest = 39;
constexpr uint64_t    kAbsentBase = 1'000'000;   // ids no leg ever inserts
constexpr int         kWriteWaitS = 120;         // one write's verdict, across a leader change
constexpr int         kClientErr  = 20000;       // result code of a write that threw
constexpr int         kConstraintViolation = 55;
constexpr int         kKeyNotFound         = 57;
constexpr uint64_t    kRpPairBase = 1'000'000;   // the users of the Foo / foo pairs (RP_PAIR_BASE in the script)
constexpr uint64_t    kRpLifeBase = 2'000'000;   // the users the lifecycle's name reuse creates for
constexpr int         kRpCharsPerUser = 13;

std::string envStr(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::string(v) : def;
}
long envInt(const char* name, long def) {
    const char* v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::strtol(v, nullptr, 10) : def;
}
void fail(const char* tag, const std::string& msg) { std::fprintf(stderr, "%s: FAIL %s\n", tag, msg.c_str()); }

std::string shared() { return envStr("KI_E2E_SHARED", "/shared"); }

void writeFileAtomic(const std::string& path, const std::string& text) {
    const std::string tmp = path + ".tmp";
    { std::ofstream f(tmp, std::ios::trunc); f << text; }
    std::rename(tmp.c_str(), path.c_str());
}

ClientConfig makeConfig() {
    ClientConfig cfg;
    cfg.gatewayChannel   = envStr("RUNE_TARGET");
    cfg.gatewayStreamId  = static_cast<int32_t>(envInt("RUNE_TARGET_STREAM", 1));
    cfg.egressChannel    = "aeron:udp?endpoint=" + envStr("RUNE_EGRESS_HOST") + ":" + std::to_string(envInt("RUNE_EGRESS_PORT", 40500));
    cfg.egressStreamId   = static_cast<int32_t>(envInt("RUNE_EGRESS_STREAM", 2));
    cfg.dataDir          = envStr("RUNE_DATA_DIR") + "/c0";
    cfg.connectTimeoutMs = static_cast<int>(envInt("RUNE_TIMEOUT_MS", 30000));
    cfg.pingTimeoutMs    = cfg.connectTimeoutMs;
    rune::examples::applyEnvCredentials(cfg);
    return cfg;
}

Account account(uint64_t id) {
    Account a;
    a.id = id;
    std::snprintf(a.name.data(), a.name.size(), "acct-%llu", static_cast<unsigned long long>(id));
    a.n = static_cast<int64_t>(id);
    return a;
}

/// One thread's writer: its own acks All | View Producer and the typed topic borrowing it.
struct Writer {
    Producer producer;
    Typed    topic;
    Writer(Client& c, const ProducerConfig& pc) : producer(c.producer(pc)), topic(c.openTopicAndWait<Account>(kTopicName, producer, 60s)) {}
    ~Writer() { producer.closeImmediately(); }
};
ProducerConfig writerConfig() {
    ProducerConfig pc;
    pc.acks     = Acks::All | Acks::View;
    pc.lingerUs = 1;
    pc.deliveryTimeoutMs = static_cast<int64_t>(kWriteWaitS) * 1000;
    return pc;
}

enum class Op { Insert, Update, Remove, RemoveExisting };
const char* opName(Op op) {
    switch (op) {
    case Op::Insert: return "insert";
    case Op::Update: return "update";
    case Op::Remove: return "remove";
    case Op::RemoveExisting: return "removeExisting";
    }
    return "?";
}

/// Runs one write; the result code is 0 for Ok, the broker's error code, 10000 + OpStatus, or kClientErr when it threw.
int write(Typed& t, Op op, uint64_t id) {
    const auto wait = std::chrono::seconds(kWriteWaitS);
    try {
        WriteResult r = [&] {
            switch (op) {
            case Op::Insert: return t.insertAndWait(account(id), wait);
            case Op::Update: return t.updateAndWait(account(id), wait);
            case Op::Remove: return t.removeAndWait(id, wait);
            case Op::RemoveExisting: return t.removeExistingAndWait(id, wait);
            }
            return t.insertAndWait(account(id), wait);
        }();
        if (r.ok()) return 0;
        return r.code() != 0 ? r.code() : 10000 + static_cast<int>(r.status());
    } catch (const std::exception& e) {
        std::fprintf(stderr, "write %s %llu threw: %s\n", opName(op), static_cast<unsigned long long>(id), e.what());
        return kClientErr;
    }
}

struct Line { uint64_t id; Op op; int code; };

/// Runs `body(writer, thread index, lines)` on KI_E2E_THREADS threads, each with its own Writer; the lines come back in one list.
/// `done` counts completed writes (the progress file reads it).
template <class Body>
std::vector<Line> runThreads(Client& c, std::atomic<int64_t>& done, Body&& body) {
    const int threads = static_cast<int>(envInt("KI_E2E_THREADS", 16));
    std::vector<std::vector<Line>> lines(static_cast<std::size_t>(threads));
    std::vector<std::unique_ptr<Writer>> writers;
    for (int i = 0; i < threads; ++i) writers.push_back(std::make_unique<Writer>(c, writerConfig()));
    std::vector<std::thread> ts;
    for (int i = 0; i < threads; ++i)
        ts.emplace_back([&, i] { body(writers[static_cast<std::size_t>(i)]->topic, i, threads, lines[static_cast<std::size_t>(i)], done); });
    for (auto& t : ts) t.join();
    std::vector<Line> all;
    for (auto& l : lines) all.insert(all.end(), l.begin(), l.end());
    return all;
}

void saveLines(const std::string& path, const std::vector<Line>& lines) {
    std::string text;
    for (const Line& l : lines) text += std::to_string(l.id) + " " + opName(l.op) + " " + std::to_string(l.code) + "\n";
    writeFileAtomic(path, text);
}

// ---- setup -----------------------------------------------------------------------------------------------------------

int runSetup() {
    Client c(makeConfig());
    c.connectAndWait();
    TopicOptions to;
    to.replicationFactor    = 3;
    to.config.cleanupPolicy = 1;   // compact
    to.keyFeatures.keyIndex = true;
    const auto blob = rune::codec::serialize(Account::descriptor());
    const auto def  = c.defineTopicAndWait(kTopicName, kParts, "id", blob, Persistence::Disk, to, {}, 60s);
    if (def.errorCode != 0) { fail("CHECKED-WRITES-SETUP", std::string("define ki_account: ") + def.detail); return 1; }
    std::printf("KI_TOPIC_ID=%d\nCHECKED-WRITES-SETUP: OK\n", def.topicId);
    std::fflush(stdout);
    return 0;
}

// ---- race ------------------------------------------------------------------------------------------------------------

struct Args {
    int      worker = 0;
    uint64_t lo = 1, hi = 0;
    uint64_t step = 1, count = 0;
    std::string tag;
};
bool parseArgs(int argc, char** argv, Args& a) {
    for (int i = 2; i < argc; ++i) {
        const std::string k = argv[i];
        if (i + 1 >= argc) return false;
        const std::string v = argv[++i];
        if (k == "--worker") a.worker = std::atoi(v.c_str());
        else if (k == "--tag") a.tag = v;
        else if (k == "--step") a.step = std::strtoull(v.c_str(), nullptr, 10);
        else if (k == "--count") a.count = std::strtoull(v.c_str(), nullptr, 10);
        else if (k == "--ids" || k == "--subjects" || k == "--users") {
            const auto dash = v.find('-');
            if (dash == std::string::npos) return false;
            a.lo = std::strtoull(v.c_str(), nullptr, 10);
            a.hi = std::strtoull(v.c_str() + dash + 1, nullptr, 10);
        } else return false;
    }
    return a.step >= 1 && (a.hi >= a.lo || a.count > 0);
}

int runRace(Args a) {
    if (a.tag.empty()) a.tag = "race";
    Client c(makeConfig());
    c.connectAndWait();
    std::atomic<int64_t> done{0};
    std::atomic<bool> finished{false};
    const std::string progress = shared() + "/" + a.tag + "-progress-w" + std::to_string(a.worker);
    std::thread reporter([&] {
        while (!finished.load()) {
            writeFileAtomic(progress, std::to_string(done.load()) + "\n");
            std::this_thread::sleep_for(250ms);
        }
    });
    const auto lines = runThreads(c, done, [&](Typed& t, int idx, int threads, std::vector<Line>& out, std::atomic<int64_t>& n) {
        for (uint64_t id = a.lo + static_cast<uint64_t>(idx); id <= a.hi; id += static_cast<uint64_t>(threads)) {
            out.push_back({id, Op::Insert, write(t, Op::Insert, id)});
            ++n;
        }
    });
    finished = true;
    reporter.join();
    writeFileAtomic(progress, std::to_string(done.load()) + "\n");
    saveLines(shared() + "/" + a.tag + "-w" + std::to_string(a.worker) + ".txt", lines);
    std::printf("CHECKED-WRITES-RACE: DONE worker=%d writes=%zu\n", a.worker, lines.size());
    std::fflush(stdout);
    return 0;
}

// ---- lifecycle -------------------------------------------------------------------------------------------------------

int runLifecycle() {
    const uint64_t n = static_cast<uint64_t>(envInt("KI_E2E_LIFECYCLE_IDS", 2000));
    Client c(makeConfig());
    c.connectAndWait();
    std::atomic<int64_t> done{0};
    int bad = 0;
    std::mutex badMu;
    const auto lines = runThreads(c, done, [&](Typed& t, int idx, int threads, std::vector<Line>& out, std::atomic<int64_t>&) {
        const auto step = [&](Op op, uint64_t id, int want) {
            const int code = write(t, op, id);
            out.push_back({id, op, code});
            if (code != want) {
                std::lock_guard<std::mutex> g(badMu);
                ++bad;
                fail("CHECKED-WRITES-LIFECYCLE", std::string(opName(op)) + " " + std::to_string(id) + " gave " + std::to_string(code) + ", want " + std::to_string(want));
            }
        };
        for (uint64_t id = 1 + static_cast<uint64_t>(idx); id <= n; id += static_cast<uint64_t>(threads)) {
            step(Op::Update, id, 0);
            step(Op::Update, kAbsentBase + id, 57);
            step(Op::RemoveExisting, id, 0);
            step(Op::RemoveExisting, id, 57);
            step(Op::Remove, id, 0);
            step(Op::Insert, id, 0);
            if (id % 4 == 0) step(Op::Remove, id, 0);
        }
    });
    saveLines(shared() + "/lifecycle-w0.txt", lines);
    if (bad != 0) return 1;
    std::printf("CHECKED-WRITES-LIFECYCLE: OK writes=%zu\n", lines.size());
    std::fflush(stdout);
    return 0;
}

// ---- verify ----------------------------------------------------------------------------------------------------------

const char* recordOpName(RecordOp op) {
    switch (op) {
    case RecordOp::Insert: return "insert";
    case RecordOp::Update: return "update";
    case RecordOp::Delete: return "delete";
    case RecordOp::Upsert: return "upsert";
    }
    return "?";
}
/// The record op an accepted write leaves in the log.
const char* logOpOf(const std::string& writeOp) {
    if (writeOp == "insert") return "insert";
    if (writeOp == "update") return "update";
    return "delete";   // remove, removeExisting
}

int runVerify() {
    // The expected log: every Ok write of every result file, as (id, record op) -> count.
    std::map<std::pair<uint64_t, std::string>, int> expected;
    int64_t total = 0;
    std::vector<std::string> files;
    std::vector<std::string> paths;
    for (const auto& e : std::filesystem::directory_iterator(shared())) {
        const std::string name = e.path().filename().string();
        for (const char* tag : {"race-w", "lifecycle-w", "restart-w"})
            if (name.rfind(tag, 0) == 0 && name.size() > 4 && name.compare(name.size() - 4, 4, ".txt") == 0) paths.push_back(e.path().string());
    }
    std::sort(paths.begin(), paths.end());
    for (const std::string& path : paths) {
        std::ifstream f(path);
        files.push_back(path);
        uint64_t id; std::string op; int code;
        while (f >> id >> op >> code) {
            if (code == 0) { ++expected[{id, logOpOf(op)}]; ++total; }
            else if (code != 56 && code != 57) { fail("CHECKED-WRITES-VERIFY", path + ": " + std::to_string(id) + " " + op + " gave " + std::to_string(code)); return 1; }
        }
    }
    if (total == 0) { fail("CHECKED-WRITES-VERIFY", "no accepted write in the result files"); return 1; }

    Client c(makeConfig());
    c.connectAndWait();
    Producer p = c.producer(writerConfig());
    Typed topic = c.openTopicAndWait<Account>(kTopicName, p, 60s);
    const TopicHandle th = topic.handle();
    if (th.partitionCount != static_cast<int32_t>(kParts)) { fail("CHECKED-WRITES-VERIFY", "ki_account has " + std::to_string(th.partitionCount) + " partitions"); return 1; }

    std::mutex mu;
    std::map<std::pair<uint64_t, std::string>, int> seen;
    int64_t delivered = 0;
    std::string malformed;
    SubscribeOptions so;
    so.from = From::earliest();
    SubscriptionSet set = c.subscribe(th, AllPartitions{}, so, [&](const Record& r) {
        std::lock_guard<std::mutex> g(mu);
        ++delivered;
        if (r.key.size() != sizeof(uint64_t)) { if (malformed.empty()) malformed = "a record key of " + std::to_string(r.key.size()) + " bytes"; return; }
        uint64_t id = 0;
        std::memcpy(&id, r.key.data(), sizeof id);
        ++seen[{id, recordOpName(r.op())}];
    });

    const auto deadline = Clock::now() + std::chrono::seconds(envInt("KI_E2E_VERIFY_S", 300));
    bool matched = false;
    for (; Clock::now() < deadline; std::this_thread::sleep_for(100ms)) {
        for (std::size_t i = 0; i < set.size(); ++i) {
            if (set[i].finished()) { fail("CHECKED-WRITES-VERIFY", "partition " + std::to_string(i) + " ended: failReason " + std::to_string(static_cast<int>(set[i].failReason())) + ", broker code " + std::to_string(set[i].brokerErrorCode())); return 1; }
        }
        std::lock_guard<std::mutex> g(mu);
        if (!malformed.empty()) { fail("CHECKED-WRITES-VERIFY", malformed); return 1; }
        if (delivered > total) break;
        if (seen == expected) { matched = true; break; }
    }
    std::this_thread::sleep_for(5s);   // a quiet window: a rejected or duplicate record lands now
    std::lock_guard<std::mutex> g(mu);
    int shown = 0;
    for (const auto& [k, n] : expected) {
        const auto it = seen.find(k);
        const int got = it == seen.end() ? 0 : it->second;
        if (got != n && shown++ < 10) fail("CHECKED-WRITES-VERIFY", "id " + std::to_string(k.first) + " " + k.second + ": delivered " + std::to_string(got) + ", accepted " + std::to_string(n));
    }
    for (const auto& [k, n] : seen)
        if (expected.count(k) == 0 && shown++ < 10) fail("CHECKED-WRITES-VERIFY", "id " + std::to_string(k.first) + " " + k.second + " delivered " + std::to_string(n) + " times but never accepted");
    if (!matched || seen != expected || delivered != total) {
        fail("CHECKED-WRITES-VERIFY", "delivered " + std::to_string(delivered) + " records, accepted " + std::to_string(total));
        return 1;
    }
    std::printf("CHECKED-WRITES-VERIFY: OK records=%lld files=%zu\n", static_cast<long long>(total), files.size());
    std::fflush(stdout);
    p.closeImmediately();
    return 0;
}

// ---- refusals --------------------------------------------------------------------------------------------------------

int runRefusals() {
    Client c(makeConfig());
    c.connectAndWait();
    TopicOptions to;
    to.keyFeatures.keyIndex = true;
    const auto blob = rune::codec::serialize(Account::descriptor());
    const auto wide = rune::codec::DescriptorBuilder("ki_wide", 1).addFixedStr("k", 9, 1, true).add("n", rune::codec::PrimType::Int64).build();
    const auto keyless = rune::codec::DescriptorBuilder("ki_keyless", 1).add("a", rune::codec::PrimType::Int64).add("n", rune::codec::PrimType::Int64).build();
    int bad = 0;
    const auto expectRefused = [&](const DefineTopicResult& r, const char* what, const char* detail) {
        if (r.errorCode != kInvalidRequest || r.detail != detail) {
            ++bad;
            fail("CHECKED-WRITES-REFUSALS", std::string(what) + ": error " + std::to_string(r.errorCode) + " '" + r.detail + "', want " + std::to_string(kInvalidRequest) + " '" + detail + "'");
        }
    };
    expectRefused(c.defineTopicAndWait("ki_transient", 1, "id", blob, Persistence::Transient, to, {}, 60s), "transient", "key.index needs a persisted topic");
    expectRefused(c.defineTopicAndWait("ki_untyped", 1, "", std::span<const uint8_t>{}, Persistence::Disk, to, {}, 60s), "untyped", "key.index needs a typed topic");
    expectRefused(c.defineTopicAndWait("ki_keyless", 1, "", rune::codec::serialize(keyless), Persistence::Disk, to, {}, 60s), "keyless", "key.index needs a key");
    expectRefused(c.defineTopicAndWait("ki_wide", 1, "k", rune::codec::serialize(wide), Persistence::Disk, to, {}, 60s), "string(9) key", "key.index needs a key of at most 8 bytes");
    if (bad != 0) return 1;
    std::printf("CHECKED-WRITES-REFUSALS: OK\n");
    std::fflush(stdout);
    return 0;
}

// ---- autoincrement ---------------------------------------------------------------------------------------------------

/// One thread's writer on ai_user, as Writer is on ki_account.
struct AiWriter {
    Producer producer;
    AiTyped  topic;
    explicit AiWriter(Client& c) : producer(c.producer(writerConfig())), topic(c.openTopicAndWait<AiUser>(kAiTopicName, producer, 60s)) {}
    ~AiWriter() { producer.closeImmediately(); }
};

/// Runs `body(topic, thread index, thread count, lines)` on KI_E2E_THREADS threads, each with its own AiWriter.
template <class Body>
std::vector<Line> runAiThreads(Client& c, Body&& body) {
    const int threads = static_cast<int>(envInt("KI_E2E_THREADS", 16));
    std::vector<std::vector<Line>> lines(static_cast<std::size_t>(threads));
    std::vector<std::unique_ptr<AiWriter>> writers;
    for (int i = 0; i < threads; ++i) writers.push_back(std::make_unique<AiWriter>(c));
    std::vector<std::thread> ts;
    for (int i = 0; i < threads; ++i)
        ts.emplace_back([&, i] { body(writers[static_cast<std::size_t>(i)]->topic, i, threads, lines[static_cast<std::size_t>(i)]); });
    for (auto& t : ts) t.join();
    std::vector<Line> all;
    for (auto& l : lines) all.insert(all.end(), l.begin(), l.end());
    return all;
}

/// insertAndWait of ai_user `id` (0 = take the next id from the broker); the line carries the id the broker answered with for a key-0 create.
Line aiInsert(AiTyped& t, uint64_t id) {
    AiUser u;
    u.userId   = id;
    u.provider = 1;
    std::snprintf(u.subject.data(), u.subject.size(), "subject-%llu", static_cast<unsigned long long>(id));
    try {
        const WriteResult r = t.insertAndWait(u, std::chrono::seconds(kWriteWaitS));
        if (r.ok()) return {id != 0 ? id : r.key(), Op::Insert, 0};
        return {id, Op::Insert, r.code() != 0 ? r.code() : 10000 + static_cast<int>(r.status())};
    } catch (const std::exception& e) {
        std::fprintf(stderr, "insert %llu threw: %s\n", static_cast<unsigned long long>(id), e.what());
        return {id, Op::Insert, kClientErr};
    }
}

int runAiSetup() {
    Client c(makeConfig());
    c.connectAndWait();
    TopicOptions to;
    to.replicationFactor         = 3;
    to.config.cleanupPolicy      = 1;   // compact
    to.keyFeatures.autoIncrement = true;
    const auto def = c.defineTopicAndWait(kAiTopicName, 1, "userId", rune::codec::serialize(AiUser::descriptor()), Persistence::Disk, to, {}, 60s);
    if (def.errorCode != 0) { fail("CHECKED-WRITES-AI-SETUP", std::string("define ai_user: ") + def.detail); return 1; }
    std::printf("AI_TOPIC_ID=%d\nCHECKED-WRITES-AI-SETUP: OK\n", def.topicId);
    std::fflush(stdout);
    return 0;
}

int runAiBackfill(Args a) {
    if (a.tag.empty()) a.tag = "ai-backfill";
    Client c(makeConfig());
    c.connectAndWait();
    const auto lines = runAiThreads(c, [&](AiTyped& t, int idx, int threads, std::vector<Line>& out) {
        for (uint64_t k = static_cast<uint64_t>(idx); a.lo + k * a.step <= a.hi; k += static_cast<uint64_t>(threads)) out.push_back(aiInsert(t, a.lo + k * a.step));
    });
    saveLines(shared() + "/" + a.tag + "-w" + std::to_string(a.worker) + ".txt", lines);
    std::printf("CHECKED-WRITES-AI-BACKFILL: DONE rows=%zu\n", lines.size());
    std::fflush(stdout);
    return 0;
}

int runAiCreate(Args a) {
    if (a.tag.empty()) a.tag = "ai-create";
    Client c(makeConfig());
    c.connectAndWait();
    std::atomic<int64_t> done{0};
    std::atomic<bool> finished{false};
    const std::string progress = shared() + "/" + a.tag + "-progress-w" + std::to_string(a.worker);
    std::thread reporter([&] {
        while (!finished.load()) {
            writeFileAtomic(progress, std::to_string(done.load()) + "\n");
            std::this_thread::sleep_for(250ms);
        }
    });
    const auto lines = runAiThreads(c, [&](AiTyped& t, int idx, int threads, std::vector<Line>& out) {
        for (uint64_t i = static_cast<uint64_t>(idx); i < a.count; i += static_cast<uint64_t>(threads)) {
            out.push_back(aiInsert(t, 0));
            ++done;
        }
    });
    finished = true;
    reporter.join();
    writeFileAtomic(progress, std::to_string(done.load()) + "\n");
    saveLines(shared() + "/" + a.tag + "-w" + std::to_string(a.worker) + ".txt", lines);
    std::printf("CHECKED-WRITES-AI-CREATE: DONE worker=%d creates=%zu\n", a.worker, lines.size());
    std::fflush(stdout);
    return 0;
}

int runAiRemove(const Args& a) {
    Client c(makeConfig());
    c.connectAndWait();
    AiWriter w(c);
    const WriteResult r = w.topic.removeExistingAndWait(a.lo, std::chrono::seconds(kWriteWaitS));
    if (!r.ok()) { fail("CHECKED-WRITES-AI-REMOVE", "removeExisting " + std::to_string(a.lo) + " gave code " + std::to_string(r.code())); return 1; }
    std::printf("CHECKED-WRITES-AI-REMOVE: OK id=%llu\n", static_cast<unsigned long long>(a.lo));
    std::fflush(stdout);
    return 0;
}

int runAiVerify() {
    std::map<uint64_t, int> expected;
    for (const auto& e : std::filesystem::directory_iterator(shared())) {
        const std::string name = e.path().filename().string();
        if ((name.rfind("ai-backfill-w", 0) != 0 && name.rfind("ai-create-w", 0) != 0) || name.compare(name.size() - 4, 4, ".txt") != 0) continue;
        std::ifstream f(e.path());
        uint64_t id; std::string op; int code;
        while (f >> id >> op >> code) {
            if (code != 0) { fail("CHECKED-WRITES-AI-VERIFY", name + ": " + std::to_string(id) + " gave " + std::to_string(code)); return 1; }
            if (++expected[id] > 1) { fail("CHECKED-WRITES-AI-VERIFY", "id " + std::to_string(id) + " was answered twice"); return 1; }
        }
    }
    if (expected.empty()) { fail("CHECKED-WRITES-AI-VERIFY", "no accepted write in the result files"); return 1; }

    Client c(makeConfig());
    c.connectAndWait();
    AiWriter w(c);
    std::mutex mu;
    std::map<uint64_t, int> seen;
    std::string malformed;
    SubscribeOptions so;
    so.from = From::earliest();
    SubscriptionSet set = c.subscribe(w.topic.handle(), AllPartitions{}, so, [&](const Record& r) {
        std::lock_guard<std::mutex> g(mu);
        if (r.key.size() != sizeof(uint64_t) || r.op() != RecordOp::Insert) { if (malformed.empty()) malformed = std::string("a ") + recordOpName(r.op()) + " record with a key of " + std::to_string(r.key.size()) + " bytes"; return; }
        uint64_t id = 0;
        std::memcpy(&id, r.key.data(), sizeof id);
        ++seen[id];
    });
    const auto deadline = Clock::now() + std::chrono::seconds(envInt("KI_E2E_VERIFY_S", 300));
    for (; Clock::now() < deadline; std::this_thread::sleep_for(100ms)) {
        for (std::size_t i = 0; i < set.size(); ++i)
            if (set[i].finished()) { fail("CHECKED-WRITES-AI-VERIFY", "partition " + std::to_string(i) + " ended: broker code " + std::to_string(set[i].brokerErrorCode())); return 1; }
        std::lock_guard<std::mutex> g(mu);
        if (!malformed.empty() || seen == expected) break;
    }
    std::this_thread::sleep_for(5s);   // a quiet window: a stray or duplicate record lands now
    std::lock_guard<std::mutex> g(mu);
    int shown = 0;
    for (const auto& [id, n] : expected)
        if (seen[id] != n && shown++ < 10) fail("CHECKED-WRITES-AI-VERIFY", "id " + std::to_string(id) + " delivered " + std::to_string(seen[id]) + " times, answered once");
    for (const auto& [id, n] : seen)
        if (expected.count(id) == 0 && shown++ < 10) fail("CHECKED-WRITES-AI-VERIFY", "id " + std::to_string(id) + " delivered " + std::to_string(n) + " times but never answered");
    if (!malformed.empty()) fail("CHECKED-WRITES-AI-VERIFY", malformed);
    if (!malformed.empty() || seen != expected) return 1;
    std::printf("CHECKED-WRITES-AI-VERIFY: OK ids=%zu\n", expected.size());
    std::fflush(stdout);
    return 0;
}

int runAiRefusals() {
    Client c(makeConfig());
    c.connectAndWait();
    TopicOptions to;
    to.config.cleanupPolicy      = 0;   // delete
    to.keyFeatures.autoIncrement = true;
    const auto r = c.defineTopicAndWait("ai_bad", 1, "userId", rune::codec::serialize(AiUser::descriptor()), Persistence::Disk, to, {}, 60s);
    const char* detail = "autoincrement needs cleanup.policy = compact";
    if (r.errorCode != kInvalidRequest || r.detail != detail) {
        fail("CHECKED-WRITES-AI-REFUSALS", "cleanup.policy = delete: error " + std::to_string(r.errorCode) + " '" + r.detail + "', want " + std::to_string(kInvalidRequest) + " '" + detail + "'");
        return 1;
    }
    std::printf("CHECKED-WRITES-AI-REFUSALS: OK\n");
    std::fflush(stdout);
    return 0;
}

// ---- RegressorsPath --------------------------------------------------------------------------------------------------

/// One thread's writer on the golden script's user and character topics.
struct RpWriter {
    Producer    producer;
    RpUserTyped users;
    RpCharTyped characters;
    explicit RpWriter(Client& c)
        : producer(c.producer(writerConfig())), users(c.openTopicAndWait<RpUser>("user", producer, 60s)),
          characters(c.openTopicAndWait<RpCharacter>("character", producer, 60s)) {}
    ~RpWriter() { producer.closeImmediately(); }
};

struct RpLine {
    std::string op;
    uint64_t    key = 0;
    int         code = kClientErr;
    uint32_t    constraintId = 0;
    int64_t     offset = -1;
    std::string name;
    uint64_t    id = 0;
    int64_t     startMs = 0, endMs = 0;   // a transaction's wall-clock epoch ms (the containers share the host's clock)
    uint64_t    char1 = 0, char2 = 0;      // the ids a transaction's creates were assigned
};

/// Runs one write; the line carries its verdict (code as write(): 0 Ok, the broker's code, 10000 + OpStatus, kClientErr when it threw).
template <class Send>
RpLine rpWrite(const char* op, uint64_t key, const std::string& name, Send&& send) {
    RpLine l{op, key, kClientErr, 0, -1, name, 0};
    try {
        const WriteResult r = send();
        l.code         = r.ok() ? 0 : (r.code() != 0 ? r.code() : 10000 + static_cast<int>(r.status()));
        l.constraintId = r.constraintId();
        l.offset       = r.offset();
        l.id           = r.key();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "%s %llu threw: %s\n", op, static_cast<unsigned long long>(key), e.what());
    }
    return l;
}

RpLine rpCreate(RpWriter& w, uint64_t user, const std::string& name) {
    RpCharacter ch;
    ch.userId = user;
    ch.gender = 1;
    std::snprintf(ch.name.data(), ch.name.size(), "%s", name.c_str());
    return rpWrite("create", user, name, [&] { return w.characters.insertAndWait(ch, std::chrono::seconds(kWriteWaitS)); });
}

RpLine rpRemoveExisting(RpWriter& w, uint64_t user, uint64_t characterId, const std::string& name) {
    RpLine l = rpWrite("remove", user, name, [&] { return w.characters.removeExistingAndWait(characterId, std::chrono::seconds(kWriteWaitS)); });
    l.id = characterId;
    return l;
}

/// <tag>-progress-w<worker>: the writes completed so far, rewritten every 250 ms while the object lives.
class Progress {
public:
    std::atomic<int64_t> done{0};
    Progress(const std::string& tag, int worker) : path_(shared() + "/" + tag + "-progress-w" + std::to_string(worker)), reporter_([this] {
        while (!finished_.load()) {
            writeFileAtomic(path_, std::to_string(done.load()) + "\n");
            std::this_thread::sleep_for(250ms);
        }
    }) {}
    ~Progress() {
        finished_ = true;
        reporter_.join();
        writeFileAtomic(path_, std::to_string(done.load()) + "\n");
    }
private:
    std::string       path_;
    std::atomic<bool> finished_{false};
    std::thread       reporter_;
};

/// Runs `body(writer, thread index, thread count, lines)` on KI_E2E_THREADS threads, each with its own RpWriter.
template <class Body>
std::vector<RpLine> runRpThreads(Client& c, Body&& body) {
    const int threads = static_cast<int>(envInt("KI_E2E_THREADS", 16));
    std::vector<std::vector<RpLine>> lines(static_cast<std::size_t>(threads));
    std::vector<std::unique_ptr<RpWriter>> writers;
    for (int i = 0; i < threads; ++i) writers.push_back(std::make_unique<RpWriter>(c));
    std::vector<std::thread> ts;
    for (int i = 0; i < threads; ++i)
        ts.emplace_back([&, i] { body(*writers[static_cast<std::size_t>(i)], i, threads, lines[static_cast<std::size_t>(i)]); });
    for (auto& t : ts) t.join();
    std::vector<RpLine> all;
    for (auto& l : lines) all.insert(all.end(), l.begin(), l.end());
    return all;
}

/// Writes the lines of <tag>-w<worker>.txt; a 55 names its constraint (resolved here: constraintName is for the Client's own thread).
void saveRpLines(Client& c, const std::string& tag, int worker, const std::vector<RpLine>& lines) {
    std::map<uint32_t, std::string> names;
    std::string text;
    for (const RpLine& l : lines) {
        std::string constraint = "-";
        if (l.code == kConstraintViolation && l.constraintId != 0) {
            auto it = names.find(l.constraintId);
            if (it == names.end()) {
                const auto n = c.constraintName(l.constraintId);
                it = names.emplace(l.constraintId, n ? n->view + "." + n->constraint : "unknown").first;
            }
            constraint = it->second;
        }
        text += l.op + " " + std::to_string(l.key) + " " + std::to_string(l.code) + " " + constraint + " " + std::to_string(l.offset) + " " +
                (l.name.empty() ? "-" : l.name) + " " + std::to_string(l.id) + " " +
                std::to_string(l.startMs) + " " + std::to_string(l.endMs) + " " + std::to_string(l.char1) + " " + std::to_string(l.char2) + "\n";
    }
    writeFileAtomic(shared() + "/" + tag + "-w" + std::to_string(worker) + ".txt", text);
}

/// The lines of every <prefix>*.txt result file in the shared directory.
std::vector<RpLine> readRpLines(std::initializer_list<const char*> prefixes) {
    std::vector<RpLine> all;
    for (const auto& e : std::filesystem::directory_iterator(shared())) {
        const std::string name = e.path().filename().string();
        bool wanted = name.size() > 4 && name.compare(name.size() - 4, 4, ".txt") == 0;
        bool prefixed = false;
        for (const char* p : prefixes) prefixed = prefixed || name.rfind(p, 0) == 0;
        if (!wanted || !prefixed) continue;
        std::ifstream f(e.path());
        RpLine l;
        std::string constraint;
        while (f >> l.op >> l.key >> l.code >> constraint >> l.offset >> l.name >> l.id >> l.startMs >> l.endMs >> l.char1 >> l.char2) all.push_back(l);
    }
    return all;
}

rune::catalog::CatalogState latestCatalog(Client& c) {
    auto r = c.catalogAsOf(rune::catalog::AsOf{.by = rune::catalog::AsOf::By::Offset, .value = std::numeric_limits<int64_t>::max()}, 30s);
    if (!r) throw std::runtime_error("the catalog could not be read");
    return std::move(*r);
}

std::vector<uint8_t> keyBytes(std::expected<std::vector<uint8_t>, std::string> k) {
    if (!k) throw std::runtime_error(k.error());
    return std::move(*k);
}

/// A view of the golden script: its topic id, row layout and key builder, read from the catalog.
struct RpView {
    int32_t                  topic = -1;
    rcv::RowLayout           layout;
    std::vector<std::string> names;
    rcv::ViewKeyBuilder      builder;

    RpView(const rune::catalog::CatalogState& state, const std::string& view) : builder([&] {
        auto b = rcv::ViewKeyBuilder::fromCatalog(state, view);
        if (!b) throw std::runtime_error(view + ": " + b.error());
        return std::move(*b);
    }()) {
        const auto* v = state.viewByName(rune::catalog::kDefaultNsId, view);
        if (v == nullptr) throw std::runtime_error("no view " + view);
        topic = v->def.viewTopicId;
        std::vector<rune::codec::PrimType> types;
        std::vector<uint16_t> widths;
        for (const auto& f : rune::codec::deserialize(v->def.outputDescriptorBlob).fields) {
            names.push_back(f.name);
            types.push_back(f.type);
            widths.push_back(f.type == rune::codec::PrimType::FixedStr ? f.fixedLen : uint16_t{0});
        }
        layout = rcv::buildRowLayout(types, widths);
    }
    [[nodiscard]] uint64_t u(std::span<const uint8_t> row, const std::string& column) const {
        const auto slot = static_cast<uint16_t>(std::find(names.begin(), names.end(), column) - names.begin());
        const rcv::FieldValue f = rcv::readSlot(row, layout, slot);
        return f.type == rune::codec::PrimType::Int64 ? static_cast<uint64_t>(f.i) : f.u;
    }
};

/// The row of `key` once it reads Ok at `minOffset` (a view that is not ready or a leader that moved is retried); nullopt after `within`.
std::optional<std::vector<uint8_t>> rpRowWhenReady(Client& c, const RpView& v, std::span<const uint8_t> key, int64_t minOffset, std::chrono::seconds within) {
    std::string last = "no answer";
    for (const auto deadline = Clock::now() + within; Clock::now() < deadline; std::this_thread::sleep_for(100ms)) {
        try {
            ViewReadOptions o;
            o.minOffset = minOffset;
            ViewRowResult r = c.getViewRowAndWait(v.topic, key, o, 10s);
            if (r.status == ViewRowResult::kOk) return std::move(r.rowBytes);
            last = "status " + std::to_string(r.status);
        } catch (const std::runtime_error& e) {
            last = e.what();
        }
    }
    std::fprintf(stderr, "view %d: no Ok row within %lld s (last: %s)\n", v.topic, static_cast<long long>(within.count()), last.c_str());
    return std::nullopt;
}

std::string rpSubject(uint64_t n) { return "steam:" + std::to_string(n); }

std::map<uint64_t, std::vector<uint8_t>> statsKeys(RpView& stats, uint64_t lo, uint64_t hi) {
    std::map<uint64_t, std::vector<uint8_t>> keys;
    for (uint64_t u = lo; u <= hi; ++u) {
        stats.builder.clear();
        keys[u] = keyBytes(stats.builder.setUInt("userId", u).key());
    }
    return keys;
}

int runRpSetup() {
    Client c(makeConfig());
    c.connectAndWait();
    const auto state = latestCatalog(c);
    const auto* user = state.topicByName(rune::catalog::kDefaultNsId, "user");
    const auto* character = state.topicByName(rune::catalog::kDefaultNsId, "character");
    if (user == nullptr || character == nullptr) { fail("CHECKED-WRITES-RP-SETUP", "the user or the character topic is missing"); return 1; }
    for (const char* view : {"user_identity", "character_name", "account_stats"})
        if (state.viewByName(rune::catalog::kDefaultNsId, view) == nullptr) { fail("CHECKED-WRITES-RP-SETUP", std::string("the view ") + view + " is missing"); return 1; }
    std::printf("RP_USER_TOPIC_ID=%d\nRP_CHARACTER_TOPIC_ID=%d\nCHECKED-WRITES-RP-SETUP: OK\n", user->topicId, character->topicId);
    std::fflush(stdout);
    return 0;
}

int runRpLogin(const Args& a) {
    Client c(makeConfig());
    c.connectAndWait();
    const auto state = latestCatalog(c);
    RpView identity(state, "user_identity");
    std::map<uint64_t, std::vector<uint8_t>> keys;
    for (uint64_t n = a.lo; n <= a.hi; ++n) {
        identity.builder.clear();
        keys[n] = keyBytes(identity.builder.setUInt("provider", 1).setString("subject", rpSubject(n)).key());
    }
    std::vector<RpLine> lines;
    {
        Progress progress("rp-login", a.worker);
        lines = runRpThreads(c, [&](RpWriter& w, int idx, int threads, std::vector<RpLine>& out) {
            for (uint64_t n = a.lo + static_cast<uint64_t>(idx); n <= a.hi; n += static_cast<uint64_t>(threads)) {
                RpUser u;
                u.provider = 1;
                const std::string subject = rpSubject(n);
                std::snprintf(u.subject.data(), u.subject.size(), "%s", subject.c_str());
                RpLine l = rpWrite("login", n, subject, [&] { return w.users.insertAndWait(u, std::chrono::seconds(kWriteWaitS)); });
                if (l.code == kConstraintViolation && l.offset >= 0) {
                    const auto row = rpRowWhenReady(c, identity, keys.at(n), l.offset, 30s);
                    l.id = row ? identity.u(*row, "userId") : 0;
                }
                out.push_back(l);
                ++progress.done;
            }
        });
    }
    saveRpLines(c, "rp-login", a.worker, lines);
    std::printf("CHECKED-WRITES-RP-LOGIN: DONE worker=%d logins=%zu\n", a.worker, lines.size());
    std::fflush(stdout);
    return 0;
}

int runRpCharacters(const Args& a) {
    Client c(makeConfig());
    c.connectAndWait();
    std::vector<RpLine> lines;
    {
        Progress progress("rp-characters", a.worker);
        lines = runRpThreads(c, [&](RpWriter& w, int idx, int threads, std::vector<RpLine>& out) {
            for (uint64_t u = a.lo + static_cast<uint64_t>(idx); u <= a.hi; u += static_cast<uint64_t>(threads)) {
                if (a.worker < 2 && u % 2 == 1) {   // worker 0 creates Foo<u>, worker 1 foo<u>: one name, two users, at the same time
                    out.push_back(rpCreate(w, kRpPairBase + u + static_cast<uint64_t>(a.worker), std::string(a.worker == 0 ? "Foo" : "foo") + std::to_string(u)));
                    ++progress.done;
                }
                for (int i = 0; i < kRpCharsPerUser; ++i) {
                    out.push_back(rpCreate(w, u, "w" + std::to_string(a.worker) + "u" + std::to_string(u) + "c" + std::to_string(i)));
                    ++progress.done;
                }
            }
        });
    }
    saveRpLines(c, "rp-characters", a.worker, lines);
    std::printf("CHECKED-WRITES-RP-CHARACTERS: DONE worker=%d creates=%zu\n", a.worker, lines.size());
    std::fflush(stdout);
    return 0;
}

int runRpRestart(const Args& a) {
    Client c(makeConfig());
    c.connectAndWait();
    const auto state = latestCatalog(c);
    RpView stats(state, "account_stats");
    const auto keys = statsKeys(stats, a.lo, a.hi);
    const auto lines = runRpThreads(c, [&](RpWriter& w, int idx, int threads, std::vector<RpLine>& out) {
        for (uint64_t u = a.lo + static_cast<uint64_t>(idx); u <= a.hi; u += static_cast<uint64_t>(threads)) {
            const RpLine refused = rpCreate(w, u, "w9u" + std::to_string(u) + "c13");
            const auto row = rpRowWhenReady(c, stats, keys.at(u), refused.offset, 120s);   // read at the refused record's offset: the fold has caught up
            out.push_back({"stats", u, row ? 0 : kClientErr, 0, -1, "-", row ? stats.u(*row, "characters") : 0});
            out.push_back(refused);
        }
    });
    saveRpLines(c, "rp-restart", a.worker, lines);
    std::printf("CHECKED-WRITES-RP-RESTART: DONE users=%zu\n", lines.size() / 2);
    std::fflush(stdout);
    return 0;
}

int runRpLifecycle(const Args& a) {
    // The first accepted character of each user, from the rp-characters result files.
    std::map<uint64_t, std::pair<uint64_t, std::string>> first;
    for (const RpLine& l : readRpLines({"rp-characters-w"}))
        if (l.op == "create" && l.code == 0 && l.key >= a.lo && l.key <= a.hi && !first.contains(l.key)) first[l.key] = {l.id, l.name};
    Client c(makeConfig());
    c.connectAndWait();
    const auto state = latestCatalog(c);
    RpView stats(state, "account_stats");
    const auto keys = statsKeys(stats, a.lo, a.hi);
    const auto lines = runRpThreads(c, [&](RpWriter& w, int idx, int threads, std::vector<RpLine>& out) {
        for (uint64_t u = a.lo + static_cast<uint64_t>(idx); u <= a.hi; u += static_cast<uint64_t>(threads)) {
            const auto it = first.find(u);
            if (it == first.end()) { out.push_back({"remove", u, kClientErr, 0, -1, "-", 0}); continue; }
            const auto& [characterId, name] = it->second;
            const RpLine removed = rpRemoveExisting(w, u, characterId, name);
            out.push_back(removed);
            if (removed.code == 0) {
                const auto row = rpRowWhenReady(c, stats, keys.at(u), removed.offset, 120s);
                out.push_back({"stats", u, row ? 0 : kClientErr, 0, -1, "-", row ? stats.u(*row, "characters") : 0});
            }
            out.push_back(rpCreate(w, kRpLifeBase + u, name));
            out.push_back(rpRemoveExisting(w, u, characterId, name));
        }
    });
    saveRpLines(c, "rp-lifecycle", a.worker, lines);
    std::printf("CHECKED-WRITES-RP-LIFECYCLE: DONE users=%zu\n", first.size());
    std::fflush(stdout);
    return 0;
}

/// The offsets the result files hold, per topic: where a write was accepted and where it was refused (55 or 57).
struct RpOffsets {
    std::set<int64_t> accepted, refused;
};

bool rpOffsets(RpOffsets& user, RpOffsets& character, const char* tag) {
    for (const RpLine& l : readRpLines({"rp-login-w", "rp-characters-w", "rp-restart-w", "rp-lifecycle-w"})) {
        if (l.op == "stats" || l.offset < 0) continue;
        RpOffsets& o = l.op == "login" ? user : character;
        if (l.code == 0) o.accepted.insert(l.offset);
        else if (l.code == kConstraintViolation || l.code == kKeyNotFound) o.refused.insert(l.offset);
        else { fail(tag, l.op + " " + std::to_string(l.key) + " gave " + std::to_string(l.code)); return false; }
    }
    return true;
}

int runRpVerify() {
    RpOffsets user, character;
    if (!rpOffsets(user, character, "CHECKED-WRITES-RP-VERIFY")) return 1;
    if (user.accepted.empty() || character.accepted.empty() || user.refused.empty() || character.refused.empty()) {
        fail("CHECKED-WRITES-RP-VERIFY", "the result files hold no accepted or no refused write of a topic");
        return 1;
    }
    Client c(makeConfig());
    c.connectAndWait();
    RpWriter w(c);
    struct Tap {
        std::mutex        mu;
        std::set<int64_t> got;
    };
    Tap userTap, characterTap;
    SubscribeOptions so;
    so.from = From::earliest();
    const auto tap = [&](Tap& t) { return [&t](const Record& r) { std::lock_guard<std::mutex> g(t.mu); t.got.insert(r.offset); }; };
    SubscriptionSet userSet      = c.subscribe(w.users.handle(), AllPartitions{}, so, tap(userTap));
    SubscriptionSet characterSet = c.subscribe(w.characters.handle(), AllPartitions{}, so, tap(characterTap));
    const auto reached = [](Tap& t, const RpOffsets& o) { std::lock_guard<std::mutex> g(t.mu); return t.got.contains(*o.accepted.rbegin()); };
    const auto deadline = Clock::now() + std::chrono::seconds(envInt("KI_E2E_VERIFY_S", 300));
    while (Clock::now() < deadline && !(reached(userTap, user) && reached(characterTap, character))) std::this_thread::sleep_for(100ms);
    std::this_thread::sleep_for(5s);   // a quiet window: a refused record that is going to be delivered lands now
    int bad = 0;
    const auto judge = [&](const char* topic, Tap& t, const RpOffsets& o) {
        std::lock_guard<std::mutex> g(t.mu);
        if (!t.got.contains(*o.accepted.rbegin())) { ++bad; fail("CHECKED-WRITES-RP-VERIFY", std::string(topic) + ": the newest accepted record never arrived"); }
        int shown = 0;
        for (const int64_t off : o.refused)
            if (t.got.contains(off) && shown++ < 10) { ++bad; fail("CHECKED-WRITES-RP-VERIFY", std::string(topic) + ": the refused write at offset " + std::to_string(off) + " was delivered"); }
    };
    judge("user", userTap, user);
    judge("character", characterTap, character);
    if (bad != 0) return 1;
    std::printf("CHECKED-WRITES-RP-VERIFY: OK refused=%zu+%zu\n", user.refused.size(), character.refused.size());
    std::fflush(stdout);
    return 0;
}

int runRpCompact() {
    // One accepted record per topic to rewrite: the first accepted login and the last accepted character of the highest user (the lifecycle removes the first of each user).
    const RpLine* login = nullptr;
    const RpLine* chosen = nullptr;
    const auto logins = readRpLines({"rp-login-w"});
    const auto creates = readRpLines({"rp-characters-w"});
    for (const RpLine& l : logins)
        if (l.code == 0 && login == nullptr) login = &l;
    for (const RpLine& l : creates)
        if (l.code == 0 && l.key < kRpPairBase && (chosen == nullptr || l.key >= chosen->key)) chosen = &l;
    if (login == nullptr || chosen == nullptr) { fail("CHECKED-WRITES-RP-COMPACT", "the result files hold no accepted login or character"); return 1; }
    Client c(makeConfig());
    c.connectAndWait();
    const auto state = latestCatalog(c);
    const int32_t userTopic      = state.topicByName(rune::catalog::kDefaultNsId, "user")->topicId;
    const int32_t characterTopic = state.topicByName(rune::catalog::kDefaultNsId, "character")->topicId;
    RpWriter w(c);
    RpUser u;
    u.userId   = login->id;
    u.provider = 1;
    std::snprintf(u.subject.data(), u.subject.size(), "%s", login->name.c_str());
    RpCharacter ch;
    ch.characterId = chosen->id;
    ch.userId      = chosen->key;
    ch.gender      = 1;
    std::snprintf(ch.name.data(), ch.name.size(), "%s", chosen->name.c_str());
    const auto clean = [&](int32_t topic) {
        const StatsResult r = c.getStatsAndWait(rune::protocol::client::StatsScope::TopicPartition, topic, 0, 10s);
        return r.status == StatsResult::kOk && r.get(rune::protocol::client::StatId::rejectedNotCompacted) == std::optional<int64_t>(0);
    };
    const auto wait = std::chrono::seconds(kWriteWaitS);
    const auto deadline = Clock::now() + std::chrono::seconds(envInt("KI_E2E_RP_COMPACT_S", 300));
    while (!(clean(userTopic) && clean(characterTopic))) {
        if (Clock::now() >= deadline) { fail("CHECKED-WRITES-RP-COMPACT", "rejectedNotCompacted never reached 0 on both topics"); return 1; }
        for (int i = 0; i < 64; ++i) {   // accepted rewrites of one key each: the segments seal and the cleaner has something to compact
            if (!w.users.updateAndWait(u, wait).ok() || !w.characters.updateAndWait(ch, wait).ok()) { fail("CHECKED-WRITES-RP-COMPACT", "an accepted update was refused"); return 1; }
        }
    }
    std::printf("CHECKED-WRITES-RP-COMPACT: OK\n");
    std::fflush(stdout);
    return 0;
}

// ---- Transactions ----------------------------------------------------------------------------------------------------

/// The ledger topic's record (hand-written like the test support's RpLedger: [blockLength:u16][version:u16] then the fields, little-endian).
struct TxnLedger {
    static constexpr std::string_view kSchemaName = "ledger";
    static constexpr uint16_t kVersion = 1, kBlockLength = 40;
    static const uint64_t kFingerprint;

    uint64_t entryId = 0, account = 0;
    int64_t  amount = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        std::memset(b, 0, encodedSize());
        std::memcpy(b, &kBlockLength, 2);
        std::memcpy(b + 2, &kVersion, 2);
        std::memcpy(b + 4, &entryId, 8);
        std::memcpy(b + 12, &account, 8);
        std::memcpy(b + 20, &amount, 8);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return entryId; }
};
const uint64_t TxnLedger::kFingerprint = rune::codec::fingerprint(rune::codec::DescriptorBuilder("ledger", 1)
    .add("entryId", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
    .add("account", rune::codec::PrimType::UInt64)
    .add("amount", rune::codec::PrimType::Int64)
    .addFixedStr("memo", 16)
    .build());
using TxnLedgerTyped = TypedTopic<TxnLedger, ProducerRef>;

constexpr int      kTxnCommitted = 6;           // transactions of two characters per user before account_stats (cap 12) is full
constexpr uint64_t kTxnEntryBase = 1'000'000'000;   // the entry ids of worker w, user u: (w + 1) * kTxnEntryBase + u * 1000 + k * 10 + {0, 1}; the overdraw's is + 900
constexpr int      kTxnWaitS = 120;

/// One transactional producer (acks All) over the character and ledger topics.
struct TxnWriter {
    Producer       producer;
    RpCharTyped    characters;
    TxnLedgerTyped ledger;
    TxnWriter(Client& c, const std::string& txnId)
        : producer(c.producer([&] {
              ProducerConfig pc;
              pc.acks                 = Acks::All;
              pc.lingerUs             = 1;
              pc.transactionalId      = txnId;
              pc.transactionTimeoutMs = 60'000;
              return pc;
          }())),
          characters(c.openTopicAndWait<RpCharacter>("character", producer, 60s)), ledger(c.openTopicAndWait<TxnLedger>("ledger", producer, 60s)) {
        producer.initTransactionsAndWait(std::chrono::seconds(kTxnWaitS));
    }
    ~TxnWriter() { producer.closeImmediately(); }
};

/// One transaction: a character per name of `names` for `user`, a ledger entry of `amount` on `account` per id from `firstEntry` (`entries` of them).
/// The line's code is 0 for a commit, the vote's code for a no, kClientErr when the answer was lost (the transaction is then aborted if it can be).
/// The line carries the transaction's start and end in epoch ms and the ids its creates were assigned.
int64_t epochMs() { return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(); }

RpLine txnRun(std::unique_ptr<TxnWriter>& w, Client& c, const std::string& txnId, uint64_t user, uint64_t account, const std::vector<std::string>& names,
              uint64_t firstEntry, int entries, int64_t amount, const std::string& tag) {
    RpLine l{"txn", user, kClientErr, 0, -1, tag, firstEntry};
    l.startMs = epochMs();
    try {
        w->producer.beginTransaction();
        uint64_t* assigned = &l.char1;
        for (const std::string& name : names) {
            RpCharacter ch;
            ch.userId = user;
            ch.gender = 1;
            std::snprintf(ch.name.data(), ch.name.size(), "%s", name.c_str());
            const WriteResult cr = w->characters.insertAndWait(ch, std::chrono::seconds(kTxnWaitS));
            if (!cr.ok()) throw std::runtime_error("a character write was refused with code " + std::to_string(cr.code()) + " status " + std::to_string(static_cast<int>(cr.status())));
            *assigned++ = cr.key();
        }
        for (int i = 0; i < entries; ++i) {
            TxnLedger e;
            e.entryId = firstEntry + static_cast<uint64_t>(i);
            e.account = account;
            e.amount  = amount;
            const WriteResult lr = w->ledger.upsertAndWait(e, std::chrono::seconds(kTxnWaitS));
            if (!lr.ok()) throw std::runtime_error("a ledger write was refused with code " + std::to_string(lr.code()) + " status " + std::to_string(static_cast<int>(lr.status())));
        }
        const TxnCommitResult r = w->producer.commitTransactionAndWait(std::chrono::seconds(kTxnWaitS));
        l.code         = r.ok ? 0 : r.code;
        l.constraintId = r.constraintId;
        l.offset       = r.failingOffset;
        l.endMs        = epochMs();
    } catch (const std::exception& e) {
        l.endMs = epochMs();   // the answer's arrival: not the abort and the writer rebuild below
        std::fprintf(stderr, "%s threw: %s\n", tag.c_str(), e.what());
        try { w->producer.abortTransactionAndWait(30s); } catch (const std::exception&) { w = std::make_unique<TxnWriter>(c, txnId); }   // a fenced or broken producer starts over
    }
    return l;
}

int runTxnSetup() {
    Client c(makeConfig());
    c.connectAndWait();
    const auto state = latestCatalog(c);
    const auto* character = state.topicByName(rune::catalog::kDefaultNsId, "character");
    const auto* ledger = state.topicByName(rune::catalog::kDefaultNsId, "ledger");
    if (character == nullptr || ledger == nullptr) { fail("CHECKED-WRITES-TXN-SETUP", "the character or the ledger topic is missing"); return 1; }
    for (const char* view : {"character_name", "account_stats", "balances"})
        if (state.viewByName(rune::catalog::kDefaultNsId, view) == nullptr) { fail("CHECKED-WRITES-TXN-SETUP", std::string("the view ") + view + " is missing"); return 1; }
    std::printf("TXN_CHARACTER_TOPIC_ID=%d\nTXN_LEDGER_TOPIC_ID=%d\nCHECKED-WRITES-TXN-SETUP: OK\n", character->topicId, ledger->topicId);
    std::fflush(stdout);
    return 0;
}

int runTxnRun(const Args& a) {
    Client c(makeConfig());
    c.connectAndWait();
    const int threads = static_cast<int>(envInt("KI_E2E_THREADS", 4));
    std::vector<std::vector<RpLine>> lines(static_cast<std::size_t>(threads));
    {
        Progress progress("txn-run", a.worker);
        std::vector<std::thread> ts;
        for (int t = 0; t < threads; ++t)
            ts.emplace_back([&, t] {
                const std::string txnId = "txn-probe-w" + std::to_string(a.worker) + "-t" + std::to_string(t);
                auto w = std::make_unique<TxnWriter>(c, txnId);
                for (uint64_t u = a.lo + static_cast<uint64_t>(t); u <= a.hi; u += static_cast<uint64_t>(threads)) {
                    const std::string base = "u" + std::to_string(u);   // the names are shared by the workers: they race for them
                    const uint64_t entries = static_cast<uint64_t>(a.worker + 1) * kTxnEntryBase + u * 1000;
                    for (int k = 0; k <= kTxnCommitted; ++k) {   // the seventh is the cap's: account_stats refuses it
                        const std::string tag = base + "k" + std::to_string(k);
                        lines[static_cast<std::size_t>(t)].push_back(txnRun(w, c, txnId, u, u, {tag + "a", tag + "b"}, entries + static_cast<uint64_t>(k) * 10, 2, 1, tag));
                        ++progress.done;
                    }
                    // One character for another user and a debit of the account's whole balance and more: balances refuses it.
                    const std::string tag = base + "od";
                    lines[static_cast<std::size_t>(t)].push_back(txnRun(w, c, txnId, kRpPairBase + u, u, {tag}, entries + 900, 1, -1000, tag));
                    ++progress.done;
                }
            });
        for (auto& t : ts) t.join();
    }
    std::vector<RpLine> all;
    for (const auto& l : lines) all.insert(all.end(), l.begin(), l.end());
    saveRpLines(c, "txn-run", a.worker, all);
    std::printf("CHECKED-WRITES-TXN-RUN: DONE worker=%d txns=%zu\n", a.worker, all.size());
    std::fflush(stdout);
    return 0;
}

/// What a read_committed reader of the character and ledger topics has seen.
struct TxnTap {
    std::mutex                  mu;
    std::map<std::string, uint64_t> names;   // name -> the id of the character that holds it
    std::set<uint64_t>          characters, entries;
    bool has(uint64_t characterId) { std::lock_guard<std::mutex> g(mu); return characters.contains(characterId); }
    bool hasEntry(uint64_t id) { std::lock_guard<std::mutex> g(mu); return entries.contains(id); }
    uint64_t idOf(const std::string& n) { std::lock_guard<std::mutex> g(mu); const auto it = names.find(n); return it == names.end() ? 0 : it->second; }
};

int runTxnVerify() {
    const auto lines = readRpLines({"txn-run-w"});
    int committed = 0, refused = 0, lost = 0;
    for (const RpLine& l : lines) {
        if (l.code == 0) ++committed;
        else if (l.code == kClientErr) ++lost;
        else if (l.code == kConstraintViolation) ++refused;
        else { fail("CHECKED-WRITES-TXN-VERIFY", l.name + " voted " + std::to_string(l.code)); return 1; }
    }
    if (committed == 0 || refused == 0) { fail("CHECKED-WRITES-TXN-VERIFY", "the result files hold no committed or no refused transaction"); return 1; }

    Client c(makeConfig());
    c.connectAndWait();
    RpWriter w(c);
    TxnTap tap;
    SubscribeOptions so;
    so.from      = From::earliest();
    so.isolation = Isolation::ReadCommitted;
    const TopicHandle ledger = c.topic("ledger");
    SubscriptionSet characterSet = c.subscribe(w.characters.handle(), AllPartitions{}, so, [&](const Record& r) {
        if (r.value.size() < 36) return;
        uint64_t id = 0;
        std::memcpy(&id, r.value.data() + 4, sizeof id);
        const char* name = reinterpret_cast<const char*>(r.value.data()) + 20;
        std::lock_guard<std::mutex> g(tap.mu);
        tap.names[std::string(name, strnlen(name, 16))] = id;
        tap.characters.insert(id);
    });
    SubscriptionSet ledgerSet = c.subscribe(ledger, AllPartitions{}, so, [&](const Record& r) {
        if (r.value.size() < 12) return;
        uint64_t id = 0;
        std::memcpy(&id, r.value.data() + 4, sizeof id);
        std::lock_guard<std::mutex> g(tap.mu);
        tap.entries.insert(id);
    });
    // The names are shared by the workers: a transaction is visible by its own character ids and ledger entries, not by its names.
    const auto names = [](const RpLine& l) { return l.name.find("od") != std::string::npos ? std::vector<std::string>{l.name} : std::vector<std::string>{l.name + "a", l.name + "b"}; };
    const auto entryCount = [](const RpLine& l) { return l.name.find("od") != std::string::npos ? 1 : 2; };
    const auto idsOf = [&](const RpLine& l) { return std::vector<uint64_t>{l.char1, l.char2}; };
    // A committed transaction's records are all delivered; wait for them, then a quiet window for a refused record that is going to land.
    const auto delivered = [&] {
        for (const RpLine& l : lines) {
            if (l.code != 0) continue;
            for (std::size_t i = 0; i < names(l).size(); ++i) if (!tap.has(idsOf(l)[i])) return false;
            for (int i = 0; i < entryCount(l); ++i) if (!tap.hasEntry(l.id + static_cast<uint64_t>(i))) return false;
        }
        return true;
    };
    for (const auto deadline = Clock::now() + std::chrono::seconds(envInt("KI_E2E_VERIFY_S", 300)); Clock::now() < deadline && !delivered();) std::this_thread::sleep_for(100ms);
    std::this_thread::sleep_for(5s);
    const auto state = latestCatalog(c);
    RpView nameView(state, "character_name");
    int bad = 0;
    std::map<uint64_t, std::pair<int64_t, int64_t>> perUser;   // the first user a transaction is for -> {characters, balance} it leaves visible
    std::map<std::string, std::string> nameOwner;               // a name -> the transaction that delivers it
    for (const RpLine& l : lines) {
        bool all = true, none = true;
        for (std::size_t i = 0; i < names(l).size(); ++i) {
            const bool v = idsOf(l)[i] != 0 && tap.has(idsOf(l)[i]);
            all = all && v;
            none = none && !v;
        }
        for (int i = 0; i < entryCount(l); ++i) { const bool v = tap.hasEntry(l.id + static_cast<uint64_t>(i)); all = all && v; none = none && !v; }
        if (l.code == 0 && !all) { ++bad; fail("CHECKED-WRITES-TXN-VERIFY", l.name + ": a committed transaction is not whole"); }
        else if (l.code != 0 && l.code != kClientErr && !none) { ++bad; fail("CHECKED-WRITES-TXN-VERIFY", l.name + ": a refused transaction is visible"); }
        else if (l.code == kClientErr && !all && !none) { ++bad; fail("CHECKED-WRITES-TXN-VERIFY", l.name + ": a transaction whose answer was lost is torn"); }
        if (!all) continue;
        // Each name is delivered by one transaction, and character_name maps it to that transaction's character.
        for (std::size_t i = 0; i < names(l).size(); ++i) {
            const std::string n = names(l)[i];
            const std::string owner = l.name + "@" + std::to_string(idsOf(l)[i]);
            if (!nameOwner.emplace(n, owner).second) { ++bad; fail("CHECKED-WRITES-TXN-VERIFY", "the name " + n + " is delivered by " + nameOwner[n] + " and " + owner); }
            if (tap.idOf(n) != idsOf(l)[i]) { ++bad; fail("CHECKED-WRITES-TXN-VERIFY", "the name " + n + " is held by character " + std::to_string(tap.idOf(n)) + ", not " + std::to_string(idsOf(l)[i])); }
            nameView.builder.clear();
            const auto row = rpRowWhenReady(c, nameView, keyBytes(nameView.builder.setString("name", n).key()), 0, 30s);
            if (!row || nameView.u(*row, "characterId") != idsOf(l)[i]) { ++bad; fail("CHECKED-WRITES-TXN-VERIFY", "character_name does not map " + n + " to character " + std::to_string(idsOf(l)[i])); }
        }
        if (l.name.find("od") == std::string::npos) { perUser[l.key].first += 2; perUser[l.key].second += 2; }
    }
    if (bad != 0) return 1;

    // The views agree with what is visible.
    RpView stats(state, "account_stats"), balances(state, "balances");
    for (const auto& [user, expect] : perUser) {
        stats.builder.clear();
        balances.builder.clear();
        const auto statsKey   = keyBytes(stats.builder.setUInt("userId", user).key());
        const auto balanceKey = keyBytes(balances.builder.setUInt("account", user).key());
        const auto [characters, balance] = expect;
        bool ok = false;
        for (const auto deadline = Clock::now() + 120s; Clock::now() < deadline && !ok; std::this_thread::sleep_for(200ms)) {
            const auto s = rpRowWhenReady(c, stats, statsKey, 0, 10s);
            const auto b = rpRowWhenReady(c, balances, balanceKey, 0, 10s);
            ok = s && b && static_cast<int64_t>(stats.u(*s, "characters")) == characters && static_cast<int64_t>(balances.u(*b, "balance")) == balance;
        }
        if (!ok) { ++bad; fail("CHECKED-WRITES-TXN-VERIFY", "user " + std::to_string(user) + ": account_stats or balances never reached " + std::to_string(characters) + " / " + std::to_string(balance)); }
    }
    if (bad != 0) return 1;
    std::printf("CHECKED-WRITES-TXN-VERIFY: OK committed=%d refused=%d lost=%d\n", committed, refused, lost);
    std::fflush(stdout);
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : envStr("RUNE_MODE");
    try {
        if (mode == "setup") return runSetup();
        if (mode == "race") {
            Args a;
            if (!parseArgs(argc, argv, a)) { std::fprintf(stderr, "usage: race --worker <i> --ids <a>-<b> [--tag <t>]\n"); return 2; }
            return runRace(a);
        }
        if (mode == "ai-setup") return runAiSetup();
        if (mode == "ai-backfill" || mode == "ai-create" || mode == "ai-remove") {
            Args a;
            if (!parseArgs(argc, argv, a)) { std::fprintf(stderr, "usage: ai-backfill --ids <a>-<b> [--step <s>] [--tag <t>] | ai-create --worker <i> --count <n> [--tag <t>] | ai-remove --ids <id>-<id>\n"); return 2; }
            return mode == "ai-backfill" ? runAiBackfill(a) : mode == "ai-create" ? runAiCreate(a) : runAiRemove(a);
        }
        if (mode == "ai-verify") return runAiVerify();
        if (mode == "ai-refusals") return runAiRefusals();
        if (mode == "txn-setup") return runTxnSetup();
        if (mode == "txn-verify") return runTxnVerify();
        if (mode == "txn-run") {
            Args a;
            if (!parseArgs(argc, argv, a)) { std::fprintf(stderr, "usage: txn-run --worker <i> --users <a>-<b>\n"); return 2; }
            return runTxnRun(a);
        }
        if (mode == "rp-setup") return runRpSetup();
        if (mode == "rp-verify") return runRpVerify();
        if (mode == "rp-compact") return runRpCompact();
        if (mode == "rp-login" || mode == "rp-characters" || mode == "rp-restart" || mode == "rp-lifecycle") {
            Args a;
            if (!parseArgs(argc, argv, a)) { std::fprintf(stderr, "usage: rp-login --worker <i> --subjects <a>-<b> | rp-characters --worker <i> --users <a>-<b> | rp-restart --users <a>-<b> | rp-lifecycle --users <a>-<b>\n"); return 2; }
            return mode == "rp-login" ? runRpLogin(a) : mode == "rp-characters" ? runRpCharacters(a) : mode == "rp-restart" ? runRpRestart(a) : runRpLifecycle(a);
        }
        if (mode == "lifecycle") return runLifecycle();
        if (mode == "verify") return runVerify();
        if (mode == "refusals") return runRefusals();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "CHECKED-WRITES-PROBE: %s threw: %s\n", mode.c_str(), e.what());
        return 1;
    }
    std::fprintf(stderr, "CHECKED-WRITES-PROBE: mode must be setup, race, lifecycle, verify, refusals, an ai-*, an rp-* or a txn-* mode (got '%s')\n", mode.c_str());
    return 2;
}
