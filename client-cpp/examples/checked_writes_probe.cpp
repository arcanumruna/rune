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
// Environment: RUNE_TARGET, RUNE_EGRESS_HOST + RUNE_EGRESS_PORT, RUNE_DATA_DIR, RUNE_USER / RUNE_PASSWORD_FILE /
// RUNE_TLS_CA_FILE, RUNE_TIMEOUT_MS, KI_E2E_SHARED (a writable directory shared between legs), KI_E2E_THREADS.

#include "ExampleAuth.h"
#include "ki_account.gen.h"

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
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
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
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

namespace {

constexpr const char* kTopicName = "ki_account";
constexpr uint32_t    kParts     = 4;
constexpr uint16_t    kInvalidRequest = 39;
constexpr uint64_t    kAbsentBase = 1'000'000;   // ids no leg ever inserts
constexpr int         kWriteWaitS = 120;         // one write's verdict, across a leader change
constexpr int         kClientErr  = 20000;       // result code of a write that threw

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
    std::string tag = "race";
};
bool parseArgs(int argc, char** argv, Args& a) {
    for (int i = 2; i < argc; ++i) {
        const std::string k = argv[i];
        if (i + 1 >= argc) return false;
        const std::string v = argv[++i];
        if (k == "--worker") a.worker = std::atoi(v.c_str());
        else if (k == "--tag") a.tag = v;
        else if (k == "--ids") {
            const auto dash = v.find('-');
            if (dash == std::string::npos) return false;
            a.lo = std::strtoull(v.c_str(), nullptr, 10);
            a.hi = std::strtoull(v.c_str() + dash + 1, nullptr, 10);
        } else return false;
    }
    return a.hi >= a.lo;
}

int runRace(const Args& a) {
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
        if (mode == "lifecycle") return runLifecycle();
        if (mode == "verify") return runVerify();
        if (mode == "refusals") return runRefusals();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "CHECKED-WRITES-PROBE: %s threw: %s\n", mode.c_str(), e.what());
        return 1;
    }
    std::fprintf(stderr, "CHECKED-WRITES-PROBE: mode must be setup, race, lifecycle, verify or refusals (got '%s')\n", mode.c_str());
    return 2;
}
