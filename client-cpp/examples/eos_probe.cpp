// examples/eos_probe.cpp — Phase 2b F Task 15: the exactly-once pipeline probe (docker/eos-e2e.sh).
//
// One binary, three modes (RUNE_MODE), all single-threaded: the Client runs in RunnerMode::Invoker, so every
// callback (group records, subscription records, completions) runs on this thread inside Client::doWork() and
// the probe needs no lock, no atomic and no hand-off between threads.
//
//   fill      a plain acked producer writes N records {id u64 LE, v u64 LE} (16 bytes) to eos_in, then writes
//             RUNE_COUNTS_FILE ("<partition> <records>" per line: every partition's end offset).
//   pipeline  a group consumer (RUNE_GROUP_ID, auto-commit off) on eos_in + a transactional producer
//             (RUNE_TXN_ID).  Per transaction: <= 500 input records, transformed v' = 2 * v, written to eos_out
//             on a path that rotates per transaction - send / batch / tryClaim / claimBatch / typed claim -
//             then sendOffsetsToTransactionAndWait, then commit.  Every 7th transaction is aborted on purpose
//             (flush, abort, seekToCommittedAndWait, redo).  Exits 0 when the committed input offsets reach the
//             end offsets of RUNE_COUNTS_FILE; exits 4 printing "EOS-PIPELINE: Fenced" when a newer producer
//             with the same transactional id took over (the zombie leg).
//   verify    a read_committed reader of eos_out: count == N, every id exactly once, every v' == 2 * v;
//             prints "EOS-VERIFY: OK count=<N>".
//   selfcheck no broker: the transform and the id-uniqueness checker against synthetic data.
//
// eos_out is a typed topic; its generated header (eos_out.gen.h, type rune::gen::EosOut) comes from
// `rune schema generate eos_out` in the e2e script, so its fingerprint is the broker's by construction.  Every
// path writes the same typed record bytes.
//
// Environment (the connection ones as rune_produce_subscribe):
//   RUNE_TARGET, RUNE_EGRESS, RUNE_DATA_DIR, RUNE_USER / RUNE_PASSWORD_FILE   the cluster seed and the login
//   RUNE_CLIENT_TOPIC_TABLE  "eos_in:0:8,eos_out:1:8" (default)
//   RUNE_EOS_N               records (default 200000)
//   RUNE_COUNTS_FILE         fill writes it, pipeline reads it
//   RUNE_TXN_ID              transactional id (default eos-worker-1)
//   RUNE_GROUP_ID            consumer group (default eos-1)
//   RUNE_PROGRESS_FILE       pipeline: the sum of the committed input offsets, rewritten after every commit
//   RUNE_TXN_PAUSE_MS        pipeline: pause after every commit (default 0)
//   RUNE_PIPELINE_TIMEOUT_S  pipeline: hard cap (default 600)
//   RUNE_TIMEOUT_MS          per-operation timeout (default 20000)

#include "ExampleAuth.h"
#include "eos_out.gen.h"   // rune::gen::EosOut {id int64 key, v int64}

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace rune::client;
using Clock = std::chrono::steady_clock;

namespace {

// ---- the record contract and its checker (shared by every mode and by selfcheck) ---------------------------------

constexpr std::size_t kInBytes     = 16;    // {id u64 LE, v u64 LE}
constexpr uint32_t    kOutBytes    = rune::gen::EosOutEncoder::kEncodedLength;   // 4-byte header + {id, v}
constexpr int         kTxnRecords  = 500;
constexpr int         kAbortEvery  = 7;
constexpr int         kPaths       = 5;
constexpr int         kMaxParts    = 64;
constexpr const char* kRejoinText  = "rebalancing (rejoin)";   // GroupConsumer::seekToCommittedAndWait's Rejoined text

int64_t valueOf(int64_t id) noexcept { return id * 7 + 3; }      // what fill writes
int64_t transform(int64_t v) noexcept { return v * 2; }          // what the pipeline writes

const char* pathName(int p) noexcept {
    static const char* const n[kPaths] = {"send", "batch", "tryClaim", "claimBatch", "typedClaim"};
    return n[p % kPaths];
}

// Exactly-once checker: every id in [0, n) seen once, with v' == transform(valueOf(id)).
class EosChecker {
public:
    enum class Add { Ok, Duplicate, OutOfRange, WrongValue };
    explicit EosChecker(int64_t n) : seen_(static_cast<std::size_t>(n), 0), n_(n) {}
    Add add(int64_t id, int64_t out) noexcept {
        if (id < 0 || id >= n_) return Add::OutOfRange;
        if (out != transform(valueOf(id))) return Add::WrongValue;
        uint8_t& s = seen_[static_cast<std::size_t>(id)];
        if (s != 0) return Add::Duplicate;
        s = 1;
        ++count_;
        return Add::Ok;
    }
    [[nodiscard]] int64_t count() const noexcept { return count_; }
    [[nodiscard]] int64_t missing() const noexcept { return n_ - count_; }
    [[nodiscard]] int64_t firstMissing() const noexcept {
        for (int64_t i = 0; i < n_; ++i) if (seen_[static_cast<std::size_t>(i)] == 0) return i;
        return -1;
    }
private:
    std::vector<uint8_t> seen_;
    int64_t              n_;
    int64_t              count_ = 0;
};

void putU64(uint8_t* p, uint64_t v) noexcept { std::memcpy(p, &v, 8); }   // little-endian hosts only (x86-64 / arm64)
uint64_t getU64(const uint8_t* p) noexcept { uint64_t v; std::memcpy(&v, p, 8); return v; }

// ---- the consumer-side decision core (pure: unit-tested by selfcheck) -------------------------------------------
// Per input partition.  `committed` is the last offset known committed and is NEVER forgotten or lowered by a
// rebalance; `nextAppend` is the strict filter (-1: not learned yet); `rebase` = the baseline must be learned again
// from the broker (after a rebalance of that partition or an in-doubt commit): the first record delivered after
// the rewind is the baseline, and one beyond `hi` (the most the broker can have committed) is a Gap, reported loudly.
struct PartState {
    int64_t committed  = -1;
    int64_t nextAppend = -1;
    int64_t hi         = INT64_MAX;
    bool    seen       = false;     // this process ever received a record of / committed this partition
    bool    rebase     = false;
};
enum class Accept { Take, Drop, Gap };

// RULE: no record at or past the committed offset is ever silently dropped.  The rewind is applied when it is issued, so
// everything delivered after that is the new instance, starting at the broker's committed offset: the first record is
// the baseline.  A record ahead of the expected offset is a Gap (loud), a record behind it a duplicate (dropped).
Accept onRecord(PartState& s, int64_t off) noexcept {
    if (s.rebase || s.nextAppend < 0) {                          // learn the baseline
        if (s.rebase && off > s.hi) return Accept::Gap;
        s.committed = off; s.seen = true; s.rebase = false; s.hi = INT64_MAX; s.nextAppend = off + 1;
        return Accept::Take;
    }
    if (off > s.nextAppend) return Accept::Gap;                  // would skip records
    if (off < s.nextAppend) return Accept::Drop;                 // a duplicate (a re-delivery below the position)
    s.nextAppend = off + 1;
    return Accept::Take;
}
// A rebalance of the partition, or an in-doubt commit that may have advanced it to `hi`: committed stays.
void markRebase(PartState& s, int64_t hi = INT64_MAX) noexcept { s.rebase = true; s.hi = hi; s.nextAppend = -1; }
// Before a rewind: the strict filter restarts at the known committed offset (a partition to relearn stays unlearned).
void beforeRewind(PartState& s) noexcept { s.nextAppend = s.rebase ? -1 : s.committed; }
// The exit rule: every partition's committed offset reached its end.  A partition this process never saw anything
// of is counted done only after a quiet period (it was fully committed before this process started); a partition
// it has seen is never done before its committed offset reaches the end.
// Also "unknown": a partition whose in-doubt commit may have landed on its last batch (hi == end): when it did, the
// rewind re-seeds at the end and nothing is ever delivered.
bool allDone(const PartState* st, const int64_t* ends, std::size_t n, int64_t idleMs) noexcept {
    bool unknown = false;
    for (std::size_t p = 0; p < n; ++p) {
        if (ends[p] == 0 || st[p].committed >= ends[p]) continue;
        if (!st[p].seen || (st[p].rebase && st[p].hi == ends[p])) { unknown = true; continue; }
        return false;
    }
    return !unknown || idleMs > 15000;
}

int selfcheck() {
    int bad = 0;
    auto expect = [&](bool ok, const char* what) { if (!ok) { std::fprintf(stderr, "EOS-SELFCHECK: FAIL %s\n", what); ++bad; } };
    EosChecker c(4);
    expect(c.add(0, transform(valueOf(0))) == EosChecker::Add::Ok, "id 0 accepted");
    expect(c.add(0, transform(valueOf(0))) == EosChecker::Add::Duplicate, "id 0 twice is a duplicate");
    expect(c.add(1, valueOf(1)) == EosChecker::Add::WrongValue, "an untransformed value is refused");
    expect(c.add(4, 0) == EosChecker::Add::OutOfRange, "id == n is out of range");
    expect(c.add(-1, 0) == EosChecker::Add::OutOfRange, "a negative id is out of range");
    for (int64_t i = 1; i < 4; ++i) expect(c.add(i, transform(valueOf(i))) == EosChecker::Add::Ok, "ids 1..3 accepted");
    expect(c.count() == 4 && c.missing() == 0 && c.firstMissing() == -1, "all four seen exactly once");
    uint8_t in[kInBytes];
    putU64(in, 41); putU64(in + 8, static_cast<uint64_t>(valueOf(41)));
    expect(static_cast<int64_t>(getU64(in)) == 41 && static_cast<int64_t>(getU64(in + 8)) == valueOf(41), "input record round trip");
    uint8_t out[kOutBytes];
    rune::gen::EosOutEncoder(std::span<uint8_t>(out, sizeof out)).id(41).v(transform(valueOf(41)));
    const rune::gen::EosOutDecoder d(std::span<const uint8_t>(out, sizeof out));
    expect(d.id() == 41 && d.v() == transform(valueOf(41)), "typed record round trip");
    {   // I2: committed survives a rebalance; the idle shortcut is for never-seen partitions only
        PartState st[2];
        const int64_t ends[2] = {10, 10};
        expect(onRecord(st[0], 0) == Accept::Take && st[0].committed == 0, "first record is the baseline");
        expect(onRecord(st[0], 0) == Accept::Drop, "a duplicate is dropped");
        expect(onRecord(st[0], 2) == Accept::Gap, "a record ahead of the expected offset is a loud gap, never dropped");
        st[0].committed = 6;
        markRebase(st[0]);
        expect(st[0].committed == 6 && st[0].rebase, "a rebalance keeps committed");
        expect(!allDone(st, ends, 2, 60000), "a seen partition below its end is never done, however long idle");
        st[0].committed = 10;
        expect(!allDone(st, ends, 2, 1000) && allDone(st, ends, 2, 16000), "a never-seen partition is done only after 15 s idle");
        st[1].committed = 10;
        expect(allDone(st, ends, 2, 0), "all committed to the end");
    }
    {   // N1: a rewind followed immediately by records at the committed offset loses none of them
        PartState s;
        onRecord(s, 0); onRecord(s, 1); onRecord(s, 2); onRecord(s, 3);      // consumed 0..3, queued
        s.committed = 2;                                                     // committed through offset 1
        beforeRewind(s);                                                     // the rewind is issued: filter restarts at 2
        expect(onRecord(s, 2) == Accept::Take && onRecord(s, 3) == Accept::Take && s.nextAppend == 4, "records at the committed offset are taken");
        PartState r;                                                         // a partition to relearn
        onRecord(r, 0); r.committed = 5; markRebase(r);
        beforeRewind(r);
        expect(onRecord(r, 5) == Accept::Take && r.committed == 5 && !r.rebase, "right after a rebase rewind the first record is the baseline");
    }
    {   // I3: an in-doubt commit relearns the baseline from the broker
        PartState s;
        onRecord(s, 0); s.committed = 5; s.nextAppend = 12;                  // local view: committed 5, batch ends at 9
        markRebase(s, 9);                                                    // the commit may have advanced to 9
        beforeRewind(s);
        expect(s.nextAppend == -1, "a relearning partition has no filter");
        PartState landed = s;
        expect(onRecord(landed, 9) == Accept::Take && landed.committed == 9 && landed.nextAppend == 10, "commit landed: resume at 9");
        PartState notLanded = s;
        expect(onRecord(notLanded, 5) == Accept::Take && notLanded.committed == 5, "commit not landed: redo from 5");
        PartState gap = s;
        expect(onRecord(gap, 11) == Accept::Gap, "a first record beyond the batch end is a loud gap");
        // Minor 1: the batch was the partition's last (hi == end) and the commit landed: nothing is ever delivered.
        PartState last;
        onRecord(last, 0); last.committed = 5; markRebase(last, 10);
        const int64_t e1[1] = {10};
        expect(!allDone(&last, e1, 1, 1000) && allDone(&last, e1, 1, 16000), "in-doubt on the last batch terminates after the idle window");
    }
    {   // N2: a Rejoined rewind is benign, every other failure is loud
        expect(std::strstr("seekToCommitted() aborted: the group is rebalancing (rejoin)", kRejoinText) != nullptr, "the Rejoined text is recognised");
        expect(std::strstr("seekToCommitted() timed out", kRejoinText) == nullptr, "a Timeout is not");
        expect(std::strstr("seekToCommitted() aborted: the session to the broker was lost", kRejoinText) == nullptr, "SessionLost is not");
    }
    for (int p = 0; p < kPaths; ++p) expect(pathName(p) != nullptr, "path names");
    if (bad == 0) std::printf("EOS-SELFCHECK: OK\n");
    return bad == 0 ? 0 : 1;
}

// ---- environment ---------------------------------------------------------------------------------------------------

std::string envStr(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::string(v) : def;
}
long envInt(const char* name, long def) {
    const char* v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::strtol(v, nullptr, 10) : def;
}

std::vector<TopicSpec> parseTopics(const std::string& s) {
    std::vector<TopicSpec> out;
    std::size_t pos = 0;
    while (pos <= s.size()) {
        const std::size_t comma = s.find(',', pos);
        const std::string tok = s.substr(pos, (comma == std::string::npos ? s.size() : comma) - pos);
        pos = comma == std::string::npos ? s.size() + 1 : comma + 1;
        const auto c1 = tok.find(':');
        const auto c2 = c1 == std::string::npos ? c1 : tok.find(':', c1 + 1);
        if (c2 == std::string::npos) continue;
        TopicSpec ts;
        ts.name           = tok.substr(0, c1);
        ts.topicId        = std::atoi(tok.c_str() + c1 + 1);
        ts.partitionCount = std::atoi(tok.c_str() + c2 + 1);
        out.push_back(std::move(ts));
    }
    return out;
}

void pump(Client& c) { if (c.doWork() == 0) std::this_thread::yield(); }
void pumpFor(Client& c, int ms) {
    const auto end = Clock::now() + std::chrono::milliseconds(ms);
    while (Clock::now() < end) pump(c);
}

void fail(const char* tag, const std::string& msg) { std::fprintf(stderr, "%s: FAIL %s\n", tag, msg.c_str()); }

// ---- fill ----------------------------------------------------------------------------------------------------------

int runFill(Client& c, const TopicHandle& in, int64_t n, int timeoutMs) {
    ProducerConfig pc;
    pc.acks             = Acks::All;
    pc.produceTimeoutMs = timeoutMs;
    Producer p = c.producer(pc);
    Completions cq(c);
    std::vector<int64_t> perPart(static_cast<std::size_t>(in.partitionCount), 0);
    int64_t acked = 0, failed = 0;
    auto drain = [&] {
        (void)cq.poll([&](const Completion& done, std::span<const uint8_t>) {
            if (done.status == OpStatus::Ok) ++acked; else ++failed;
        });
    };
    for (int64_t id = 0; id < n;) {
        uint8_t rec[kInBytes];
        putU64(rec, static_cast<uint64_t>(id));
        putU64(rec + 8, static_cast<uint64_t>(valueOf(id)));
        const Key key(id);
        const SendResult r = p.trySend(in, key, std::span<const uint8_t>(rec, sizeof rec), cq, static_cast<Token>(id + 1));
        if (r == SendResult::Ok) { ++perPart[static_cast<std::size_t>(key.partition(in.partitionCount))]; ++id; continue; }
        if (r != SendResult::BackPressured && r != SendResult::QueueFull) {
            fail("EOS-FILL", "send refused, result " + std::to_string(static_cast<int>(r)));
            return 1;
        }
        pump(c);
        drain();
        if (failed != 0) { fail("EOS-FILL", std::to_string(failed) + " records failed"); return 1; }
    }
    const auto deadline = Clock::now() + std::chrono::seconds(300);
    while (acked + failed < n && Clock::now() < deadline) { pump(c); drain(); }
    if (failed != 0 || acked != n) {
        fail("EOS-FILL", "acked=" + std::to_string(acked) + " failed=" + std::to_string(failed) + " of " + std::to_string(n));
        return 1;
    }
    const std::string counts = envStr("RUNE_COUNTS_FILE");
    if (!counts.empty()) {
        std::ofstream f(counts, std::ios::trunc);
        for (std::size_t i = 0; i < perPart.size(); ++i) f << i << ' ' << perPart[i] << '\n';
        if (!f) { fail("EOS-FILL", "cannot write " + counts); return 1; }
    }
    std::printf("EOS-FILL: OK count=%lld\n", static_cast<long long>(acked));
    return 0;
}

// ---- pipeline ------------------------------------------------------------------------------------------------------

struct InRec { int32_t part; int64_t off; int64_t id; int64_t v; };

// The consumer side's state, owned by the one thread.  Per input partition: the first offset ever seen (== the
// committed offset the group started at), the committed offset, and the next offset the strict filter accepts -
// a record is queued only if it is exactly the next one, so a re-delivery after a rewind (or an in-flight record
// of a dropped fetch) is never queued twice and nothing is ever skipped.
struct InState {
    std::array<PartState, kMaxParts> part{};
    std::vector<InRec>               q;               // records not yet in a transaction: q[head..]
    std::size_t                      head = 0;
    int64_t                          rebalances = 0;  // onAssigned / onRevoked count
    std::string                      error;           // a Gap: the run must stop with this text
    InState() { q.reserve(1u << 18); }
    // A rebalance: queued records are stale, every partition handed over relearns its baseline (committed stays).
    void rebalance(const std::vector<TopicPartition>& parts) {
        for (const TopicPartition& tp : parts) {
            if (tp.partition >= 0 && static_cast<std::size_t>(tp.partition) < part.size()) markRebase(part[static_cast<std::size_t>(tp.partition)]);
        }
        q.clear(); head = 0; ++rebalances;
    }
    [[nodiscard]] std::size_t pending() const noexcept { return q.size() - head; }
    void compact() { if (head == q.size()) { q.clear(); head = 0; } }
};

enum class Outcome { Committed, Aborted, Fenced };

class Pipeline {
public:
    Pipeline(Client& c, GroupConsumer& gc, Producer& tp, rune::client::TypedTopic<rune::gen::EosOut, ProducerRef>& typed,
             TopicHandle in, TopicHandle out, std::string groupId, std::vector<int64_t> ends, InState& st)
        : c_(c), gc_(gc), tp_(tp), typed_(typed), in_(in), out_(out), group_(std::move(groupId)), ends_(std::move(ends)), st_(st) {
        batch_.reserve(kTxnRecords);
        entries_.reserve(kMaxParts);
        for (auto& v : byPart_) v.reserve(kTxnRecords);
    }

    int run(int pauseMs, long timeoutS, const std::string& progressFile) {
        const auto deadline = Clock::now() + std::chrono::seconds(timeoutS);
        auto idleSince = Clock::now();
        while (!done(Clock::now() - idleSince)) {
            if (Clock::now() > deadline) { fail("EOS-PIPELINE", "timed out after " + std::to_string(timeoutS) + " s"); return 1; }
            if (!st_.error.empty()) { fail("EOS-PIPELINE", st_.error); return 1; }
            if (gc_.finished()) { fail("EOS-PIPELINE", "the group consumer ended"); return 1; }
            pump(c_);
            if (st_.pending() == 0) continue;
            idleSince = Clock::now();
            gather();
            const Outcome o = transaction();
            if (o == Outcome::Fenced) {
                std::printf("EOS-PIPELINE: Fenced after %lld commits, %lld aborts (a newer producer holds '%s')\n",
                            static_cast<long long>(commits_), static_cast<long long>(aborts_), txnId_.c_str());
                std::fflush(stdout);
                return 4;
            }
            writeProgress(progressFile);
            if (pauseMs > 0) pumpFor(c_, pauseMs);
        }
        std::printf("EOS-PIPELINE: DONE commits=%lld aborts=%lld records=%lld rebalances=%lld\n",
                    static_cast<long long>(commits_), static_cast<long long>(aborts_), static_cast<long long>(records_),
                    static_cast<long long>(st_.rebalances));
        std::fflush(stdout);
        return 0;
    }
    void setTxnId(std::string id) { txnId_ = std::move(id); }

private:
    bool done(Clock::duration idle) const {
        return allDone(st_.part.data(), ends_.data(), ends_.size(),
                       std::chrono::duration_cast<std::chrono::milliseconds>(idle).count());
    }

    void gather() {
        batch_.clear();
        while (st_.pending() > 0 && static_cast<int>(batch_.size()) < kTxnRecords) batch_.push_back(st_.q[st_.head++]);
        st_.compact();
    }

    Outcome transaction() {
        ++attempt_;
        const int path = static_cast<int>(attempt_ % kPaths);
        const bool abortOnPurpose = attempt_ % kAbortEvery == 0;
        const int64_t epoch = st_.rebalances;
        entries_.clear();
        for (const InRec& r : batch_) {
            auto it = std::find_if(entries_.begin(), entries_.end(), [&](const OffsetEntry& e) { return e.partition == r.part; });
            if (it == entries_.end()) entries_.push_back(OffsetEntry{in_.topicId, r.part, r.off + 1});
            else it->offset = std::max<int64_t>(it->offset, r.off + 1);
        }
        commitIssued_ = false;
        try {
            tp_.beginTransaction();
            produce(path);
            tp_.sendOffsetsToTransactionAndWait(entries_, group_);
            if (abortOnPurpose || st_.rebalances != epoch) {
                // Records of an aborted transaction must reach the log (they stay invisible to read_committed).
                tp_.flushAndWait();
                tp_.abortTransactionAndWait();
                return rewound(Outcome::Aborted, abortOnPurpose ? "on purpose" : "rebalance", false);
            }
            commitIssued_ = true;
            tp_.commitTransactionAndWait();
        } catch (const std::exception& ex) {
            const std::string why = ex.what();   // the exception object ends with this block
            return resolve(why);
        }
        return committed(epoch);
    }

    Outcome committed(int64_t epoch) {
        for (const OffsetEntry& e : entries_) {
            PartState& s = st_.part[static_cast<std::size_t>(e.partition)];
            s.committed = e.offset; s.seen = true;
        }
        ++commits_;
        records_ += static_cast<int64_t>(batch_.size());
        if (st_.rebalances != epoch) {   // a rebalance during the transaction may have fetched the old offsets
            rewindConsumer();            // a failed rewind propagates: exit 1 with its cause, never a silent stall
        }
        return Outcome::Committed;
    }

    // The transaction threw: decide from the producer's state what happened (a commit whose answer was lost is
    // retried - the same commit is idempotent), then abort + rewind everything else.
    Outcome resolve(std::string why) {
        for (int attempt = 0; attempt < 20; ++attempt) {
            const TxnState s = tp_.state();
            try {
                switch (s) {
                case TxnState::FatalError:
                    // A fence is the only fatal error the zombie leg may report as such.
                    if (why.find("enced") == std::string::npos) {
                        // A fence first seen on a produce path has a generic text: in FatalError every op completes
                        // with the stored status, so one more op names the cause.
                        try { tp_.abortTransactionAndWait(); } catch (const std::exception& e) { why = e.what(); }
                    }
                    if (why.find("enced") != std::string::npos) return Outcome::Fenced;
                    throw std::runtime_error("fatal producer error (not a fence): " + why);
                case TxnState::CommittingTransaction:
                    commitIssued_ = true;
                    tp_.commitTransactionAndWait();
                    return committed(st_.rebalances);
                case TxnState::InTransaction:
                case TxnState::AbortableError:
                case TxnState::AbortingTransaction:
                    tp_.abortTransactionAndWait();
                    return rewound(Outcome::Aborted, why, false);
                case TxnState::Ready:
                    // Ready right after a commit call that threw: the commit may have landed. In doubt.
                    return rewound(Outcome::Aborted, why, commitIssued_);
                default:
                    break;   // Uninitialized / Initializing: not expected here; pump and look again
                }
            } catch (const std::logic_error&) {
                throw;
            } catch (const std::exception& ex) {
                if (s == TxnState::FatalError) throw;
                why = ex.what();   // copied into the string before the exception ends
            }
            pumpFor(c_, 200);
        }
        throw std::runtime_error("transaction unresolved: " + why);
    }

    // inDoubt: a commit call threw and the producer is Ready - the broker may or may not have the offsets of this
    // batch.  The partitions of the batch relearn their baseline from the broker (seekToCommitted decides), never
    // from the probe's local offsets; a first record beyond the batch's end offset is a loud Gap.
    Outcome rewound(Outcome o, const std::string& why, bool inDoubt) {
        ++aborts_;
        std::fprintf(stderr, "EOS-PIPELINE: transaction %lld (%s) aborted (%s)%s, rewinding\n", static_cast<long long>(attempt_),
                     pathName(static_cast<int>(attempt_ % kPaths)), why.c_str(), inDoubt ? ", commit in doubt" : "");
        if (inDoubt) for (const OffsetEntry& e : entries_) markRebase(st_.part[static_cast<std::size_t>(e.partition)], e.offset);
        rewindConsumer();
        return o;
    }

    static bool isRejoin(const char* what) { return std::strstr(what, kRejoinText) != nullptr; }

    void rewindConsumer() {
        // Strict filter first: whatever the old fetches still push is dropped unless it is exactly the next offset.
        for (PartState& p : st_.part) beforeRewind(p);
        st_.q.clear();
        st_.head = 0;
        try {
            gc_.seekToCommittedAndWait();
        } catch (const std::runtime_error& ex) {
            if (!isRejoin(ex.what())) throw;   // Timeout / SessionLost / Closed / ...: loud
            // Rejoined: the rejoin re-seeds every position from the committed offsets itself and the rebalance
            // callbacks mark the partitions handed over for a new baseline: nothing left to do here.
        }
    }

    // ---- the five produce paths: the same typed record bytes ----------------------------------------------------
    void encodeOut(uint8_t* dst, const InRec& r) const {
        rune::gen::EosOutEncoder(std::span<uint8_t>(dst, kOutBytes)).id(r.id).v(transform(r.v));
    }
    void produce(int path) {
        switch (path) {
        case 0: sendPath(); break;
        case 1: batchPath(); break;
        case 2: tryClaimPath(); break;
        case 3: claimBatchPath(); break;
        default: typedClaimPath(); break;
        }
    }
    void splitByPartition() {
        for (auto& v : byPart_) v.clear();
        for (std::size_t i = 0; i < batch_.size(); ++i) {
            const int part = Key(batch_[i].id).partition(out_.partitionCount);
            byPart_[static_cast<std::size_t>(part)].push_back(static_cast<uint32_t>(i));
        }
    }
    void sendPath() {
        uint8_t buf[kOutBytes];
        for (const InRec& r : batch_) {
            encodeOut(buf, r);
            tp_.send(out_, Key(r.id), std::span<const uint8_t>(buf, sizeof buf));
        }
    }
    void batchPath() {
        splitByPartition();
        uint8_t buf[kOutBytes];
        for (const auto& idx : byPart_) {
            if (idx.empty()) continue;
            for (;;) {   // a refused / back-pressured batch is rebuilt whole
                BatchBuilder b = tp_.batch(out_, Key(batch_[idx[0]].id));
                for (const uint32_t i : idx) { encodeOut(buf, batch_[i]); b.add(std::span<const uint8_t>(buf, sizeof buf)); }
                const OpResult r = b.send();
                if (r == OpResult::Accepted) break;
                if (r != OpResult::BackPressured) throw std::runtime_error("batch send refused, result " + std::to_string(static_cast<int>(r)));
                pump(c_);
            }
        }
    }
    void tryClaimPath() {
        for (const InRec& r : batch_) {
            for (;;) {
                Claim cl = tp_.tryClaim(out_, Key(r.id), kOutBytes);
                if (cl) { encodeOut(cl.buffer().data(), r); cl.commit(); break; }
                if (cl.result() != SendResult::BackPressured) throw std::runtime_error("tryClaim refused, result " + std::to_string(static_cast<int>(cl.result())));
                pump(c_);
            }
        }
    }
    void claimBatchPath() {
        splitByPartition();
        for (const auto& idx : byPart_) {
            if (idx.empty()) continue;
            for (;;) {
                BatchClaim bc = tp_.claimBatch(out_, Key(batch_[idx[0]].id), static_cast<uint32_t>(idx.size()), kOutBytes);
                if (bc) {
                    for (uint32_t k = 0; k < idx.size(); ++k) encodeOut(bc.record(k).data(), batch_[idx[k]]);
                    bc.commit();
                    break;
                }
                if (bc.result() != SendResult::BackPressured) throw std::runtime_error("claimBatch refused, result " + std::to_string(static_cast<int>(bc.result())));
                pump(c_);
            }
        }
    }
    void typedClaimPath() {
        for (const InRec& r : batch_) {
            for (;;) {
                auto w = typed_.claim(r.id);
                if (w) { w.v(transform(r.v)); w.commit(); break; }
                if (w.result() != SendResult::BackPressured) throw std::runtime_error("typed claim refused, result " + std::to_string(static_cast<int>(w.result())));
                pump(c_);
            }
        }
    }

    void writeProgress(const std::string& file) const {
        if (file.empty()) return;
        int64_t sum = 0;
        for (const PartState& p : st_.part) if (p.committed > 0) sum += p.committed;
        const std::string tmp = file + ".tmp";
        { std::ofstream f(tmp, std::ios::trunc); f << sum << '\n'; }
        std::rename(tmp.c_str(), file.c_str());
    }

    Client&        c_;
    GroupConsumer& gc_;
    Producer&      tp_;
    rune::client::TypedTopic<rune::gen::EosOut, ProducerRef>& typed_;
    TopicHandle    in_, out_;
    std::string    group_, txnId_;
    std::vector<int64_t>  ends_;
    InState&       st_;
    std::vector<InRec>        batch_;
    std::vector<OffsetEntry>  entries_;
    std::array<std::vector<uint32_t>, kMaxParts> byPart_;
    bool commitIssued_ = false;
    int64_t attempt_ = 0, commits_ = 0, aborts_ = 0, records_ = 0;
};

std::vector<int64_t> readCounts(const std::string& file, int parts) {
    std::vector<int64_t> ends(static_cast<std::size_t>(parts), 0);
    std::ifstream f(file);
    long p, n;
    while (f >> p >> n) if (p >= 0 && p < parts) ends[static_cast<std::size_t>(p)] = n;
    return ends;
}

int runPipeline(Client& c, const TopicHandle& in, const std::string& groupId, const std::string& txnId, InState& st,
                GroupConsumer& gc, Producer& tp) {
    c.connectAndWait();
    tp.initTransactionsAndWait();
    auto typed = c.openTopicAndWait<rune::gen::EosOut>("eos_out", tp);
    const TopicHandle out = typed.handle();
    if (out.topicId != c.topic("eos_out").topicId) {   // the table's id is hard-coded: creation order in eos-e2e.rql
        fail("EOS-PIPELINE", "eos_out is topic " + std::to_string(out.topicId) + " on the broker, RUNE_CLIENT_TOPIC_TABLE says " + std::to_string(c.topic("eos_out").topicId));
        return 1;
    }
    const std::string countsFile = envStr("RUNE_COUNTS_FILE");
    if (countsFile.empty()) { fail("EOS-PIPELINE", "RUNE_COUNTS_FILE is required"); return 1; }
    Pipeline pl(c, gc, tp, typed, in, out, groupId, readCounts(countsFile, in.partitionCount), st);
    pl.setTxnId(txnId);
    std::printf("EOS-PIPELINE: READY txnId=%s group=%s pid=%lld epoch=%d\n", txnId.c_str(), groupId.c_str(),
                static_cast<long long>(tp.producerId()), static_cast<int>(tp.producerEpoch()));
    std::fflush(stdout);
    return pl.run(static_cast<int>(envInt("RUNE_TXN_PAUSE_MS", 0)), envInt("RUNE_PIPELINE_TIMEOUT_S", 600), envStr("RUNE_PROGRESS_FILE"));
}

// ---- verify --------------------------------------------------------------------------------------------------------

int runVerify(Client& c, const TopicHandle& out, int64_t n) {
    EosChecker chk(n);
    int64_t total = 0, dups = 0, bad = 0;
    std::string firstProblem;
    SubscribeOptions opts;
    opts.from      = From::earliest();
    opts.isolation = Isolation::ReadCommitted;
    std::vector<Subscription> subs;
    subs.reserve(static_cast<std::size_t>(out.partitionCount));
    for (int32_t p = 0; p < out.partitionCount; ++p) {
        subs.push_back(c.subscribe(out, p, opts, [&](const Record& r) {
            ++total;
            if (r.value.size() < kOutBytes) { ++bad; if (firstProblem.empty()) firstProblem = "short record"; return; }
            const rune::gen::EosOutDecoder d(r.value);
            switch (chk.add(d.id(), d.v())) {
            case EosChecker::Add::Ok: break;
            case EosChecker::Add::Duplicate: ++dups; if (firstProblem.empty()) firstProblem = "duplicate id " + std::to_string(d.id()); break;
            case EosChecker::Add::OutOfRange: ++bad; if (firstProblem.empty()) firstProblem = "id out of range " + std::to_string(d.id()); break;
            case EosChecker::Add::WrongValue: ++bad; if (firstProblem.empty()) firstProblem = "wrong v' for id " + std::to_string(d.id()); break;
            }
        }));
    }
    // Complete once every id is in, then a quiet window to catch an extra (duplicate or aborted-record) arrival.
    const auto deadline = Clock::now() + std::chrono::seconds(300);
    while (chk.count() < n && dups == 0 && bad == 0 && Clock::now() < deadline) pump(c);
    pumpFor(c, 5000);
    for (auto& s : subs) s.unsubscribe();
    // Proof that aborted data was in the log and read_committed filtered it: a read_uncommitted count must exceed N.
    int64_t uncommitted = 0;
    {
        SubscribeOptions uo;
        uo.from      = From::earliest();
        uo.isolation = Isolation::ReadUncommitted;
        std::vector<Subscription> usubs;
        usubs.reserve(static_cast<std::size_t>(out.partitionCount));
        auto lastRecord = Clock::now();
        for (int32_t p = 0; p < out.partitionCount; ++p) {
            usubs.push_back(c.subscribe(out, p, uo, [&](const Record&) { ++uncommitted; lastRecord = Clock::now(); }));
        }
        const auto udeadline = Clock::now() + std::chrono::seconds(300);
        while (Clock::now() < udeadline && (uncommitted <= n || Clock::now() - lastRecord < std::chrono::seconds(3))) pump(c);
        for (auto& s : usubs) s.unsubscribe();
    }
    if (uncommitted <= n) {
        std::fprintf(stderr, "EOS-VERIFY: FAIL read_uncommitted count=%lld is not above N=%lld: no aborted data was in the log\n",
                     static_cast<long long>(uncommitted), static_cast<long long>(n));
        return 1;
    }
    if (chk.count() != n || dups != 0 || bad != 0 || total != n) {
        std::fprintf(stderr, "EOS-VERIFY: FAIL count=%lld of %lld total=%lld duplicates=%lld bad=%lld missing=%lld firstMissing=%lld first=%s\n",
                     static_cast<long long>(chk.count()), static_cast<long long>(n), static_cast<long long>(total),
                     static_cast<long long>(dups), static_cast<long long>(bad), static_cast<long long>(chk.missing()),
                     static_cast<long long>(chk.firstMissing()), firstProblem.c_str());
        return 1;
    }
    std::printf("EOS-VERIFY: uncommitted=%lld (> N: aborted records were in the log)\n", static_cast<long long>(uncommitted));
    std::printf("EOS-VERIFY: OK count=%lld\n", static_cast<long long>(n));
    return 0;
}

}  // namespace

int main() {
    const std::string mode = envStr("RUNE_MODE");
    if (mode == "selfcheck") return selfcheck();
    const std::string target = envStr("RUNE_TARGET"), egress = envStr("RUNE_EGRESS"), dataDir = envStr("RUNE_DATA_DIR");
    if (target.empty() || egress.empty() || dataDir.empty()) {
        std::fprintf(stderr, "EOS-PROBE: RUNE_TARGET, RUNE_EGRESS and RUNE_DATA_DIR are required\n");
        return 1;
    }
    const int     timeoutMs = static_cast<int>(envInt("RUNE_TIMEOUT_MS", 20000));
    const int64_t n         = envInt("RUNE_EOS_N", 200000);
    ClientConfig cfg;
    cfg.gatewayChannel   = target;
    cfg.gatewayStreamId  = static_cast<int32_t>(envInt("RUNE_TARGET_STREAM", 1));
    cfg.egressChannel    = egress;
    cfg.egressStreamId   = static_cast<int32_t>(envInt("RUNE_EGRESS_STREAM", 2));
    cfg.dataDir          = dataDir;
    cfg.connectTimeoutMs = timeoutMs;
    cfg.pingTimeoutMs    = timeoutMs;
    cfg.runnerMode       = RunnerMode::Invoker;   // this thread drives the client: no thread, no lock, no hand-off
    cfg.topics           = parseTopics(envStr("RUNE_CLIENT_TOPIC_TABLE", "eos_in:0:8,eos_out:1:8"));
    rune::examples::applyEnvCredentials(cfg);
    try {
        Client c(cfg);
        const TopicHandle in  = c.topic("eos_in");
        const TopicHandle out = c.topic("eos_out");
        if (!in.valid() || !out.valid() || in.partitionCount > kMaxParts || out.partitionCount > kMaxParts) {
            std::fprintf(stderr, "EOS-PROBE: eos_in / eos_out missing from RUNE_CLIENT_TOPIC_TABLE (or more than %d partitions)\n", kMaxParts);
            return 1;
        }
        if (mode == "fill") { c.connectAndWait(); return runFill(c, in, n, timeoutMs); }
        if (mode == "verify") { c.connectAndWait(); return runVerify(c, out, n); }
        if (mode == "pipeline") {
            const std::string groupId = envStr("RUNE_GROUP_ID", "eos-1"), txnId = envStr("RUNE_TXN_ID", "eos-worker-1");
            InState st;
            GroupConfig gcfg;
            gcfg.autoCommit       = false;            // the offsets travel in the transaction
            gcfg.sessionTimeoutMs = 10000;
            gcfg.autoOffsetReset  = OffsetReset::Earliest;
            GroupConsumer gc = c.subscribeGroup(groupId, {"eos_in"}, gcfg);
            const auto reset = [&st](std::vector<TopicPartition> parts) { st.rebalance(parts); };
            gc.onAssigned(reset);
            gc.onRevoked(reset);
            gc.onRecords([&st](const RecordSet& rs) {
                const auto p = static_cast<std::size_t>(rs.partition);
                if (p >= st.part.size()) return;
                rs.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                    if (val.size() < kInBytes || !st.error.empty()) return;
                    switch (onRecord(st.part[p], off)) {
                    case Accept::Drop: return;
                    case Accept::Gap:
                        st.error = "partition " + std::to_string(p) + ": record at offset " + std::to_string(off) + " but " +
                                   (st.part[p].rebase ? "the batch ends at " + std::to_string(st.part[p].hi)
                                                      : "the next expected offset is " + std::to_string(st.part[p].nextAppend)) +
                                   " (records would be skipped)";
                        return;
                    case Accept::Take: break;
                    }
                    const auto* b = reinterpret_cast<const uint8_t*>(val.data());
                    st.q.push_back(InRec{rs.partition, off, static_cast<int64_t>(getU64(b)), static_cast<int64_t>(getU64(b + 8))});
                });
            });
            ProducerConfig pc;
            pc.acks             = Acks::All;
            pc.transactionalId  = txnId;
            pc.produceTimeoutMs = timeoutMs;
            Producer tp = c.producer(pc);
            const int rc = runPipeline(c, in, groupId, txnId, st, gc, tp);
            gc.close();
            return rc;
        }
        std::fprintf(stderr, "EOS-PROBE: RUNE_MODE must be fill, pipeline, verify or selfcheck (got '%s')\n", mode.c_str());
        return 1;
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "EOS-PROBE: FAIL %s: %s\n", mode.c_str(), ex.what());
        return 1;
    }
}
