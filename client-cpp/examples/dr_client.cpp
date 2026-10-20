// examples/dr_client.cpp — DR-1 configurable disaster-recovery driver
//
// A richer, fully env-driven produce/subscribe driver used by the Rune
// disaster-recovery E2E harness (rune-broker/dr-e2e).  It reuses the same
// rune::client API as rune_produce_subscribe but adds the knobs the DR matrix
// sweeps over, and emits MACHINE-PARSEABLE result lines on stdout so the
// pytest harness can assert invariants (no-loss / ordering / exactly-once /
// resume-from-offset / durable-after-restart).
//
// ─────────────────────────────────────────────────────────────────────────
// Environment (all optional except RUNE_TARGET / RUNE_EGRESS / RUNE_DATA_DIR):
//
//   Connection:
//     RUNE_TARGET          node client ingress channel (required)
//                            e.g. "aeron:udp?endpoint=rune-1:40200"
//     RUNE_TARGET_STREAM   int, default 1
//     RUNE_EGRESS          this client's own egress channel (required)
//                            e.g. "aeron:udp?endpoint=dr-client-0:40400"
//     RUNE_EGRESS_STREAM   int, default 2
//     RUNE_DATA_DIR        Aeron driver dir (required)
//     RUNE_CLIENT_TOPIC_TABLE          "name:id:partitions,..." default "events:0:1"
//     RUNE_TIMEOUT_MS      per-op timeout ms, default 10000
//     RUNE_ACKS            "leader" | "all", default "leader".
//                            "all" → ProducerConfig::acks = Acks::All so the
//                            broker waits for majority-ISR durability before
//                            resolving the ProduceResponse.  A "PRODUCED" line
//                            then means majority-committed, not just leader-acked.
//
//   Workload:
//     RUNE_MODE            "produce" | "subscribe" | "produce_subscribe" | "group"
//                          | "txn_produce" | "txn_consume"
//                            default "produce_subscribe"
//
//   Transactional produce (RUNE_MODE=txn_produce):
//     RUNE_TXN_ID          transactional producer id (required for txn_produce)
//     RUNE_TXN_COMMIT      1=commit (default), 0=abort
//
//   Transactional consume (RUNE_MODE=txn_consume):
//     RUNE_ISOLATION       "read_committed" | "read_uncommitted" (default)
//                            read_committed → GroupConsumer with IsolationLevel::ReadCommitted
//     RUNE_RECORDS         records to produce (produce modes), default 20
//     RUNE_KEYS            number of distinct keys to spread records across,
//                            default 1.  Record i uses key (i % RUNE_KEYS).
//     RUNE_CLIENT_ID       integer id of this client instance, default 0.
//                            Used to disjoint the key space + seq space across
//                            concurrent clients: effective key = clientId*KEYS+k
//                            and the record payload carries clientId so the
//                            harness can attribute delivered records per client.
//     RUNE_RATE_HZ         produce rate cap (records/sec); 0 = unthrottled
//                            (default 0).
//
//   Subscribe:
//     RUNE_FROM            "earliest" | "latest" | "offset:<N>"  default
//                            "earliest".
//     RUNE_EXPECT          number of records the subscriber should collect
//                            before declaring success; default = RECORDS (for
//                            produce_subscribe) or 0 (subscribe-only waits the
//                            full RUNE_SUBSCRIBE_MS window).
//     RUNE_SUBSCRIBE_MS    max ms to wait while collecting, default
//                            RUNE_TIMEOUT_MS*3.
//     RUNE_SLOW_SUBSCRIBER_MS  per-record artificial handler delay (ms) to
//                            model a slow/backpressured consumer, default 0.
//                            NB: applied OFF the agent thread (buffered) so we
//                            never violate the "handler must not block" rule.
//     RUNE_SUB_PARTITIONS  number of partitions to subscribe to,
//                            default 1 (= partition 0 only, backward-compatible).
//                            Set to the topic's partitionCount to cover ALL
//                            partitions in multi-partition end-to-end tests.
//     RUNE_SUB_PARTITION_BASE  first partition index to subscribe to,
//                            default 0.  Combined with RUNE_SUB_PARTITIONS=K,
//                            subscribes to partitions [base..base+K-1].  Used
//                            in multi-leader tests to target a single partition
//                            on its correct leader without triggering redirects.
//
//   Group consumer (RUNE_MODE=group):
//     RUNE_GROUP_ID        consumer-group id (required for group mode)
//     RUNE_AUTOCOMMIT_MS   auto-commit interval ms, default 1000
//     RUNE_MEMBER_ID       optional member id hint (informational only)
//
// ─────────────────────────────────────────────────────────────────────────
// Output (stdout, one field-per-token, stable across releases):
//
//   PRODUCED cid=<c> key=<k> seq=<s> part=<p> offset=<o>
//   DELIVERED cid=<c> key=<k> seq=<s> part=<p> offset=<o>
//   SUMMARY produced=<np> delivered=<nd> mode=<m> rc=<rc>
//
//   For DELIVERED, cid/key/seq are decoded from the record VALUE payload,
//   which the producer encodes as "cid=<c>;key=<k>;seq=<s>".  A record whose
//   value does not match that schema (e.g. produced by another driver run) is
//   still emitted with cid=-1 key=-1 seq=-1 but counted.
//
//   Group mode (RUNE_MODE=group) emits:
//   ASSIGN cid=<c> parts=<p0,p1,...>
//   REVOKE cid=<c> parts=<p0,p1,...>
//   GROUPREC cid=<c> topic=<t> partition=<p> offset=<o> rec_cid=<rc> rec_key=<rk> rec_seq=<rs>
//   GROUPSUMMARY cid=<c> consumed=<n> rc=<rc>
//
//   Txn produce mode (RUNE_MODE=txn_produce) emits:
//   TXNPRODUCE cid=<c> key=<k> seq=<s> part=<p>
//   TXNRESULT txn_id=<id> cid=<c> outcome=committed|aborted
//   SUMMARY produced=<np> delivered=0 mode=txn_produce rc=<rc>
//
//   Txn consume mode (RUNE_MODE=txn_consume) emits the same DELIVERED / SUMMARY
//   lines as subscribe mode plus:
//   TXNCONSUME_SUMMARY cid=<c> consumed=<n> isolation=<iso>
//
// Exit code: 0 on success, 1 on workload assertion failure, 2 on exception.

#include "ExampleAuth.h"
#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

// F8c C6: one acked send waited on a Completions (examples only — the library
// deliberately offers no per-record *AndWait sugar).  false = rejected or the
// wait timed out (`out.status` = Timeout then; the op still completes later).
static bool sendAndWaitAck(Completions& cq, Producer& p, const TopicHandle& th, const Key& key,
                           std::span<const uint8_t> v, Token t, int timeoutMs, Completion& out) {
    for (;;) {
        const SendResult r = p.trySend(th, key, v, cq, t);
        if (r == SendResult::Ok) break;
        if (r == SendResult::QueueFull) { (void)cq.poll([](const Completion&, std::span<const uint8_t>) {}); continue; }
        if (r != SendResult::BackPressured) { out.status = OpStatus::Closed; return false; }
        std::this_thread::yield();
    }
    const auto w = cq.waitFor(t, std::chrono::milliseconds(timeoutMs),
                              [&](const Completion& c, std::span<const uint8_t>) { out = c; });
    if (w != Completions::Wait::Done) { out.status = OpStatus::Timeout; return false; }
    return true;
}

// ---------------------------------------------------------------------------
// Env helpers
// ---------------------------------------------------------------------------
static std::string getEnvStr(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::string(v) : def;
}
static int getEnvInt(const char* name, int def) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::atoi(v) : def;
}

// ---------------------------------------------------------------------------
// Topic config parser: "name:id:partitions,..."
// ---------------------------------------------------------------------------
static std::vector<TopicSpec> parseTopics(const std::string& s) {
    std::vector<TopicSpec> out;
    if (s.empty()) return out;
    std::size_t pos = 0;
    while (pos <= s.size()) {
        const std::size_t comma = s.find(',', pos);
        const std::string tok = s.substr(pos, (comma == std::string::npos ? s.size() : comma) - pos);
        pos = (comma == std::string::npos ? s.size() + 1 : comma + 1);
        if (tok.empty()) continue;
        const auto c1 = tok.find(':');
        if (c1 == std::string::npos) continue;
        const auto c2 = tok.find(':', c1 + 1);
        if (c2 == std::string::npos) continue;
        TopicSpec ts;
        ts.name           = tok.substr(0, c1);
        ts.topicId        = std::atoi(tok.c_str() + c1 + 1);
        ts.partitionCount = std::atoi(tok.c_str() + c2 + 1);
        out.push_back(std::move(ts));
    }
    return out;
}

// ---------------------------------------------------------------------------
// From parser: "earliest" | "latest" | "offset:<N>"
// ---------------------------------------------------------------------------
static From parseFrom(const std::string& s) {
    if (s.rfind("offset:", 0) == 0) {
        return From::offset(std::atoll(s.c_str() + 7));
    }
    if (s == "latest") return From::latest();
    return From::earliest();
}

// Payload schema: "cid=<c>;key=<k>;seq=<s>".  Decoded on the subscribe side.
static std::string encodePayload(int cid, int key, int seq) {
    return "cid=" + std::to_string(cid) + ";key=" + std::to_string(key) +
           ";seq=" + std::to_string(seq);
}
static void decodePayload(const std::string& v, int& cid, int& key, int& seq) {
    cid = key = seq = -1;
    const char* p = v.c_str();
    const char* c = std::strstr(p, "cid=");
    const char* k = std::strstr(p, "key=");
    const char* s = std::strstr(p, "seq=");
    if (c) cid = std::atoi(c + 4);
    if (k) key = std::atoi(k + 4);
    if (s) seq = std::atoi(s + 4);
}

// ---------------------------------------------------------------------------
// Produce `records` records across `keys` distinct keys at up to `rateHz`.
// Emits "PRODUCED ..." per acked record.  Returns #acked.
// `acksPolicy`: Acks::Leader (default) or Acks::All (majority-durable).
// ---------------------------------------------------------------------------
static int doProduce(Client& c, const TopicHandle& th, int cid, int records,
                     int keys, int rateHz, int timeoutMs, int32_t partitionCount,
                     Acks acksPolicy) {
    ProducerConfig pcfg;
    pcfg.acks             = acksPolicy;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 16;
    pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
    auto p = c.producer(pcfg);
    Completions cq(c);

    const int64_t interNs =
        (rateHz > 0) ? static_cast<int64_t>(1'000'000'000LL / rateHz) : 0;
    auto nextSlot = std::chrono::steady_clock::now();

    int acked = 0;
    for (int seq = 0; seq < records; ++seq) {
        if (interNs > 0) {
            std::this_thread::sleep_until(nextSlot);
            nextSlot += std::chrono::nanoseconds(interNs);
        }
        const int keyIdx = keys > 0 ? (seq % keys) : 0;
        // Disjoint the key space across clients so concurrent producers never
        // collide on a key (keeps per-key ordering attributable per client).
        const uint64_t effKey =
            static_cast<uint64_t>(cid) * 1'000'000ULL + static_cast<uint64_t>(keyIdx);
        const std::string valStr = encodePayload(cid, keyIdx, seq);
        const std::vector<uint8_t> val(valStr.begin(), valStr.end());

        Key key(effKey);
        const int32_t part = key.partition(partitionCount);
        Completion done;
        const bool waited = sendAndWaitAck(cq, p, th, key, std::span<const uint8_t>(val),
                                           static_cast<Token>(seq) + 1, timeoutMs, done);
        if (!waited && done.status == OpStatus::Timeout) {
            // MD1 Fix 7c: log the timeout but continue (same resilience as the
            // exception path below — individual timeouts must not abort the batch).
            std::cerr << "CLIENT: FAIL produce cid=" << cid << " seq=" << seq
                      << " TIMEOUT\n";
            continue;
        }
        if (waited && done.status == OpStatus::Ok) {
            const int64_t offset = done.value;
            std::cout << "PRODUCED cid=" << cid << " key=" << keyIdx
                      << " seq=" << seq << " part=" << part << " offset=" << offset << "\n";
            std::cout.flush();
            ++acked;
        } else {
            const std::string why = "produce failed (status=" + std::to_string(static_cast<int>(done.status)) +
                                    " errorCode=" + std::to_string(done.errorCode) + ")";
            // MD1 Fix 7c: log the failure but continue the produce loop.
            // A single-record failure (e.g. NotLeader / dead partition leader
            // in Phase-1 availability tests) must not abort the entire batch;
            // the alive partitions must still get their records.
            std::cerr << "CLIENT: FAIL produce cid=" << cid << " seq=" << seq
                      << ": " << why << "\n";
            continue;
        }
    }
    return acked;
}

// ---------------------------------------------------------------------------
// Subscribe from `from` and collect until `expect` records (or timeout).
// Emits "DELIVERED ..." per record.  Returns #delivered.
//
// `subPartitions`: number of partitions to subscribe to.
//   1 (default) = 1 partition starting at subPartitionBase.
//   >1           = subscribe to ALL partitions [base..base+N-1] so that the
//                  harness can assert assert_no_committed_loss across all of them.
// `subPartitionBase`: first partition index (default 0).  Combined with
//   subPartitions=K subscribes to partitions [base..base+K-1].  Used in
//   multi-leader tests to target one partition on its correct leader directly,
//   avoiding the session-retarget bounce that single-session cross-leader
//   subscribes cause.
//
// The agent-thread handler only COPIES the record into a queue (never blocks).
// A separate drain thread emits the DELIVERED lines and applies the optional
// slow-subscriber delay, so we model a slow consumer without violating the
// "handler must not block the agent thread" contract.
// ---------------------------------------------------------------------------
static int doSubscribe(Client& c, const TopicHandle& th, From from, int expect,
                       int windowMs, int slowMs, int expectCidMin,
                       int expectCidExact, int subPartitions,
                       int subPartitionBase = 0) {
    struct Rec { int64_t offset; int32_t partition; std::string value; };
    std::mutex mtx;
    std::deque<Rec> q;
    std::atomic<int> delivered{0};

    SubscribeOptions opts;
    opts.from      = from;
    opts.isolation = Isolation::ReadUncommitted;

    // Subscribe to each partition and keep handles alive until we're done.
    // Thread the partition index into each handler so DELIVERED lines carry part=.
    const int nParts     = (subPartitions > 0) ? subPartitions : 1;
    const int partBase   = (subPartitionBase >= 0) ? subPartitionBase : 0;
    std::vector<Subscription> subs;
    subs.reserve(nParts);
    for (int i = 0; i < nParts; ++i) {
        const int part = partBase + i;
        subs.push_back(c.subscribe(th, part, opts, [&mtx, &q, part](const Record& r) {
            std::string v(reinterpret_cast<const char*>(r.value.data()), r.value.size());
            std::lock_guard<std::mutex> lk(mtx);
            q.push_back(Rec{r.offset, static_cast<int32_t>(part), std::move(v)});
        }));
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(windowMs);
    while (std::chrono::steady_clock::now() < deadline) {
        Rec rec;
        bool have = false;
        {
            std::lock_guard<std::mutex> lk(mtx);
            if (!q.empty()) { rec = std::move(q.front()); q.pop_front(); have = true; }
        }
        if (have) {
            if (slowMs > 0) std::this_thread::sleep_for(std::chrono::milliseconds(slowMs));
            int cid, key, seq;
            decodePayload(rec.value, cid, key, seq);
            std::cout << "DELIVERED cid=" << cid << " key=" << key
                      << " seq=" << seq << " part=" << rec.partition
                      << " offset=" << rec.offset << "\n";
            std::cout.flush();
            // Only records from this run's cid range (>= expectCidMin), and if
            // expectCidExact >= 0 only that exact cid, count toward `expect`.
            // Lets a subscriber on a persistent/shared partition drain past
            // older records and (for multi-client runs) target just its OWN
            // records without racing other producers.
            const bool counts =
                cid >= expectCidMin &&
                (expectCidExact < 0 || cid == expectCidExact);
            if (counts) {
                const int d = delivered.fetch_add(1) + 1;
                if (expect > 0 && d >= expect) break;
            }
        } else {
            std::this_thread::sleep_for(2ms);
        }
    }

    // Drain anything still queued after the loop broke on `expect` — harmless.
    for (auto& sub : subs) sub.unsubscribe();
    return delivered.load();
}

// ---------------------------------------------------------------------------
// Group consumer mode (RUNE_MODE=group).
//
// Joins a consumer group using Client::subscribeGroup().  Records arrive via
// the broker's server-side partition assignment.  The handler decodes each
// record payload and emits GROUPREC lines parseable by the harness.
//
// Output lines:
//   ASSIGN  cid=<c> parts=<p0,p1,...>
//   REVOKE  cid=<c> parts=<p0,p1,...>
//   GROUPREC cid=<c> topic=<t> partition=<p> offset=<o> rec_cid=<rc> rec_key=<rk> rec_seq=<rs>
//   GROUPSUMMARY cid=<c> consumed=<n> rc=<rc>
//
// Returns the total number of records consumed (all partitions).
// ---------------------------------------------------------------------------
static int doGroup(Client& client,
                   const std::string& groupId,
                   const std::vector<std::string>& topicNames,
                   int cid,
                   int windowMs,
                   int autoCommitMs) {
    // Thread-safe record queue: the GroupConsumer runner thread pushes here
    // and the drain loop below pulls.
    struct GRec {
        std::string topicName;
        int32_t     partition;
        int64_t     offset;
        int         rec_cid, rec_key, rec_seq;
        int32_t     generation;
    };
    std::mutex        qMtx;
    std::deque<GRec>  q;
    std::atomic<int>  consumed{0};

    GroupConfig gcfg;
    gcfg.autoCommit           = true;
    gcfg.autoCommitIntervalMs = autoCommitMs;
    gcfg.sessionTimeoutMs     = 30000;
    gcfg.autoOffsetReset      = OffsetReset::Earliest;

    // We need the topic name map (topicId → name) for output.
    // The Client::topic() call resolves by name; GroupConsumer only gets topicId
    // in the RecordSet.  Build a reverse map from the cfg.topics list.
    // Because we only have one topic in group mode (enforced by the harness),
    // we emit the first topic name for all records.
    const std::string& primaryTopic = topicNames.empty() ? "unknown" : topicNames[0];

    GroupConsumer gc = client.subscribeGroup(groupId, topicNames, gcfg);

    // NOTE: the callbacks below fire on the GroupConsumer runner thread, which is
    // the same thread that updates gc.generation().  Capturing &gc lets each
    // callback stamp the CURRENT assignment generation onto its output — the
    // harness single-owner invariant is generation-aware, so a (partition,offset)
    // legitimately re-read by a NEW owner after a rebalance (different generation)
    // is not flagged, while a true same-generation split still fails the test.
    gc.onAssigned([cid, primaryTopic, &gc](std::vector<TopicPartition> parts) {
        std::ostringstream oss;
        oss << "ASSIGN cid=" << cid << " gen=" << gc.generation() << " parts=";
        for (std::size_t i = 0; i < parts.size(); ++i) {
            if (i > 0) oss << ",";
            oss << parts[i].partition;
        }
        std::cout << oss.str() << "\n";
        std::cout.flush();
        (void)primaryTopic;
    });

    gc.onRevoked([cid, primaryTopic, &gc](std::vector<TopicPartition> parts) {
        std::ostringstream oss;
        oss << "REVOKE cid=" << cid << " gen=" << gc.generation() << " parts=";
        for (std::size_t i = 0; i < parts.size(); ++i) {
            if (i > 0) oss << ",";
            oss << parts[i].partition;
        }
        std::cout << oss.str() << "\n";
        std::cout.flush();
        (void)primaryTopic;
    });

    gc.onRecords([&qMtx, &q, &gc, primaryTopic, cid](const RecordSet& rs) {
        // RecordSet::forEachRecord hands over each record once: it skips the
        // records a straddling re-delivery carries below the position.
        rs.forEachRecord([&](std::string_view /*key*/, std::string_view val,
                             int64_t off, int64_t /*ts*/) {
            int rc = -1, rk = -1, rs_seq = -1;
            const std::string valStr(val);
            decodePayload(valStr, rc, rk, rs_seq);
            const int32_t gen = gc.generation();
            std::lock_guard<std::mutex> lk(qMtx);
            q.push_back({primaryTopic, rs.partition, off, rc, rk, rs_seq, gen});
        });
        (void)cid;
    });

    // All callbacks registered before connect() — setup-phase contract satisfied.
    std::cerr << "CLIENT: group connecting...\n";
    client.connectAndWait();
    std::cerr << "CLIENT: group connected\n";

    // Drain the queue until windowMs expires.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(windowMs);
    while (std::chrono::steady_clock::now() < deadline) {
        GRec rec;
        bool have = false;
        {
            std::lock_guard<std::mutex> lk(qMtx);
            if (!q.empty()) { rec = std::move(q.front()); q.pop_front(); have = true; }
        }
        if (have) {
            std::cout << "GROUPREC cid=" << cid
                      << " gen=" << rec.generation
                      << " topic=" << rec.topicName
                      << " partition=" << rec.partition
                      << " offset=" << rec.offset
                      << " rec_cid=" << rec.rec_cid
                      << " rec_key=" << rec.rec_key
                      << " rec_seq=" << rec.rec_seq << "\n";
            std::cout.flush();
            consumed.fetch_add(1);
        } else {
            std::this_thread::sleep_for(2ms);
        }
    }

    gc.close();
    return consumed.load();
}

// ---------------------------------------------------------------------------
// TX-5: Transactional produce mode (RUNE_MODE=txn_produce).
//
// Runs init→begin→produce(records)→commit/abort on a Producer with a transactionalId.
// Emits:
//   TXNPRODUCE cid=<c> key=<k> seq=<s> part=<p>   (per record sent)
//   TXNRESULT txn_id=<id> cid=<c> outcome=committed|aborted
//   SUMMARY produced=<n> delivered=0 mode=txn_produce rc=<rc>
//
// Returns 0 on success (commit or intentional abort), 1 on unexpected error.
// ---------------------------------------------------------------------------
static int doTxnProduce(Client& c, const TopicHandle& th,
                        const std::string& txnId, int cid, int records, int keys,
                        bool doCommit, int timeoutMs) {
    ProducerConfig pcfg;
    pcfg.acks             = Acks::All;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 16;
    pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);

    pcfg.transactionalId  = txnId;
    auto tp = c.producer(pcfg);

    // initTransactions (F8c C5: cold-path blocking form; the op itself is
    // bounded by produceTimeoutMs of duty time)
    try {
        tp.initTransactionsAndWait(std::chrono::milliseconds(timeoutMs * 2));
    } catch (const std::exception& ex) {
        std::cerr << "CLIENT: FAIL txn initTransactions: " << ex.what() << "\n";
        return 1;
    }

    tp.beginTransaction();

    // Send records
    int sent = 0;
    for (int seq = 0; seq < records; ++seq) {
        const int keyIdx = keys > 0 ? (seq % keys) : 0;
        const uint64_t effKey = static_cast<uint64_t>(cid) * 1'000'000ULL +
                                static_cast<uint64_t>(keyIdx);
        const std::string valStr = encodePayload(cid, keyIdx, seq);
        const std::vector<uint8_t> val(valStr.begin(), valStr.end());
        Key key(effKey);
        const int32_t part = key.partition(th.partitionCount);
        tp.send(th, key, std::span<const uint8_t>(val));
        std::cout << "TXNPRODUCE cid=" << cid << " key=" << keyIdx
                  << " seq=" << seq << " part=" << part << "\n";
        std::cout.flush();
        ++sent;
    }

    // commit or abort
    const std::string outcome = doCommit ? "committed" : "aborted";
    if (!doCommit) {
        // Kafka parity: an abort drops records not yet produced — flush first so the aborted records
        // reach the log (read_uncommitted consumers see them; read_committed must not).  A failed
        // flush is logged; the abort still runs (the transaction must never be left open).
        try {
            tp.flushAndWait(std::chrono::milliseconds(timeoutMs * 2));
        } catch (const std::exception& ex) {
            std::cerr << "CLIENT: TXNFLUSH failed cid=" << cid << ": " << ex.what() << "\n";
        }
    }
    try {
        if (doCommit) tp.commitTransactionAndWait(std::chrono::milliseconds(timeoutMs * 2));
        else tp.abortTransactionAndWait(std::chrono::milliseconds(timeoutMs * 2));
    } catch (const std::exception& ex) {
        std::cerr << "CLIENT: FAIL txn " << (doCommit ? "commitTransaction" : "abortTransaction")
                  << ": " << ex.what() << "\n";
        return 1;
    }

    std::cout << "TXNRESULT txn_id=" << txnId << " cid=" << cid
              << " outcome=" << outcome << "\n";
    std::cout.flush();
    std::cout << "SUMMARY produced=" << sent << " delivered=0"
              << " mode=txn_produce rc=0\n";
    std::cout.flush();
    return 0;
}

// ---------------------------------------------------------------------------
// TX-5: Transactional consume mode (RUNE_MODE=txn_consume).
//
// Subscribes as a group consumer with the configured isolation level.
// Emits DELIVERED lines (same as subscribe mode) plus:
//   TXNCONSUME_SUMMARY cid=<c> consumed=<n> isolation=<iso>
// ---------------------------------------------------------------------------
static int doTxnConsume(Client& c,
                        const std::vector<std::string>& topicNames,
                        int cid, int windowMs, int autoCommitMs,
                        const std::string& groupId,
                        IsolationLevel isolation) {
    struct GRec {
        std::string topicName;
        int32_t     partition;
        int64_t     offset;
        int         rec_cid, rec_key, rec_seq;
        int32_t     generation;
    };
    std::mutex        qMtx;
    std::deque<GRec>  q;
    std::atomic<int>  consumed{0};

    GroupConfig gcfg;
    gcfg.autoCommit           = true;
    gcfg.autoCommitIntervalMs = autoCommitMs;
    gcfg.sessionTimeoutMs     = 30000;
    gcfg.autoOffsetReset      = OffsetReset::Earliest;
    gcfg.isolation            = isolation;

    const std::string& primaryTopic = topicNames.empty() ? "unknown" : topicNames[0];

    GroupConsumer gc = c.subscribeGroup(groupId, topicNames, gcfg);

    gc.onAssigned([cid, &gc](std::vector<TopicPartition> parts) {
        std::ostringstream oss;
        oss << "ASSIGN cid=" << cid << " gen=" << gc.generation() << " parts=";
        for (std::size_t i = 0; i < parts.size(); ++i) {
            if (i > 0) oss << ",";
            oss << parts[i].partition;
        }
        std::cout << oss.str() << "\n";
        std::cout.flush();
    });

    gc.onRevoked([cid, &gc](std::vector<TopicPartition> parts) {
        std::ostringstream oss;
        oss << "REVOKE cid=" << cid << " gen=" << gc.generation() << " parts=";
        for (std::size_t i = 0; i < parts.size(); ++i) {
            if (i > 0) oss << ",";
            oss << parts[i].partition;
        }
        std::cout << oss.str() << "\n";
        std::cout.flush();
    });

    gc.onRecords([&qMtx, &q, &gc, primaryTopic, cid](const RecordSet& rs) {
        // RecordSet::forEachRecord hands over each record once: it skips the
        // records a straddling re-delivery carries below the position.
        rs.forEachRecord([&](std::string_view /*key*/, std::string_view val,
                             int64_t off, int64_t /*ts*/) {
            int rc = -1, rk = -1, rs_seq = -1;
            const std::string valStr(val);
            decodePayload(valStr, rc, rk, rs_seq);
            const int32_t gen = gc.generation();
            std::lock_guard<std::mutex> lk(qMtx);
            q.push_back({primaryTopic, rs.partition, off, rc, rk, rs_seq, gen});
        });
        (void)cid;
    });

    std::cerr << "CLIENT: txn_consume group connecting...\n";
    c.connectAndWait();
    std::cerr << "CLIENT: txn_consume group connected\n";

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(windowMs);
    while (std::chrono::steady_clock::now() < deadline) {
        GRec rec;
        bool have = false;
        {
            std::lock_guard<std::mutex> lk(qMtx);
            if (!q.empty()) { rec = std::move(q.front()); q.pop_front(); have = true; }
        }
        if (have) {
            // Emit as DELIVERED for parser compatibility
            std::cout << "DELIVERED cid=" << rec.rec_cid << " key=" << rec.rec_key
                      << " seq=" << rec.rec_seq << " part=" << rec.partition
                      << " offset=" << rec.offset << "\n";
            std::cout.flush();
            consumed.fetch_add(1);
        } else {
            std::this_thread::sleep_for(2ms);
        }
    }

    gc.close();
    const int n = consumed.load();
    const std::string isoStr = (isolation == IsolationLevel::ReadCommitted)
        ? "read_committed" : "read_uncommitted";
    std::cout << "TXNCONSUME_SUMMARY cid=" << cid << " consumed=" << n
              << " isolation=" << isoStr << "\n";
    std::cout.flush();
    std::cout << "SUMMARY produced=0 delivered=" << n
              << " mode=txn_consume rc=0\n";
    std::cout.flush();
    return 0;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    if (std::getenv("RUNE_TOPICS") != nullptr) {
        std::cerr << "CLIENT: RUNE_TOPICS was renamed to RUNE_CLIENT_TOPIC_TABLE (the client's name:topicId:partitions table)\n";
        return 2;
    }
    const std::string target       = getEnvStr("RUNE_TARGET");
    const int         targetStream = getEnvInt("RUNE_TARGET_STREAM", 1);
    const std::string egress       = getEnvStr("RUNE_EGRESS");
    const int         egressStream = getEnvInt("RUNE_EGRESS_STREAM", 2);
    const std::string dataDir      = getEnvStr("RUNE_DATA_DIR");
    const std::string topicsStr    = getEnvStr("RUNE_CLIENT_TOPIC_TABLE", "events:0:1");
    const int         timeoutMs    = getEnvInt("RUNE_TIMEOUT_MS", 10000);

    const std::string mode         = getEnvStr("RUNE_MODE", "produce_subscribe");
    const int         records      = getEnvInt("RUNE_RECORDS", 20);
    const int         keys         = std::max(1, getEnvInt("RUNE_KEYS", 1));
    const int         cid          = getEnvInt("RUNE_CLIENT_ID", 0);
    const int         rateHz       = getEnvInt("RUNE_RATE_HZ", 0);

    const From        from         = parseFrom(getEnvStr("RUNE_FROM", "earliest"));
    const int         subWindowMs  = getEnvInt("RUNE_SUBSCRIBE_MS", timeoutMs * 3);
    const int         slowMs       = getEnvInt("RUNE_SLOW_SUBSCRIBER_MS", 0);
    const int         expectDefault =
        (mode == "subscribe") ? 0 : records;
    const int         expect       = getEnvInt("RUNE_EXPECT", expectDefault);
    // Records with cid >= expectCidMin count toward `expect` (default: all).
    const int         expectCidMin = getEnvInt("RUNE_EXPECT_CID_MIN", -2147483647);
    // If >=0, ONLY records with this exact cid count toward `expect` (own-cid).
    const int         expectCidExact = getEnvInt("RUNE_EXPECT_CID_EXACT", -1);
    // RUNE_SUB_PARTITIONS: number of partitions to subscribe.
    // Default 1 = partition 0 only (backward-compatible).  Set to the topic's
    // partitionCount (from RUNE_CLIENT_TOPIC_TABLE) to cover ALL partitions end-to-end.
    const int         subPartitions     = getEnvInt("RUNE_SUB_PARTITIONS", 1);
    // RUNE_SUB_PARTITION_BASE: first partition index to subscribe, default 0.
    // Combined with RUNE_SUB_PARTITIONS=K subscribes partitions [base..base+K-1].
    // Use this to target a single partition on its correct leader without
    // triggering cross-leader session-retarget bounces.
    const int         subPartitionBase  = getEnvInt("RUNE_SUB_PARTITION_BASE", 0);

    // RUNE_ACKS: "leader" (default) | "all".
    // "all" → Acks::All so the broker holds the ProduceResponse until a
    // majority of ISR nodes have replicated the batch (majority-durable).
    // A PRODUCED line under acks=All means majority-committed.
    const std::string acksStr  = getEnvStr("RUNE_ACKS", "leader");
    const Acks        acksPolicy =
        (acksStr == "all") ? Acks::All : Acks::Leader;

    // Group mode env vars.
    const std::string groupId        = getEnvStr("RUNE_GROUP_ID");
    const int         autoCommitMs   = getEnvInt("RUNE_AUTOCOMMIT_MS", 1000);

    // TX-5: transactional produce env vars.
    const std::string txnId          = getEnvStr("RUNE_TXN_ID");
    const bool        txnCommit      = (getEnvInt("RUNE_TXN_COMMIT", 1) != 0);

    // TX-5: transactional consume isolation.
    const std::string isolationStr   = getEnvStr("RUNE_ISOLATION", "read_uncommitted");
    const IsolationLevel txnIsolation =
        (isolationStr == "read_committed")
            ? IsolationLevel::ReadCommitted
            : IsolationLevel::ReadUncommitted;

    if (target.empty() || egress.empty() || dataDir.empty()) {
        std::cerr << "CLIENT: RUNE_TARGET, RUNE_EGRESS and RUNE_DATA_DIR are required\n";
        return 1;
    }

    if (mode == "group" && groupId.empty()) {
        std::cerr << "CLIENT: RUNE_GROUP_ID is required for mode=group\n";
        return 1;
    }

    if (mode == "txn_produce" && txnId.empty()) {
        std::cerr << "CLIENT: RUNE_TXN_ID is required for mode=txn_produce\n";
        return 1;
    }

    if (mode == "txn_consume" && groupId.empty()) {
        std::cerr << "CLIENT: RUNE_GROUP_ID is required for mode=txn_consume\n";
        return 1;
    }

    std::cerr << "CLIENT: cid=" << cid << " mode=" << mode
              << " target=" << target << " records=" << records
              << " keys=" << keys << " rateHz=" << rateHz
              << " from(wire)=" << from.toWire() << " expect=" << expect
              << " slowMs=" << slowMs << " subPartitions=" << subPartitions
              << " subPartitionBase=" << subPartitionBase
              << " acks=" << acksStr << "\n";

    ClientConfig cfg;
    cfg.gatewayChannel   = target;
    cfg.gatewayStreamId  = targetStream;
    cfg.egressChannel    = egress;
    cfg.egressStreamId   = egressStream;
    cfg.dataDir          = dataDir;
    rune::examples::applyEnvCredentials(cfg);   // RUNE_USER / RUNE_PASSWORD_FILE
    cfg.connectTimeoutMs = static_cast<int64_t>(timeoutMs);
    cfg.pingTimeoutMs    = static_cast<int64_t>(timeoutMs);
    cfg.idleMode         = IdleMode::LowLatency;
    cfg.topics           = parseTopics(topicsStr);
    if (cfg.topics.empty()) {
        std::cerr << "CLIENT: no valid topics parsed from RUNE_CLIENT_TOPIC_TABLE='" << topicsStr << "'\n";
        return 1;
    }

    // client<->broker TLS/AEAD (Task 16 encrypted e2e): enable crypto from env.
    //   RUNE_CLIENT_TLS=1          offer crypto (v(crypto) ConnectRequest + TLS 1.3 handshake)
    //   RUNE_TLS_TRUST_PEM=<path>  CA-bundle file to trust; unset => system store
    //   RUNE_TLS_EXPECT_HOST=<h>   hostname checked against the broker cert SAN (e.g. localhost)
    //   RUNE_TLS_REQUIRE=1         fail the connection unless it ends up encrypted (anti-downgrade)
    if (getEnvInt("RUNE_CLIENT_TLS", 0) != 0) {
        cfg.offerCrypto   = true;
        cfg.requireCrypto = getEnvInt("RUNE_TLS_REQUIRE", 0) != 0;
        cfg.tlsExpectHost = getEnvStr("RUNE_TLS_EXPECT_HOST");
        const std::string trustPath = getEnvStr("RUNE_TLS_TRUST_PEM");
        if (!trustPath.empty()) {
            std::ifstream tf(trustPath, std::ios::binary);
            if (!tf) {
                std::cerr << "CLIENT: cannot read RUNE_TLS_TRUST_PEM='" << trustPath << "'\n";
                return 1;
            }
            std::ostringstream ss;
            ss << tf.rdbuf();
            cfg.tlsTrustPem = ss.str();  // PEM text (makeClient's trust bundle)
        }  // else leave tlsTrustPem = nullopt => trust the system store
        std::cerr << "CLIENT: TLS enabled (offerCrypto=1 require=" << (cfg.requireCrypto ? 1 : 0)
                  << " expectHost='" << cfg.tlsExpectHost << "' trust="
                  << (trustPath.empty() ? std::string("system") : trustPath) << ")\n";
    }

    int rc = 0;
    int produced = 0, delivered = 0;

    try {
        Client c(cfg);
        c.onError([](const std::string& msg) { std::cerr << "CLIENT: onError: " << msg << "\n"; });

        if (mode == "txn_produce") {
            // TX-5: txn_produce — init→begin→send→commit/abort.
            // The transactional Producer (ProducerConfig::transactionalId) is created in doTxnProduce,
            // after connect() here, like any other producer.
            std::cerr << "CLIENT: txn_produce mode txnId=" << txnId
                      << " commit=" << txnCommit << " records=" << records << "\n";
            std::cerr << "CLIENT: connecting...\n";
            c.connectAndWait();
            std::cerr << "CLIENT: connected\n";
            const TopicHandle thTxn = c.topic(cfg.topics[0].name);
            if (!thTxn.valid()) {
                std::cerr << "CLIENT: topic '" << cfg.topics[0].name << "' not valid\n";
                return 1;
            }
            rc = doTxnProduce(c, thTxn, txnId, cid, records, keys,
                              txnCommit, timeoutMs);
            c.close();
            return rc;
        }

        if (mode == "txn_consume") {
            // TX-5: txn_consume — GroupConsumer with configured isolation level.
            // subscribeGroup MUST be called before connect() — doTxnConsume handles that.
            std::vector<std::string> topicNames;
            topicNames.reserve(cfg.topics.size());
            for (const auto& ts : cfg.topics) topicNames.push_back(ts.name);
            std::cerr << "CLIENT: txn_consume mode groupId=" << groupId
                      << " isolation=" << isolationStr
                      << " windowMs=" << subWindowMs << "\n";
            rc = doTxnConsume(c, topicNames, cid, subWindowMs, autoCommitMs,
                              groupId, txnIsolation);
            c.close();
            return rc;
        }

        if (mode == "group") {
            // Group mode: register callbacks BEFORE connect() (setup-phase contract).
            // Build the list of topic names from cfg.topics.
            std::vector<std::string> topicNames;
            topicNames.reserve(cfg.topics.size());
            for (const auto& ts : cfg.topics) topicNames.push_back(ts.name);

            std::cerr << "CLIENT: group mode groupId=" << groupId
                      << " topics=" << topicsStr
                      << " windowMs=" << subWindowMs
                      << " autoCommitMs=" << autoCommitMs << "\n";

            // subscribeGroup + callback registration must happen before connect()
            // so the runner sees the callbacks under the happens-before that
            // connect() publishes.  doGroup returns after the windowMs drain.
            const int consumed = doGroup(c, groupId, topicNames, cid, subWindowMs, autoCommitMs);
            std::cout << "GROUPSUMMARY cid=" << cid
                      << " consumed=" << consumed
                      << " rc=0\n";
            std::cout.flush();
            std::cout << "SUMMARY produced=0 delivered=" << consumed
                      << " mode=group rc=0\n";
            std::cout.flush();
            c.close();
            return 0;
        }

        std::cerr << "CLIENT: connecting...\n";
        c.connectAndWait();
        std::cerr << "CLIENT: connected\n";

        // No settle window needed: DR-BUG-1 is fixed. Producer registration
        // is now safe to call at any time after Client construction via the
        // lock-free copy-on-write snapshot in ProducerHub.

        const TopicHandle th = c.topic(cfg.topics[0].name);
        if (!th.valid()) {
            std::cerr << "CLIENT: topic '" << cfg.topics[0].name << "' not valid\n";
            return 1;
        }

        const bool doP = (mode == "produce" || mode == "produce_subscribe");
        const bool doS = (mode == "subscribe" || mode == "produce_subscribe");

        if (doP) {
            produced = doProduce(c, th, cid, records, keys, rateHz, timeoutMs, th.partitionCount, acksPolicy);
            if (produced < records) {
                std::cerr << "CLIENT: FAIL produced " << produced << "/" << records << "\n";
                rc = 1;
            }
        }

        if (doS && rc == 0) {
            delivered = doSubscribe(c, th, from, expect, subWindowMs, slowMs,
                                    expectCidMin, expectCidExact, subPartitions,
                                    subPartitionBase);
            if (expect > 0 && delivered < expect) {
                std::cerr << "CLIENT: FAIL delivered " << delivered << "/" << expect << "\n";
                rc = 1;
            }
        }

        std::cout << "SUMMARY produced=" << produced << " delivered=" << delivered
                  << " mode=" << mode << " rc=" << rc << "\n";
        std::cout.flush();
        c.close();
    } catch (const std::exception& ex) {
        std::cerr << "CLIENT: FATAL exception: " << ex.what() << "\n";
        std::cout << "SUMMARY produced=" << produced << " delivered=" << delivered
                  << " mode=" << mode << " rc=2\n";
        std::cout.flush();
        rc = 2;
    }
    return rc;
}
