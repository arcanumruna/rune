#pragma once
// CheckedWritesStressDriver.h - the seeded cluster stress of the checked-writes verdict pipeline (plan 04 Task 11, matrix D; plan 05 Task 9, matrix D).
// Three real brokers (ThreeNodes), three real clients writing All|View to one typed, compacted source with two TestReject members
// (object 7 TagEquals{9}, object 8 CapLiveKeys{n}, replaced, dropped and installed again by the run) installed on every node, and
// an ordinary projection view.  A raw and a group consumer read the source from offset 0.  step() draws one seeded action; quiesce() brings the cluster to rest and checkAll() judges the committed history with the independent
// CheckedWritesModel and checks the named invariants against every node.  Every wait is a bounded poll: the deadline is the failure.
// Key-index mode (plan 05): the source is the compacted `ki` topic {id uint64 key, name string(16), n int64} with two partitions and
// key.index on, no TestReject members; three typed clients send insert / update / removeExisting / remove on 64 ids, one write at a
// time, and IntentVerdictsExact and KeyIndexEqualsLiveKeys join the invariants above, which then run on both partitions.
#include "CheckedWritesChecks.h"
#include "CheckedWritesModel.h"
#include "client/ClusterClient.h"
#include "client/support/KeyIndexRecords.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/Verdict.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace rune::node::cw {

using namespace std::chrono_literals;

class CheckedWritesStressDriver {
public:
    using Clock = std::chrono::steady_clock;
    using Invariant = std::function<bool(CheckedWritesStressDriver&, std::string*)>;
    static constexpr uint64_t kObjTag = 7, kObjCap = 8;
    static constexpr int kTagKeys = 12, kKiKeys = 64, kKiParts = 2;
    enum class Op : uint8_t { Insert, Update, RemoveExisting, Remove, Upsert };

    /// What a node holds of one partition, read on its own threads.
    struct NodeSnap {
        int id = 0, part = 0;
        int64_t leo = 0, hw = 0, logStart = 0;
        bool leader = false;                                 ///< the replica led when it was read
        VerdictRig::RigLog log;                              ///< below the HW
        std::map<int64_t, ModelVerdict> index;               ///< the node's RejectedOffsetIndex
        uint64_t mismatches = 0, unknownDuplicates = 0, skippedMalformed = 0;
        int64_t committedThrough = 0, covered = 0;
        std::set<uint64_t> keys;                             ///< key-index mode: the ids the node's key index holds
        std::size_t keyCount = 0;
    };
    /// One send and what the writer was told.  Key-index mode: `v` is the record's token (what readLogOf reads of its name; 0 for
    /// a tombstone), `code` the broker's 56 / 57, `hwBefore` / `hwAfter` the partition's high watermark around the write.
    struct Sent {
        uint64_t id = 0;
        int64_t v = 0;
        uint8_t tag = 0;
        int part = 0;
        Op op = Op::Upsert;
        uint16_t code = 0;
        int64_t offset = -1, hwBefore = 0, hwAfter = 0;
        bool windowKnown = false;                            ///< hwBefore / hwAfter bracket the write alone (no node went down during it)
        enum class Result : uint8_t { Ok, Violation, Unknown } result = Result::Unknown;
    };

    CheckedWritesStressDriver(rune::node::test::catalog_cluster::ThreeNodes& cluster, uint64_t seed, bool keyIndex = false)
        : c_(cluster), rng_(seed), seed_(seed), keyIndex_(keyIndex), parts_(keyIndex ? kKiParts : 1), keys_(keyIndex ? kKiKeys : kTagKeys),
          seen_(static_cast<std::size_t>(parts_)), data_(static_cast<std::size_t>(parts_)), changes_(static_cast<std::size_t>(parts_)), models_(static_cast<std::size_t>(parts_)),
          deliveredOffsets_(static_cast<std::size_t>(parts_)), rawIntents_(static_cast<std::size_t>(parts_)), groupOffsets_(static_cast<std::size_t>(parts_)) {
        registerDefaults_();
        if (keyIndex_) registerKeyIndexInvariants_();
    }
    ~CheckedWritesStressDriver() {
        group_.reset();
        subs_.clear();
        subReaders_.clear();
        reader_.reset();
        writers_.clear();
        kiWriters_.clear();
    }

    /// The typed topic (replicas 3), the projection view, TestReject on every node (not in key-index mode) and three clients.
    void setUp() {
        namespace tcc = rune::node::test::catalog_cluster;
        ASSERT_TRUE(c_.awaitCatalogLeader(30s));
        const int32_t ctl = tcc::awaitController(c_);
        ASSERT_GT(ctl, 0);
        auto admin = tcc::connectClient(c_, ctl);
        rune::client::TopicOptions to;
        to.replicationFactor = 3;
        to.config.cleanupPolicy = 1;
        to.config.segmentRollBytes = 64 * 1024;
        to.config.minCompactionLagMs = 100;
        to.config.minCleanableDirtyRatioPpm = 1;
        to.config.fileDeleteDelayMs = 50;
        to.keyFeatures.keyIndex = keyIndex_;
        const auto desc = keyIndex_ ? rune::codec::serialize(rune::node::test::kiAccountDescriptor()) : rune::codec::serialize(verdictSourceDescriptor());
        const auto src = admin->defineTopicAndWait(topicName_(), static_cast<uint32_t>(parts_), "id", desc, rune::client::Persistence::Disk, to, {}, 60s);
        ASSERT_EQ(src.errorCode, 0) << src.detail;
        topicId_ = src.topicId;
        const auto view = admin->createViewAndWait(viewName_(), "id", 0, viewPlan_(topicId_), {}, 60s);
        ASSERT_EQ(view.errorCode, 0) << view.detail;
        viewTopicId_ = view.viewTopicId;
        admin->close();
        admin.reset();
        std::vector<int32_t> leaders;
        ASSERT_TRUE(tcc::awaitLeaders(c_, topicId_, parts_, leaders, 30s));
        for (const int id : c_.runningNodes()) prepareNode_(id);
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        rebuildWriters_(-1);
    }

    [[nodiscard]] std::string keyIndexTally() const { return std::to_string(pinned56_) + " KeyExists and " + std::to_string(pinned57_) + " KeyNotFound pinned to their record, " + std::to_string(provenCompacted_) + " rejections proven compacted away, " + std::to_string(sent_.size()) + " writes"; }

    /// One seeded action.
    void step() {
        const unsigned r = static_cast<unsigned>(rng_() % 100);
        const int down = downNode_();
        if (r < 55) {
            sendBurst_(-1);
        } else if (r < 65 && down == 0) {
            crashLeaderDuringSends();
        } else if (r < 80) {
            if (down != 0) restartNode_(down, false);
            else restartNonLeader_();
        } else if (r < 92) {
            if (keyIndex_) sendBurst_(-1);   // no constraint set to change: another burst
            else changeConstraintSet_();
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(50 + static_cast<int>(rng_() % 150)));
        }
    }

    /// Crashes a random partition's leader in the middle of a burst of writes.
    void crashLeaderDuringSends() {
        const int32_t victim = partitionLeader_(randomPart_());
        if (victim <= 0) return;
        ++crashes_;
        sendBurst_(victim);
        writersStale_ = true;
    }

    /// Every node back, one leader per partition, every replica caught up and the judge past every record.
    void quiesce() {
        for (int id = 1; id <= 3; ++id)
            if (!c_.running(id)) restartNode_(id, false);
        std::vector<int32_t> leaders;
        ASSERT_TRUE(rune::node::test::catalog_cluster::awaitLeaders(c_, topicId_, parts_, leaders, wait_(60s)));
        const auto deadline = Clock::now() + wait_(90s);
        std::string last;
        for (int poll = 0; poll < kPollCap && Clock::now() < deadline; ++poll, std::this_thread::sleep_for(100ms)) {
            snapAll_();
            if (atRest_(&last)) {
                recordHistory_(&snaps_);
                return;
            }
        }
        ADD_FAILURE() << "seed " << seed_ << ": the cluster did not come to rest (" << last << ")";
    }

    void registerInvariant(std::string name, Invariant fn) { invariants_.emplace_back(std::move(name), std::move(fn)); }

    /// Judges the history and runs every invariant; the name of the first that fails (and why) lands in `failed`.
    [[nodiscard]] bool checkAll(std::string* failed) {
        awaitCompaction_();
        compactedAway_ = 0;   // measured before the invariants: the first failure ends checkAll
        for (const NodeSnap& s : snaps_) {
            std::set<int64_t> present;
            for (const auto& rec : s.log.data) present.insert(rec.offset);
            std::size_t away = 0;
            for (const auto& kv : data_[s.part]) away += present.count(kv.first) == 0 ? 1 : 0;
            compactedAway_ = std::max(compactedAway_, away);
        }
        for (int p = 0; p < parts_; ++p) {
            models_[p] = CheckedWritesModel();
            if (keyIndex_) {
                models_[p].addKeyIndexRule();
            } else {
                models_[p].declareRule(kObjTag, tagRule_().fingerprint(), CheckedWritesModel::tagEquals(9));
                for (const uint32_t cap : caps_) models_[p].declareRule(kObjCap, capRule_(cap).fingerprint(), CheckedWritesModel::capLiveKeys(cap));
            }
            models_[p].reset(history_(p));
        }
        for (const auto& [name, fn] : invariants_) {
            std::string why;
            if (!fn(*this, &why)) {
                if (failed != nullptr) *failed = "seed " + std::to_string(seed_) + " " + name + ": " + why;
                return false;
            }
        }
        return true;
    }

    /// How often each fault ran: leader crashes, follower or node restarts, restarts cut short.
    [[nodiscard]] std::array<int, 3> faults() const noexcept { return {crashes_, restarts_, bounces_}; }
    [[nodiscard]] const CheckedWritesModel& model(int part = 0) const noexcept { return models_[static_cast<std::size_t>(part)]; }
    [[nodiscard]] const std::vector<NodeSnap>& snaps() const noexcept { return snaps_; }
    [[nodiscard]] const std::vector<Sent>& sent() const noexcept { return sent_; }
    [[nodiscard]] int32_t viewTopicId() const noexcept { return viewTopicId_; }
    /// The most records the history holds that one node's log no longer holds (a compaction ran).
    [[nodiscard]] std::size_t compactedAway() const { return compactedAway_; }

    /// Key-index mode: one write of `id` through the first client, kept like a burst's write but with no history read after it.
    Sent kiWrite(Op op, uint64_t id) {
        Sent s;
        s.id = id;
        s.op = op;
        s.part = static_cast<int>(rune::client::Key(id).partition(parts_));
        s.windowKnown = true;
        s.hwBefore = highWatermark_(s.part);
        kiSend_(*kiWriters_.front(), &s);
        s.hwAfter = highWatermark_(s.part);
        sent_.push_back(s);
        return s;
    }
    /// The data offsets the partition's leader holds below its high watermark.
    std::set<int64_t> leaderOffsets(int part) {
        std::vector<int32_t> leaders;
        std::set<int64_t> out;
        if (!rune::node::test::catalog_cluster::awaitLeaders(c_, topicId_, parts_, leaders, 5s)) return out;
        c_.node(leaders[static_cast<std::size_t>(part)]).onOwnerThreadForTest(topicId_, part, [&out](rune::replication::PartitionReplica& r) {
            for (const auto& d : readLogOf(*r.log(), r.highWatermark()).data) out.insert(d.offset);
            return 0;
        });
        return out;
    }
    [[nodiscard]] int32_t topicId() const noexcept { return topicId_; }
    /// Key-index mode: `fn` sees every write just before it is sent, and may throw in its place.
    void setKiSendHookForTest(std::function<void(const Sent&)> fn) { kiSendHook_ = std::move(fn); }
    /// `fn` runs before every view row lookup, and may throw in its place.
    void setViewLookupHookForTest(std::function<void()> fn) { viewLookupHook_ = std::move(fn); }
    /// Runs the one registered invariant `name` against the history as it stands.
    [[nodiscard]] bool checkInvariantForTest(const std::string& name, std::string* why) {
        for (const auto& [n, fn] : invariants_)
            if (n == name) return fn(*this, why);
        *why = "no invariant " + name;
        return false;
    }
    /// Holds (or lets go) every leadership scan on node `id`: its RC build sees nothing resident, so a partition it leads stays
    /// unjudged and its index empty.
    void holdLeadershipScan(int id, bool hold) {
        for (int p = 0; p < parts_; ++p)
            c_.node(id).onWorkerThreadForTest(worker_(id, p), [hold](Worker& wk) {
                wk.setRcResidencyProbeForTest(hold ? +[](void*, const uint8_t*, std::size_t) -> std::size_t { return 0; } : nullptr, nullptr);
                return 0;
            });
    }
    [[nodiscard]] int rcStatus(int id, int part) {
        const int32_t t = topicId_;
        return c_.node(id).onWorkerThreadForTest(worker_(id, part), [t, part](Worker& wk) { return wk.rcStatusForTest(t, part); });
    }

private:
    // ---- clients ----
    struct Writer {
        std::unique_ptr<rune::client::Client> client;
        std::optional<rune::client::Producer> prod;
        std::unique_ptr<rune::client::Completions> cq;
        std::optional<rune::client::TopicHandle> topic;
        uint64_t next = 1;

        Writer(rune::client::ClientConfig cfg, int32_t topicId) {
            cfg.topics.push_back(rune::client::TopicSpec{"vt", topicId, 1});
            client = std::make_unique<rune::client::Client>(cfg);
            rune::client::ProducerConfig pc;
            pc.acks = rune::client::Acks::All | rune::client::Acks::View;
            pc.lingerUs = 1;
            prod.emplace(client->producer(pc));
            client->connectAndWait(30s);
            cq = std::make_unique<rune::client::Completions>(*client);
            topic.emplace(client->topic("vt"));
        }
        ~Writer() {
            if (prod) prod->closeImmediately();
            if (client) client->close();
        }
        uint64_t send(uint64_t id, const std::string& value) {
            const uint64_t tok = next++;
            rune::client::SendResult r = rune::client::SendResult::BackPressured;
            while (r == rune::client::SendResult::BackPressured)
                r = prod->trySend(*topic, rune::client::Key(id), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(value.data()), value.size()), *cq, tok);
            prod->flush();
            return tok;
        }
        std::optional<rune::client::Completion> await(uint64_t tok, std::chrono::nanoseconds within) {
            std::optional<rune::client::Completion> out;
            (void)cq->waitFor(tok, within, [&](const rune::client::Completion& d, std::span<const uint8_t>) { out = d; });
            return out;
        }
    };

    /// Key-index mode: a client with a typed topic over its All|View producer.
    struct KiWriter {
        std::unique_ptr<rune::client::Client> client;
        std::optional<rune::client::Producer> prod;
        std::optional<rune::client::TypedTopic<rune::node::test::KiAccount, rune::client::ProducerRef>> topic;

        explicit KiWriter(rune::client::ClientConfig cfg) {
            client = std::make_unique<rune::client::Client>(cfg);
            rune::client::ProducerConfig pc;
            pc.acks = rune::client::Acks::All | rune::client::Acks::View;
            pc.lingerUs = 1;
            prod.emplace(client->producer(pc));
            client->connectAndWait(30s);
            topic.emplace(client->openTopicAndWait<rune::node::test::KiAccount>("ki", *prod, 30s));
        }
        ~KiWriter() {
            if (prod) prod->closeImmediately();
            if (client) client->close();
        }
    };

    static std::string value_(uint64_t id, int64_t v, uint16_t tag) {
        std::string val;
        const auto le16 = [&](uint16_t x) { val.push_back(static_cast<char>(x)); val.push_back(static_cast<char>(x >> 8)); };
        le16(18); le16(1);
        for (int i = 0; i < 8; ++i) val.push_back(static_cast<char>(id >> (8 * i)));
        for (int i = 0; i < 8; ++i) val.push_back(static_cast<char>(static_cast<uint64_t>(v) >> (8 * i)));
        le16(tag);
        return val;
    }
    const char* topicName_() const { return keyIndex_ ? "ki" : "vt"; }
    const char* viewName_() const { return keyIndex_ ? "ki_v" : "vt_v"; }
    /// The projection: id, v, tag; of the key.index topic id and its name, whose first 8 bytes are the write's token (the row's v).
    std::vector<uint8_t> viewPlan_(int32_t src) const {
        namespace rv = rune::view;
        rv::ViewPlan p;
        p.viewName = viewName_();
        p.key = "id";
        p.sources.push_back(rv::SourceSpec{src, "id"});
        p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::UInt64, 0, "id", rv::MergeRule::Replace});
        if (keyIndex_) {
            p.output.push_back(rv::AuthoredField{"name", rune::codec::PrimType::FixedStr, 0, "name", rv::MergeRule::Replace});
        } else {
            p.output.push_back(rv::AuthoredField{"v", rune::codec::PrimType::Int64, 0, "v", rv::MergeRule::Replace});
            p.output.push_back(rv::AuthoredField{"tag", rune::codec::PrimType::UInt16, 0, "tag", rv::MergeRule::Replace});
        }
        return rv::serializeAuthored(p);
    }
    static TestRejectRule tagRule_() { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = 9; return r; }
    static TestRejectRule capRule_(uint32_t cap) { TestRejectRule r; r.mode = TestRejectRule::Mode::CapLiveKeys; r.cap = cap; return r; }
    static constexpr int32_t kTagTopic = 0x40000701, kCapTopicBase = 0x40000710;

    // ---- nodes ----
    int32_t worker_(int id, int part = 0) { return rune::metadata::ShardRouter(c_.node(id).workerCountForTest()).workerForPartition(topicId_, part); }

    /// The node serves the partition and holds the cluster feature level: TestReject goes on its worker.
    void prepareNode_(int id) {
        BrokerNode& n = c_.node(id);
        const int32_t w = worker_(id);
        for (int part = 0; part < parts_; ++part) {
            bool ready = false;
            const auto deadline = Clock::now() + wait_(60s);
            for (int poll = 0; poll < kPollCap && Clock::now() < deadline && !ready; ++poll, std::this_thread::sleep_for(50ms))
                ready = n.onWorkerThreadForTest(worker_(id, part), [this, part](Worker& wk) {
                    return wk.clusterFeatureLevelForTest() >= rune::replication::MetadataFeature::kCheckedWrites && wk.writeStateForTest(topicId_, part) != nullptr;
                });
            ASSERT_TRUE(ready) << "node " << id << " never served partition " << part << " of the source";
        }
        if (keyIndex_) return;
        const int32_t src = topicId_;
        const int32_t capTopic = capTopic_;
        const TestRejectRule capRule = capRule_(capValue_);
        n.onWorkerThreadForTest(w, [src, capTopic, capRule](Worker& wk) {
            installReject_(wk, src, "vt_rej7", kTagTopic, kObjTag, tagRule_());
            if (capTopic != 0) installReject_(wk, src, "vt_rej8", capTopic, kObjCap, capRule);
            return 0;
        });
    }
    static void installReject_(Worker& wk, int32_t src, const char* name, int32_t viewTopic, uint64_t object, TestRejectRule rule) {
        const auto desc = verdictSourceDescriptor();
        rune::view::ViewPlan v;
        v.viewName = name; v.key = "id"; v.sources = {{src, "id"}};
        v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rune::view::MergeRule::Replace}};
        v.persistence = 1;
        const rune::codec::Descriptor* srcs[] = {&desc};
        auto comp = rune::view::compileView(v, srcs, viewTopic);
        EXPECT_TRUE(comp.has_value());
        wk.viewSet()->installTestVerdictViewForTest(comp->plan, comp->rowLayout, desc, false, object, rule);
    }

    /// The CapLiveKeys member is dropped, or installed again with another cap, on every running node.  A restart or failover
    /// before the judge crossed the change would wait for a plan that is gone, so the change is settled (a write acked and
    /// marked past it) before the next fault.
    void changeConstraintSet_() {
        settle_();
        const int32_t oldTopic = capTopic_;
        if (oldTopic != 0) {
            capTopic_ = 0;
        } else {
            capValue_ = 5 + static_cast<uint32_t>(rng_() % 7);
            capTopic_ = kCapTopicBase + (++capSeq_);
            caps_.insert(capValue_);
        }
        const int32_t newTopic = capTopic_;
        const TestRejectRule rule = capRule_(capValue_);
        const int32_t src = topicId_;
        for (const int id : c_.runningNodes()) {
            c_.node(id).onWorkerThreadForTest(worker_(id), [=](Worker& wk) {
                if (oldTopic != 0) wk.viewSet()->uninstallView(oldTopic);
                if (newTopic != 0) installReject_(wk, src, "vt_rej8", newTopic, kObjCap, rule);
                return 0;
            });
        }
        settle_();
    }
    /// Writes until one is acked, then waits for the leader's judge and committed markers to cover everything written.
    void settle_() {
        if (::testing::Test::HasFatalFailure()) return;
        bool acked = false;
        for (int i = 0; i < 20 && !acked; ++i) {
            const std::size_t before = sent_.size();
            sendBurst_(-1);
            for (std::size_t k = before; k < sent_.size(); ++k) acked = acked || sent_[k].result != Sent::Result::Unknown;
        }
        ASSERT_TRUE(acked) << "seed " << seed_ << ": no write was answered after a constraint-set change";
        for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline; std::this_thread::sleep_for(50ms)) {
            std::vector<NodeSnap> snaps;
            for (const int id : c_.runningNodes()) snaps.push_back(snapNode_(id, 0));
            const NodeSnap* lead = leaderOf_(snaps, 0);
            if (lead != nullptr && lead->committedThrough >= dataEnd_(lead->log) && lead->hw == lead->leo) return;
        }
        ADD_FAILURE() << "seed " << seed_ << ": the judge did not cover the writes after a constraint-set change";
    }

    int downNode_() const {
        for (int id = 1; id <= 3; ++id) if (!c_.running(id)) return id;
        return 0;
    }
    /// A node's counters die with it: the ones that must stay zero are read just before it goes.
    void checkCounters_(int id) {
        if (!keyIndex_) return;
        for (int p = 0; p < parts_; ++p) {
            const NodeSnap s = snapNode_(id, p);
            EXPECT_EQ(s.mismatches, 0u) << "seed " << seed_ << ": node " << id << " partition " << p << " before stopping";
            EXPECT_EQ(s.unknownDuplicates, 0u) << "seed " << seed_ << ": node " << id << " partition " << p << " before stopping";
            EXPECT_EQ(s.skippedMalformed, 0u) << "seed " << seed_ << ": node " << id << " partition " << p << " before stopping";
        }
    }
    void restartNode_(int id, bool bounce) {
        c_.startNode(id);
        ++restarts_;
        if (bounce) {
            ++bounces_;   // a restart in the middle of the rebuild
            std::this_thread::sleep_for(std::chrono::milliseconds(100 + static_cast<int>(rng_() % 400)));
            c_.stop(id);
            c_.startNode(id);
        }
        prepareNode_(id);
        writersStale_ = true;
        std::vector<int32_t> leaders;
        ASSERT_TRUE(rune::node::test::catalog_cluster::awaitLeaders(c_, topicId_, parts_, leaders, wait_(60s)));
        recordHistory_();
    }
    int randomPart_() { return parts_ > 1 ? static_cast<int>(rng_() % static_cast<unsigned>(parts_)) : 0; }
    int32_t partitionLeader_(int part) {
        std::vector<int32_t> leaders;
        EXPECT_TRUE(rune::node::test::catalog_cluster::awaitLeaders(c_, topicId_, parts_, leaders, wait_(60s)));
        return leaders.empty() ? -1 : leaders[static_cast<std::size_t>(part)];
    }
    /// Stops a node that does not lead a random partition (it may lead the other) and starts it again; one in three is cut short.
    void restartNonLeader_() {
        const int32_t leader = partitionLeader_(randomPart_());
        std::vector<int> followers;
        for (int id = 1; id <= 3; ++id) if (id != leader) followers.push_back(id);
        const int id = followers[rng_() % followers.size()];
        checkCounters_(id);
        c_.stop(id);
        restartNode_(id, rng_() % 3 == 0);
    }

    // ---- writes ----
    void rebuildWriters_(int avoid) {
        writers_.clear();
        kiWriters_.clear();
        std::vector<int> seeds;
        for (const int id : c_.runningNodes()) if (id != avoid) seeds.push_back(id);
        for (std::size_t i = 0; i < 3; ++i) {
            rune::client::ClientConfig cfg = rune::node::test::catalog_cluster::clientConfigFor(c_, seeds[i % seeds.size()]);
            cfg.connectTimeoutMs = 30'000;
            cfg.pingTimeoutMs = 30'000;
            if (keyIndex_) kiWriters_.push_back(std::make_unique<KiWriter>(cfg));
            else writers_.push_back(std::make_unique<Writer>(cfg, topicId_));
        }
        writersStale_ = false;
    }
    void sendBurst_(int victim) {
        if (writersStale_ || victim > 0) rebuildWriters_(victim);
        if (keyIndex_) return kiBurst_(victim);
        Writer& w = *writers_[rng_() % writers_.size()];
        const int n = 1 + static_cast<int>(rng_() % 80);
        std::vector<std::pair<uint64_t, std::size_t>> toks;
        std::jthread killer;   // joined on unwind too: a send that throws mid-crash must not destroy it joinable
        for (int i = 0; i < n; ++i) {
            Sent s;
            s.id = 1 + rng_() % keys_;
            s.tag = (rng_() % 100) < 20 ? 9 : static_cast<uint8_t>(1 + rng_() % 3);
            s.v = ++vSeq_;
            if (victim > 0 && i == n / 2) { checkCounters_(victim); killer = std::jthread([this, victim] { c_.crash(victim); }); }
            toks.emplace_back(w.send(s.id, value_(s.id, s.v, s.tag)), sent_.size());
            sent_.push_back(s);
        }
        if (killer.joinable()) killer.join();
        for (const auto& [tok, idx] : toks) {
            const auto d = w.await(tok, 60s);
            if (d && d->status == rune::client::OpStatus::Ok) sent_[idx].result = Sent::Result::Ok;
            else if (d && d->status == rune::client::OpStatus::BrokerError && d->errorCode == static_cast<uint16_t>(rune::protocol::client::ErrorCode::ConstraintViolation))
                sent_[idx].result = Sent::Result::Violation;
        }
        recordHistory_();
    }
    /// Key-index mode: 1 to 6 typed writes through one client, each waited for, with the partition's high watermark around it so an
    /// answer without an offset (a rejection carries none) is found again in the log.  Every outcome is one the model can account for.
    void kiBurst_(int victim) {
        KiWriter& w = *kiWriters_[rng_() % kiWriters_.size()];
        const int n = 1 + static_cast<int>(rng_() % 6);
        std::jthread killer;   // joined on unwind too: a send that throws mid-crash must not destroy it joinable
        for (int i = 0; i < n && !::testing::Test::HasFatalFailure(); ++i) {
            Sent s;
            s.id = 1 + rng_() % keys_;
            const unsigned o = static_cast<unsigned>(rng_() % 100);
            s.op = o < 35 ? Op::Insert : o < 60 ? Op::Update : o < 80 ? Op::RemoveExisting : Op::Remove;
            s.part = static_cast<int>(rune::client::Key(s.id).partition(parts_));
            s.windowKnown = !killer.joinable() && (victim <= 0 || i < n / 2);
            if (s.windowKnown) s.hwBefore = highWatermark_(s.part);
            if (victim > 0 && i == n / 2) { checkCounters_(victim); killer = std::jthread([this, victim] { c_.crash(victim); }); }
            kiSend_(w, &s);
            // A node going down cannot be asked for its high watermark: a rejected removeExisting, found only by its window, is not
            // judged.  An insert or update is found by its token, and a 56 / 57 answers a committed record, which survives the crash.
            if (s.windowKnown) s.hwAfter = highWatermark_(s.part);
            else if (s.result == Sent::Result::Violation && s.op == Op::RemoveExisting) s.result = Sent::Result::Unknown;

            if (s.result == Sent::Result::Unknown) writersStale_ = true;   // no answer: the write may or may not be in the log
            sent_.push_back(s);
        }
        if (killer.joinable()) killer.join();
        recordHistory_();
    }
    /// Sends `s` (its id and op) through `w`: its token is the new record's name, its result the answer.
    void kiSend_(KiWriter& w, Sent* s) {
        if (kiSendHook_) kiSendHook_(*s);
        const rune::node::test::KiAccount rec{s->id, "n" + std::to_string(++vSeq_), vSeq_};
        if (s->op == Op::Insert || s->op == Op::Update) std::memcpy(&s->v, rec.name.data(), std::min<std::size_t>(rec.name.size(), sizeof(s->v)));
        const rune::client::WriteResult r = s->op == Op::Insert ? w.topic->insertAndWait(rec, 10s)
                                          : s->op == Op::Update ? w.topic->updateAndWait(rec, 10s)
                                          : s->op == Op::RemoveExisting ? w.topic->removeExistingAndWait(s->id, 10s)
                                          : w.topic->removeAndWait(s->id, 10s);
        classify_(r, s);
    }
    /// Ok, KeyExists (insert), KeyNotFound (update, removeExisting) and the outcomes that carry no verdict (a timeout, a lost
    /// session, NotLeader, VerdictUnknown) are the ones a write can come back with; anything else fails the step.
    void classify_(const rune::client::WriteResult& r, Sent* s) {
        using Status = rune::client::OpStatus;
        namespace pc = rune::protocol::client;
        switch (r.status()) {
        case Status::Ok:
            s->result = Sent::Result::Ok;
            s->offset = r.offset();
            return;
        case Status::BrokerError:
            s->code = r.code();
            if ((s->op == Op::Insert && s->code == static_cast<uint16_t>(pc::ErrorCode::KeyExists)) ||
                ((s->op == Op::Update || s->op == Op::RemoveExisting) && s->code == static_cast<uint16_t>(pc::ErrorCode::KeyNotFound))) {
                s->result = Sent::Result::Violation;
                ++(s->code == static_cast<uint16_t>(pc::ErrorCode::KeyExists) ? keyExists_ : keyNotFound_);
                return;
            }
            if (s->code == static_cast<uint16_t>(pc::ErrorCode::NotLeader)) return;
            break;
        case Status::Timeout: case Status::VerdictUnknown: case Status::DeliveryFailed: case Status::SessionLost: case Status::NotConnected: case Status::Closed:
            return;
        default:
            break;
        }
        ADD_FAILURE() << "seed " << seed_ << ": write of id " << s->id << " (op " << static_cast<int>(s->op) << ") came back with status " << static_cast<int>(r.status()) << " code " << r.code();
    }
    /// The highest high watermark any running node reports for the partition.
    int64_t highWatermark_(int part) {
        int64_t hw = 0;
        for (const int id : c_.runningNodes())
            c_.node(id).onOwnerThreadForTest(topicId_, part, [&hw](rune::replication::PartitionReplica& r) {
                hw = std::max(hw, r.highWatermark());
                return 0;
            });
        return hw;
    }

    // ---- reading the cluster ----
    NodeSnap snapNode_(int id, int part) {
        NodeSnap s;
        s.id = id;
        s.part = part;
        BrokerNode& n = c_.node(id);
        n.onOwnerThreadForTest(topicId_, part, [&s](rune::replication::PartitionReplica& r) {
            s.leo = r.logEndOffset();
            s.hw = r.highWatermark();
            s.logStart = r.log()->logStartOffset();
            s.leader = r.role() == rune::replication::Role::Leader;
            s.log = readLogOf(*r.log(), s.hw);
            return 0;
        });
        n.onWorkerThreadForTest(worker_(id, part), [&s, this](Worker& wk) {
            if (const PartitionWriteState* ws = wk.writeStateForTest(topicId_, s.part)) {
                ws->rejected.forEachIn(0, INT64_MAX, [&](const rune::storage::RejectedOffsetIndex::Entry& e) { s.index[e.offset] = {e.code, e.constraintId}; });
                s.mismatches = ws->rejected.mismatches();
                s.committedThrough = ws->markers.committedThrough();
                s.covered = ws->rejected.coveredThrough();
            }
            if (const VerdictSourceStats* st = wk.verdictStatsForTest(topicId_, s.part)) {
                s.unknownDuplicates = st->verdictUnknownDuplicates;
                s.skippedMalformed = st->keyIndexSkippedMalformed;
            }
            if (keyIndex_) {
                s.keyCount = wk.viewSet()->keyIndexSizeForTest(topicId_, s.part);
                for (uint64_t k = 1; k <= static_cast<uint64_t>(kKiKeys); ++k) {
                    std::string key(8, '\0');
                    for (int i = 0; i < 8; ++i) key[static_cast<std::size_t>(i)] = static_cast<char>(k >> (8 * i));
                    if (wk.viewSet()->keyIndexContainsForTest(topicId_, s.part, key)) s.keys.insert(k);
                }
            }
            return 0;
        });
        return s;
    }
    void snapAll_() {
        snaps_.clear();
        for (const int id : c_.runningNodes())
            for (int p = 0; p < parts_; ++p) snaps_.push_back(snapNode_(id, p));
    }
    /// The partition's leader (its worker holds the judge and the index): the replica that led when it was read, else the metadata's
    /// leader; a node mid-restart is passed over.
    const NodeSnap* leaderOf_(const std::vector<NodeSnap>& snaps, int part) {
        for (const NodeSnap& s : snaps) if (s.part == part && s.leader) return &s;
        std::vector<int32_t> leaders;
        const int32_t id = rune::node::test::catalog_cluster::awaitLeaders(c_, topicId_, parts_, leaders, 5s) ? leaders[static_cast<std::size_t>(part)] : -1;
        const NodeSnap* best = nullptr;
        for (const NodeSnap& s : snaps) if (s.part == part && s.id == id) return &s;
        for (const NodeSnap& s : snaps) if (s.part == part && (best == nullptr || s.hw > best->hw)) best = &s;
        return best;
    }
    /// The write's [hwBefore, hwAfter) window has an offset no replica ever showed: the record was compacted away everywhere before
    /// a history read could see it.  Without a gap, a rejection with no record is a wrong verdict.
    bool windowHasGap_(const Sent& s) const {
        if (!s.windowKnown) return false;
        for (int64_t o = s.hwBefore; o < s.hwAfter; ++o) if (seen_[static_cast<std::size_t>(s.part)].count(o) == 0) return true;
        return false;
    }
    /// Why windowHasGap_ proves nothing for `s`.
    std::string windowProof_(const Sent& s) const {
        if (!s.windowKnown) return "its window is unknown (answered while a node went down)";
        return "its window [" + std::to_string(s.hwBefore) + ", " + std::to_string(s.hwAfter) + ") of partition " + std::to_string(s.part) + " has no unseen offset";
    }
    /// One past the last data record: what the judge and the committed markers must cover (a trailing change record needs no marker).
    static int64_t dataEnd_(const VerdictRig::RigLog& l) { return l.data.empty() ? 0 : l.data.back().offset + 1; }

    /// The committed records ever seen: a compaction may remove them from every log later, the model still judges them.  Every
    /// running node's log is read below its own high watermark: only a leader compacts, so a committed record its cleaner dropped
    /// before this read is still on the followers.
    void recordHistory_(const std::vector<NodeSnap>* given = nullptr) {
        const auto record = [this](int part, const VerdictRig::RigLog& log) {
            for (const auto& d : log.data) {
                const auto at = data_[static_cast<std::size_t>(part)].emplace(d.offset, d);
                if (!at.second) EXPECT_TRUE(at.first->second.id == d.id && at.first->second.v == d.v && at.first->second.tag == d.tag && at.first->second.del == d.del && at.first->second.intent == d.intent) << "seed " << seed_ << ": two replicas hold different records at offset " << d.offset << " of partition " << part;
                seen_[static_cast<std::size_t>(part)].insert(d.offset);
            }
            for (const auto& ch : log.changes) {
                changes_[static_cast<std::size_t>(part)][ch.offset] = ch;
                seen_[static_cast<std::size_t>(part)].insert(ch.offset);
            }
            for (const auto& m : log.markers) seen_[static_cast<std::size_t>(part)].insert(m.offset);
        };
        if (given != nullptr) {
            for (const NodeSnap& s : *given) record(s.part, s.log);
            return;
        }
        for (const int id : c_.runningNodes())
            for (int p = 0; p < parts_; ++p) {
                VerdictRig::RigLog log;
                c_.node(id).onOwnerThreadForTest(topicId_, p, [&log](rune::replication::PartitionReplica& r) {
                    log = readLogOf(*r.log(), r.highWatermark());
                    return 0;
                });
                record(p, log);
            }
    }
    VerdictRig::RigLog history_(int part) const {
        VerdictRig::RigLog out;
        for (const auto& kv : data_[static_cast<std::size_t>(part)]) out.data.push_back(kv.second);
        for (const auto& kv : changes_[static_cast<std::size_t>(part)]) out.changes.push_back(kv.second);
        return out;
    }

    /// Every node read, one leader per partition, every replica caught up and that leader's judge past every record: a leadership
    /// that moved after quiesce() (a restarted node taking its partition back) is not at rest until its scan has rebuilt the index.
    bool atRest_(std::string* last) {
        bool ok = snaps_.size() == 3 * static_cast<std::size_t>(parts_);
        last->clear();
        for (int p = 0; p < parts_; ++p) {
            const NodeSnap* lead = leaderOf_(snaps_, p);
            const int64_t end = lead != nullptr ? dataEnd_(lead->log) : 0;
            ok = ok && lead != nullptr && lead->leader && lead->hw == lead->leo;
            for (const NodeSnap& s : snaps_) ok = ok && (s.part != p || lead == nullptr || (s.leo == lead->leo && s.hw == lead->hw && (&s == lead || !s.leader)));
            ok = ok && lead != nullptr && lead->committedThrough >= end && lead->covered >= end;
            *last += lead == nullptr ? "no leader" : "p" + std::to_string(p) + " leo " + std::to_string(lead->leo) + " hw " + std::to_string(lead->hw) + " committed " + std::to_string(lead->committedThrough) + " end " + std::to_string(end);
            *last += " ";
        }
        if (ok) return true;
        for (const int id : c_.runningNodes()) {
            const int32_t t = topicId_;
            for (int p = 0; p < parts_; ++p)
                *last += " | j" + std::to_string(id) + "=" + std::to_string(c_.node(id).onWorkerThreadForTest(worker_(id, p), [t, p](Worker& wk) {
                    const VerdictSource* j = wk.viewSet()->verdictSource(t, p);
                    return j == nullptr ? 0 : 1 + (j->waitingForInstall() ? 2 : 0) + (j->stalled() ? 4 : 0);
                })) + " ";
        }
        for (const NodeSnap& s : snaps_) *last += " | n" + std::to_string(s.id) + "p" + std::to_string(s.part) + (s.leader ? " leader" : "") + " leo " + std::to_string(s.leo) + " hw " + std::to_string(s.hw) + " cov " + std::to_string(s.covered) + " com " + std::to_string(s.committedThrough);
        return false;
    }

    /// Waits, at rest, for the cleaner to remove a superseded record from a node's log: the invariants judge that snapshot.
    void awaitCompaction_() {
        const auto deadline = Clock::now() + wait_(60s);
        std::string last;
        for (int poll = 0; poll < kPollCap && Clock::now() < deadline; ++poll, std::this_thread::sleep_for(200ms)) {
            snapAll_();
            recordHistory_(&snaps_);
            if (!atRest_(&last)) continue;
            for (const NodeSnap& s : snaps_) if (s.log.data.size() < data_[static_cast<std::size_t>(s.part)].size()) return;
            last = "at rest, nothing compacted";
        }
        std::size_t committed = 0;
        for (const auto& d : data_) committed += d.size();
        ADD_FAILURE() << "seed " << seed_ << ": no node's log at rest lost a superseded record in " << wait_(60s).count() << " s (" << committed << " committed records; " << last << ")";
    }

    /// A key-index poll is a 10 s deadline; 04's keep theirs.
    std::chrono::seconds wait_(std::chrono::seconds legacy) const { return keyIndex_ ? 10s : legacy; }
    static constexpr int kPollCap = 4000;

    // ---- invariants ----
    /// The offsets the raw subscription and the group consumer delivered from 0 on each partition, once the leader's accepted records
    /// have all arrived at both.
    struct Delivered { std::vector<std::set<int64_t>> raw, group; };
    Delivered delivered_(const std::vector<std::set<int64_t>>& want) {
        if (subs_.empty()) {
            const NodeSnap* lead = leaderOf_(snaps_, 0);
            rune::client::ClientConfig cfg = rune::node::test::catalog_cluster::clientConfigFor(c_, lead->id);
            cfg.connectTimeoutMs = 30'000;
            cfg.pingTimeoutMs = 30'000;
            cfg.topics.push_back(rune::client::TopicSpec{topicName_(), topicId_, parts_});
            reader_ = std::make_unique<rune::client::Client>(cfg);
            reader_->connectAndWait(30s);
            rune::client::SubscribeOptions o;
            o.from = rune::client::From::earliest();
            for (int p = 0; p < parts_; ++p) {   // a raw subscription is served by the node it connects to: the partition's leader
                rune::client::ClientConfig pcfg = rune::node::test::catalog_cluster::clientConfigFor(c_, leaderOf_(snaps_, p)->id);
                pcfg.connectTimeoutMs = 30'000;
                pcfg.pingTimeoutMs = 30'000;
                pcfg.topics = cfg.topics;
                subReaders_.push_back(std::make_unique<rune::client::Client>(pcfg));
                subReaders_.back()->connectAndWait(30s);
                subs_.push_back(std::make_unique<rune::client::Subscription>(subReaders_.back()->subscribe(subReaders_.back()->topic(topicName_()), p, o, [this](const rune::client::Record& r) {
                    std::lock_guard<std::mutex> g(deliveredMu_);
                    deliveredOffsets_[static_cast<std::size_t>(r.partition)].insert(r.offset);
                    rawIntents_[static_cast<std::size_t>(r.partition)][r.offset] = rune::protocol::record::intentOf(r.attributes);
                })));
            }
            rune::client::GroupConfig gcfg;
            gcfg.autoCommit = false;
            group_ = std::make_unique<rune::client::GroupConsumer>(reader_->subscribeGroup("cw-group", {topicName_()}, gcfg));
            group_->onRecords([this](const rune::client::RecordSet& rs) {
                std::lock_guard<std::mutex> g(deliveredMu_);
                rs.forEachRecord([&](std::string_view, std::string_view, int64_t off, int64_t) { groupOffsets_[static_cast<std::size_t>(rs.partition)].insert(off); });
            });
        }
        const auto holds = [&](const std::vector<std::set<int64_t>>& got) {
            for (int p = 0; p < parts_; ++p)
                if (!std::includes(got[static_cast<std::size_t>(p)].begin(), got[static_cast<std::size_t>(p)].end(), want[static_cast<std::size_t>(p)].begin(), want[static_cast<std::size_t>(p)].end())) return false;
            return true;
        };
        const auto deadline = Clock::now() + wait_(60s);
        for (int poll = 0; poll < kPollCap && Clock::now() < deadline; ++poll, std::this_thread::sleep_for(50ms)) {
            std::lock_guard<std::mutex> g(deliveredMu_);
            if (holds(deliveredOffsets_) && holds(groupOffsets_)) break;
        }
        std::this_thread::sleep_for(500ms);   // a rejected record would arrive with its neighbours
        std::lock_guard<std::mutex> g(deliveredMu_);
        return {deliveredOffsets_, groupOffsets_};
    }

    /// The leader's view instance folded every committed data record.
    bool awaitViewFolded_() {
        const int32_t viewTopic = viewTopicId_;
        for (int p = 0; p < parts_; ++p) {
            const NodeSnap* lead = leaderOf_(snaps_, p);
            if (lead == nullptr) return false;
            const int64_t end = dataEnd_(lead->log);
            const auto deadline = Clock::now() + wait_(60s);
            bool folded = false;
            for (int poll = 0; poll < kPollCap && Clock::now() < deadline && !folded; ++poll, std::this_thread::sleep_for(50ms))
                folded = c_.node(lead->id).onWorkerThreadForTest(worker_(lead->id, p), [viewTopic, p, end](Worker& wk) {
                    return wk.viewSet()->instanceServeState(viewTopic, p).foldCursor >= end;
                });
            if (!folded) return false;
        }
        return true;
    }
    /// A view row lookup: the key's row, no row, or no answer from any try (`tries` of them).
    enum class Lookup { Present, Absent, Failed };
    Lookup viewRow_(uint64_t id, std::pair<int64_t, uint16_t>* out, int* tries) {
        rune::client::ViewRowResult row;
        const auto deadline = Clock::now() + wait_(60s);
        *tries = 0;
        for (int poll = 0; poll < kPollCap && Clock::now() < deadline; ++poll, std::this_thread::sleep_for(100ms)) {
            ++*tries;
            try {
                if (viewLookupHook_) viewLookupHook_();
                row = reader_->getViewRowAndWait(viewTopicId_, id, 0, 10s);
            } catch (const std::exception&) { continue; }
            if (row.status == rune::client::ViewRowResult::kAbsentKey) return Lookup::Absent;
            if (row.status != rune::client::ViewRowResult::kOk) continue;
            int64_t v = 0;
            for (int i = 0; i < 8; ++i) v |= static_cast<int64_t>(row.rowBytes[9 + static_cast<std::size_t>(i)]) << (8 * i);   // [bitmap][id][v][tag]; the key-index row [bitmap][id][name]
            *out = {v, static_cast<uint16_t>(row.rowBytes[17] | (row.rowBytes[18] << 8))};
            return Lookup::Present;
        }
        return Lookup::Failed;
    }

    void registerDefaults_() {
        registerInvariant("AckedVisibleUnlessRejected", [](CheckedWritesStressDriver& d, std::string* why) {
            std::set<int64_t> accepted, rejected;   // key-index mode: tombstones carry no token (v 0) and are judged by IntentVerdictsExact
            for (int p = 0; p < d.parts_; ++p) {
                for (const auto& rec : d.models_[static_cast<std::size_t>(p)].acceptedRecords()) accepted.insert(rec.v);
                for (const auto& kv : d.models_[static_cast<std::size_t>(p)].verdicts()) rejected.insert(d.data_[static_cast<std::size_t>(p)].at(kv.first).v);
            }
            for (const Sent& s : d.sent_) {
                if (d.keyIndex_ && s.v == 0) continue;
                if (s.result == Sent::Result::Ok && accepted.count(s.v) == 0) { *why = "v " + std::to_string(s.v) + " was acked Ok but no accepted record carries it"; return false; }
                if (s.result == Sent::Result::Violation && rejected.count(s.v) == 0) {
                    if (!d.keyIndex_) { *why = "v " + std::to_string(s.v) + " was acked as a violation but no rejected record carries it"; return false; }
                    bool carried = false;
                    for (const auto& kv : d.data_[static_cast<std::size_t>(s.part)]) carried = carried || (!kv.second.del && kv.second.v == s.v);
                    if (carried) { *why = "v " + std::to_string(s.v) + " was answered " + std::to_string(s.code) + " but the model accepted its record"; return false; }
                    if (d.windowHasGap_(s)) continue;   // compacted away on every replica before a history read saw it
                    *why = "v " + std::to_string(s.v) + " was answered " + std::to_string(s.code) + ", no record carries it and " + d.windowProof_(s);
                    return false;
                }
            }
            return true;
        });
        registerInvariant("NoRejectedDeliveredOrFolded", [](CheckedWritesStressDriver& d, std::string* why) {
            std::vector<std::set<int64_t>> want(static_cast<std::size_t>(d.parts_));   // a compaction may have removed superseded records: only what the log still holds is due
            for (int p = 0; p < d.parts_; ++p) {
                const NodeSnap* lead = d.leaderOf_(d.snaps_, p);
                std::set<int64_t> present;
                for (const auto& rec : lead->log.data) present.insert(rec.offset);
                for (const auto& rec : d.models_[static_cast<std::size_t>(p)].acceptedRecords()) if (present.count(rec.offset) != 0) want[static_cast<std::size_t>(p)].insert(rec.offset);
            }
            const auto [raw, group] = d.delivered_(want);
            for (int p = 0; p < d.parts_; ++p) {
                const std::size_t i = static_cast<std::size_t>(p);
                std::set<int64_t> rejected;
                for (const auto& kv : d.models_[i].verdicts()) rejected.insert(kv.first);
                for (const auto& [who, got] : {std::pair<const char*, const std::set<int64_t>&>{"raw", raw[i]}, {"group", group[i]}}) {
                    for (const int64_t off : rejected)
                        if (got.count(off) != 0) { *why = std::string(who) + ": rejected offset " + std::to_string(off) + " of partition " + std::to_string(p) + " was delivered"; return false; }
                    for (const int64_t off : want[i])
                        if (got.count(off) == 0) { *why = std::string(who) + ": accepted offset " + std::to_string(off) + " of partition " + std::to_string(p) + " was not delivered (" + (d.data_[i].at(off).del ? "tombstone" : "put") + ", intent " + std::to_string(d.data_[i].at(off).intent) + ", partition hw " + std::to_string(d.leaderOf_(d.snaps_, p)->hw) + ", delivered " + std::to_string(got.size()) + " of " + std::to_string(want[i].size()) + ")"; return false; }
                }
                if (d.keyIndex_) {   // the intent a consumer reads is the one the record was written with
                    std::lock_guard<std::mutex> g(d.deliveredMu_);
                    for (const auto& [off, intent] : d.rawIntents_[i]) {
                        const auto rec = d.data_[i].find(off);
                        if (rec != d.data_[i].end() && rec->second.intent != intent) { *why = "offset " + std::to_string(off) + " of partition " + std::to_string(p) + " was delivered with intent " + std::to_string(intent) + ", written with " + std::to_string(rec->second.intent); return false; }
                    }
                }
            }
            return true;
        });
        registerInvariant("SameVerdictEveryLeaderAndRefold", [](CheckedWritesStressDriver& d, std::string* why) {
            for (const NodeSnap& s : d.snaps_) {
                const std::size_t i = static_cast<std::size_t>(s.part);
                const NodeSnap* lead = d.leaderOf_(d.snaps_, s.part);
                const std::string at = "node " + std::to_string(s.id) + (d.parts_ > 1 ? " partition " + std::to_string(s.part) : std::string()) + ": ";
                if (s.mismatches != 0) { *why = at + "the index counted " + std::to_string(s.mismatches) + " mismatches"; return false; }
                if (s.unknownDuplicates != 0) { *why = at + "verdictUnknownDuplicates " + std::to_string(s.unknownDuplicates); return false; }
                if (s.skippedMalformed != 0) { *why = at + "keyIndexSkippedMalformed " + std::to_string(s.skippedMalformed); return false; }
                std::set<int64_t> present;
                for (const auto& rec : s.log.data) present.insert(rec.offset);
                if (&s == lead) {   // a follower builds its index when it takes the lead
                    for (const auto& [off, verdict] : s.index) {
                        const auto it = d.models_[i].verdicts().find(off);
                        if (it == d.models_[i].verdicts().end() || !(it->second == verdict)) { *why = at + "index entry " + std::to_string(off) + " (" + std::to_string(verdict.code) + "," + std::to_string(verdict.constraintId) + ") is " + (it == d.models_[i].verdicts().end() ? std::string("not rejected by the model") : "the model's " + std::to_string(it->second.code) + "," + std::to_string(it->second.constraintId)); return false; }
                    }
                    for (const auto& [off, verdict] : d.models_[i].verdicts()) {
                        if (present.count(off) != 0 && s.index.count(off) == 0) { *why = at + "rejected offset " + std::to_string(off) + " is in the log but not in the index"; return false; }
                    }
                }
                if (!s.log.markers.empty() && !s.log.changes.empty()) {
                    const std::string problem = markerUnionProblem(s.log.markers, s.log.changes.front().offset);
                    if (!problem.empty()) { *why = at + problem; return false; }
                }
                for (const auto& m : s.log.markers) {
                    for (const auto& [off, code, cid] : m.entries) {
                        const auto it = d.models_[i].verdicts().find(off);
                        if (it == d.models_[i].verdicts().end() || !(it->second == ModelVerdict{code, cid})) { *why = at + "a marker entry " + std::to_string(off) + " is not the model's verdict"; return false; }
                    }
                }
                if (lead != nullptr && s.hw == lead->hw) {
                    for (const auto& rec : s.log.data) {
                        const auto it = d.data_[i].find(rec.offset);
                        if (it == d.data_[i].end() || it->second.v != rec.v || it->second.id != rec.id) { *why = at + "a record at " + std::to_string(rec.offset) + " is not the committed one"; return false; }
                    }
                }
            }
            return true;
        });
        registerInvariant("ViewEqualsFreshRefold", [](CheckedWritesStressDriver& d, std::string* why) {
            if (!d.reader_) (void)d.delivered_(std::vector<std::set<int64_t>>(static_cast<std::size_t>(d.parts_)));
            if (!d.awaitViewFolded_()) { *why = "the view never folded the leader's committed records"; return false; }
            std::map<uint64_t, std::pair<int64_t, uint8_t>> proj;   // a key lives on one partition
            for (int p = 0; p < d.parts_; ++p) proj.merge(d.models_[static_cast<std::size_t>(p)].projection());
            for (uint64_t id = 1; id <= static_cast<uint64_t>(d.keys_); ++id) {
                const auto it = proj.find(id);
                std::pair<int64_t, uint16_t> row;
                int tries = 0;
                const Lookup got = d.viewRow_(id, &row, &tries);
                if (got == Lookup::Failed) { *why = "key " + std::to_string(id) + ": the lookup failed after " + std::to_string(tries) + " tries"; return false; }
                if (it == proj.end()) {
                    if (got == Lookup::Present) { *why = "key " + std::to_string(id) + " has a row but no live accepted record"; return false; }
                    continue;
                }
                if (got == Lookup::Absent) { *why = "key " + std::to_string(id) + " has no row"; return false; }
                if (row.first != it->second.first || row.second != it->second.second) { *why = "key " + std::to_string(id) + " row (" + std::to_string(row.first) + ", " + std::to_string(row.second) + ") != model"; return false; }
            }
            return true;
        });
        registerInvariant("CompactionKeepsAcceptedValue", [](CheckedWritesStressDriver& d, std::string* why) {
            for (const NodeSnap& s : d.snaps_) {
                std::set<int64_t> present;
                for (const auto& rec : s.log.data) present.insert(rec.offset);
                for (const auto& [id, latest] : d.models_[static_cast<std::size_t>(s.part)].latestAcceptedByKey()) {
                    if (latest.del) continue;
                    if (present.count(latest.offset) == 0) { *why = "node " + std::to_string(s.id) + " lost the latest accepted record of key " + std::to_string(id) + " at " + std::to_string(latest.offset); return false; }
                }
            }
            return true;
        });
    }

    /// The two invariants of plan 05 (matrix D): every 56 / 57 reported (an answer, the index, a marker) is the model's and an
    /// accepted write never gets one; each partition's key index holds the model's live keys.
    void registerKeyIndexInvariants_() {
        registerInvariant("IntentVerdictsExact", [](CheckedWritesStressDriver& d, std::string* why) {
            d.pinned56_ = d.pinned57_ = d.provenCompacted_ = 0;
            const auto intentOf = [](Op op) -> uint8_t { return op == Op::Insert ? 1 : op == Op::Update || op == Op::RemoveExisting ? 2 : 0; };
            for (int p = 0; p < d.parts_; ++p) {   // a token names one write
                std::map<int64_t, int> seen;
                for (const auto& kv : d.data_[static_cast<std::size_t>(p)])
                    if (!kv.second.del && ++seen[kv.second.v] > 1) { *why = "partition " + std::to_string(p) + ": the write with token " + std::to_string(kv.second.v) + " is in the log twice"; return false; }
            }
            for (const Sent& s : d.sent_) {
                if (s.result == Sent::Result::Unknown) continue;
                const std::size_t i = static_cast<std::size_t>(s.part);
                const auto& data = d.data_[i];
                const auto& verdicts = d.models_[i].verdicts();
                const bool tombstone = s.op == Op::RemoveExisting || s.op == Op::Remove;
                // The record: an Ok carries its offset; a rejection is found by its token, a rejected removeExisting among the
                // tombstones of its id appended while the write was in flight (the writes are serialized).
                std::vector<int64_t> candidates, wrongIntent;
                if (s.result == Sent::Result::Ok) candidates.push_back(s.offset);
                else for (const auto& [off, rec] : data) {
                    if (rec.id != s.id || rec.del != tombstone) continue;
                    if (!tombstone) { if (rec.v == s.v) candidates.push_back(off); }
                    else if (off >= s.hwBefore && off < s.hwAfter) (rec.intent == intentOf(s.op) ? candidates : wrongIntent).push_back(off);
                }
                if (candidates.empty()) {
                    if (s.result == Sent::Result::Ok) { *why = "the write of id " + std::to_string(s.id) + " acked at offset " + std::to_string(s.offset) + " has no record in the log"; return false; }
                    if (!wrongIntent.empty()) { *why = "the removeExisting of id " + std::to_string(s.id) + " answered " + std::to_string(s.code) + " has no intent-2 tombstone in its window [" + std::to_string(s.hwBefore) + ", " + std::to_string(s.hwAfter) + ") of partition " + std::to_string(s.part) + ", but the tombstone at " + std::to_string(wrongIntent.front()) + " carries intent " + std::to_string(data.at(wrongIntent.front()).intent); return false; }
                    if (d.windowHasGap_(s)) { ++d.provenCompacted_; continue; }   // compacted away on every replica before a history read saw it
                    *why = "the write of id " + std::to_string(s.id) + " answered " + std::to_string(s.code) + " has no record in the log and " + d.windowProof_(s);
                    return false;
                }
                bool judged = false;
                for (const int64_t off : candidates) {
                    const auto rec = data.find(off);
                    if (rec == data.end() || rec->second.id != s.id || rec->second.del != tombstone || rec->second.intent != intentOf(s.op)) { *why = "offset " + std::to_string(off) + " does not carry the intent " + std::to_string(intentOf(s.op)) + " the write was sent with"; return false; }
                    const auto it = verdicts.find(off);
                    judged = judged || (s.result == Sent::Result::Ok ? it == verdicts.end() : it != verdicts.end() && it->second == ModelVerdict{s.code, 0});
                }
                if (judged && s.result == Sent::Result::Violation) ++(s.code == 56 ? d.pinned56_ : d.pinned57_);
                if (!judged) { *why = "the write of id " + std::to_string(s.id) + " at offset " + std::to_string(candidates.front()) + " was answered " + std::to_string(s.result == Sent::Result::Ok ? 0 : s.code) + ", not the model's verdict"; return false; }
            }
            for (const NodeSnap& s : d.snaps_) {
                const NodeSnap* lead = d.leaderOf_(d.snaps_, s.part);
                const auto& verdicts = d.models_[static_cast<std::size_t>(s.part)].verdicts();
                for (const auto& [off, verdict] : s.index)
                    if (&s == lead && (verdict.code == 56 || verdict.code == 57)) {
                        const auto it = verdicts.find(off);
                        if (it == verdicts.end() || !(it->second == verdict)) { *why = "partition " + std::to_string(s.part) + ": the index reports " + std::to_string(verdict.code) + " at " + std::to_string(off) + ", not the model's verdict"; return false; }
                    }
                for (const auto& m : s.log.markers)
                    for (const auto& [off, code, cid] : m.entries)
                        if ((code == 56 || code == 57) && (verdicts.count(off) == 0 || !(verdicts.at(off) == ModelVerdict{code, cid}))) { *why = "partition " + std::to_string(s.part) + ": a marker reports " + std::to_string(code) + " at " + std::to_string(off) + ", not the model's verdict"; return false; }
            }
            if (d.pinned56_ == 0 || d.pinned57_ == 0) { *why = "the run pinned " + std::to_string(d.pinned56_) + " KeyExists and " + std::to_string(d.pinned57_) + " KeyNotFound answers to a record (" + std::to_string(d.keyExists_) + " / " + std::to_string(d.keyNotFound_) + " answered): it did no real work"; return false; }
            return true;
        });
        registerInvariant("KeyIndexEqualsLiveKeys", [](CheckedWritesStressDriver& d, std::string* why) {
            for (int p = 0; p < d.parts_; ++p) {
                const NodeSnap* lead = d.leaderOf_(d.snaps_, p);
                if (lead == nullptr) { *why = "partition " + std::to_string(p) + " has no leader"; return false; }
                std::set<uint64_t> live;
                for (uint64_t id = 1; id <= static_cast<uint64_t>(kKiKeys); ++id) if (d.models_[static_cast<std::size_t>(p)].isLiveAtJudge(id)) live.insert(id);
                if (lead->keys != live || lead->keyCount != live.size()) { *why = "partition " + std::to_string(p) + ": the key index holds " + std::to_string(lead->keyCount) + " keys, the model's live set " + std::to_string(live.size()); return false; }
            }
            return true;
        });
    }

    rune::node::test::catalog_cluster::ThreeNodes& c_;
    std::mt19937_64 rng_;
    uint64_t seed_;
    bool keyIndex_;
    int parts_, keys_;
    int32_t topicId_ = -1, viewTopicId_ = -1;
    int32_t capTopic_ = kCapTopicBase, capSeq_ = 0;   // the installed CapLiveKeys view's topic (0 = dropped)
    uint32_t capValue_ = 8;
    std::set<uint32_t> caps_{8};
    int64_t vSeq_ = 0;
    int crashes_ = 0, restarts_ = 0, bounces_ = 0;
    int keyExists_ = 0, keyNotFound_ = 0;   ///< answered 56 / 57 (key-index mode)
    int pinned56_ = 0, pinned57_ = 0, provenCompacted_ = 0;   ///< answers matched to their judged record / proven compacted away
    bool writersStale_ = false;
    std::vector<std::unique_ptr<Writer>> writers_;
    std::vector<std::unique_ptr<KiWriter>> kiWriters_;
    std::function<void(const Sent&)> kiSendHook_;
    std::function<void()> viewLookupHook_;
    std::vector<Sent> sent_;
    std::vector<std::set<int64_t>> seen_;   ///< per partition: every offset any replica showed as a data record, change record or marker
    std::vector<std::map<int64_t, VerdictRig::RigLogRec>> data_;   ///< per partition
    std::vector<std::map<int64_t, VerdictRig::RigChange>> changes_;
    std::vector<NodeSnap> snaps_;
    std::vector<CheckedWritesModel> models_;
    std::vector<std::pair<std::string, Invariant>> invariants_;
    std::unique_ptr<rune::client::Client> reader_;
    std::vector<std::unique_ptr<rune::client::Client>> subReaders_;
    std::vector<std::unique_ptr<rune::client::Subscription>> subs_;
    std::unique_ptr<rune::client::GroupConsumer> group_;
    std::mutex deliveredMu_;
    std::vector<std::set<int64_t>> deliveredOffsets_, groupOffsets_;
    std::vector<std::map<int64_t, uint8_t>> rawIntents_;
    std::size_t compactedAway_ = 0;
};

}  // namespace rune::node::cw
