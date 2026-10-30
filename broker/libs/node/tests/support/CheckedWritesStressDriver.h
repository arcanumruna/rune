#pragma once
// CheckedWritesStressDriver.h - the seeded cluster stress of the checked-writes verdict pipeline (plan 04 Task 11, matrix D).
// Three real brokers (ThreeNodes), three real clients writing All|View to one typed, compacted source with two TestReject members
// (object 7 TagEquals{9}, object 8 CapLiveKeys{n}, replaced, dropped and installed again by the run) installed on every node, and
// an ordinary projection view.  A raw and a group consumer read the source from offset 0.  step() draws one seeded action; quiesce() brings the cluster to rest and checkAll() judges the committed history with the independent
// CheckedWritesModel and checks the named invariants against every node.  Every wait is a bounded poll: the deadline is the failure.
#include "CheckedWritesChecks.h"
#include "CheckedWritesModel.h"
#include "client/ClusterClient.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/Verdict.h>

#include <algorithm>
#include <array>
#include <chrono>
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
    static constexpr int kKeys = 12;

    /// What a node holds of the partition, read on its own threads.
    struct NodeSnap {
        int id = 0;
        int64_t leo = 0, hw = 0, logStart = 0;
        VerdictRig::RigLog log;                              ///< below the HW
        std::map<int64_t, ModelVerdict> index;               ///< the node's RejectedOffsetIndex
        uint64_t mismatches = 0, unknownDuplicates = 0;
        int64_t committedThrough = 0, covered = 0;
    };
    /// One send and what the writer was told.
    struct Sent {
        uint64_t id = 0;
        int64_t v = 0;
        uint8_t tag = 0;
        enum class Result : uint8_t { Ok, Violation, Unknown } result = Result::Unknown;
    };

    CheckedWritesStressDriver(rune::node::test::catalog_cluster::ThreeNodes& cluster, uint64_t seed) : c_(cluster), rng_(seed), seed_(seed) {
        registerDefaults_();
    }
    ~CheckedWritesStressDriver() {
        group_.reset();
        sub_.reset();
        reader_.reset();
        writers_.clear();
    }

    /// The typed topic (replicas 3), the projection view, TestReject on every node and three clients.
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
        const auto src = admin->defineTopicAndWait("vt", 1, "id", rune::codec::serialize(verdictSourceDescriptor()), rune::client::Persistence::Disk, to, {}, 60s);
        ASSERT_EQ(src.errorCode, 0) << src.detail;
        topicId_ = src.topicId;
        const auto view = admin->createViewAndWait("vt_v", "id", 0, viewPlan_(topicId_), {}, 60s);
        ASSERT_EQ(view.errorCode, 0) << view.detail;
        viewTopicId_ = view.viewTopicId;
        admin->close();
        admin.reset();
        std::vector<int32_t> leaders;
        ASSERT_TRUE(tcc::awaitLeaders(c_, topicId_, 1, leaders, 30s));
        for (const int id : c_.runningNodes()) prepareNode_(id);
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        rebuildWriters_(-1);
    }

    /// One seeded action.
    void step() {
        const unsigned r = static_cast<unsigned>(rng_() % 100);
        const int down = downNode_();
        if (r < 55) {
            note_(1);
            sendBurst_(-1);
        } else if (r < 65 && down == 0) {
            note_(2);
            crashLeaderDuringSends_();
        } else if (r < 80) {
            note_(3);
            if (down != 0) restartNode_(down, false);
            else restartFollower_();
        } else if (r < 92) {
            note_(4);
            changeConstraintSet_();
        } else {
            note_(5);
            std::this_thread::sleep_for(std::chrono::milliseconds(50 + static_cast<int>(rng_() % 150)));
        }
    }

    /// Every node back, one leader, every replica caught up and the judge past every record.
    void quiesce() {
        for (int id = 1; id <= 3; ++id)
            if (!c_.running(id)) restartNode_(id, false);
        std::vector<int32_t> leaders;
        ASSERT_TRUE(rune::node::test::catalog_cluster::awaitLeaders(c_, topicId_, 1, leaders, 60s));
        const auto deadline = Clock::now() + 90s;
        std::string last;
        while (Clock::now() < deadline) {
            std::vector<NodeSnap> snaps;
            for (const int id : c_.runningNodes()) snaps.push_back(snapNode_(id));
            const NodeSnap* lead = leaderOf_(snaps);
            const int64_t end = lead != nullptr ? dataEnd_(lead->log) : 0;
            bool ok = lead != nullptr && snaps.size() == 3 && lead->hw == lead->leo;
            for (const NodeSnap& s : snaps) ok = ok && s.leo == lead->leo && s.hw == lead->hw;
            ok = ok && lead->committedThrough >= end && lead->covered >= end;
            if (ok) {
                recordHistory_(&snaps);
                return;
            }
            last = lead == nullptr ? "no leader" : "leo " + std::to_string(lead->leo) + " hw " + std::to_string(lead->hw) + " committed " + std::to_string(lead->committedThrough) + " end " + std::to_string(end);
            for (const int id : c_.runningNodes()) {
                const int32_t t = topicId_;
                last += " | j" + std::to_string(id) + "=" + std::to_string(c_.node(id).onWorkerThreadForTest(worker_(id), [t](Worker& wk) {
                    const VerdictSource* j = wk.viewSet()->verdictSource(t, 0);
                    return j == nullptr ? 0 : 1 + (j->waitingForInstall() ? 2 : 0) + (j->stalled() ? 4 : 0);
                })) + " ";
            }
            for (const NodeSnap& s : snaps) last += " | n" + std::to_string(s.id) + " leo " + std::to_string(s.leo) + " hw " + std::to_string(s.hw) + " cov " + std::to_string(s.covered) + " com " + std::to_string(s.committedThrough);
            std::this_thread::sleep_for(100ms);
        }
        ADD_FAILURE() << "seed " << seed_ << ": the cluster did not come to rest (" << last << ")";
    }

    void registerInvariant(std::string name, Invariant fn) { invariants_.emplace_back(std::move(name), std::move(fn)); }

    /// Judges the history and runs every invariant; the name of the first that fails (and why) lands in `failed`.
    [[nodiscard]] bool checkAll(std::string* failed) {
        snapAll_();
        recordHistory_(&snaps_);
        awaitCompaction_();
        model_ = CheckedWritesModel();
        model_.declareRule(kObjTag, tagRule_().fingerprint(), CheckedWritesModel::tagEquals(9));
        for (const uint32_t cap : caps_) model_.declareRule(kObjCap, capRule_(cap).fingerprint(), CheckedWritesModel::capLiveKeys(cap));
        model_.reset(history_());
        for (const auto& [name, fn] : invariants_) {
            std::string why;
            if (!fn(*this, &why)) {
                if (failed != nullptr) *failed = "seed " + std::to_string(seed_) + " " + name + ": " + why;
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] uint64_t traceHash() const noexcept { return trace_; }
    /// How often each fault ran: leader crashes, follower or node restarts, restarts cut short.
    [[nodiscard]] std::array<int, 3> faults() const noexcept { return {crashes_, restarts_, bounces_}; }
    [[nodiscard]] const CheckedWritesModel& model() const noexcept { return model_; }
    [[nodiscard]] const std::vector<NodeSnap>& snaps() const noexcept { return snaps_; }
    [[nodiscard]] const std::vector<Sent>& sent() const noexcept { return sent_; }
    [[nodiscard]] int32_t viewTopicId() const noexcept { return viewTopicId_; }
    /// The most records the history holds that one node's log no longer holds (a compaction ran).
    [[nodiscard]] std::size_t compactedAway() const { return compactedAway_; }

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

    static std::string value_(uint64_t id, int64_t v, uint16_t tag) {
        std::string val;
        const auto le16 = [&](uint16_t x) { val.push_back(static_cast<char>(x)); val.push_back(static_cast<char>(x >> 8)); };
        le16(18); le16(1);
        for (int i = 0; i < 8; ++i) val.push_back(static_cast<char>(id >> (8 * i)));
        for (int i = 0; i < 8; ++i) val.push_back(static_cast<char>(static_cast<uint64_t>(v) >> (8 * i)));
        le16(tag);
        return val;
    }
    static std::vector<uint8_t> viewPlan_(int32_t src) {
        namespace rv = rune::view;
        rv::ViewPlan p;
        p.viewName = "vt_v";
        p.key = "id";
        p.sources.push_back(rv::SourceSpec{src, "id"});
        p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::UInt64, 0, "id", rv::MergeRule::Replace});
        p.output.push_back(rv::AuthoredField{"v", rune::codec::PrimType::Int64, 0, "v", rv::MergeRule::Replace});
        p.output.push_back(rv::AuthoredField{"tag", rune::codec::PrimType::UInt16, 0, "tag", rv::MergeRule::Replace});
        return rv::serializeAuthored(p);
    }
    static TestRejectRule tagRule_() { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = 9; return r; }
    static TestRejectRule capRule_(uint32_t cap) { TestRejectRule r; r.mode = TestRejectRule::Mode::CapLiveKeys; r.cap = cap; return r; }
    static constexpr int32_t kTagTopic = 0x40000701, kCapTopicBase = 0x40000710;

    // ---- nodes ----
    int32_t worker_(int id) { return rune::metadata::ShardRouter(c_.node(id).workerCountForTest()).workerForPartition(topicId_, 0); }

    /// The node serves the partition and holds the cluster feature level: TestReject goes on its worker.
    void prepareNode_(int id) {
        BrokerNode& n = c_.node(id);
        const int32_t w = worker_(id);
        bool ready = false;
        for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline && !ready; std::this_thread::sleep_for(50ms))
            ready = n.onWorkerThreadForTest(w, [this](Worker& wk) {
                return wk.clusterFeatureLevelForTest() >= rune::replication::MetadataFeature::kCheckedWrites && wk.writeStateForTest(topicId_, 0) != nullptr;
            });
        ASSERT_TRUE(ready) << "node " << id << " never served the source";
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
            for (const int id : c_.runningNodes()) snaps.push_back(snapNode_(id));
            const NodeSnap* lead = leaderOf_(snaps);
            if (lead != nullptr && lead->committedThrough >= dataEnd_(lead->log) && lead->hw == lead->leo) return;
        }
        ADD_FAILURE() << "seed " << seed_ << ": the judge did not cover the writes after a constraint-set change";
    }

    int downNode_() const {
        for (int id = 1; id <= 3; ++id) if (!c_.running(id)) return id;
        return 0;
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
        ASSERT_TRUE(rune::node::test::catalog_cluster::awaitLeaders(c_, topicId_, 1, leaders, 60s));
        recordHistory_();
    }
    int32_t partitionLeader_() {
        std::vector<int32_t> leaders;
        EXPECT_TRUE(rune::node::test::catalog_cluster::awaitLeaders(c_, topicId_, 1, leaders, 60s));
        return leaders.empty() ? -1 : leaders[0];
    }
    void restartFollower_() {
        const int32_t leader = partitionLeader_();
        std::vector<int> followers;
        for (int id = 1; id <= 3; ++id) if (id != leader) followers.push_back(id);
        const int id = followers[rng_() % followers.size()];
        c_.stop(id);
        restartNode_(id, rng_() % 3 == 0);
    }

    // ---- writes ----
    void rebuildWriters_(int avoid) {
        writers_.clear();
        std::vector<int> seeds;
        for (const int id : c_.runningNodes()) if (id != avoid) seeds.push_back(id);
        for (std::size_t i = 0; i < 3; ++i) {
            rune::client::ClientConfig cfg = rune::node::test::catalog_cluster::clientConfigFor(c_, seeds[i % seeds.size()]);
            cfg.connectTimeoutMs = 30'000;
            cfg.pingTimeoutMs = 30'000;
            writers_.push_back(std::make_unique<Writer>(cfg, topicId_));
        }
        writersStale_ = false;
    }
    void sendBurst_(int victim) {
        if (writersStale_ || victim > 0) rebuildWriters_(victim);
        Writer& w = *writers_[rng_() % writers_.size()];
        const int n = 1 + static_cast<int>(rng_() % 80);
        std::vector<std::pair<uint64_t, std::size_t>> toks;
        std::thread killer;
        for (int i = 0; i < n; ++i) {
            Sent s;
            s.id = 1 + rng_() % kKeys;
            s.tag = (rng_() % 100) < 20 ? 9 : static_cast<uint8_t>(1 + rng_() % 3);
            s.v = ++vSeq_;
            if (victim > 0 && i == n / 2) killer = std::thread([this, victim] { c_.crash(victim); });
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
    void crashLeaderDuringSends_() {
        const int32_t victim = partitionLeader_();
        if (victim <= 0) return;
        ++crashes_;
        sendBurst_(victim);
        writersStale_ = true;
    }

    // ---- reading the cluster ----
    NodeSnap snapNode_(int id) {
        NodeSnap s;
        s.id = id;
        BrokerNode& n = c_.node(id);
        n.onOwnerThreadForTest(topicId_, 0, [&s](rune::replication::PartitionReplica& r) {
            s.leo = r.logEndOffset();
            s.hw = r.highWatermark();
            s.logStart = r.log()->logStartOffset();
            s.log = readLogOf(*r.log(), s.hw);
            return 0;
        });
        n.onWorkerThreadForTest(worker_(id), [&s, this](Worker& wk) {
            if (const PartitionWriteState* ws = wk.writeStateForTest(topicId_, 0)) {
                ws->rejected.forEachIn(0, INT64_MAX, [&](const rune::storage::RejectedOffsetIndex::Entry& e) { s.index[e.offset] = {e.code, e.constraintId}; });
                s.mismatches = ws->rejected.mismatches();
                s.committedThrough = ws->markers.committedThrough();
                s.covered = ws->rejected.coveredThrough();
            }
            if (const VerdictSourceStats* st = wk.verdictStatsForTest(topicId_, 0)) s.unknownDuplicates = st->verdictUnknownDuplicates;
            return 0;
        });
        return s;
    }
    /// The partition's leader (its worker holds the judge and the index); a node mid-restart is passed over.
    const NodeSnap* leaderOf_(const std::vector<NodeSnap>& snaps) {
        std::vector<int32_t> leaders;
        const int32_t id = rune::node::test::catalog_cluster::awaitLeaders(c_, topicId_, 1, leaders, 5s) ? leaders[0] : -1;
        const NodeSnap* best = nullptr;
        for (const NodeSnap& s : snaps) if (s.id == id) return &s;
        for (const NodeSnap& s : snaps) if (best == nullptr || s.hw > best->hw) best = &s;
        return best;
    }
    /// One past the last data record: what the judge and the committed markers must cover (a trailing change record needs no marker).
    static int64_t dataEnd_(const VerdictRig::RigLog& l) { return l.data.empty() ? 0 : l.data.back().offset + 1; }

    /// The committed records ever seen: a compaction may remove them from every log later, the model still judges them.
    void recordHistory_(const std::vector<NodeSnap>* given = nullptr) {
        std::vector<NodeSnap> own;
        if (given == nullptr) {
            for (const int id : c_.runningNodes()) own.push_back(snapNode_(id));
            given = &own;
        }
        const NodeSnap* lead = leaderOf_(*given);
        if (lead == nullptr) return;
        for (const auto& d : lead->log.data) data_[d.offset] = d;
        for (const auto& ch : lead->log.changes) changes_[ch.offset] = ch;
    }
    VerdictRig::RigLog history_() const {
        VerdictRig::RigLog out;
        for (const auto& kv : data_) out.data.push_back(kv.second);
        for (const auto& kv : changes_) out.changes.push_back(kv.second);
        return out;
    }

    void snapAll_() {
        snaps_.clear();
        for (const int id : c_.runningNodes()) snaps_.push_back(snapNode_(id));
    }
    /// Waits for the cleaner to remove a superseded record from a node's log (the history is already recorded).
    void awaitCompaction_() {
        for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline; std::this_thread::sleep_for(200ms)) {
            snapAll_();
            for (const NodeSnap& s : snaps_) if (s.log.data.size() < data_.size()) return;
        }
        ADD_FAILURE() << "seed " << seed_ << ": no node's log lost a superseded record in 60 s (" << data_.size() << " committed records)";
    }

    void note_(unsigned code) { trace_ = (trace_ ^ code) * 1099511628211ull; }

    // ---- invariants ----
    /// The offsets the raw subscription and the group consumer delivered from 0, once the leader's accepted records have all arrived
    /// at both.
    std::pair<std::set<int64_t>, std::set<int64_t>> delivered_(const std::set<int64_t>& want) {
        if (!sub_) {
            const NodeSnap* lead = leaderOf_(snaps_);
            rune::client::ClientConfig cfg = rune::node::test::catalog_cluster::clientConfigFor(c_, lead->id);
            cfg.connectTimeoutMs = 30'000;
            cfg.pingTimeoutMs = 30'000;
            cfg.topics.push_back(rune::client::TopicSpec{"vt", topicId_, 1});
            reader_ = std::make_unique<rune::client::Client>(cfg);
            reader_->connectAndWait(30s);
            rune::client::SubscribeOptions o;
            o.from = rune::client::From::earliest();
            sub_ = std::make_unique<rune::client::Subscription>(reader_->subscribe(reader_->topic("vt"), 0, o, [this](const rune::client::Record& r) {
                std::lock_guard<std::mutex> g(deliveredMu_);
                deliveredOffsets_.insert(r.offset);
            }));
            rune::client::GroupConfig gcfg;
            gcfg.autoCommit = false;
            group_ = std::make_unique<rune::client::GroupConsumer>(reader_->subscribeGroup("cw-group", {"vt"}, gcfg));
            group_->onRecords([this](const rune::client::RecordSet& rs) {
                std::lock_guard<std::mutex> g(deliveredMu_);
                rs.forEachRecord([&](std::string_view, std::string_view, int64_t off, int64_t) { groupOffsets_.insert(off); });
            });
        }
        const auto holds = [&](const std::set<int64_t>& got) { return std::includes(got.begin(), got.end(), want.begin(), want.end()); };
        for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline; std::this_thread::sleep_for(50ms)) {
            std::lock_guard<std::mutex> g(deliveredMu_);
            if (holds(deliveredOffsets_) && holds(groupOffsets_)) break;
        }
        std::this_thread::sleep_for(500ms);   // a rejected record would arrive with its neighbours
        std::lock_guard<std::mutex> g(deliveredMu_);
        return {deliveredOffsets_, groupOffsets_};
    }

    /// The leader's view instance folded every committed data record.
    bool awaitViewFolded_() {
        const NodeSnap* lead = leaderOf_(snaps_);
        if (lead == nullptr) return false;
        const int64_t end = dataEnd_(lead->log);
        const int32_t viewTopic = viewTopicId_;
        for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline; std::this_thread::sleep_for(50ms)) {
            const int64_t folded = c_.node(lead->id).onWorkerThreadForTest(worker_(lead->id), [viewTopic](Worker& wk) {
                return wk.viewSet()->instanceServeState(viewTopic, 0).foldCursor;
            });
            if (folded >= end) return true;
        }
        return false;
    }
    bool viewRow_(uint64_t id, std::pair<int64_t, uint16_t>* out) {
        rune::client::ViewRowResult row;
        for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline; std::this_thread::sleep_for(100ms)) {
            try { row = reader_->getViewRowAndWait(viewTopicId_, id, 0, 10s); } catch (const std::exception&) { continue; }
            if (row.status == rune::client::ViewRowResult::kOk || row.status == rune::client::ViewRowResult::kAbsentKey) break;
        }
        if (row.status != rune::client::ViewRowResult::kOk) return false;
        int64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= static_cast<int64_t>(row.rowBytes[9 + static_cast<std::size_t>(i)]) << (8 * i);   // [bitmap][id][v][tag]
        *out = {v, static_cast<uint16_t>(row.rowBytes[17] | (row.rowBytes[18] << 8))};
        return true;
    }
    bool rowAbsent_(uint64_t id) {
        for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline; std::this_thread::sleep_for(100ms)) {
            rune::client::ViewRowResult row;
            try { row = reader_->getViewRowAndWait(viewTopicId_, id, 0, 10s); } catch (const std::exception&) { continue; }
            if (row.status == rune::client::ViewRowResult::kAbsentKey) return true;
            if (row.status == rune::client::ViewRowResult::kOk) return false;
        }
        return false;
    }

    void registerDefaults_() {
        registerInvariant("AckedVisibleUnlessRejected", [](CheckedWritesStressDriver& d, std::string* why) {
            std::set<int64_t> accepted, rejected;
            for (const auto& rec : d.model_.acceptedRecords()) accepted.insert(rec.v);
            for (const auto& kv : d.model_.verdicts()) rejected.insert(d.data_.at(kv.first).v);
            for (const Sent& s : d.sent_) {
                if (s.result == Sent::Result::Ok && accepted.count(s.v) == 0) { *why = "v " + std::to_string(s.v) + " was acked Ok but no accepted record carries it"; return false; }
                if (s.result == Sent::Result::Violation && rejected.count(s.v) == 0) { *why = "v " + std::to_string(s.v) + " was acked as a violation but no rejected record carries it"; return false; }
            }
            return true;
        });
        registerInvariant("NoRejectedDeliveredOrFolded", [](CheckedWritesStressDriver& d, std::string* why) {
            const NodeSnap* lead = d.leaderOf_(d.snaps_);
            std::set<int64_t> present, want, rejected;   // a compaction may have removed superseded records: only what the log still holds is due
            for (const auto& rec : lead->log.data) present.insert(rec.offset);
            for (const auto& rec : d.model_.acceptedRecords()) if (present.count(rec.offset) != 0) want.insert(rec.offset);
            for (const auto& kv : d.model_.verdicts()) rejected.insert(kv.first);
            const auto [raw, group] = d.delivered_(want);
            for (const auto& [who, got] : {std::pair<const char*, const std::set<int64_t>&>{"raw", raw}, {"group", group}}) {
                for (const int64_t off : rejected)
                    if (got.count(off) != 0) { *why = std::string(who) + ": rejected offset " + std::to_string(off) + " was delivered"; return false; }
                for (const int64_t off : want)
                    if (got.count(off) == 0) { *why = std::string(who) + ": accepted offset " + std::to_string(off) + " was not delivered"; return false; }
            }
            return true;
        });
        registerInvariant("SameVerdictEveryLeaderAndRefold", [](CheckedWritesStressDriver& d, std::string* why) {
            const NodeSnap* lead = d.leaderOf_(d.snaps_);
            for (const NodeSnap& s : d.snaps_) {
                const std::string at = "node " + std::to_string(s.id) + ": ";
                if (s.mismatches != 0) { *why = at + "the index counted " + std::to_string(s.mismatches) + " mismatches"; return false; }
                if (s.unknownDuplicates != 0) { *why = at + "verdictUnknownDuplicates " + std::to_string(s.unknownDuplicates); return false; }
                std::set<int64_t> present;
                for (const auto& rec : s.log.data) present.insert(rec.offset);
                if (&s == lead) {   // a follower builds its index when it takes the lead
                    for (const auto& [off, verdict] : s.index) {
                        const auto it = d.model_.verdicts().find(off);
                        if (it == d.model_.verdicts().end() || !(it->second == verdict)) { *why = at + "index entry " + std::to_string(off) + " is not the model's verdict"; return false; }
                    }
                    for (const auto& [off, verdict] : d.model_.verdicts()) {
                        if (present.count(off) != 0 && s.index.count(off) == 0) { *why = at + "rejected offset " + std::to_string(off) + " is in the log but not in the index"; return false; }
                    }
                }
                if (!s.log.markers.empty() && !s.log.changes.empty()) {
                    const std::string problem = markerUnionProblem(s.log.markers, s.log.changes.front().offset);
                    if (!problem.empty()) { *why = at + problem; return false; }
                }
                for (const auto& m : s.log.markers) {
                    for (const auto& [off, code, cid] : m.entries) {
                        const auto it = d.model_.verdicts().find(off);
                        if (it == d.model_.verdicts().end() || !(it->second == ModelVerdict{code, cid})) { *why = at + "a marker entry " + std::to_string(off) + " is not the model's verdict"; return false; }
                    }
                }
                if (lead != nullptr && s.hw == lead->hw) {
                    for (const auto& rec : s.log.data) {
                        const auto it = d.data_.find(rec.offset);
                        if (it == d.data_.end() || it->second.v != rec.v || it->second.id != rec.id) { *why = at + "a record at " + std::to_string(rec.offset) + " is not the committed one"; return false; }
                    }
                }
            }
            return true;
        });
        registerInvariant("ViewEqualsFreshRefold", [](CheckedWritesStressDriver& d, std::string* why) {
            if (!d.reader_) (void)d.delivered_({});
            if (!d.awaitViewFolded_()) { *why = "the view never folded the leader's committed records"; return false; }
            const auto proj = d.model_.projection();
            for (uint64_t id = 1; id <= kKeys; ++id) {
                const auto it = proj.find(id);
                if (it == proj.end()) {
                    if (!d.rowAbsent_(id)) { *why = "key " + std::to_string(id) + " has a row but no live accepted record"; return false; }
                    continue;
                }
                std::pair<int64_t, uint16_t> row;
                if (!d.viewRow_(id, &row)) { *why = "key " + std::to_string(id) + " has no row"; return false; }
                if (row.first != it->second.first || row.second != it->second.second) { *why = "key " + std::to_string(id) + " row (" + std::to_string(row.first) + ", " + std::to_string(row.second) + ") != model"; return false; }
            }
            return true;
        });
        registerInvariant("CompactionKeepsAcceptedValue", [](CheckedWritesStressDriver& d, std::string* why) {
            std::set<int64_t> inHistory;
            for (const auto& kv : d.data_) inHistory.insert(kv.first);
            std::size_t away = 0;
            for (const NodeSnap& s : d.snaps_) {
                std::set<int64_t> present;
                for (const auto& rec : s.log.data) present.insert(rec.offset);
                for (const auto& [id, latest] : d.model_.latestAcceptedByKey()) {
                    if (latest.del) continue;
                    if (present.count(latest.offset) == 0) { *why = "node " + std::to_string(s.id) + " lost the latest accepted record of key " + std::to_string(id) + " at " + std::to_string(latest.offset); return false; }
                }
                away = std::max(away, inHistory.size() - present.size());
            }
            d.compactedAway_ = away;
            return true;
        });
    }

    rune::node::test::catalog_cluster::ThreeNodes& c_;
    std::mt19937_64 rng_;
    uint64_t seed_;
    uint64_t trace_ = 1469598103934665603ull;
    int32_t topicId_ = -1, viewTopicId_ = -1;
    int32_t capTopic_ = kCapTopicBase, capSeq_ = 0;   // the installed CapLiveKeys view's topic (0 = dropped)
    uint32_t capValue_ = 8;
    std::set<uint32_t> caps_{8};
    int64_t vSeq_ = 0;
    int crashes_ = 0, restarts_ = 0, bounces_ = 0;
    bool writersStale_ = false;
    std::vector<std::unique_ptr<Writer>> writers_;
    std::vector<Sent> sent_;
    std::map<int64_t, VerdictRig::RigLogRec> data_;
    std::map<int64_t, VerdictRig::RigChange> changes_;
    std::vector<NodeSnap> snaps_;
    CheckedWritesModel model_;
    std::vector<std::pair<std::string, Invariant>> invariants_;
    std::unique_ptr<rune::client::Client> reader_;
    std::unique_ptr<rune::client::Subscription> sub_;
    std::unique_ptr<rune::client::GroupConsumer> group_;
    std::set<int64_t> groupOffsets_;
    std::mutex deliveredMu_;
    std::set<int64_t> deliveredOffsets_;
    std::size_t compactedAway_ = 0;
};

}  // namespace rune::node::cw
