#pragma once
// CatalogTrio.h — catalog history Task 11: THREE brokers' catalog write paths
// over one in-memory replication network (Task 20 promotes this to
// CatalogClusterHarness).
//
// Each broker b (ids 1..3) owns what its admin thread owns in production: a real
// disk __catalog Log, a PartitionReplica over it (the production catalog config:
// minIsr = majority, acks=all), its CatalogApplier, SchemaRegistry, ViewRegistry
// and AdminWorker, with recording deps.  The replicas talk through TrioNetwork:
// every message is queued and delivered on the next pump (a deterministic,
// injected clock — no sleeps; every drive loop is capped and fails loudly).
//
// The controller is the test: moveCatalogLeadership(to) hands the leadership to
// `to` at a new epoch (every live broker gets its role, as the committed (-6, 0)
// assignment would deliver it); isolate / kill shrink the leader's ISR as the
// controller's AlterIsr would.  The network can HOLD a leader's high watermark:
// fetch requests to it are dropped and its responses parked until released to
// chosen followers (replicateTo) — an event appended, replicated (or not), and
// never committed under that leader.
//
// The data plane (F24): every broker that applies a CreateTopic builds a real
// PartitionReplica per partition (minIsr 1, acks=leader) — Leader on the broker
// the trio's placement map names (placementLeader), Follower elsewhere — so
// produce/consumeFrom really append and read through the placed leader.  (Data
// replication itself is not modelled: each data replica has its own null link.)
//
// Single-threaded test fixture: plain members, no atomics.

#include "support/CatalogDdlRig.h"   // Answer / DefineArgs / descriptor()

#include <rune/node/AdminChannels.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/CatalogApplier.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>

#include <rune/catalog/CatalogEvent.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

namespace rune::node::test {

namespace rr = rune::replication;

/// The in-memory replication network of the trio (one queue; delivered per pump).
class TrioNetwork {
public:
    void attach(rr::NodeId id, rr::PartitionReplica* r) { replicas_[id] = r; }
    void detach(rr::NodeId id) { replicas_.erase(id); }

    void isolate(rr::NodeId id) { isolated_.insert(id); }
    void deisolate(rr::NodeId id) { isolated_.erase(id); }
    [[nodiscard]] bool isolated(rr::NodeId id) const { return isolated_.contains(id); }

    /// Requests to `leader` are lost and its responses parked (its HW cannot move).
    void holdLeader(rr::NodeId leader) { held_ = leader; }
    /// The parked responses of the held leader to these followers are delivered on the next pump.
    void release(const std::set<rr::NodeId>& followers) {
        std::vector<Msg> keep;
        for (Msg& m : parked_) {
            if (followers.contains(m.to)) queue_.push_back(std::move(m));
            else keep.push_back(std::move(m));
        }
        parked_.swap(keep);
    }
    /// The hold ends; still-parked responses are lost (the followers re-fetch).
    void unhold() { held_ = -1; parked_.clear(); }

    void sendRequest(rr::NodeId from, rr::NodeId to, const rr::FetchRequestMsg& req) {
        if (lost_(from, to) || to == held_) return;
        Msg m; m.kind = Msg::Kind::FetchReq; m.from = from; m.to = to; m.req = req;
        queue_.push_back(std::move(m));
    }
    void sendResponse(rr::NodeId from, rr::NodeId to, const rr::FetchResponseMsg& resp) {
        if (lost_(from, to)) return;
        Msg m; m.kind = Msg::Kind::FetchResp; m.from = from; m.to = to; m.resp = resp;
        m.records.assign(resp.records.begin(), resp.records.end());
        if (from == held_) parked_.push_back(std::move(m));
        else queue_.push_back(std::move(m));
    }
    void sendEpochRequest(rr::NodeId from, rr::NodeId to, const rr::OffsetsForLeaderEpochReqMsg& req) {
        if (lost_(from, to)) return;
        Msg m; m.kind = Msg::Kind::EpochReq; m.from = from; m.to = to; m.epochReq = req;
        queue_.push_back(std::move(m));
    }
    void sendEpochResponse(rr::NodeId from, rr::NodeId to, const rr::OffsetsForLeaderEpochRespMsg& resp) {
        if (lost_(from, to)) return;
        Msg m; m.kind = Msg::Kind::EpochResp; m.from = from; m.to = to; m.epochResp = resp;
        queue_.push_back(std::move(m));
    }

    /// Deliver everything queued before this call (new sends wait for the next pump).
    int pump(int64_t nowNs) {
        std::vector<Msg> batch;
        batch.swap(queue_);
        int n = 0;
        for (Msg& m : batch) {
            const auto it = replicas_.find(m.to);
            if (it == replicas_.end() || it->second == nullptr || lost_(m.from, m.to)) continue;
            rr::PartitionReplica& r = *it->second;
            switch (m.kind) {
            case Msg::Kind::FetchReq:
                if (r.role() == rr::Role::Leader) r.onFetchRequest(m.req, nowNs);
                break;
            case Msg::Kind::FetchResp:
                m.resp.records = std::span<const uint8_t>(m.records.data(), m.records.size());
                r.applyReplicated(m.resp);
                break;
            case Msg::Kind::EpochReq:  r.onOffsetsForLeaderEpochRequest(m.epochReq); break;
            case Msg::Kind::EpochResp: r.applyOffsetsForLeaderEpochResponse(m.epochResp); break;
            }
            ++n;
        }
        return n;
    }

private:
    struct Msg {
        enum class Kind : uint8_t { FetchReq, FetchResp, EpochReq, EpochResp } kind = Kind::FetchReq;
        rr::NodeId from = -1, to = -1;
        rr::FetchRequestMsg req{};
        rr::FetchResponseMsg resp{};
        std::vector<uint8_t> records;
        rr::OffsetsForLeaderEpochReqMsg epochReq{};
        rr::OffsetsForLeaderEpochRespMsg epochResp{};
    };
    [[nodiscard]] bool lost_(rr::NodeId from, rr::NodeId to) const {
        return isolated_.contains(from) || isolated_.contains(to);
    }
    std::map<rr::NodeId, rr::PartitionReplica*> replicas_;
    std::set<rr::NodeId> isolated_;
    rr::NodeId           held_ = -1;
    std::vector<Msg>     queue_;
    std::vector<Msg>     parked_;
};

/// One broker's end of the network: it knows who sends.
class TrioLink final : public rr::PartitionTransport {
public:
    TrioLink(TrioNetwork& net, rr::NodeId self) : net_(net), self_(self) {}
    bool sendFetchRequest(const rr::FetchRequestMsg& req) override { (void)req; return true; }   // unicast only
    bool sendFetchRequestTo(rr::NodeId leader, const rr::FetchRequestMsg& req) override {
        net_.sendRequest(self_, leader, req);
        return true;
    }
    bool sendFetchResponse(rr::NodeId to, const rr::FetchResponseMsg& resp) override {
        net_.sendResponse(self_, to, resp);
        return true;
    }
    bool sendOffsetsForLeaderEpochRequest(rr::NodeId to, const rr::OffsetsForLeaderEpochReqMsg& req) override {
        net_.sendEpochRequest(self_, to, req);
        return true;
    }
    bool sendOffsetsForLeaderEpochResponse(rr::NodeId to, const rr::OffsetsForLeaderEpochRespMsg& resp) override {
        net_.sendEpochResponse(self_, to, resp);
        return true;
    }
    bool sendInternalIsr(rr::NodeId, const rr::InternalIsrMsg&) override { return true; }
    bool sendInternalIsrAck(rr::NodeId, const rr::InternalIsrAckMsg&) override { return true; }

private:
    TrioNetwork& net_;
    rr::NodeId   self_;
};

namespace trio_detail {
inline int gSeq = 0;   // single-threaded test binaries
}

class CatalogTrio {
public:
    using Answer     = CatalogDdlRig::Answer;
    using DefineArgs = CatalogDdlRig::DefineArgs;
    static constexpr int32_t kBrokers = 3;
    static constexpr int     kCap     = 20'000;   // every drive loop's hard iteration cap (pumps of 1 ms)
    static constexpr int64_t kSession = 4242;

    explicit CatalogTrio(const char* tag)
        : root_(std::filesystem::temp_directory_path() /
                (std::string("rune_trio_") + tag + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(trio_detail::gSeq++))) {
        std::filesystem::remove_all(root_);
        for (int32_t b = 1; b <= kBrokers; ++b) brokers_.push_back(open_(b));
        for (int32_t b = 1; b <= kBrokers; ++b) isr_.push_back(b);
        moveCatalogLeadership(1);
        if (!awaitRole(1, "LeaderServing", std::chrono::seconds(10)))
            throw std::logic_error("CatalogTrio: broker 1 never served DDL");
    }
    ~CatalogTrio() {
        brokers_.clear();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }
    CatalogTrio(const CatalogTrio&)            = delete;
    CatalogTrio& operator=(const CatalogTrio&) = delete;

    // ── the controller ───────────────────────────────────────────────────────
    /// The catalog leadership moves to `to` at a new epoch with the current ISR
    /// (`to` first; it must be in it — the leader is never unclean).  Every live
    /// broker gets its role now; nothing is pumped.
    void moveCatalogLeadership(int32_t to) {
        if (std::find(isr_.begin(), isr_.end(), to) == isr_.end())
            throw std::logic_error("CatalogTrio: broker " + std::to_string(to) + " is not in the ISR (unclean)");
        ++epoch_;
        ++partitionEpoch_;
        leader_ = to;
        std::vector<int32_t> isr{to};
        for (int32_t b : isr_) if (b != to) isr.push_back(b);
        isr_ = isr;
        for (auto& br : brokers_) {
            if (!br->alive) continue;
            AdminRoleMsg r;
            r.epoch = epoch_;
            r.leaderId = to;
            r.partitionEpoch = partitionEpoch_;
            if (br->id == to) {
                r.leader = true;
                r.isrCount = static_cast<int32_t>(isr_.size());
                for (std::size_t i = 0; i < isr_.size(); ++i) r.isr[i] = isr_[i];
            }
            br->admin->onCatalogRole(r);
        }
    }
    /// The leader's ISR changes at the same leader epoch (a committed AlterIsr).
    void updateIsr(std::vector<int32_t> isr) {
        isr_ = std::move(isr);
        AdminRoleMsg r;
        r.leader = true;
        r.epoch = epoch_;
        r.leaderId = leader_;
        r.partitionEpoch = ++partitionEpoch_;
        r.isrCount = static_cast<int32_t>(isr_.size());
        for (std::size_t i = 0; i < isr_.size(); ++i) r.isr[i] = isr_[i];
        broker_(leader_).admin->onCatalogRole(r);
    }
    /// `b` stops fetching (its log is kept); the controller drops it from the ISR.
    void isolate(int32_t b) {
        net_.isolate(b);
        shrinkIsr_(b);
    }
    /// `b` is reachable again (it catches up as a follower, outside the ISR).
    void deisolate(int32_t b) { net_.deisolate(b); }
    /// `b` dies: never driven again; the controller drops it from the ISR.
    void kill(int32_t b) {
        broker_(b).alive = false;
        net_.isolate(b);
        shrinkIsr_(b);
    }

    // ── the network ──────────────────────────────────────────────────────────
    /// `b`'s high watermark cannot move: requests to it are lost, its responses parked.
    void holdHighWatermark(int32_t b) { net_.holdLeader(b); }
    /// The parked responses reach these followers (their acks are still lost).
    void replicateTo(std::set<int32_t> followers) {
        std::set<rr::NodeId> f(followers.begin(), followers.end());
        net_.release(f);
        for (int i = 0; i < 8; ++i) pump();
    }
    void releaseHighWatermark() { net_.unhold(); }

    // ── the applier seam ─────────────────────────────────────────────────────
    void holdApply(int32_t b) { broker_(b).admin->holdApplyForTest(true); }
    void releaseApply(int32_t b) { broker_(b).admin->holdApplyForTest(false); }

    // ── DDL ──────────────────────────────────────────────────────────────────
    Answer define(int32_t b, DefineArgs a) { return await_(b, submitDefineNoWait(b, std::move(a))); }
    int64_t submitDefineNoWait(int32_t b, DefineArgs a) {
        DefineTopicMsg m;
        m.session = kSession; m.corr = ++corr_;
        m.name = a.name; m.partitionCount = a.partitions; m.validate = a.validate;
        m.keyField = a.typed ? a.keyField : std::string{};
        if (a.typed) m.blob = CatalogDdlRig::descriptor(a.name);
        m.persistence = a.persistence; m.replicationFactor = a.rf; m.config = a.config;
        DdlMeta meta;
        meta.statement     = a.statement;
        meta.sessionId     = kSession;
        meta.sourceAddress = "aeron:udp?endpoint=trio-client:40123";
        meta.clientName    = a.clientName;
        meta.requestId     = a.requestId.none() ? rune::protocol::RequestId{0x7710, ++rid_} : a.requestId;
        m.meta = std::move(meta);
        const int64_t corr = m.corr;
        broker_(b).admin->submit(DdlRequest{std::move(m)});
        for (int i = 0; i < 4; ++i) pump();
        return corr;
    }
    /// DropTopic (74) through `b` — answered by a DdlResponse (it carries a detail).
    Answer dropTopic(int32_t b, const std::string& name) {
        DropTopicMsg m;
        m.session = kSession; m.corr = ++corr_; m.name = name;
        m.meta.sessionId  = kSession;
        m.meta.clientName = "trio";
        m.meta.requestId  = rune::protocol::RequestId{0x7710, ++rid_};
        const int64_t corr = m.corr;
        broker_(b).admin->submit(DdlRequest{std::move(m)});
        return await_(b, corr);
    }
    /// `n` fresh topics defined through `b`, each answered OK (committed).
    void commitEvents(int32_t b, int n) {
        for (int i = 0; i < n; ++i) {
            const Answer a = define(b, {.name = "ev" + std::to_string(evSeq_++), .partitions = 1});
            if (a.code != 0)
                throw std::logic_error("CatalogTrio: commitEvents answer " + std::to_string(a.code) + " (" +
                                       a.detail + ")");
        }
    }
    [[nodiscard]] std::optional<Answer> answerFor(int32_t b, int64_t corr) const {
        const Broker& br = broker_(b);
        const auto it = br.answers.find(corr);
        if (it == br.answers.end()) return std::nullopt;
        return it->second;
    }
    /// Every answer `b` sent for `corr` (exactly-once checks).
    [[nodiscard]] std::size_t answersSentFor(int32_t b, int64_t corr) const {
        const Broker& br = broker_(b);
        const auto it = br.answerCounts.find(corr);
        return it == br.answerCounts.end() ? 0u : it->second;
    }

    // ── data plane (F24) ─────────────────────────────────────────────────────
    /// The broker that leads every partition of the topics created from now on.
    void setPlacementLeader(int32_t b) { placementLeader_ = b; }
    /// Append `bytes` (one record) through `b`'s leader replica of `topic` p0
    /// (acks=leader); nullopt when `b` does not lead it or the append is refused.
    std::optional<int64_t> produce(int32_t b, const std::string& topic, std::vector<uint8_t> bytes) {
        Broker& br = broker_(b);
        const rune::catalog::TopicObject* t = br.admin->applier().state().topicByName(topic);
        if (t == nullptr) return std::nullopt;
        const auto it = br.data.find(t->topicId);
        if (it == br.data.end() || it->second.empty() || it->second[0]->replica->role() != rr::Role::Leader)
            return std::nullopt;
        rr::PartitionReplica& r = *it->second[0]->replica;
        const rr::Offset off = r.appendKeyed(std::span<const uint8_t>{}, std::span<const uint8_t>(bytes),
                                             /*acksAll=*/false);
        if (off < 0) return std::nullopt;
        r.onLeaderLogAppended();
        return off;
    }
    /// The value of the record at `offset` of `topic` p0 on `b` (empty: none).
    std::vector<uint8_t> consumeFrom(int32_t b, const std::string& topic, int64_t offset) {
        Broker& br = broker_(b);
        const rune::catalog::TopicObject* t = br.admin->applier().state().topicByName(topic);
        if (t == nullptr) return {};
        const auto it = br.data.find(t->topicId);
        if (it == br.data.end() || it->second.empty()) return {};
        const std::vector<uint8_t> data = it->second[0]->log->read(offset, 1 << 20);
        std::vector<uint8_t> out;
        std::size_t pos = 0;
        while (pos < data.size() && out.empty()) {
            rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
            if (!reader.ensureValid()) break;
            const auto tl = static_cast<std::size_t>(reader.totalLength());
            reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                if (off == offset) out.assign(val.begin(), val.end());
            });
            if (tl == 0) break;
            pos += tl;
        }
        return out;
    }

    // ── observation ──────────────────────────────────────────────────────────
    [[nodiscard]] AdminWorker& admin(int32_t b) { return *broker_(b).admin; }
    [[nodiscard]] rr::PartitionReplica& replica(int32_t b) { return *broker_(b).replica; }
    [[nodiscard]] int32_t leader() const noexcept { return leader_; }
    bool awaitRole(int32_t b, std::string_view role, std::chrono::milliseconds within) {
        const int64_t steps = std::max<int64_t>(1, within.count());   // one pump = 1 ms of trio time
        for (int64_t i = 0; i < steps && i < kCap && broker_(b).admin->roleName() != role; ++i) pump();
        return broker_(b).admin->roleName() == role;
    }
    /// Pump until every live broker's log has the same end offset and applied it (capped).
    bool awaitConverged() {
        for (int i = 0; i < kCap; ++i) {
            if (converged_()) return true;
            pump();
        }
        return converged_();
    }
    /// Every event `b`'s log holds (committed or not), in offset order.
    [[nodiscard]] std::vector<rune::catalog::RecordedEvent> recordedLog(int32_t b) const {
        return readLog_(*broker_(b).log);
    }
    /// Events carrying `rid` in `b`'s log (default: the leader's).
    [[nodiscard]] std::size_t countEventsWithRequestId(const rune::protocol::RequestId& rid, int32_t b = -1) const {
        std::size_t n = 0;
        for (const auto& r : recordedLog(b < 0 ? leader_ : b)) n += r.event.principal.requestId == rid ? 1 : 0;
        return n;
    }
    /// Converged, and every live broker's applied catalog has the same digest.
    bool allDigestsEqual() {
        if (!awaitConverged()) return false;
        std::optional<uint64_t> d;
        for (const auto& br : brokers_) {
            if (!br->alive) continue;
            const uint64_t mine = br->admin->applier().state().digest();
            if (d && *d != mine) return false;
            d = mine;
        }
        return d.has_value();
    }

    // ── driving ──────────────────────────────────────────────────────────────
    void pump() {
        now_ += 1'000'000;
        for (auto& br : brokers_)
            if (br->alive) (void)br->admin->doWork(now_);
        (void)net_.pump(now_);
    }

private:
    struct DataPartition {
        std::unique_ptr<rune::storage::Log>   log;
        std::unique_ptr<rr::PartitionReplica> replica;
    };
    struct Broker {
        int32_t                               id = 0;
        bool                                  alive = true;
        std::unique_ptr<rune::storage::Log>   log;
        std::unique_ptr<TrioLink>             link;
        std::unique_ptr<rr::PartitionReplica> replica;
        SchemaRegistry                        schemas;
        ViewRegistry                          views;
        std::unique_ptr<AdminWorker>          admin;
        std::map<int64_t, Answer>             answers;
        std::map<int64_t, std::size_t>        answerCounts;
        rr::NullPartitionTransport            dataLink;
        std::map<int32_t, std::vector<std::unique_ptr<DataPartition>>> data;   // topicId → partitions
        ~Broker() {
            admin.reset();
            data.clear();
            replica.reset();
            log.reset();
        }
    };

    Broker& broker_(int32_t b) { return *brokers_.at(static_cast<std::size_t>(b - 1)); }
    const Broker& broker_(int32_t b) const { return *brokers_.at(static_cast<std::size_t>(b - 1)); }

    void shrinkIsr_(int32_t b) {
        if (b == leader_) return;   // the leader's own death: the test moves the leadership
        std::vector<int32_t> isr;
        for (int32_t x : isr_) if (x != b) isr.push_back(x);
        if (isr != isr_ && broker_(leader_).alive) updateIsr(std::move(isr));
        else isr_ = std::move(isr);
    }

    bool converged_() {
        std::optional<int64_t> leo;
        for (const auto& br : brokers_) {
            if (!br->alive || net_.isolated(br->id)) continue;
            const int64_t l = br->replica->logEndOffset();
            if (leo && *leo != l) return false;
            if (br->admin->applier().nextOffset() != l) return false;
            leo = l;
        }
        return true;
    }

    static std::vector<rune::catalog::RecordedEvent> readLog_(rune::storage::Log& log) {
        std::vector<rune::catalog::RecordedEvent> out;
        int64_t next = 0;
        const int64_t leo = log.logEndOffset();
        while (next < leo) {
            const std::vector<uint8_t> data = log.read(next, 1 << 22);
            if (data.empty()) break;
            std::size_t pos = 0;
            const int64_t before = next;
            while (pos < data.size()) {
                rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                if (!reader.ensureValid()) break;
                const auto tl = static_cast<std::size_t>(reader.totalLength());
                reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                    if (off < next) return;
                    auto ev = rune::catalog::decode(
                        std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                    if (!ev) throw std::logic_error("CatalogTrio: a logged event does not decode");
                    out.push_back(rune::catalog::RecordedEvent{off, std::move(*ev)});
                    next = off + 1;
                });
                if (tl == 0) break;
                pos += tl;
            }
            if (next == before) break;
        }
        return out;
    }

    Answer await_(int32_t b, int64_t corr) {
        for (int i = 0; i < kCap && !broker_(b).answers.contains(corr); ++i) pump();
        const auto it = broker_(b).answers.find(corr);
        if (it == broker_(b).answers.end())
            throw std::logic_error("CatalogTrio: corr " + std::to_string(corr) + " was never answered by broker " +
                                   std::to_string(b));
        return it->second;
    }

    void onTopicApplied_(Broker& br, const rune::catalog::TopicObject& t) {
        auto& parts = br.data[t.topicId];
        if (!parts.empty()) return;   // idempotent (the same effect again)
        for (int32_t p = 0; p < t.partitions; ++p) {
            auto dp = std::make_unique<DataPartition>();
            const auto dir = root_ / ("b" + std::to_string(br.id)) / ("t" + std::to_string(t.topicId) + "-" +
                                                                      std::to_string(p));
            std::filesystem::create_directories(dir);
            dp->log = std::make_unique<rune::storage::Log>(dir.string());
            dp->log->recover();
            rr::PartitionConfig c;
            c.topicId     = t.topicId;
            c.partitionId = p;
            c.selfId      = br.id;
            c.minIsr      = 1;
            const bool leads = br.id == placementLeader_;
            const std::vector<rr::NodeId> replicas{1, 2, 3};
            const std::vector<rr::NodeId> isr{placementLeader_};
            dp->replica = std::make_unique<rr::PartitionReplica>(c, leads ? rr::Role::Leader : rr::Role::Follower, 1,
                                                                 replicas, isr, dp->log.get(), &br.dataLink);
            parts.push_back(std::move(dp));
        }
    }

    std::unique_ptr<Broker> open_(int32_t id) {
        auto br = std::make_unique<Broker>();
        br->id = id;
        const auto dir = root_ / ("b" + std::to_string(id)) / "__catalog";
        std::filesystem::create_directories(dir);
        br->log = std::make_unique<rune::storage::Log>(dir.string());
        br->log->recover();
        br->link = std::make_unique<TrioLink>(net_, id);
        NodeConfig nc;
        nc.nodeId = id;
        // ISR changes are the controller's (the test's), never a lag across the clock jumps.
        nc.replicaLagMaxMs = 3'600'000;
        nc.fetchIntervalNs = 2'000'000;
        const std::vector<rr::NodeId> all{1, 2, 3};
        br->replica = std::make_unique<rr::PartitionReplica>(makeCatalogPartitionConfig(nc, kBrokers),
                                                             rr::Role::Follower, 0, all, all, br->log.get(),
                                                             br->link.get());
        net_.attach(id, br->replica.get());

        Broker* self = br.get();
        auto record = [self](int64_t corr, Answer a) {
            ++self->answerCounts[corr];
            self->answers[corr] = std::move(a);
        };
        AdminDeps deps;
        deps.brokerCount = kBrokers;
        deps.topicMinIsr = 1;
        deps.respondDefine = [record](int64_t, int64_t corr, int32_t topicId, uint16_t version, uint16_t code) {
            record(corr, Answer{code, topicId, version, {}, 0, -1, 0, Answer::Kind::Define});
        };
        deps.respondAlter = [record](int64_t, int64_t corr, int32_t topicId, uint16_t version, uint16_t code) {
            record(corr, Answer{code, topicId, version, {}, 0, -1, 0, Answer::Kind::Alter});
        };
        deps.respondCreateView = [record](int64_t, int64_t corr, int32_t viewTopicId, uint64_t fp, uint16_t code) {
            record(corr, Answer{code, viewTopicId, 0, {}, 0, -1, fp, Answer::Kind::CreateView});
        };
        deps.respondDdl = [record](int64_t, int64_t corr, uint16_t code, uint64_t objectId, uint32_t newVersion,
                                   int32_t gate, std::string_view detail) {
            record(corr, Answer{code, -1, newVersion, std::string(detail), objectId, gate, 0, Answer::Kind::Ddl});
        };
        deps.respondError = [record](int64_t, int64_t corr, uint16_t code, std::string_view message) {
            record(corr, Answer{code, -1, 0, std::string(message), 0, -1, 0, Answer::Kind::Error});
        };
        deps.respondGet = [](int64_t, int64_t, int32_t, uint16_t, int32_t, const std::vector<uint8_t>&, uint16_t) {};
        br->admin = std::make_unique<AdminWorker>(br->schemas, id, std::move(deps), &br->views, br->replica.get(), nullptr);
        CatalogEffects fx;
        fx.topicApplied = [this, self](const rune::catalog::TopicObject& t) { onTopicApplied_(*self, t); };
        (void)br->admin->bootCatalog(BootEffects{CatalogEffects{}}, LiveEffects{std::move(fx)});
        return br;
    }

    std::filesystem::path                root_;
    TrioNetwork                          net_;
    std::vector<std::unique_ptr<Broker>> brokers_;
    std::vector<int32_t>                 isr_;
    int32_t                              leader_          = -1;
    int32_t                              epoch_           = 0;
    int32_t                              partitionEpoch_  = 0;
    int32_t                              placementLeader_ = 1;
    int64_t                              now_             = 1'000'000'000;
    int64_t                              corr_            = 0;
    uint64_t                             rid_             = 0;
    int                                  evSeq_           = 0;
};

} // namespace rune::node::test
