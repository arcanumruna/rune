#pragma once
// CatalogReadRig.h — catalog history Task 12: the CatalogReadServer fixture,
// shared by CatalogReadServerTest.cpp and CatalogAllocRegressionTest.cpp (own binary).
//
//   * a real disk Log under a temp dir (`<tmp>/__catalog`);
//   * the catalog replica: a LEADER with ISR {self} (minIsr 1: an append commits
//     on the leader's own pass — commitEvents), or a FOLLOWER of broker 2 fed by
//     a scripted source leader in a sibling dir (followerAppend /
//     followerSetHighWatermark: what the follower holds vs what it knows committed);
//   * the server, whose egress records every frame (and can be made "full");
//   * decoders: what each session received — SubscribeResponses, the offsets of
//     every delivered record, the caught-up markers, ErrorResponses.
// Single-threaded test fixture: plain members, no atomics.

#include <rune/node/CatalogPartition.h>
#include <rune/node/CatalogReadServer.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/Log.h>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

namespace rune::node::test {

namespace read_rig_detail {
inline int gSeq = 0;   // single-threaded test binaries
}

class ReadRig {
public:
    static constexpr int32_t kSelf   = 1;
    static constexpr int32_t kLeader = 2;   // the follower rig's leader
    static constexpr int32_t kEpoch  = 1;

    explicit ReadRig(const char* tag, bool asFollower = false, std::size_t maxSubs = 64)
        : root_(std::filesystem::temp_directory_path() /
                (std::string("rune_readrig_") + tag + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(read_rig_detail::gSeq++))),
          asFollower_(asFollower),
          replica_(open_(root_ / "__catalog", log_, asFollower ? Mode::Follower : Mode::Leader)),
          server(*replica_, [this](const CatalogReadFrameHead& h, std::span<const uint8_t> f) { return record_(h, f); },
                 maxSubs, /*idPrefix=*/int64_t{kSelf + 1} << 40) {
        frames.reserve(4096);
        if (asFollower_) source_ = open_(root_ / "source", sourceLog_, Mode::Leader);
    }
    ~ReadRig() {
        replica_.reset();
        source_.reset();
        log_.reset();
        sourceLog_.reset();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }
    ReadRig(const ReadRig&)            = delete;
    ReadRig& operator=(const ReadRig&) = delete;

    // ── The catalog ────────────────────────────────────────────────────────
    /// Leader: `n` events appended and committed (ISR {self}: the leader's pass).
    void commitEvents(int n) {
        for (int i = 0; i < n; ++i) appendOne_(*replica_);
        (void)replica_->doWork(now_ += 1'000'000);
        if (replica_->highWatermark() != replica_->logEndOffset())
            throw std::logic_error("ReadRig: the leader's HW did not reach its LEO");
    }
    /// Follower: `n` more records replicated from the source leader; the HW the
    /// follower learns stays the one last set (followerSetHighWatermark).
    /// Follower: `n` more records whose leader reports HW `hw` (a new lineage after a truncation).
    void followerAppendWithHighWatermark(int n, int64_t hw) {
        followerHw_ = hw;
        followerAppend(n);
    }
    void followerAppend(int n) {
        for (int i = 0; i < n; ++i) appendOne_(*source_);
        (void)source_->doWork(now_ += 1'000'000);
        const int64_t from = replica_->logEndOffset();
        const rune::storage::LogReadView v = sourceLog_->readView(from, 1 << 20);
        apply_(std::span<const uint8_t>(v.data, v.len), followerHw_);
        if (replica_->logEndOffset() != source_->logEndOffset())
            throw std::logic_error("ReadRig: the follower did not apply the source's records");
    }
    /// Follower: an HW-only update from the leader (the committed prefix it knows).
    void followerSetHighWatermark(int64_t hw) {
        followerHw_ = hw;
        apply_({}, hw);
        if (replica_->highWatermark() != hw)
            throw std::logic_error("ReadRig: follower HW " + std::to_string(replica_->highWatermark()) +
                                   " != " + std::to_string(hw));
    }
    [[nodiscard]] rune::replication::PartitionReplica& replica() { return *replica_; }

    // ── The egress ─────────────────────────────────────────────────────────
    /// One egress frame: its head (subscriptionId 0 = a reliable answer; an empty
    /// frame with a subscriptionId = a pause ack) and the client frame bytes.
    struct Frame { int64_t session; std::vector<uint8_t> bytes; int64_t subscriptionId = 0; int64_t baseOffset = -1; };
    std::vector<Frame> frames;
    bool egressFull = false;    // the egress refuses every frame while set
    int  egressBudget = -1;     // >= 0: the egress takes this many more frames, then refuses

    [[nodiscard]] std::optional<rune::protocol::client::SubscribeResponse> subscribeResponse(int64_t session,
                                                                                           int64_t corr) const {
        namespace pc = rune::protocol::client;
        for (const Frame& f : frames) {
            if (f.session != session) continue;
            if (auto r = pc::SubscribeResponse::decode(f.bytes.data(), f.bytes.size()); r && r->correlationId == corr)
                return r;
        }
        return std::nullopt;
    }
    [[nodiscard]] bool subscribeAnswered(int64_t session, int64_t corr) const {
        const auto r = subscribeResponse(session, corr);
        return r && r->errorCode == 0;
    }
    [[nodiscard]] rune::protocol::client::ErrorCode subscribeError(int64_t session, int64_t corr) const {
        const auto r = subscribeResponse(session, corr);
        return r ? static_cast<rune::protocol::client::ErrorCode>(r->errorCode)
                 : static_cast<rune::protocol::client::ErrorCode>(-1);
    }
    [[nodiscard]] int64_t subscriptionIdOf(int64_t session, int64_t corr) const {
        const auto r = subscribeResponse(session, corr);
        return r ? r->subscriptionId : 0;
    }
    /// The offset of every RECORD delivered to `session`, in delivery order.
    [[nodiscard]] std::vector<int64_t> deliveredOffsets(int64_t session) const {
        std::vector<int64_t> out;
        forEachRecords_(session, [&](const rune::protocol::client::Records& r) {
            namespace b = rune::protocol::batch;
            std::size_t pos = 0;
            while (pos + static_cast<std::size_t>(b::kRecordBatchOverhead) <= r.recordBatch.size()) {
                rune::protocol::RecordBatchReader rd(r.recordBatch.data() + pos, r.recordBatch.size() - pos);
                for (int32_t k = 0; k <= rd.lastOffsetDelta(); ++k) out.push_back(rd.baseOffset() + k);
                pos += static_cast<std::size_t>(rd.totalLength());
            }
        });
        return out;
    }
    /// The baseOffset of every caught-up marker (an empty Records) sent to `session`.
    [[nodiscard]] std::vector<int64_t> markers(int64_t session) const {
        std::vector<int64_t> out;
        forEachRecords_(session, [&](const rune::protocol::client::Records& r) {
            if (r.recordBatch.empty()) out.push_back(r.baseOffset);
        });
        return out;
    }
    /// Every Records frame of `session`: topicId / partition / subscriptionId / correlationId checks.
    [[nodiscard]] std::vector<rune::protocol::client::Records> records(int64_t session) const {
        std::vector<rune::protocol::client::Records> out;
        forEachRecords_(session, [&](const rune::protocol::client::Records& r) { out.push_back(r); });
        return out;
    }
    [[nodiscard]] std::vector<rune::protocol::client::ErrorResponse> errors(int64_t session) const {
        namespace pc = rune::protocol::client;
        std::vector<pc::ErrorResponse> out;
        for (const Frame& f : frames)
            if (f.session == session)
                if (auto e = pc::ErrorResponse::decode(f.bytes.data(), f.bytes.size())) out.push_back(*e);
        return out;
    }
    /// The template id of every frame of `session`, in order.
    [[nodiscard]] std::vector<uint16_t> templates(int64_t session) const {
        std::vector<uint16_t> out;
        for (const Frame& f : frames)
            if (f.session == session)
                if (auto h = rune::protocol::client::MessageHeader::decode(f.bytes.data(), f.bytes.size()))
                    out.push_back(h->templateId);
        return out;
    }

private:
    enum class Mode { Leader, Follower };
    std::filesystem::path root_;
    bool                  asFollower_;
    int64_t followerHw_ = 0;
    int64_t now_        = 1'000'000;
    int     seq_        = 0;
    std::unique_ptr<rune::storage::Log>                  log_;
    std::unique_ptr<rune::storage::Log>                  sourceLog_;
    rune::replication::NullPartitionTransport            transport_;
    std::unique_ptr<rune::replication::PartitionReplica> replica_;
    std::unique_ptr<rune::replication::PartitionReplica> source_;

public:
    CatalogReadServer server;   // after replica_ (it holds a reference)

private:
    bool record_(const CatalogReadFrameHead& h, std::span<const uint8_t> f) {
        if (egressFull) return false;
        if (egressBudget == 0) return false;
        if (egressBudget > 0) --egressBudget;
        if (onFrame) return onFrame(h, f);
        frames.push_back(Frame{h.session, std::vector<uint8_t>(f.begin(), f.end()), h.subscriptionId, h.baseOffset});
        return true;
    }

public:
    /// When set, every frame the ring takes goes here instead of `frames`
    /// (fix round 0: the control-thread flow under test).
    std::function<bool(const CatalogReadFrameHead&, std::span<const uint8_t>)> onFrame;

private:
    template <class F>
    void forEachRecords_(int64_t session, F&& fn) const {
        namespace pc = rune::protocol::client;
        for (const Frame& f : frames)
            if (f.session == session)
                if (auto r = pc::Records::decode(f.bytes.data(), f.bytes.size())) fn(*r);
    }

    std::unique_ptr<rune::replication::PartitionReplica> open_(const std::filesystem::path& dir,
                                                               std::unique_ptr<rune::storage::Log>& log, Mode m) {
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
        log = std::make_unique<rune::storage::Log>(dir.string());
        log->recover();
        NodeConfig nc;
        nc.nodeId = kSelf;
        const bool solo = m == Mode::Leader;
        rune::replication::PartitionConfig cfg = makeCatalogPartitionConfig(nc, solo ? 1 : 2);
        const std::vector<rune::replication::NodeId> replicas =
            solo ? std::vector<rune::replication::NodeId>{kSelf}
                 : std::vector<rune::replication::NodeId>{kSelf, kLeader};
        auto r = std::make_unique<rune::replication::PartitionReplica>(
            cfg, rune::replication::Role::Follower, 0, replicas, replicas, log.get(), &transport_);
        if (solo) r->becomeLeader(kEpoch, {kSelf}, /*partitionEpoch=*/0);
        else      r->becomeFollower(kEpoch, kLeader);
        (void)r->doWork(now_ += 1'000'000);
        return r;
    }
    void appendOne_(rune::replication::PartitionReplica& r) {
        const std::string v = "catalog-event-" + std::to_string(seq_++) + std::string(64, 'x');
        const uint8_t key[8] = {static_cast<uint8_t>(seq_), 0, 0, 0, 0, 0, 0, 0};
        const auto base = r.appendKeyed(std::span<const uint8_t>(key, 8),
                                        std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()),
                                                                 v.size()),
                                        /*acksAll=*/true);
        if (base < 0) throw std::logic_error("ReadRig: append refused (" + std::to_string(base) + ")");
        r.onLeaderLogAppended();
    }
    void apply_(std::span<const uint8_t> records, int64_t hw) {
        rune::replication::FetchResponseMsg resp{};
        resp.leaderEpoch    = kEpoch;
        resp.partition      = kCatalogPartition;
        resp.highWatermark  = hw;
        resp.logStartOffset = 0;
        resp.errorCode      = 0;
        resp.topicId        = kCatalogTopicId;
        resp.records        = records;
        replica_->applyReplicated(resp);
        (void)replica_->doWork(now_ += 1'000'000);
    }
};

} // namespace rune::node::test
