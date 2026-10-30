// TransientViewRig.h - checked writes Part 10 (spec 9a): persisted views over a TRANSIENT source, driven at the
// WorkerViewSet level.  The source ring, its restarts, evictions and recreations are simulated here; the view set is the
// production one, driven exactly as Worker::driveViewsOf_ drives it (driveRebuild, then driveFolds, with the ring's
// TransientBounds).  Nothing waits on wall time: every pass is an explicit call and every drive loop has a hard cap.
//
// The "disk" is the rig's per-view image of the checkpoint file: after every pass the deltas pushed on the channel are
// appended to it (what the IO agent does), and restart() replays that image into a fresh WorkerViewSet.
#pragma once

#include <rune/codec/Descriptor.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewCheckpoint.h>
#include <rune/view/ViewRow.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <unistd.h>

namespace transient_view_rig {

enum class Kind : uint8_t { Replace, CountByGrp, MinByGrp };
inline constexpr int32_t kSrcTopic = 5;
inline constexpr int64_t kKeyDomain = 64;   // keys and groups the table probes cover

[[nodiscard]] inline int32_t viewId(Kind k) noexcept { return 0x40000A00 + static_cast<int32_t>(k); }

struct Rec {
    int64_t key;
    int64_t grp;
    int64_t hp;
    bool    tombstone = false;
};

namespace detail {
inline std::string keyBytes(int64_t id) {
    std::string b(8, '\0');
    for (int i = 0; i < 8; ++i) b[static_cast<std::size_t>(i)] = static_cast<char>(static_cast<uint64_t>(id) >> (8 * i));
    return b;
}
// [blockLength u16][version u16][characterId i64][grp i64][hp i64]
inline std::string value(const Rec& r) {
    std::string b;
    auto le16 = [&](uint16_t v) { b.push_back(static_cast<char>(v)); b.push_back(static_cast<char>(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(static_cast<char>(static_cast<uint64_t>(v) >> (8 * i))); };
    le16(24); le16(1); le64(r.key); le64(r.grp); le64(r.hp);
    return b;
}
inline rune::codec::Descriptor source() {
    using P = rune::codec::PrimType;
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", P::Int64, 1, /*isKey=*/true)
        .add("grp", P::Int64)
        .add("hp", P::Int64)
        .build();
}
inline rune::view::ViewPlan plan(Kind k, uint8_t persistence) {
    using P = rune::codec::PrimType; using R = rune::view::MergeRule;
    rune::view::ViewPlan v;
    v.persistence = persistence;
    v.sizing.chunkBytes = 64u << 10;   // small tables: the matrices build thousands of instances
    v.sizing.capacity   = 256;
    switch (k) {
        case Kind::Replace:
            v.viewName = "tv_rows"; v.key = "characterId"; v.sources = {{kSrcTopic, "characterId"}};
            v.output = {{"characterId", P::Int64, 0, "characterId", R::Replace},
                        {"grp", P::Int64, 0, "grp", R::Replace},
                        {"hp", P::Int64, 0, "hp", R::Replace}};
            break;
        case Kind::CountByGrp:
            v.viewName = "tv_count"; v.key = "grp"; v.sources = {{kSrcTopic, "grp"}};
            v.output = {{"grp", P::Int64, 0, "grp", R::Replace}, {"n", P::Int64, 0, "", R::CountAll}};
            break;
        case Kind::MinByGrp:
            v.viewName = "tv_min"; v.key = "grp"; v.sources = {{kSrcTopic, "grp"}};
            v.output = {{"grp", P::Int64, 0, "grp", R::Replace}, {"minHp", P::Int64, 0, "hp", R::Min}};
            break;
    }
    return v;
}
}  // namespace detail

class ViewSetRig {
public:
    explicit ViewSetRig(std::string tag)
        : dir_((std::filesystem::temp_directory_path() / ("rune-tv-" + tag + "-" + std::to_string(::getpid()))).string()),
          src_(detail::source()) {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
        std::filesystem::create_directories(dir_);
        boot_();
    }
    ~ViewSetRig() { std::error_code ec; std::filesystem::remove_all(dir_, ec); }
    ViewSetRig(const ViewSetRig&)            = delete;
    ViewSetRig& operator=(const ViewSetRig&) = delete;

    void install(Kind kind, bool persisted = true) {
        const rune::codec::Descriptor* srcs[] = {&src_};
        auto comp = rune::view::compileView(detail::plan(kind, persisted ? 0 : 1), srcs, viewId(kind)).value();
        installed_.push_back(Installed{kind, persisted, comp.plan, comp.rowLayout});
        views_->installView(comp.plan, comp.rowLayout, src_, persisted);
    }

    /// One single-record batch per Rec, at logEnd.
    void append(std::span<const Rec> recs) {
        for (const Rec& r : recs) push_(batch_(r, /*pid=*/-1), r, /*data=*/true);
    }
    /// A transactional data batch of producer `pid` (never decided until abortMarker).
    void appendTransactional(int64_t pid, const Rec& r) { push_(batch_(r, pid), r, /*data=*/true); }
    /// An ABORT control batch of producer `pid` at logEnd.
    void appendAbortMarker(int64_t pid) {
        std::vector<uint8_t> b = rune::transaction::ControlMarker::build(pid, 0, rune::transaction::ControlType::Abort, 0);
        for (int i = 0; i < 8; ++i) b[static_cast<std::size_t>(i)] = static_cast<uint8_t>(static_cast<uint64_t>(logEnd_) >> (8 * i));
        push_(std::move(b), Rec{0, 0, 0}, /*data=*/false);
    }

    /// The ring drops [logStart, newLogStart).
    void evictTo(int64_t newLogStart) {
        std::erase_if(ring_, [&](const Batch& b) { return b.offset < newLogStart; });
        logStart_ = std::max(logStart_, newLogStart);
        logEnd_   = std::max(logEnd_, logStart_);
    }
    /// An empty ring at `logStart` (a restart or a recreation of the source).
    void newRing(int64_t logStart, uint32_t incarnation) {
        ring_.clear();
        logStart_ = logEnd_ = logStart;
        incarnation_ = incarnation;
        generation_  = 0;
        cutOffset_   = 0;
    }
    /// A content change at `offset`: generation + 1, the tail is replaced.
    void cutAt(int64_t offset, std::span<const Rec> replacement) {
        std::erase_if(ring_, [&](const Batch& b) { return b.offset >= offset; });
        logEnd_ = std::max(offset, logStart_);
        ++generation_;
        cutOffset_ = offset;
        append(replacement);
    }

    /// driveRebuild + driveFolds as Worker::driveViewsOf_ runs them, then the deltas go to the file image.
    int pass(uint64_t leaderGen, int64_t maxRecordsPerRead = INT64_MAX) {
        readCap_ = maxRecordsPerRead;
        const rune::node::TransientBounds tb{logStart_, logEnd_, incarnation_, generation_, cutOffset_};
        auto rr = [this](int64_t from, int64_t) { return readRange_(from); };
        int work = views_->driveRebuild(kSrcTopic, 0, rr, logEnd_, aborted_, nowNs_, &tb, leaderGen, true);
        work += views_->driveFolds(kSrcTopic, 0, rr, logEnd_, aborted_, nowNs_, &tb, leaderGen, true);
        drainDeltas_();
        return work;
    }
    [[nodiscard]] bool driveUntilCaughtUp(uint64_t leaderGen, int cap = 256) {
        for (int i = 0; i < cap; ++i) {
            (void)pass(leaderGen);
            if (caughtUp_()) return true;
        }
        return false;
    }

    /// A new channel and a new view set; every persisted view is recovered from the file image minus its last
    /// `dropLastDeltas` records (the tail that never reached the disk).  The ring is whatever the test set before the call.
    /// `aborted` is handed to every later pass, as the worker's RC state would be after the restart.
    void restart(const rune::view::AbortedTxnFilter& aborted = rune::view::kNoAbortedTxns, std::size_t dropLastDeltas = 0) {
        aborted_ = aborted;
        for (auto& [id, recs] : image_) recs.resize(recs.size() - std::min(dropLastDeltas, recs.size()));
        boot_();
        for (const Installed& in : installed_) {
            if (!in.persisted) { views_->installView(in.plan, in.layout, src_, false); continue; }
            const auto& recs = image_[viewId(in.kind)];
            std::vector<std::span<const uint8_t>> spans;
            for (const auto& r : recs) spans.emplace_back(r.data(), r.size());
            views_->installViewForRecovery(in.plan, in.layout, src_, true, 0, {}, spans, logEnd_);
        }
    }

    [[nodiscard]] std::optional<int64_t> grp(int64_t key) const { return slot_(Kind::Replace, key, 1); }
    [[nodiscard]] std::optional<int64_t> hp(int64_t key) const { return slot_(Kind::Replace, key, 2); }
    [[nodiscard]] std::optional<int64_t> count(int64_t grp) const { return slot_(Kind::CountByGrp, grp, 1); }
    [[nodiscard]] std::optional<int64_t> minHp(int64_t grp) const { return slot_(Kind::MinByGrp, grp, 1); }
    [[nodiscard]] rune::node::WorkerViewSet::RowStatus status(Kind kind, int64_t key) const {
        const rune::view::RowLayout* L = nullptr;
        rune::node::WorkerViewSet::RowStatus st = rune::node::WorkerViewSet::RowStatus::NotFound;
        (void)views_->getRow(viewId(kind), 0, static_cast<uint64_t>(key), L, &st);
        return st;
    }
    [[nodiscard]] const rune::node::ViewFoldCounters& counters(Kind kind) const {
        static const rune::node::ViewFoldCounters kNone{};
        const rune::node::ViewFoldCounters* c = views_->foldCounters(viewId(kind), 0);
        return c != nullptr ? *c : kNone;
    }
    /// Every row of the view, key first, in key order (probed over the rig's key domain).
    [[nodiscard]] std::vector<std::vector<uint8_t>> tableImage(Kind kind) const {
        std::vector<std::vector<uint8_t>> out;
        for (int64_t k = 0; k < kKeyDomain; ++k) {
            const rune::view::RowLayout* L = nullptr;
            const uint8_t* row = views_->getRow(viewId(kind), 0, static_cast<uint64_t>(k), L);
            if (row == nullptr || L == nullptr) continue;
            std::vector<uint8_t> img(8 + L->rowLen);
            std::memcpy(img.data(), &k, 8);
            std::memcpy(img.data() + 8, row, L->rowLen);
            out.push_back(std::move(img));
        }
        return out;
    }

    /// True once the view pushed a checkpoint delta: a restart then recovers a cursor, not an empty file.
    [[nodiscard]] bool hasCheckpoint(Kind kind) const {
        const auto it = image_.find(viewId(kind));
        return it != image_.end() && !it->second.empty();
    }

    /// The data records the ring holds at or above `from`, in offset order.
    [[nodiscard]] std::vector<Rec> ringRecs(int64_t from = 0) const {
        std::vector<Rec> out;
        for (const Batch& b : ring_) if (b.data && b.offset >= from) out.push_back(b.rec);
        return out;
    }
    [[nodiscard]] int64_t logStart() const noexcept { return logStart_; }
    [[nodiscard]] int64_t logEnd() const noexcept { return logEnd_; }
    [[nodiscard]] rune::node::WorkerViewSet& views() { return *views_; }

private:
    struct Installed {
        Kind kind; bool persisted;
        rune::view::CompiledViewPlan plan;
        rune::view::RowLayout layout;
    };
    struct Batch { int64_t offset; std::vector<uint8_t> bytes; Rec rec; bool data; };

    void boot_() {
        views_.reset();
        channel_ = std::make_unique<rune::node::ViewSnapshotChannel>();
        views_   = std::make_unique<rune::node::WorkerViewSet>(*channel_, dir_);
    }
    [[nodiscard]] std::vector<uint8_t> batch_(const Rec& r, int64_t pid) const {
        rune::protocol::RecordBatchBuilder b(logEnd_, 0);
        if (pid >= 0) { b.setTransactional(true); b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(0); }
        const std::string k = detail::keyBytes(r.key);
        const std::string v = r.tombstone ? std::string() : detail::value(r);
        b.append(k, v, 0);
        return b.build();
    }
    void push_(std::vector<uint8_t> bytes, const Rec& r, bool data) {
        ring_.push_back(Batch{logEnd_, std::move(bytes), r, data});
        ++logEnd_;
    }
    [[nodiscard]] rune::storage::LogReadView readRange_(int64_t from) {
        scratch_.clear();
        int64_t first = -1, next = -1, n = 0;
        for (const Batch& b : ring_) {
            if (b.offset < std::max(from, logStart_)) continue;
            if (n++ >= readCap_) break;
            if (first < 0) first = b.offset;
            next = b.offset + 1;
            scratch_.insert(scratch_.end(), b.bytes.begin(), b.bytes.end());
        }
        if (scratch_.empty()) return {};
        return rune::storage::LogReadView{scratch_.data(), scratch_.size(), first, next};
    }
    void drainDeltas_() {
        channel_->deltas.drain([&](int32_t, aeron::concurrent::AtomicBuffer& buf, rune::node::OneToOneByteRing::index_t off,
                                   rune::node::OneToOneByteRing::index_t len) {
            const uint8_t* p = buf.buffer() + off;
            int32_t view = 0;
            std::memcpy(&view, p, 4);
            image_[view].emplace_back(p + rune::node::ViewSnapshotChannel::kDeltaHeaderBytes, p + len);
        });
    }
    [[nodiscard]] bool caughtUp_() const {
        for (const Installed& in : installed_) {
            const int32_t id = viewId(in.kind);
            if (views_->readiness(id, 0) != rune::node::ViewReadiness::Ready || views_->foldCursorForTest(id, 0) < logEnd_) return false;
        }
        return true;
    }
    [[nodiscard]] std::optional<int64_t> slot_(Kind kind, int64_t key, uint16_t slot) const {
        const rune::view::RowLayout* L = nullptr;
        const uint8_t* row = views_->getRow(viewId(kind), 0, static_cast<uint64_t>(key), L);
        if (row == nullptr || L == nullptr) return std::nullopt;
        return rune::view::readSlot(row, *L, slot).i;
    }

    std::string                        dir_;
    rune::codec::Descriptor            src_;
    std::vector<Installed>             installed_;
    std::unique_ptr<rune::node::ViewSnapshotChannel> channel_;
    std::unique_ptr<rune::node::WorkerViewSet>       views_;
    std::map<int32_t, std::vector<std::vector<uint8_t>>> image_;   // per view: the checkpoint file's records
    std::vector<Batch>                 ring_;
    std::vector<uint8_t>               scratch_;
    int64_t                            logStart_ = 0, logEnd_ = 0, cutOffset_ = 0, readCap_ = INT64_MAX, nowNs_ = 0;
    uint32_t                           incarnation_ = 1;
    uint64_t                           generation_  = 0;
    rune::view::AbortedTxnFilter       aborted_ = rune::view::kNoAbortedTxns;
};

/// The reference: the per-key map the views fold.
struct TransientViewModel {
    std::map<int64_t, Rec> rows;

    void apply(const Rec& r) {
        if (r.tombstone) rows.erase(r.key); else rows[r.key] = r;
    }
    void replay(std::span<const Rec> ringReadInOrder) { for (const Rec& r : ringReadInOrder) apply(r); }
    [[nodiscard]] std::optional<int64_t> hp(int64_t key) const {
        const auto it = rows.find(key);
        return it == rows.end() ? std::nullopt : std::optional<int64_t>(it->second.hp);
    }
    [[nodiscard]] std::optional<int64_t> count(int64_t grp) const {
        int64_t n = 0;
        for (const auto& [k, r] : rows) n += r.grp == grp;
        return n == 0 ? std::nullopt : std::optional<int64_t>(n);
    }
    [[nodiscard]] std::optional<int64_t> minHp(int64_t grp) const {
        std::optional<int64_t> m;
        for (const auto& [k, r] : rows) if (r.grp == grp && (!m || r.hp < *m)) m = r.hp;
        return m;
    }
};

}  // namespace transient_view_rig
