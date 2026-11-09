#pragma once
// WriteState.h — checked writes Part 02: what a worker knows about a typed topic's writes.
//
// TopicWriteSpec is the per-topic spec (the key facts and one materialized Validator + Descriptor per schema version),
// owned by the worker that holds it (shared-nothing; installed by cold ops).  PartitionWriteState is the per-partition
// handle next to RcSlot: a typed partition's state points at its topic's spec, an untyped partition has none.
// checkTypedRecord is the one per-record rule set; checkTypedBatch is the bounded walk over it (hot path: no
// allocation, no lock, no virtual call).
#include <rune/catalog/CatalogState.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/PrimType.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Validator.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/CatalogTypes.h>
#include <rune/node/AutoInc.h>
#include <rune/node/VerdictState.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/ControlRecordLayout.h>
#include <rune/storage/RejectedOffsetIndex.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace rune::node {

inline constexpr uint16_t kNoKeyField  = 0xFFFF;
inline constexpr uint32_t kNoKeyOffset = 0xFFFFFFFF;

/// The WriteSpecUpdate tail: [topicId i32][keyFieldId u16][keyType u8][keyWidth u8][flags u8: bit0 legacyValidate, bit1 keyIndex,
/// bit2 autoIncrement], then, with autoIncrement only, [partitioner u8][autoIncrementMax u64][autoIncrementReserve u32]
/// [partitionCount i32] (26 bytes in all).
struct TopicWriteSpecWire {
    int32_t              topicId        = -1;
    uint16_t             keyFieldId     = kNoKeyField;   ///< the key field's durable tag; kNoKeyField = a keyless topic
    rune::codec::PrimType keyType       = rune::codec::PrimType::Int32;
    uint8_t              keyWidth       = 0;             ///< 0 = the key cannot be checked (a key type from before the topic key rule)
    bool                 legacyValidate = false;         ///< the catalog's SchemaDef::validate: validates below kCheckedWrites
    bool                 keyIndex       = false;         ///< the topic's key.index feature
    bool                 autoIncrement  = false;         ///< the topic's autoincrement feature; the fields below ride the wire only with it
    rune::protocol::Partitioner partitioner = rune::protocol::Partitioner::Fnv1a;
    uint64_t             autoIncrementMax     = rune::protocol::kAutoIncrementMaxDefault;       ///< the effective cap
    uint32_t             autoIncrementReserve = rune::protocol::kAutoIncrementReserveDefault;
    int32_t              partitionCount = 0;
    bool operator==(const TopicWriteSpecWire&) const = default;
};
inline constexpr std::size_t kTopicWriteSpecWireLen = 9;
inline constexpr std::size_t kTopicWriteSpecAutoIncWireLen = 26;

[[nodiscard]] constexpr std::size_t topicWriteSpecWireLen(const TopicWriteSpecWire& w) noexcept {
    return w.autoIncrement ? kTopicWriteSpecAutoIncWireLen : kTopicWriteSpecWireLen;
}

/// Writes topicWriteSpecWireLen(w) bytes; 0 when `out` is too small.
inline std::size_t encodeTopicWriteSpec(const TopicWriteSpecWire& w, std::span<uint8_t> out) noexcept {
    const std::size_t len = topicWriteSpecWireLen(w);
    if (out.size() < len) return 0;
    std::memcpy(out.data(), &w.topicId, 4);
    std::memcpy(out.data() + 4, &w.keyFieldId, 2);
    out[6] = static_cast<uint8_t>(w.keyType);
    out[7] = w.keyWidth;
    out[8] = static_cast<uint8_t>((w.legacyValidate ? 1 : 0) | (w.keyIndex ? 2 : 0) | (w.autoIncrement ? 4 : 0));
    if (w.autoIncrement) {
        out[9] = static_cast<uint8_t>(w.partitioner);
        std::memcpy(out.data() + 10, &w.autoIncrementMax, 8);
        std::memcpy(out.data() + 18, &w.autoIncrementReserve, 4);
        std::memcpy(out.data() + 22, &w.partitionCount, 4);
    }
    return len;
}

[[nodiscard]] inline std::optional<TopicWriteSpecWire> decodeTopicWriteSpec(std::span<const uint8_t> in) noexcept {
    if (in.size() != kTopicWriteSpecWireLen && in.size() != kTopicWriteSpecAutoIncWireLen) return std::nullopt;
    const bool autoInc = (in[8] & 4) != 0;
    if ((in[8] & 0xF8) != 0 || autoInc != (in.size() == kTopicWriteSpecAutoIncWireLen)) return std::nullopt;
    TopicWriteSpecWire w;
    std::memcpy(&w.topicId, in.data(), 4);
    std::memcpy(&w.keyFieldId, in.data() + 4, 2);
    w.keyType        = static_cast<rune::codec::PrimType>(in[6]);
    w.keyWidth       = in[7];
    w.legacyValidate = (in[8] & 1) != 0;
    w.keyIndex       = (in[8] & 2) != 0;
    if (autoInc) {
        if (in[9] > static_cast<uint8_t>(rune::protocol::Partitioner::Modulo)) return std::nullopt;
        w.autoIncrement = true;
        w.partitioner   = static_cast<rune::protocol::Partitioner>(in[9]);
        std::memcpy(&w.autoIncrementMax, in.data() + 10, 8);
        std::memcpy(&w.autoIncrementReserve, in.data() + 18, 4);
        std::memcpy(&w.partitionCount, in.data() + 22, 4);
    }
    return w;
}

/// Cold: nullopt for an untyped topic.  The key field is the live field named by the catalog's keyField.
[[nodiscard]] inline std::optional<TopicWriteSpecWire> writeSpecWireOf(const rune::catalog::TopicObject& t) {
    if (!t.schema) return std::nullopt;
    TopicWriteSpecWire w;
    w.topicId        = t.topicId;
    w.legacyValidate = t.schema->validate;
    w.keyIndex       = t.schema->keyFeatures.keyIndex;
    if (const auto& f = t.schema->keyFeatures; f.autoIncrement) {
        w.autoIncrement        = true;
        w.partitioner          = f.partitioner;
        w.autoIncrementMax     = f.autoIncrementMax;
        w.autoIncrementReserve = f.autoIncrementReserve;
        w.partitionCount       = t.partitions;
    }
    if (t.schema->keyField.empty()) return w;
    const rune::codec::Descriptor d = rune::codec::deserialize(t.schema->evolvingBlob);
    for (const auto& f : d.fields) {
        if (f.untilVersion != 0xFFFF || f.name != t.schema->keyField) continue;
        w.keyFieldId = f.id;
        w.keyType    = f.type;
        w.keyWidth   = rune::codec::isTopicKeyType(f.type, rune::codec::fieldWidth(f)) ? static_cast<uint8_t>(rune::codec::fieldWidth(f)) : 0;
        break;
    }
    return w;
}

struct SpecVersion {
    rune::codec::Validator  validator;
    rune::codec::Descriptor descriptor;
    uint32_t                keyValueOffset = kNoKeyOffset;   ///< record byte offset of the key field's value (4 + its block offset)
};

class TopicWriteSpec {
public:
    int32_t               topicId        = -1;
    uint16_t              keyFieldId     = kNoKeyField;
    rune::codec::PrimType keyType        = rune::codec::PrimType::Int32;
    uint8_t               keyWidth       = 0;
    bool                  legacyValidate = false;
    bool                  keyIndex       = false;
    bool                  autoIncrement  = false;
    rune::protocol::Partitioner partitioner = rune::protocol::Partitioner::Fnv1a;
    uint64_t              autoIncrementMax     = rune::protocol::kAutoIncrementMaxDefault;
    uint32_t              autoIncrementReserve = rune::protocol::kAutoIncrementReserveDefault;
    int32_t               partitionCount = 0;
    /// The topic's evolving descriptor (every field ever declared), from the FilterSchema op; the key index decodes each writer version through it.
    std::optional<rune::codec::Descriptor> evolving;

    [[nodiscard]] const SpecVersion* versionFor(uint16_t version) const noexcept {
        return version < versions_.size() ? versions_[version].get() : nullptr;
    }
    [[nodiscard]] const rune::codec::Validator* validatorFor(uint16_t version) const noexcept {
        const SpecVersion* sv = versionFor(version);
        return sv != nullptr ? &sv->validator : nullptr;
    }
    [[nodiscard]] const rune::codec::Descriptor* descriptorFor(uint16_t version) const noexcept {
        const SpecVersion* sv = versionFor(version);
        return sv != nullptr ? &sv->descriptor : nullptr;
    }
    [[nodiscard]] uint16_t maxVersion() const noexcept { return maxVersion_; }
    [[nodiscard]] bool hasFacts() const noexcept { return hasFacts_; }

    /// Cold.  Installs (or replaces) one schema version's validator and descriptor.
    void installVersion(uint16_t version, rune::codec::Descriptor materialized) {
        if (versions_.size() <= version) versions_.resize(static_cast<std::size_t>(version) + 1);
        auto sv = std::make_unique<SpecVersion>(SpecVersion{rune::codec::Validator(materialized), std::move(materialized), kNoKeyOffset});
        setKeyOffset_(*sv);
        versions_[version] = std::move(sv);
        if (version > maxVersion_) maxVersion_ = version;
    }

    /// Cold.  Takes the key facts and recomputes every installed version's key offset.
    void setFacts(const TopicWriteSpecWire& w) {
        topicId        = w.topicId;
        keyFieldId     = w.keyFieldId;
        keyType        = w.keyType;
        keyWidth       = w.keyWidth;
        legacyValidate = w.legacyValidate;
        keyIndex       = w.keyIndex;
        autoIncrement  = w.autoIncrement;
        partitioner    = w.partitioner;
        autoIncrementMax     = w.autoIncrementMax;
        autoIncrementReserve = w.autoIncrementReserve;
        partitionCount = w.partitionCount;
        hasFacts_      = true;
        for (auto& sv : versions_)
            if (sv) setKeyOffset_(*sv);
    }

private:
    void setKeyOffset_(SpecVersion& sv) const noexcept {
        sv.keyValueOffset = kNoKeyOffset;
        if (!hasFacts_ || keyFieldId == kNoKeyField) return;
        for (const auto& f : sv.descriptor.fields)
            if (f.id == keyFieldId) { sv.keyValueOffset = 4 + f.offset; return; }
    }

    std::vector<std::unique_ptr<SpecVersion>> versions_;   // index = schema version; grown only by installVersion
    uint16_t maxVersion_ = 0;
    bool     hasFacts_   = false;
};

/// One owned typed partition's write state, next to RcSlot.  Later parts append their per-partition members here.
class VerdictSource;

/// Part 11: a transaction on a checked-writes source, from its TxnJudge record to its marker.
enum class TxnPhase : uint8_t { Scanning, AwaitJudge, Deferred, Judging, Locked, Finalized };
struct TxnVoteAnswer { uint16_t code = 0; uint32_t constraintId = 0; int64_t failingOffset = -1; };
struct TxnJudgeStats {
    uint64_t votesYes = 0, votesNo = 0, votesNoConstraint = 0, votesNoKeyExists = 0, votesNoKeyNotFound = 0, votesNoTooLarge = 0,
             votesNoConflict = 0, voteLatencyNsMax = 0, voteLatencyNsTotal = 0, foldPausedOnTxnLockNs = 0, markerWithoutVote = 0,
             txnOverlayWaits = 0, lockPauses = 0, checkpointBackfills = 0, vetoedCheckpointPushes = 0,
             judgeRecordRefusals = 0,   ///< a TxnJudge the table refused (full, or the producer's entry holds an overlay): the judge waits
             ordinaryWaits = 0,         ///< an ordinary transaction the judge could not track (its table full): the judge waits
             supersedeKeysOverflow = 0, ///< an ordinary transaction with more keys than the one-step set holds: applied in segments, one per pass
             undecidedReplaceRefusals = 0, ///< a producer's next TxnJudge refused while its previous transaction has no marker
             joinerTxnWaits = 0,        ///< a joiner's set switch waits at an undecided J (D3; bounded by the transaction timeout)
             atomicTooLarge = 0;        ///< an atomic batch past txnJudgedRecordsMax: every record rejected (62); produce refuses it (rule 10), so this counts replicated, pre-upgrade and larger-knob leaders' batches
};
struct DeferredTxn {
    int64_t     producerId = -1;
    int16_t     epoch = -1;
    int64_t     firstOffset = -1, lastOffset = -1, judgeOffset = -1, markerOffset = -1;
    bool        commit = false;
    int32_t     records = 0;
    bool        explicitIntent = false;
    TxnPhase    phase = TxnPhase::Scanning;
    TxnVoteAnswer answer{};
    bool        judged = false;   ///< `answer` is the judge's, not a default
    int32_t     overlay = -1;
    int64_t     voteReceivedNs = 0;
    int64_t     holdFrom = INT64_MAX;   ///< below firstOffset: an applied ordinary span holds it (the coverage never splits that span)
};
/// `judgeOffset`: the J the vote waits for; -1 while the worker's vote-time scan runs (only the worker answers those).
struct ParkedVote { int64_t token = 0; int32_t originNode = -1; int64_t producerId = -1; int16_t epoch = -1; int64_t judgeOffset = -1; int64_t receivedNs = 0; };

/// The transactions of one source between their TxnJudge record and their marker, in judge-offset order.  Everything is reserved
/// by reserve(); no call allocates after it.  A full table refuses the record (nullptr): the judge waits there, never a verdict.
class TxnJudgeState {
public:
    static constexpr std::size_t kMaxTxns = 1024, kMaxParkedVotes = 256;

    void reserve() {   // cold
        txns_.reserve(kMaxTxns);
        parked_.reserve(kMaxParkedVotes);
    }
    [[nodiscard]] bool reserved() const noexcept { return txns_.capacity() >= kMaxTxns; }

    /// The latest entry of the producer.
    [[nodiscard]] DeferredTxn* find(int64_t producerId) noexcept {
        for (std::size_t i = txns_.size(); i-- > 0;)
            if (txns_[i].producerId == producerId) return &txns_[i];
        return nullptr;
    }
    [[nodiscard]] const DeferredTxn* find(int64_t producerId) const noexcept { return const_cast<TxnJudgeState*>(this)->find(producerId); }
    /// A producer's next transaction replaces its previous entry; a full table gives up its oldest Finalized one.  nullptr: the judge
    /// waits (the previous entry holds an overlay or has no marker yet, or every entry is live).  A record at or below the producer's
    /// entry is a re-feed.
    DeferredTxn* onJudgeRecord(int64_t judgeOffset, const rune::storage::control_record::TxnJudgeRecord& r) noexcept {
        if (DeferredTxn* old = find(r.producerId)) {
            if (old->judgeOffset >= judgeOffset) return old;
            if (old->overlay >= 0) return nullptr;
            if (old->markerOffset < 0) {   // undecided: dropping it would let K pass a transaction nothing applies
                ++stats.undecidedReplaceRefusals;
                return nullptr;
            }
            giveUp_(txns_.begin() + (old - txns_.data()));
        }
        if (txns_.size() >= kMaxTxns) {
            const auto done = std::find_if(txns_.begin(), txns_.end(), [&](const DeferredTxn& t) { return t.phase == TxnPhase::Finalized && t.overlay < 0 && !hasParked_(t.producerId); });
            if (done == txns_.end()) return nullptr;
            giveUp_(done);
        }
        DeferredTxn& t = txns_.emplace_back();
        t.producerId = r.producerId;
        t.epoch = r.producerEpoch;
        t.firstOffset = r.firstOffset;
        t.judgeOffset = judgeOffset;
        t.phase = TxnPhase::AwaitJudge;
        return &t;
    }
    /// Records the marker; only the judge moves the phase.
    void onMarker(int64_t producerId, int64_t offset, bool commit) noexcept {
        DeferredTxn* t = find(producerId);
        if (t == nullptr || t->markerOffset >= 0 || offset < t->judgeOffset) return;   // below the J: the producer's earlier transaction
        t->markerOffset = offset;
        t->commit = commit;
    }
    /// Leadership scan begin: nothing at or above `offset` is known any more, and the entries kept are judged anew.  The owner released
    /// every overlay of this state first.
    void truncateFrom(int64_t offset) noexcept {
        while (!txns_.empty() && txns_.back().judgeOffset >= offset) txns_.pop_back();
        for (DeferredTxn& t : txns_) {
            // Judged and decided below the cut: the judgement is the log's (deterministic), so a vote parked on it keeps its answer.
            if (t.phase == TxnPhase::Finalized && t.judged && t.markerOffset >= 0 && t.markerOffset < offset) continue;
            if (t.markerOffset >= offset) {
                t.markerOffset = -1;
                t.commit = false;
            }
            t.phase = TxnPhase::AwaitJudge;
            t.overlay = -1;
            t.answer = {};
            t.judged = false;
            t.records = 0;
            t.holdFrom = INT64_MAX;   // the old span's hold: the judge sets it again if it applies the span again
        }
    }
    /// The owner gives back every overlay an entry holds: before a leadership scan (truncateFrom) or a re-activation.
    template <class Fn>
    void releaseOverlays(Fn&& release) noexcept {
        for (DeferredTxn& t : txns_)
            if (t.overlay >= 0) {
                release(t.overlay);
                t.overlay = -1;
            }
    }
    /// An entry truncateFrom reset whose transaction lies below `covered`, the markers' coverage, with its marker known: a judge
    /// before this one applied it (K never passes a transaction that is not Finalized), so nothing is left to do.  Not from the
    /// judge's cursor: a reset Locked entry with J < cursor <= M still owes its apply (T5 N1).
    void finalizeDecided(int64_t covered) noexcept {
        for (DeferredTxn& t : txns_)
            if (t.phase == TxnPhase::AwaitJudge && t.markerOffset >= 0 && t.firstOffset < covered) t.phase = TxnPhase::Finalized;
    }
    /// The first Locked entry whose marker is below `hw`: the judge finalizes it (P11-4).
    [[nodiscard]] DeferredTxn* lockedDecided(int64_t hw) noexcept {
        for (DeferredTxn& t : txns_)
            if (t.phase == TxnPhase::Locked && t.markerOffset >= 0 && t.markerOffset < hw) return &t;
        return nullptr;
    }
    /// The first offset of the earliest entry not Finalized (INT64_MAX = none): K stays below it.  A Locked entry with its marker
    /// is not applied yet, so a marker alone decides nothing here.
    [[nodiscard]] int64_t earliestUndecidedFirst() const noexcept {
        int64_t first = INT64_MAX;
        for (const DeferredTxn& t : txns_)
            if (t.phase != TxnPhase::Finalized) first = std::min({first, t.firstOffset, t.holdFrom});
        return first;
    }
    /// An ordinary transaction [first, marker] was applied: an undecided entry holding K inside (first, marker] holds it at `first`.
    void holdBelowSpan(int64_t first, int64_t marker) noexcept {
        for (DeferredTxn& t : txns_)
            if (const int64_t at = std::min(t.firstOffset, t.holdFrom); t.phase != TxnPhase::Finalized && first < at && at <= marker) t.holdFrom = first;
    }
    /// onJudgeRecord would take a new record of the producer now: its entry (if any) holds no overlay and has its marker, and the
    /// table has room.  The worker appends a TxnJudge only then, so the judge never waits on a record the worker wrote.
    [[nodiscard]] bool acceptsJudge(int64_t producerId) const noexcept {
        for (std::size_t i = txns_.size(); i-- > 0;)
            if (txns_[i].producerId == producerId) return txns_[i].overlay < 0 && txns_[i].markerOffset >= 0;
        return txns_.size() < kMaxTxns ||
               std::any_of(txns_.begin(), txns_.end(), [&](const DeferredTxn& t) { return t.phase == TxnPhase::Finalized && t.overlay < 0 && !hasParked_(t.producerId); });
    }
    /// A Locked transaction judged below `cursor`: a member folded past it holds rows its apply has not changed yet (P11-8).
    [[nodiscard]] bool lockedBelow(int64_t cursor) const noexcept {
        return std::any_of(txns_.begin(), txns_.end(), [&](const DeferredTxn& t) { return t.phase == TxnPhase::Locked && t.judgeOffset < cursor; });
    }
    /// The lowest TxnJudge a judge may still read (the cleaner keeps it and every one above): `w`, the committed markers' coverage,
    /// or the J of an entry whose transaction is not decided below `w`, including one the table gave up.
    [[nodiscard]] int64_t judgeRecordsNeededFrom(int64_t w) noexcept {
        if (givenUpMarker_ < w) {   // every entry given up is decided below w
            givenUpJ_ = INT64_MAX;
            givenUpMarker_ = -1;
        }
        int64_t from = std::min(w, givenUpJ_);
        for (const DeferredTxn& t : txns_)
            if (t.phase != TxnPhase::Finalized || t.markerOffset < 0 || t.markerOffset >= w) from = std::min(from, t.judgeOffset);
        return from;
    }
    /// False: full, the caller answers NotLeader.
    [[nodiscard]] bool parkVote(const ParkedVote& v) noexcept {
        if (parked_.size() >= kMaxParkedVotes) return false;
        parked_.push_back(v);
        return true;
    }
    /// fn(const ParkedVote&, const DeferredTxn*) for every parked vote bound to a J that has an answer: the judge's (Locked, or
    /// Finalized by a judgement), or none (nullptr: the entry of that J is gone, replaced, or Finalized without a judgement), which
    /// the caller answers NotLeader, never with an unjudged default.  Those votes leave the park; a scanning vote (J -1) stays.
    template <class Fn>
    int drainAnswered(Fn&& fn) noexcept {
        int n = 0;
        std::size_t kept = 0;
        for (std::size_t i = 0; i < parked_.size(); ++i) {
            const ParkedVote& v = parked_[i];
            const DeferredTxn* t = v.judgeOffset < 0 ? nullptr : find(v.producerId);
            const bool gone = v.judgeOffset >= 0 && (t == nullptr || t->judgeOffset != v.judgeOffset || (t->phase == TxnPhase::Finalized && !t->judged));
            if (gone) {
                fn(v, static_cast<const DeferredTxn*>(nullptr));
                ++n;
            } else if (t != nullptr && t->judged && (t->phase == TxnPhase::Locked || t->phase == TxnPhase::Finalized)) {
                fn(v, t);
                ++n;
            } else {
                parked_[kept++] = v;
            }
        }
        parked_.resize(kept);
        return n;
    }
    /// fn(const ParkedVote&) for the producer's scanning votes (J -1), which leave the park; `bindTo` >= 0 binds them to that J instead.
    template <class Fn>
    int takeScanning(int64_t producerId, int64_t bindTo, Fn&& fn) noexcept {
        int n = 0;
        std::size_t kept = 0;
        for (std::size_t i = 0; i < parked_.size(); ++i) {
            ParkedVote& v = parked_[i];
            if (v.producerId == producerId && v.judgeOffset < 0) {
                ++n;
                if (bindTo >= 0) v.judgeOffset = bindTo;
                else { fn(v); continue; }
            }
            parked_[kept++] = v;
        }
        parked_.resize(kept);
        return n;
    }
    /// Demotion: fn(const ParkedVote&) for every parked vote, and the park empties.
    template <class Fn>
    void dropParked(Fn&& fn) noexcept {
        for (const ParkedVote& v : parked_) fn(v);
        parked_.clear();
    }
    [[nodiscard]] std::size_t parkedCount() const noexcept { return parked_.size(); }
    [[nodiscard]] std::size_t lockedCount() const noexcept {
        std::size_t n = 0;
        for (const DeferredTxn& t : txns_) n += t.phase == TxnPhase::Locked;
        return n;
    }

    TxnJudgeStats stats;

private:
    [[nodiscard]] bool hasParked_(int64_t producerId) const noexcept {
        return std::any_of(parked_.begin(), parked_.end(), [&](const ParkedVote& v) { return v.producerId == producerId; });
    }

    void giveUp_(std::vector<DeferredTxn>::iterator t) noexcept {
        givenUpJ_ = std::min(givenUpJ_, t->judgeOffset);
        givenUpMarker_ = std::max(givenUpMarker_, t->markerOffset < 0 ? INT64_MAX : t->markerOffset);
        txns_.erase(t);
    }

    std::vector<DeferredTxn> txns_;
    std::vector<ParkedVote>  parked_;
    int64_t                  givenUpJ_ = INT64_MAX, givenUpMarker_ = -1;   ///< the entries replaced or evicted: their lowest J, latest marker
};

/// The rejected index's ceiling (P4-11).
inline constexpr std::size_t kIndexMax = std::size_t{1} << 24;

struct PartitionWriteState {
    const TopicWriteSpec*              spec = nullptr;
    int32_t                            partition = -1;       ///< the partition this state is of (its replica may be gone first)
    rune::storage::RejectedOffsetIndex rejected;       ///< reserved when the partition becomes a verdict source (VerdictSource.cpp)
    ConstraintSetTimeline              constraintSets;
    MarkerLedger                       markers;
    TxnJudgeState                      txn;                  ///< Part 11: the transactions between their TxnJudge record and their marker
    RetainedVerdictPlans               retainedPlans;        ///< the type-20 records of the log (P9-8)
    VerdictSource*                     verdicts = nullptr;   ///< the judge, once this worker leads and hosts verdict views
    int64_t                            judgedThrough = 0;    ///< the judge's cursor, published at the end of each of its passes
    uint64_t                           mismatchesLogged = 0; ///< rejected-index determinism alarms already reported
    uint64_t                           seenLogGeneration = UINT64_MAX;   ///< the replica's log generation the timeline and ledger are in step with
    int64_t                            scanHw = 0;           ///< the replica HW at the start of this pass: the scan feed commits ledger slots below it
    bool                               growOwed = false;     ///< the judge saw the rejected index cross 75 %, or stalled for room: the worker's cold maintenance grows it
    std::size_t                        rejectedMax = kIndexMax;   ///< the rejected index's ceiling (a rig lowers it)
    std::size_t                        growNeed = 0;        ///< the size a stalled whole-batch rejection needs (size + its records); 0 when none
    AutoIncState                       autoInc;              ///< the partition's id counter (an autoincrement topic only)
    AssignedBaseWindow                 assignedBases;        ///< the last assigned bases per producer, for retried key-0 batches
    AutoIncStats                       autoIncStats;

    /// Cold: doubles the rejected index (up to rejectedMax) once it is 75 % full, or grows it to growNeed when a whole-batch rejection waits for room.
    void growRejected() {
        const std::size_t cap = rejected.capacity();
        const std::size_t want = growNeed != 0 ? std::max(cap * 2, growNeed) : rejected.size() * 4 >= cap * 3 ? cap * 2 : 0;
        growOwed = false;
        growNeed = 0;
        if (cap != 0 && want > cap) rejected.grow(std::min(want, rejectedMax));
    }
    /// A judge is attached or the latest logged set is non-empty.
    [[nodiscard]] bool isVerdictSource() const noexcept;
    /// The latest logged set has a member that can reject (Unique, Check or TestReject).
    [[nodiscard]] bool isConstrainedSource() const noexcept;
    /// Every offset below it has a verdict this node knows; INT64_MAX when no offset needs one.
    [[nodiscard]] int64_t verdictKnownThrough() const noexcept {
        if (constraintSets.size() == 0) return INT64_MAX;
        if (verdicts != nullptr) return judgedThrough;
        const int64_t k = rejected.coveredThrough();
        return constraintSets.latest().empty() && k > constraintSets.lastChangeOffset() ? INT64_MAX : k;
    }
};

enum class TypedCheck : uint8_t { Accept, Reject, VersionAhead };

namespace detail {
[[nodiscard]] inline uint16_t le16(const char* p) noexcept {
    return static_cast<uint16_t>(static_cast<uint8_t>(p[0]) | (static_cast<uint8_t>(p[1]) << 8));
}
}  // namespace detail

/// The one per-record rule set.  `level` is the cluster metadata feature level: at kCheckedWrites every typed topic
/// validates fully (UTF-8, wire key = key field); below it only a topic created with `validate` checks, as before.
/// A tombstone (empty or null value) is never decoded: its key must be the key field's width.  A record of a version
/// above the highest installed one is VersionAhead (retriable: the version's SchemaUpdate has not landed here yet).
[[nodiscard]] inline TypedCheck checkTypedRecord(const TopicWriteSpec& spec, const rune::protocol::RecordSpan& r, int16_t level) noexcept {
    const bool checked = level >= rune::replication::MetadataFeature::kCheckedWrites;
    if (!checked && !spec.legacyValidate) return TypedCheck::Accept;
    const bool keyRule = checked && spec.keyFieldId != kNoKeyField && spec.keyWidth != 0;
    if (r.value.empty()) {
        if (spec.keyFieldId == kNoKeyField) return TypedCheck::Reject;
        return (keyRule && r.key.size() != spec.keyWidth) ? TypedCheck::Reject : TypedCheck::Accept;
    }
    if (r.value.size() < 4) return TypedCheck::Reject;
    const uint16_t v = detail::le16(r.value.data() + 2);
    const SpecVersion* sv = spec.versionFor(v);
    if (sv == nullptr) return v > spec.maxVersion() ? TypedCheck::VersionAhead : TypedCheck::Reject;
    const auto check = checked ? rune::codec::StringCheck::Utf8 : rune::codec::StringCheck::None;
    if (!sv->validator.validate({reinterpret_cast<const uint8_t*>(r.value.data()), r.value.size()}, check).ok) return TypedCheck::Reject;
    if (keyRule && (sv->keyValueOffset == kNoKeyOffset || r.key.size() != spec.keyWidth ||
                    std::memcmp(r.key.data(), r.value.data() + sv->keyValueOffset, spec.keyWidth) != 0))
        return TypedCheck::Reject;
    return TypedCheck::Accept;
}

/// The bounded walk over checkTypedRecord: the first record that is not accepted decides; a batch whose framing does
/// not fit is Reject.  Below the level a topic without `validate` is not walked at all.
[[nodiscard]] inline TypedCheck checkTypedBatch(const TopicWriteSpec& spec, std::span<const uint8_t> batch, int16_t level) noexcept {
    if (level < rune::replication::MetadataFeature::kCheckedWrites && !spec.legacyValidate) return TypedCheck::Accept;
    TypedCheck result = TypedCheck::Accept;
    const bool walked = rune::protocol::forEachRecordSpan(batch.data(), batch.size(), [&](const rune::protocol::RecordSpan& r) {
        if (result == TypedCheck::Accept) result = checkTypedRecord(spec, r, level);
    });
    return walked ? result : TypedCheck::Reject;
}

}  // namespace rune::node
