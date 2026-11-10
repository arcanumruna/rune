#pragma once
// CheckedWritesModel.h - the reference judge of the checked-writes matrix and the model checks.  Written independently of
// VerdictSource: it reads the data records, the constraint-set change records, the TxnJudge records and the COMMIT / ABORT markers of a log and judges them in
// apply order (ascending offsets, or a committed transaction moved to its marker, P11-0) with its own copy of the TestReject rules.  Every non-rejected record reaches every member's table (a joining member builds from the
// start of the log unjudged), so all members' live keys are the live keys of the accepted records.
#include "CheckedWritesRig.h"

#include <rune/codec/Record.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/view/ViewPlan.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

namespace rune::node::cw {

struct ModelVerdict {
    uint16_t code = 0;
    uint32_t constraintId = 0;
    bool operator==(const ModelVerdict&) const = default;
};

/// A transaction's answer: the vote at its TxnJudge (P11-1), or the judgement at its commit marker when it has none (P11-7).
struct ModelVote {
    uint16_t code = 0;
    uint32_t constraintId = 0;
    int64_t failingOffset = -1;
    int64_t judgeOffset = -1;   ///< -1 without a TxnJudge
};

/// One transaction of the log (a producer's records from its first to its marker).  `vote` is set when the model judged it whole.
struct ModelTxn {
    int64_t pid = -1, first = -1, judge = -1, marker = -1;   ///< judge, marker: -1 when the log has none
    int64_t appliedAt = -1;   ///< the judge position it applied at: its marker, or the offset of the unit that met its locks first
    bool commit = false, applied = false;
    std::optional<ModelVote> vote;
    std::vector<int64_t> offsets;   ///< its records, ascending
};

class CheckedWritesModel {
public:
    using LogRec = VerdictRig::RigLogRec;
    static constexpr uint16_t kConstraintViolation = 55, kKeyExists = 56, kKeyNotFound = 57, kTxnConflict = 63;   // ErrorCode
    using RigChange = VerdictRig::RigChange;
    static constexpr uint8_t kUnique = static_cast<uint8_t>(rune::view::VerdictViewKind::Unique), kCheck = static_cast<uint8_t>(rune::view::VerdictViewKind::Check), kTestReject = static_cast<uint8_t>(rune::view::VerdictViewKind::TestReject);   // a change member's kind

    /// The model's own copy of a TestReject rule.
    struct Rule {
        enum class Mode : uint8_t { TagEquals, OffsetSet, CapLiveKeys };
        Mode mode = Mode::TagEquals;
        uint8_t tag = 0;
        uint32_t cap = 0;
        std::vector<int64_t> offsets;
    };
    static Rule tagEquals(uint8_t tag) { Rule r; r.mode = Rule::Mode::TagEquals; r.tag = tag; return r; }
    static Rule capLiveKeys(uint32_t cap) { Rule r; r.mode = Rule::Mode::CapLiveKeys; r.cap = cap; return r; }
    static Rule offsetSet(std::vector<int64_t> o) { Rule r; r.mode = Rule::Mode::OffsetSet; r.offsets = std::move(o); return r; }

    /// A change record names a member by (object id, plan fingerprint); the fingerprint of a test rule is what links it to `rule`.
    void declareRule(uint64_t viewObjectId, uint64_t fingerprint, Rule rule) { rules_[{viewObjectId, fingerprint}] = std::move(rule); }

    /// Extension point (05 key index, 06 ids, 09 unique/check, 11 transactions): test-only, std::function is allowed here.
    using RuleFn = std::function<std::optional<ModelVerdict>(const LogRec&, const CheckedWritesModel&)>;
    void addRule(uint64_t viewObjectId, RuleFn fn) { extra_[viewObjectId] = std::move(fn); }

    /// The key index (object id 0): the P5-3 table, from whether the key is live among the accepted records before the judged one.
    void addKeyIndexRule() {
        addRule(0, [](const LogRec& rec, const CheckedWritesModel& m) -> std::optional<ModelVerdict> {
            const bool live = m.isLiveAtJudge(rec.id);
            if (!rec.del && rec.intent == 1 && live) return ModelVerdict{kKeyExists, 0};
            if (rec.intent == 2 && !live) return ModelVerdict{kKeyNotFound, 0};
            return std::nullopt;
        });
    }
    /// An OK ack of a key-0 batch: its `count` records sit at baseOffset, baseOffset + 1, ... and carry the ids assignedBase + i * P.
    struct AutoIncAck { int64_t baseOffset; uint64_t assignedBase; std::size_t count; };

    /// An autoincrement source (06): the key index rule (an assigned id is fresh, an explicit one meets the key index like any insert), and the shape the
    /// id predicates below judge against.  The rig owns `partition` of `shape.partitions`.
    void addAutoIncRule(const AutoIncShape& shape, int32_t partition) {
        addKeyIndexRule();
        partitions_ = static_cast<uint64_t>(shape.partitions);
        partition_ = static_cast<uint64_t>(partition);
    }
    /// NoIdTwiceNoReuse: no id is in two acks, and no id is in two of the key-0 records the acks name.
    [[nodiscard]] bool noIdTwiceNoReuse(const VerdictRig::RigLog& log, const std::vector<AutoIncAck>& acks) const {
        std::set<uint64_t> acked, stored;
        for (const AutoIncAck& a : acks)
            for (std::size_t i = 0; i < a.count; ++i) {
                if (!acked.insert(a.assignedBase + i * partitions_).second) return false;
                if (const LogRec* r = recordAt_(log, a.baseOffset + static_cast<int64_t>(i)); r != nullptr && !stored.insert(r->id).second) return false;
            }
        return true;
    }
    /// IdsFollowTheStride: a key-0 batch's keys are base + i * P, with base % P == p.
    [[nodiscard]] bool idsFollowTheStride(const VerdictRig::RigLog& log, const std::vector<AutoIncAck>& acks) const {
        for (const AutoIncAck& a : acks) {
            if (a.assignedBase % partitions_ != partition_) return false;
            for (std::size_t i = 0; i < a.count; ++i)
                if (const LogRec* r = recordAt_(log, a.baseOffset + static_cast<int64_t>(i)); r != nullptr && r->id != a.assignedBase + i * partitions_) return false;
        }
        return true;
    }
    /// ReservationCoversEveryId: the n of every logged id is within the reservation of a kReservation record at a lower offset.
    [[nodiscard]] bool reservationCoversEveryId(const VerdictRig::RigLog& log) const {
        for (const LogRec& d : log.data) {
            uint64_t covered = 0;
            for (const auto& [offset, reservedThroughN] : log.reservations)
                if (offset < d.offset) covered = std::max(covered, reservedThroughN);
            if (d.id / partitions_ > covered) return false;
        }
        return true;
    }
    /// AckedIdsMatchTheLog: the record at an ack's base offset has the ack's assignedBase for its key, and every value carries its own key.
    [[nodiscard]] bool ackedIdsMatchTheLog(const VerdictRig::RigLog& log, const std::vector<AutoIncAck>& acks) const {
        for (const AutoIncAck& a : acks) {
            const LogRec* first = recordAt_(log, a.baseOffset);
            if (first == nullptr || first->id != a.assignedBase) return false;
            for (std::size_t i = 0; i < a.count; ++i)
                if (const LogRec* r = recordAt_(log, a.baseOffset + static_cast<int64_t>(i)); r == nullptr || r->valueId != r->id) return false;
        }
        return true;
    }
    /// CounterAboveEveryId: after an activation the counter is above the n of every logged id, and the reservation reaches the last id the counter handed out.
    [[nodiscard]] bool counterAboveEveryId(const VerdictRig::RigLog& log, const AutoIncReady& counter) const {
        for (const LogRec& d : log.data)
            if (d.id / partitions_ >= counter.nextN) return false;
        return counter.reservedThroughN + 1 >= counter.nextN;
    }

    /// Part 09: the unique view over lower(name) (ordinal 0 of `viewObjectId`) and the count check over the user's characters (ordinal 1 of
    /// `viewObjectId`, `count(*) <= cap`).  Both read the accepted records of the log only: claimants per lowered name and the live characters are
    /// updated in offset order whether or not a member judges the record (unjudged below a member's change record, P9-3).
    void addLowerNameUniqueRule(uint64_t viewObjectId) { uniqueView_ = viewObjectId; }
    void addCountCheckRule(uint64_t viewObjectId, int64_t cap) { countView_ = viewObjectId; defaultCap_ = cap; }
    /// The cap of the count member with this plan fingerprint (a replace names a new one); other fingerprints use the cap of addCountCheckRule.
    void declareCountCap(uint64_t viewObjectId, uint64_t fingerprint, int64_t cap) { caps_[{viewObjectId, fingerprint}] = cap; }
    /// The lowered name -> the smallest live claimant.
    [[nodiscard]] std::map<std::string, uint64_t> uniqueOwners() const {
        std::map<std::string, uint64_t> out;
        for (const auto& [name, ids] : claimants_) out[name] = *ids.begin();
        return out;
    }
    /// The live characters that claim a name owned by a smaller key.
    [[nodiscard]] std::size_t uniqueExtras() const {
        std::size_t n = 0;
        for (const auto& [name, ids] : claimants_) n += ids.size() - 1;
        return n;
    }
    [[nodiscard]] std::map<uint64_t, int64_t> liveCountByUser() const {
        std::map<uint64_t, int64_t> out;
        for (const auto& [id, c] : characters_) ++out[c.user];
        return out;
    }

    /// The key's state while reset() judges: every accepted record before the judged one has been applied, in apply order.
    [[nodiscard]] bool isLiveAtJudge(uint64_t id) const { return live_.count(id) != 0; }

    [[nodiscard]] static uint32_t constraintIdOf(uint64_t viewObjectId) noexcept { return static_cast<uint32_t>((viewObjectId << 16) | 1u); }

    /// Judges `log` from scratch, walking it in offset order.  A plain record or atomic batch is judged at its offset.  A transaction is judged at its
    /// TxnJudge (P11-4) or, when it has none and the partition should have voted, at its commit marker (P11-7); it applies at its marker (P11-0), or earlier
    /// when a later write touches its locked rows.  Change records switch the constraint set.
    void reset(const VerdictRig::RigLog& log) {
        verdicts_.clear(); accepted_.clear(); latest_.clear(); live_.clear(); claimants_.clear(); characters_.clear();
        txns_.clear(); locked_.clear();
        judgeStop_ = INT64_MAX;
        collectTxns_(log);
        struct Event { int64_t offset; enum class Kind : uint8_t { Change, Data, Judge, Marker } kind; std::size_t at; };
        std::vector<Event> events;
        for (std::size_t i = 0; i < log.changes.size(); ++i) events.push_back({log.changes[i].offset, Event::Kind::Change, i});
        for (std::size_t i = 0; i < log.data.size(); ++i)
            if (!log.data[i].txn) events.push_back({log.data[i].offset, Event::Kind::Data, i});
        for (std::size_t i = 0; i < txns_.size(); ++i) {
            if (txns_[i].judge >= 0) events.push_back({txns_[i].judge, Event::Kind::Judge, i});
            if (txns_[i].marker >= 0) events.push_back({txns_[i].marker, Event::Kind::Marker, i});
        }
        std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) { return a.offset < b.offset; });
        std::vector<std::pair<uint64_t, uint64_t>> set;   // the current members, ascending object id
        std::size_t judgedData = 0;   // the data records below this index were judged with their atomic batch
        int64_t joinStop = INT64_MAX;
        for (const Event& e : events) {
            bool go = true;
            switch (e.kind) {
            case Event::Kind::Change: {
                std::vector<std::pair<uint64_t, uint64_t>> next;
                for (const auto& m : log.changes[e.at].members) next.emplace_back(m.viewObjectId, m.planFingerprint);
                std::sort(next.begin(), next.end());
                const bool joins = std::any_of(next.begin(), next.end(), [&](const auto& m) { return !std::binary_search(set.begin(), set.end(), m); });
                if (joins && std::any_of(txns_.begin(), txns_.end(), [&](const ModelTxn& t) { return t.judge >= 0 && t.judge < e.offset && t.marker < 0; }))
                    joinStop = std::min(joinStop, e.offset + 1);   // D3: a joining member's switch waits for every voted transaction without a marker
                set = std::move(next);
                break;
            }
            case Event::Kind::Data: {
                if (e.at < judgedData) break;
                judgedData = e.at + 1;
                const LogRec& first = log.data[e.at];
                while (first.atomic && judgedData < log.data.size() && log.data[judgedData].atomic && !log.data[judgedData].txn && log.data[judgedData].batchBase == first.batchBase) ++judgedData;
                go = judgeUnit_(log, set, e.at, judgedData);
                break;
            }
            case Event::Kind::Judge: go = judgeTxn_(log, set, e.at); break;
            case Event::Kind::Marker: go = endTxn_(log, set, e.at); break;
            }
            if (!go) break;
        }
        judgeStop_ = std::min(judgeStop_, joinStop);
        for (const ModelTxn& t : txns_)
            if (t.judge < 0 && t.marker < 0) judgeStop_ = std::min(judgeStop_, t.first);   // P11-2: the judge waits at an undecided transaction's first record
        for (bool moved = true; moved;) {   // a transaction without a TxnJudge is decided at its marker: past the stop, its records' verdicts are not made yet
            moved = false;
            for (const ModelTxn& t : txns_)
                if (t.judge < 0 && t.marker >= judgeStop_ && t.first < judgeStop_) judgeStop_ = t.first, moved = true;
        }
        std::sort(accepted_.begin(), accepted_.end(), [](const LogRec& a, const LogRec& b) { return a.offset < b.offset; });   // delivery is in offset order
    }

    [[nodiscard]] const std::vector<ModelTxn>& txns() const { return txns_; }
    /// The producer's latest transaction's answer (nullopt: none, or never judged whole).
    [[nodiscard]] std::optional<ModelVote> vote(int64_t pid) const {
        const auto t = std::find_if(txns_.rbegin(), txns_.rend(), [&](const ModelTxn& x) { return x.pid == pid; });
        return t == txns_.rend() ? std::nullopt : t->vote;
    }
    /// INT64_MAX when the whole log was judged; else the offset the verdicts hold below: an undecided transaction's first record, or the
    /// record that waits for a locked transaction's marker (P11-4).
    [[nodiscard]] int64_t judgeStop() const { return judgeStop_; }

    /// The verdict markers of `log` never cover past a transaction the judge could not have decided (P11-2): "" when they hold.  A marker
    /// appended before a transaction's marker ends at or before the transaction's first record, and no marker ends inside the span of a
    /// transaction without a TxnJudge (first, marker], where a restart would meet the marker with none of its records ahead.
    [[nodiscard]] std::string coverageProblem(const VerdictRig::RigLog& log) const {
        for (const auto& v : log.markers)
            for (const ModelTxn& t : txns_) {
                const int64_t through = v.header.finalThrough;
                if ((t.marker < 0 || v.offset < t.marker) && through > t.first)
                    return "the marker at " + std::to_string(v.offset) + " ends at " + std::to_string(through) + ", past the first record " + std::to_string(t.first) + " of producer " + std::to_string(t.pid) + "'s undecided transaction";
                if (t.judge < 0 && t.commit && t.first < through && through <= t.marker)
                    return "the marker at " + std::to_string(v.offset) + " ends at " + std::to_string(through) + ", inside the span " + std::to_string(t.first) + ".." + std::to_string(t.marker) + " of producer " + std::to_string(t.pid) + "'s transaction";
            }
        return "";
    }

    /// The rejected offsets only; an accepted record has no entry.
    [[nodiscard]] const std::map<int64_t, ModelVerdict>& verdicts() const { return verdicts_; }
    [[nodiscard]] const std::vector<LogRec>& acceptedRecords() const { return accepted_; }
    /// The latest accepted record of each key (a tombstone counts: the key is deleted).
    [[nodiscard]] const std::map<uint64_t, LogRec>& latestAcceptedByKey() const { return latest_; }
    [[nodiscard]] std::map<uint64_t, std::pair<int64_t, uint8_t>> projection() const {
        std::map<uint64_t, std::pair<int64_t, uint8_t>> out;
        for (const auto& [id, r] : live_) out[id] = {r.v, r.tag};
        return out;
    }
    [[nodiscard]] std::map<uint8_t, int64_t> countByTag() const {
        std::map<uint8_t, int64_t> out;
        for (const auto& [id, r] : live_) ++out[r.tag];
        return out;
    }
    /// count(*) grouped by the key itself: 1 for every live key (a group has one contributor).
    [[nodiscard]] std::map<uint64_t, int64_t> countById() const {
        std::map<uint64_t, int64_t> out;
        for (const auto& kv : live_) out[kv.first] = 1;
        return out;
    }
    [[nodiscard]] std::size_t liveKeys() const { return live_.size(); }

private:
    static const LogRec* recordAt_(const VerdictRig::RigLog& log, int64_t offset) {
        const auto it = std::lower_bound(log.data.begin(), log.data.end(), offset, [](const LogRec& r, int64_t o) { return r.offset < o; });
        return it != log.data.end() && it->offset == offset ? &*it : nullptr;
    }

    struct Chain {
        std::optional<ModelVerdict> verdict;   ///< the first failing record's
        int64_t failingOffset = -1;
        std::set<std::string> rows;   ///< the rows the records touch (P11-4 locks)
    };

    /// The producers' transactions, from the records, TxnJudge records and markers of `log` in offset order.
    void collectTxns_(const VerdictRig::RigLog& log) {
        struct Item { int64_t offset; enum class Kind : uint8_t { Record, Judge, Marker } kind; std::size_t at; };
        std::vector<Item> items;
        for (std::size_t i = 0; i < log.data.size(); ++i)
            if (log.data[i].txn) items.push_back({log.data[i].offset, Item::Kind::Record, i});
        for (std::size_t i = 0; i < log.judges.size(); ++i) items.push_back({log.judges[i].offset, Item::Kind::Judge, i});
        for (std::size_t i = 0; i < log.txnMarkers.size(); ++i) items.push_back({log.txnMarkers[i].offset, Item::Kind::Marker, i});
        std::sort(items.begin(), items.end(), [](const Item& a, const Item& b) { return a.offset < b.offset; });
        std::map<int64_t, std::size_t> open;   // producer -> its transaction without a marker yet
        for (const Item& it : items) {
            if (it.kind == Item::Kind::Record) {
                const LogRec& rec = log.data[it.at];
                const auto [at, fresh] = open.try_emplace(rec.producerId, txns_.size());
                if (fresh) txns_.push_back({.pid = rec.producerId, .first = rec.offset});
                txns_[at->second].offsets.push_back(rec.offset);
            } else if (it.kind == Item::Kind::Judge) {
                const auto& j = log.judges[it.at];
                const auto at = open.find(j.producerId);
                if (at != open.end() && txns_[at->second].judge < 0 && txns_[at->second].first == j.firstOffset) txns_[at->second].judge = j.offset;
            } else {
                const auto& m = log.txnMarkers[it.at];
                const auto at = open.find(m.producerId);
                if (at == open.end()) continue;
                txns_[at->second].marker = m.offset;
                txns_[at->second].commit = m.commit;
                open.erase(at);
            }
        }
    }

    [[nodiscard]] std::optional<ModelVerdict> firstVerdict_(const std::vector<std::pair<uint64_t, uint64_t>>& set, const LogRec& rec) const {
        for (const auto& [objectId, fingerprint] : set)
            if (const auto verdict = judge_(objectId, fingerprint, rec)) return verdict;
        return std::nullopt;
    }
    void apply_(const LogRec& rec) {
        latest_[rec.id] = rec;
        if (rec.del) live_.erase(rec.id); else live_[rec.id] = rec;
        if (uniqueView_ || countView_) applyCharacter_(rec);
    }
    /// The rows `rec` reads or writes now: its key, and with the unique and count views the lowered name and the user group, old and new.
    void addRows_(std::set<std::string>& rows, const LogRec& rec) const {
        rows.insert("k" + std::to_string(rec.id));
        if (const auto old = characters_.find(rec.id); old != characters_.end()) {
            if (uniqueView_) rows.insert("n" + old->second.lowered);
            if (countView_) rows.insert("g" + std::to_string(old->second.user));
        }
        if (rec.del) return;
        if (uniqueView_) rows.insert("n" + lowered_(rec.name));
        if (countView_) rows.insert("g" + std::to_string(rec.v));
    }
    /// `recs` judged in order, each against the records before it (a chained overlay); the model's state is left as it was.  `txn`: the
    /// records after the failing one are chained for their rows (a transaction's locks), else the chain ends there.
    Chain chain_(const std::vector<const LogRec*>& recs, const std::vector<std::pair<uint64_t, uint64_t>>& set, bool txn) {
        auto saved = std::make_tuple(latest_, live_, claimants_, characters_);
        Chain out;
        for (const LogRec* rec : recs) {
            addRows_(out.rows, *rec);
            if (!out.verdict && (out.verdict = firstVerdict_(set, *rec))) {
                out.failingOffset = rec->offset;
                if (!txn) break;
            }
            apply_(*rec);
        }
        std::tie(latest_, live_, claimants_, characters_) = std::move(saved);
        return out;
    }

    /// P11-4: a unit that touches the rows of a locked transaction waits for that transaction's marker, and is judged after the transaction's
    /// outcome.  False: the marker is not in the log; nothing from `offset` on is judged.
    bool settle_(const VerdictRig::RigLog& log, const std::set<std::string>& rows, int64_t offset) {
        for (std::size_t i = 0; i < locked_.size();) {
            if (std::none_of(rows.begin(), rows.end(), [&](const std::string& r) { return locked_[i].rows.count(r) != 0; })) { ++i; continue; }
            if (txns_[locked_[i].txn].marker < 0) {
                judgeStop_ = offset;
                return false;
            }
            finalize_(log, i, offset);
        }
        return true;
    }
    void finalize_(const VerdictRig::RigLog& log, std::size_t lockedAt, int64_t at) {
        const std::size_t t = locked_[lockedAt].txn;
        locked_.erase(locked_.begin() + static_cast<std::ptrdiff_t>(lockedAt));
        if (txns_[t].commit) applyTxn_(log, t, /*superseded=*/false, at);
    }
    /// A transaction's records applied at its marker, in offset order.  `superseded`: an ordinary transaction (P11-3a) applies nothing of a key whose
    /// latest accepted record is another writer's above the transaction's last on it; the record is still delivered.
    void applyTxn_(const VerdictRig::RigLog& log, std::size_t t, bool superseded, int64_t at) {
        ModelTxn& txn = txns_[t];
        txn.applied = true;
        txn.appliedAt = at;
        std::map<uint64_t, int64_t> last;
        for (const int64_t offset : txn.offsets) last[recordAt_(log, offset)->id] = offset;
        for (const int64_t offset : txn.offsets) {
            const LogRec& rec = *recordAt_(log, offset);
            accepted_.push_back(rec);
            if (const auto seen = latest_.find(rec.id); superseded && seen != latest_.end() && seen->second.offset > last.at(rec.id)) continue;
            apply_(rec);
        }
    }

    /// A plain record or an atomic batch (data[from, to)) at its offset.  False: the judge stopped (settle_).
    bool judgeUnit_(const VerdictRig::RigLog& log, const std::vector<std::pair<uint64_t, uint64_t>>& set, std::size_t from, std::size_t to) {
        std::vector<const LogRec*> recs;
        for (std::size_t i = from; i < to; ++i) recs.push_back(&log.data[i]);
        if (!locked_.empty()) {
            std::set<std::string> rows = chain_(recs, set, false).rows;
            if (!settle_(log, rows, recs.front()->offset)) return false;
        }
        if (recs.size() == 1) {
            if (const auto verdict = firstVerdict_(set, *recs.front())) {
                verdicts_[recs.front()->offset] = *verdict;
                return true;
            }
        } else if (const Chain judged = chain_(recs, set, false); judged.verdict) {
            for (const LogRec* rec : recs) verdicts_[rec->offset] = *judged.verdict;   // P11-6: the first failing record's verdict stands for the batch
            return true;
        }
        for (const LogRec* rec : recs) {
            accepted_.push_back(*rec);
            apply_(*rec);
        }
        return true;
    }

    /// Whether the other transaction's records are applied before `end` in judge order: committed, voted yes below `end`, or applied at a marker below it.
    /// A vote without its marker never gets here: its rows are locked, and settle_ stops the judge at the first unit that meets them.
    [[nodiscard]] static bool appliedBefore_(const ModelTxn& other, int64_t end) {
        if (other.marker < 0 || !other.commit) return false;
        return other.judge >= 0 ? other.judge < end && other.vote && other.vote->code == 0 : other.marker < end && other.applied;
    }
    /// P11-3: the offset of the first accepted record of another writer on a key of the transaction, above the transaction's last record on it and
    /// below `end`; -1 for none.
    [[nodiscard]] int64_t conflictBefore_(const VerdictRig::RigLog& log, const ModelTxn& txn, int64_t end) const {
        std::map<uint64_t, int64_t> last;
        for (const int64_t offset : txn.offsets) last[recordAt_(log, offset)->id] = offset;
        for (const LogRec& r : log.data) {
            if (r.offset <= txn.first || r.offset >= end || (r.txn && r.producerId == txn.pid)) continue;
            const auto key = last.find(r.id);
            if (key == last.end() || r.offset <= key->second || verdicts_.count(r.offset) != 0) continue;
            if (!r.txn) return r.offset;
            const auto other = std::find_if(txns_.begin(), txns_.end(), [&](const ModelTxn& x) { return x.pid == r.producerId && std::binary_search(x.offsets.begin(), x.offsets.end(), r.offset); });
            if (other != txns_.end() && appliedBefore_(*other, end)) return r.offset;
        }
        return -1;
    }
    /// The transaction judged whole at `end` (its TxnJudge, or its marker): the constraints first, then the conflict scan (R1).  Sets its vote; true when it voted yes.
    bool judgeWhole_(const VerdictRig::RigLog& log, const std::vector<std::pair<uint64_t, uint64_t>>& set, std::size_t t, std::set<std::string>& rows) {
        ModelTxn& txn = txns_[t];
        std::vector<const LogRec*> recs;
        for (const int64_t offset : txn.offsets) recs.push_back(recordAt_(log, offset));
        const Chain judged = chain_(recs, set, true);
        rows = judged.rows;
        const int64_t end = txn.judge >= 0 ? txn.judge : txn.marker;
        if (judged.verdict) txn.vote = ModelVote{judged.verdict->code, judged.verdict->constraintId, judged.failingOffset, txn.judge};
        else if (const int64_t hit = conflictBefore_(log, txn, end); hit >= 0) txn.vote = ModelVote{kTxnConflict, 0, hit, txn.judge};
        else txn.vote = ModelVote{0, 0, -1, txn.judge};
        return txn.vote->code == 0;
    }
    /// The rows of the transaction's records meet the locks of the transactions that voted before: they finalize first.  False: see settle_.
    bool settleTxn_(const VerdictRig::RigLog& log, const std::vector<std::pair<uint64_t, uint64_t>>& set, std::size_t t, int64_t offset) {
        if (locked_.empty()) return true;
        std::vector<const LogRec*> recs;
        for (const int64_t o : txns_[t].offsets) recs.push_back(recordAt_(log, o));
        return settle_(log, chain_(recs, set, true).rows, offset);
    }
    bool judgeTxn_(const VerdictRig::RigLog& log, const std::vector<std::pair<uint64_t, uint64_t>>& set, std::size_t t) {
        if (!settleTxn_(log, set, t, txns_[t].judge)) return false;
        std::set<std::string> rows;
        (void)judgeWhole_(log, set, t, rows);
        locked_.push_back({t, std::move(rows)});   // whatever the vote: the marker may still commit it
        return true;
    }
    /// P11-1: the partition votes when it is a verdict source and the set at the transaction's first record or the latest one is constrained, or a record carries an intent.
    [[nodiscard]] bool shouldVote_(const VerdictRig::RigLog& log, const ModelTxn& txn) const {
        const auto constrainedAt = [&](int64_t offset) {
            const RigChange* at = nullptr;
            for (const auto& c : log.changes) if (c.offset < offset) at = &c;
            return at != nullptr && std::any_of(at->members.begin(), at->members.end(), [](const auto& m) { return m.kind == kUnique || m.kind == kCheck || m.kind == kTestReject; });
        };
        const bool source = !log.changes.empty() && log.changes.front().offset < txn.marker;
        const bool intent = std::any_of(txn.offsets.begin(), txn.offsets.end(), [&](int64_t o) { return recordAt_(log, o)->intent != 0; });
        return source && (intent || constrainedAt(txn.first) || constrainedAt(txn.marker));
    }
    bool endTxn_(const VerdictRig::RigLog& log, const std::vector<std::pair<uint64_t, uint64_t>>& set, std::size_t t) {
        ModelTxn& txn = txns_[t];
        if (txn.judge >= 0) {   // voted: it holds its rows until here
            const auto at = std::find_if(locked_.begin(), locked_.end(), [&](const Locked& l) { return l.txn == t; });
            if (at != locked_.end()) finalize_(log, static_cast<std::size_t>(at - locked_.begin()), txn.marker);
            return true;
        }
        if (!txn.commit) return true;
        if (!shouldVote_(log, txn)) {
            applyTxn_(log, t, /*superseded=*/true, txn.marker);
            return true;
        }
        if (!settleTxn_(log, set, t, txn.marker)) return false;   // P11-7: judged at the marker
        std::set<std::string> rows;
        if (judgeWhole_(log, set, t, rows)) applyTxn_(log, t, /*superseded=*/false, txn.marker);
        else for (const int64_t offset : txn.offsets) verdicts_[offset] = ModelVerdict{txn.vote->code, txn.vote->constraintId};
        return true;
    }

    static std::string lowered_(const std::string& name) {
        std::array<uint8_t, 64> out{};
        const auto r = rune::codec::lowerUtf8({reinterpret_cast<const uint8_t*>(name.data()), name.size()}, out);
        return std::string(reinterpret_cast<const char*>(out.data()), r.len);
    }
    void applyCharacter_(const LogRec& rec) {
        if (const auto at = characters_.find(rec.id); at != characters_.end()) {
            if (auto c = claimants_.find(at->second.lowered); c != claimants_.end()) {
                c->second.erase(rec.id);
                if (c->second.empty()) claimants_.erase(c);
            }
            characters_.erase(at);
        }
        if (rec.del) return;
        const std::string name = lowered_(rec.name);
        characters_[rec.id] = {static_cast<uint64_t>(rec.v), name};
        claimants_[name].insert(rec.id);
    }
    [[nodiscard]] std::size_t liveOf_(uint64_t user) const {
        std::size_t n = 0;
        for (const auto& [id, c] : characters_) n += c.user == user ? 1 : 0;
        return n;
    }
    /// A live record is refused when its lowered name has an owner that is another key and the record is not already one of the claimants.
    std::optional<ModelVerdict> judgeUnique_(uint64_t objectId, const LogRec& rec) const {
        if (rec.del) return std::nullopt;
        const auto at = claimants_.find(lowered_(rec.name));
        if (at == claimants_.end() || at->second.count(rec.id) != 0) return std::nullopt;
        return ModelVerdict{kConstraintViolation, static_cast<uint32_t>(objectId << 16)};
    }
    /// P9-4: the group a record joins or leaves is refused above the cap when the record changes its count; a delete is never refused.
    std::optional<ModelVerdict> judgeCount_(uint64_t objectId, uint64_t fingerprint, const LogRec& rec) const {
        if (rec.del) return std::nullopt;
        const auto cap = caps_.find({objectId, fingerprint});
        const auto limit = static_cast<std::size_t>(cap == caps_.end() ? defaultCap_ : cap->second);
        const ModelVerdict refused{kConstraintViolation, constraintIdOf(objectId)};
        const auto user = static_cast<uint64_t>(rec.v);
        const auto before = characters_.find(rec.id);
        if (before == characters_.end()) return liveOf_(user) + 1 > limit ? std::optional<ModelVerdict>(refused) : std::nullopt;
        if (before->second.user == user) return std::nullopt;   // the count is unchanged
        if (liveOf_(before->second.user) - 1 > limit || liveOf_(user) + 1 > limit) return refused;   // the old group's row survives when it keeps a character
        return std::nullopt;
    }

    std::optional<ModelVerdict> judge_(uint64_t objectId, uint64_t fingerprint, const LogRec& rec) const {
        if (uniqueView_ && objectId == *uniqueView_) return judgeUnique_(objectId, rec);
        if (countView_ && objectId == *countView_) return judgeCount_(objectId, fingerprint, rec);
        if (const auto x = extra_.find(objectId); x != extra_.end()) return x->second(rec, *this);
        const auto it = rules_.find({objectId, fingerprint});
        if (it == rules_.end() || rec.del) return std::nullopt;   // a tombstone is never rejected by a TestReject rule
        const Rule& r = it->second;
        bool reject = false;
        switch (r.mode) {
        case Rule::Mode::TagEquals: reject = rec.tag == r.tag; break;
        case Rule::Mode::OffsetSet: reject = std::find(r.offsets.begin(), r.offsets.end(), rec.offset) != r.offsets.end(); break;
        case Rule::Mode::CapLiveKeys: reject = live_.count(rec.id) == 0 && live_.size() >= r.cap; break;
        }
        return reject ? std::optional<ModelVerdict>(ModelVerdict{kConstraintViolation, constraintIdOf(objectId)}) : std::nullopt;
    }

    std::map<std::pair<uint64_t, uint64_t>, Rule> rules_;
    std::map<uint64_t, RuleFn> extra_;
    std::map<int64_t, ModelVerdict> verdicts_;
    std::vector<LogRec> accepted_;
    std::map<uint64_t, LogRec> latest_;
    std::map<uint64_t, LogRec> live_;
    uint64_t partitions_ = 1, partition_ = 0;
    struct Locked { std::size_t txn; std::set<std::string> rows; };   ///< a transaction that voted and awaits its marker, with the rows it holds
    std::vector<ModelTxn> txns_;
    std::vector<Locked> locked_;
    int64_t judgeStop_ = INT64_MAX;

    struct Character { uint64_t user; std::string lowered; };
    std::optional<uint64_t> uniqueView_, countView_;
    int64_t defaultCap_ = 12;
    std::map<std::pair<uint64_t, uint64_t>, int64_t> caps_;
    std::map<std::string, std::set<uint64_t>> claimants_;   ///< lowered name -> the live keys that claim it
    std::map<uint64_t, Character> characters_;              ///< live key -> user and lowered name
};

// ---- Part 07: keyed views.  The model reads the source records of the log and derives view keys with its own copy of the encoding
// (big-endian, a signed integer's sign bit flipped, a string zero padded, lower() for ASCII and U+023A only - the matrix writes nothing else). ----

/// A column as one comparable string: the model's and the table's values are both folded through this.
[[nodiscard]] inline std::string modelCanon(const rune::codec::FieldValue& v) {
    using rune::codec::PrimType;
    if (!v.present) return "-";
    switch (v.type) {
    case PrimType::Int32: case PrimType::Int64: return "i" + std::to_string(v.i);
    case PrimType::FixedStr: {
        std::string s(v.bytes);
        while (!s.empty() && s.back() == '\0') s.pop_back();
        return "s" + s;
    }
    default: return "u" + std::to_string(v.u);
    }
}

/// The column of a model row in the table's own types, so that modelCanon compares it.
[[nodiscard]] inline std::string modelCanonOf(rune::codec::PrimType t, int64_t i, uint64_t u) {
    using rune::codec::PrimType;
    return (t == PrimType::Int32 || t == PrimType::Int64) ? "i" + std::to_string(i) : "u" + std::to_string(u);
}

[[nodiscard]] inline std::string modelLower(const std::string& s) {
    std::string out;
    for (std::size_t k = 0; k < s.size(); ++k) {
        const auto c = static_cast<uint8_t>(s[k]);
        if (c >= 'A' && c <= 'Z') out.push_back(static_cast<char>(c + 32));
        else if (c == 0xC8 && k + 1 < s.size() && static_cast<uint8_t>(s[k + 1]) == 0xBA) { out += "\xE2\xB1\xA5"; ++k; }
        else if (c < 0x80) out.push_back(static_cast<char>(c));
        else throw std::logic_error("modelLower: a character the matrix never writes");
    }
    return out;
}

/// One key component of a record, `width` bytes: nullopt = the writer's version has no such column.
[[nodiscard]] inline std::optional<std::vector<uint8_t>> modelComponent(const rune::codec::FieldValue& v, bool lower, uint16_t width, std::string& column) {
    using rune::codec::PrimType;
    if (!v.present) return std::nullopt;
    std::vector<uint8_t> out(width, 0);
    if (v.type == PrimType::FixedStr) {
        std::string s(v.bytes);
        if (const auto nul = s.find('\0'); nul != std::string::npos) s.resize(nul);
        if (lower) s = modelLower(s);
        if (s.size() > width) throw std::logic_error("modelComponent: wider than the component");
        std::copy(s.begin(), s.end(), out.begin());
        column = "s" + s;
        return out;
    }
    const bool isSigned = v.type == PrimType::Int32 || v.type == PrimType::Int64;
    uint64_t x = isSigned ? static_cast<uint64_t>(v.i) ^ (uint64_t{1} << (8 * width - 1)) : v.u;
    for (std::size_t k = width; k-- > 0; x >>= 8) out[k] = static_cast<uint8_t>(x);
    column = modelCanon(v);
    return out;
}

/// view key -> slot -> column; for an owner-mapped view also wire key -> the key of its row.
struct KeyedModel {
    std::map<std::vector<uint8_t>, std::map<uint16_t, std::string>> rows;
    std::map<std::string, std::vector<uint8_t>> owners;
    std::size_t skippedNoKey = 0;
};

[[nodiscard]] inline const rune::codec::FieldDescriptor* modelField(const rune::codec::Descriptor& d, uint16_t id) {
    for (const auto& f : d.fields) if (f.id == id) return &f;
    return nullptr;
}

/// Folds the source records of `log` (offset order; an empty value is a tombstone) into the rows `plan` keeps: one row per live source key,
/// from its last record, under the key that record derives.  A record whose writer version lacks a key column erases the source key's row.
[[nodiscard]] inline KeyedModel refoldKeyedView(const rune::view::CompiledViewPlan& plan, const rune::codec::Descriptor& src, const std::vector<RawRec>& log) {
    KeyedModel m;
    std::map<std::string, std::vector<uint8_t>> current;   // wire key -> the key of its row
    for (const RawRec& rec : log) {
        std::optional<std::vector<uint8_t>> key;
        std::map<uint16_t, std::string> cols;
        if (!rec.value.empty()) {
            key = std::vector<uint8_t>{};
            for (const auto& c : plan.keyComponents) {
                std::string column;
                const auto part = modelComponent(rune::codec::readField(src, rec.value, c.srcFieldId), c.keyFunction != 0, c.width, column);
                if (!part) { key.reset(); ++m.skippedNoKey; break; }
                key->insert(key->end(), part->begin(), part->end());
                cols[c.outSlot] = column;
            }
            if (key)
                for (const auto& o : plan.output) cols.emplace(o.outSlot, modelCanon(rune::codec::readField(src, rec.value, o.srcFieldId)));
        }
        if (const auto at = current.find(rec.key); at != current.end()) {
            if (!key || *key != at->second) m.rows.erase(at->second);
            current.erase(at);
        }
        if (!key) continue;
        m.rows[*key] = std::move(cols);
        current[rec.key] = *key;
    }
    if (rune::view::ownerMappedOf(plan)) m.owners = std::move(current);
    return m;
}

/// The rows of a group-by view: group key -> slot -> column.  A contributor is a source key; a write moves it to the head of its group
/// (last() is the head), a tombstone or a record without a group column retracts it, a group with no contributor has no row.
[[nodiscard]] inline std::map<std::vector<uint8_t>, std::map<uint16_t, std::string>> refoldKeyedAggregate(
    const rune::view::CompiledViewPlan& plan, const rune::codec::Descriptor& src, const std::vector<RawRec>& log) {
    using rune::view::MergeRule;
    struct Contributor { std::vector<uint8_t> group; std::map<uint16_t, std::string> keyCols; std::map<uint16_t, rune::codec::FieldValue> in; std::size_t seq; };
    std::map<std::string, Contributor> live;
    std::size_t seq = 0;
    for (const RawRec& rec : log) {
        ++seq;
        live.erase(rec.key);
        if (rec.value.empty()) continue;
        Contributor c{{}, {}, {}, seq};
        bool absent = false;
        for (const auto& k : plan.keyComponents) {
            std::string column;
            const auto part = modelComponent(rune::codec::readField(src, rec.value, k.srcFieldId), k.keyFunction != 0, k.width, column);
            if (!part) { absent = true; break; }
            c.group.insert(c.group.end(), part->begin(), part->end());
            c.keyCols[k.outSlot] = column;
        }
        if (absent) continue;
        for (const auto& o : plan.output)
            if (o.rule != MergeRule::CountAll && o.rule != MergeRule::Replace) c.in[o.outSlot] = rune::codec::readField(src, rec.value, o.srcFieldId);
        live.emplace(rec.key, std::move(c));
    }
    std::map<std::vector<uint8_t>, std::vector<const Contributor*>> groups;
    for (const auto& kv : live) groups[kv.second.group].push_back(&kv.second);
    std::map<std::vector<uint8_t>, std::map<uint16_t, std::string>> rows;
    for (const auto& [group, members] : groups) {
        std::map<uint16_t, std::string> cols = members.front()->keyCols;
        for (const auto& o : plan.output) {
            if (o.rule == MergeRule::Replace) continue;   // a group key column
            const bool isSigned = o.type == rune::codec::PrimType::Int32 || o.type == rune::codec::PrimType::Int64;
            const auto num = [&](const Contributor* c) { return c->in.at(o.outSlot); };
            int64_t si = 0;
            uint64_t su = 0;
            switch (o.rule) {
            case MergeRule::CountAll: si = static_cast<int64_t>(members.size()); su = members.size(); break;
            case MergeRule::Sum: for (const auto* c : members) { si += num(c).i; su += num(c).u; } break;
            case MergeRule::Min: case MergeRule::Max: {
                const auto lt = [&](const Contributor* a, const Contributor* b) { return isSigned ? num(a).i < num(b).i : num(a).u < num(b).u; };
                const Contributor* best = members.front();
                for (const auto* c : members) if (o.rule == MergeRule::Min ? lt(c, best) : lt(best, c)) best = c;
                si = num(best).i; su = num(best).u;
                break;
            }
            case MergeRule::Last: {
                const Contributor* head = members.front();
                for (const auto* c : members) if (c->seq > head->seq) head = c;
                si = num(head).i; su = num(head).u;
                break;
            }
            default: throw std::logic_error("refoldKeyedAggregate: a rule the matrix never uses");
            }
            cols[o.outSlot] = modelCanonOf(o.type, si, su);
        }
        rows[group] = std::move(cols);
    }
    return rows;
}

}  // namespace rune::node::cw
