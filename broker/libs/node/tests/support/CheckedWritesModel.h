#pragma once
// CheckedWritesModel.h - the reference judge of the checked-writes matrix and the model checks.  Written independently of
// VerdictSource: it reads the data records and the constraint-set change records of a log and judges them in apply order (ascending offsets, or a committed transaction moved to its marker, P11-0) with
// its own copy of the TestReject rules.  Every non-rejected record reaches every member's table (a joining member builds from the
// start of the log unjudged), so all members' live keys are the live keys of the accepted records.
#include "CheckedWritesRig.h"

#include <rune/codec/Record.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/view/ViewPlan.h>

#include <array>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace rune::node::cw {

struct ModelVerdict {
    uint16_t code = 0;
    uint32_t constraintId = 0;
    bool operator==(const ModelVerdict&) const = default;
};

class CheckedWritesModel {
public:
    using LogRec = VerdictRig::RigLogRec;
    static constexpr uint16_t kConstraintViolation = 55, kKeyExists = 56, kKeyNotFound = 57;   // ErrorCode

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

    /// Judges `log` from scratch: data records in apply order (ascending offsets, or a committed transaction moved to its marker, P11-0), change records ascending.
    void reset(const VerdictRig::RigLog& log) {
        verdicts_.clear(); accepted_.clear(); latest_.clear(); live_.clear(); claimants_.clear(); characters_.clear();
        std::vector<std::pair<uint64_t, uint64_t>> set;   // the current members, ascending object id
        std::size_t next = 0;
        for (const LogRec& rec : log.data) {
            while (next < log.changes.size() && log.changes[next].offset < rec.offset) {
                set.clear();
                for (const auto& m : log.changes[next].members) set.emplace_back(m.viewObjectId, m.planFingerprint);
                std::sort(set.begin(), set.end());
                ++next;
            }
            std::optional<ModelVerdict> verdict;
            for (const auto& [objectId, fingerprint] : set) {
                verdict = judge_(objectId, fingerprint, rec);
                if (verdict) break;
            }
            if (verdict) { verdicts_[rec.offset] = *verdict; continue; }
            accepted_.push_back(rec);
            if (const auto seen = latest_.find(rec.id); seen != latest_.end() && seen->second.offset > rec.offset) continue;   // P11-3a: applied after a later offset, delivered but superseded
            latest_[rec.id] = rec;
            if (rec.del) live_.erase(rec.id); else live_[rec.id] = rec;
            if (uniqueView_ || countView_) applyCharacter_(rec);
        }
        std::sort(accepted_.begin(), accepted_.end(), [](const LogRec& a, const LogRec& b) { return a.offset < b.offset; });   // delivery is in offset order
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
