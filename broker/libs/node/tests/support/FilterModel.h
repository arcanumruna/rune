#pragma once
// FilterModel.h — Phase 2c Task 15: an INDEPENDENT brute-force model of what a filtered subscription receives.
//
// It shares no code with FilterGroup, FilterIndex or CompiledFilter (it includes none of them): records are plain
// structs, a member is its filter terms as plain values plus a start offset, and matching is a linear scan of
// every record against every term.  The stress test (FilterStressRegressionTest.cpp) compares the engine to it.
//
// What the model knows (the rules the engine is specified to follow, restated here, not imported):
//  - A record is delivered to a member when its value is present, long enough to carry a header, of a known
//    schema version, and EVERY term of the member's filter holds in that version.  A term on a field that the
//    record's version does not have never holds ("absent"); otherwise Eq compares, In looks the value up.
//  - Null values (tombstones) and values shorter than the 4-byte header (malformed) are never delivered.
//  - A record whose version is not known yet is delivered once the version is known.  If the wait for the schema
//    ran out first (expireUnresolved), the records pending at that moment are lost for the members that were
//    live: excluded, counted as unknown-version, never delivered.
//  - A member evaluates the whole batch that holds its start offset and every batch after it; it is delivered
//    the matching records at or after its start.
//
// Counters.  The engine counts an excluded record in the member (catch-up) or in the group (shared scan), once
// per evaluation, and its two modes differ in one way the model must not paper over: catch-up reports "absent"
// when ANY term is absent in the record's version, the shared scan only when the index term is absent or the
// index term matched.  The model therefore gives a [lo, hi] range per counter:
//    lo - every record the member's range holds is evaluated at least once, in the mode that counts the least
//         (for "absent": some term absent and no present term mismatching, which is mode- and index-choice-free);
//         (null, malformed and unknown-version are member-independent: a record counts once whoever evaluates it);
//    hi - in the mode that counts the most, once per member that holds the record in its range, plus one more
//         evaluation of a whole batch for every refused send of it (a refused batch is evaluated again; the lo of a
//         LIVE member's refused batches counts it too: that evaluation is in a counter that survives).
//  The two coincide, and the stress test then demands equality, whenever a single member is the only one that ever
//  evaluates (the "solo" seeds) and the category is not "absent".
#include <algorithm>
#include <cstdint>
#include <optional>
#include <set>
#include <vector>

namespace rune::node::filtermodel {

/// The three filterable fields of the test schema, in the order their tags sort (playerId 0, worldId 1, channel 2).
enum class Field : uint8_t { Player = 0, World = 1, Channel = 2 };

struct Rec {
    int64_t                 offset = 0;
    uint16_t                version = 1;
    std::optional<uint32_t> worldId;
    std::optional<int32_t>  channel;    // empty from version 3 on (the field is dropped)
    uint64_t                playerId = 0;
    bool                    null = false;
    bool                    shortValue = false;   // a value shorter than the 4-byte header
};

struct Term {
    Field                field = Field::World;
    std::vector<int64_t> values;   // one value = Eq; two or more = In (sorted, distinct)
};

enum class Kind : uint8_t { Match, NoMatch, Absent, Null, Malformed, Unknown, Pending };
enum class Cat : uint8_t { Absent, Unknown, Null, Malformed };
struct Bounds { uint64_t lo = 0, hi = 0; };

struct Member {
    std::vector<Term>   terms;
    int64_t             start = 0;
    bool                live = true;
    std::set<int64_t>   lost;                 // offsets excluded: their version was still unknown when the wait ran out
    std::vector<int64_t> exp;                 // cache: the offsets this member must receive, ascending
    std::size_t         expScanned = 0;       // records of the log folded into `exp`
    bool                dirty = false;        // `exp` must be rebuilt (a version arrived, records were lost)
};

class Model {
public:
    struct Batch { int64_t base = 0; uint32_t count = 0; };

    explicit Model(uint16_t knownVersion) : known_(knownVersion) {}

    // ---- the log ----
    /// Appends one batch; offsets are assigned here.
    void appendBatch(std::vector<Rec> rs) {
        const int64_t base = end();
        for (std::size_t i = 0; i < rs.size(); ++i) rs[i].offset = base + static_cast<int64_t>(i);
        batches_.push_back({base, static_cast<uint32_t>(rs.size())});
        recs_.insert(recs_.end(), rs.begin(), rs.end());
    }
    [[nodiscard]] int64_t end() const noexcept { return static_cast<int64_t>(recs_.size()); }
    [[nodiscard]] const std::vector<Rec>& records() const noexcept { return recs_; }
    [[nodiscard]] const std::vector<Batch>& batches() const noexcept { return batches_; }
    /// The base offset of the batch holding `offset` (offset itself at or past the end).
    [[nodiscard]] int64_t batchBase(int64_t offset) const noexcept {
        if (offset >= end()) return offset;
        auto it = std::upper_bound(batches_.begin(), batches_.end(), offset,
                                   [](int64_t o, const Batch& b) { return o < b.base; });
        return (it - 1)->base;
    }

    /// One past the last offset of the batch holding `offset`.
    [[nodiscard]] int64_t batchEnd(int64_t offset) const noexcept {
        if (offset >= end()) return offset;
        auto it = std::upper_bound(batches_.begin(), batches_.end(), offset,
                                   [](int64_t o, const Batch& b) { return o < b.base; });
        return (it - 1)->base + (it - 1)->count;
    }

    // ---- the schema ----
    [[nodiscard]] uint16_t known() const noexcept { return known_; }
    /// Version known()+1 arrives.
    void schemaArrived() { ++known_; for (auto& m : members_) m.dirty = true; }
    /// Does a record of `version` carry `f`?  v1: playerId worldId channel; v2 adds guild; v3 drops channel; v4 adds level.
    [[nodiscard]] static bool present(uint16_t version, Field f) noexcept {
        if (f == Field::Channel) return version <= 2;
        return true;
    }

    // ---- members ----
    std::size_t addMember(std::vector<Term> terms, int64_t start) {
        Member m;
        m.terms = std::move(terms);
        m.start = start;
        members_.push_back(std::move(m));
        return members_.size() - 1;
    }
    void removeMember(std::size_t i) { members_[i].live = false; }
    [[nodiscard]] const Member& member(std::size_t i) const noexcept { return members_[i]; }
    [[nodiscard]] std::size_t memberCount() const noexcept { return members_.size(); }
    [[nodiscard]] int64_t rangeStart(std::size_t i) const noexcept { return batchBase(members_[i].start); }

    /// Is the record waiting for a schema?  A null or short value carries no version, so it never waits.
    [[nodiscard]] bool unresolved(const Rec& r) const noexcept { return !r.null && !r.shortValue && r.version > known_; }

    /// The wait for the schema ran out: every record of a still-unknown version in the range of a live member
    /// is lost for that member (it was passed over, counted unknown-version).
    void expireUnresolved() {
        for (auto& m : members_) {
            if (!m.live) continue;
            const int64_t from = batchBase(m.start);
            for (int64_t o = from; o < end(); ++o)
                if (unresolved(recs_[static_cast<std::size_t>(o)])) m.lost.insert(o);
            m.dirty = true;
        }
    }
    /// Offset of the last record that still waits for its version, or -1.
    [[nodiscard]] int64_t lastUnresolved() const noexcept {
        for (int64_t o = end() - 1; o >= 0; --o)
            if (unresolved(recs_[static_cast<std::size_t>(o)])) return o;
        return -1;
    }

    // ---- classification ----
    struct Eval { Kind kind = Kind::NoMatch; bool absentLo = false; };

    [[nodiscard]] Eval evaluate(const Member& m, const Rec& r) const {
        Eval e;
        if (m.lost.count(r.offset) != 0) { e.kind = Kind::Unknown; return e; }
        if (r.null) { e.kind = Kind::Null; return e; }
        if (r.shortValue) { e.kind = Kind::Malformed; return e; }
        if (unresolved(r)) { e.kind = Kind::Pending; return e; }
        bool anyAbsent = false, presentMismatch = false;
        for (const Term& t : m.terms) {
            if (!present(r.version, t.field)) { anyAbsent = true; continue; }
            if (std::find(t.values.begin(), t.values.end(), valueOf(r, t.field)) == t.values.end()) presentMismatch = true;
        }
        if (anyAbsent) { e.kind = Kind::Absent; e.absentLo = !presentMismatch; return e; }
        e.kind = presentMismatch ? Kind::NoMatch : Kind::Match;
        return e;
    }

    /// The offsets the member must receive (ascending): matching records at or after its start, not lost.
    /// Records of a version that is not known yet are not in it (they join when the version arrives).
    [[nodiscard]] const std::vector<int64_t>& expected(std::size_t i) {
        Member& m = members_[i];
        if (m.dirty) { m.exp.clear(); m.expScanned = 0; m.dirty = false; }
        for (; m.expScanned < recs_.size(); ++m.expScanned) {
            const Rec& r = recs_[m.expScanned];
            if (r.offset >= m.start && evaluate(m, r).kind == Kind::Match) m.exp.push_back(r.offset);
        }
        return m.exp;
    }

    // ---- counters ----
    /// A live member's "absent" count.  `retryBases` = the base of every batch whose send to it was refused.
    [[nodiscard]] Bounds absentBounds(std::size_t i, const std::vector<int64_t>& retryBases) const {
        const Member& m = members_[i];
        Bounds b;
        for (int64_t o = batchBase(m.start); o < end(); ++o) {
            const Eval e = evaluate(m, recs_[static_cast<std::size_t>(o)]);
            if (e.kind != Kind::Absent) continue;
            ++b.hi;
            if (e.absentLo) ++b.lo;
        }
        b.hi += retryExtra(m, retryBases, Cat::Absent);
        return b;
    }
    /// The group-wide total of one category (group counters + every live member's counters).
    /// `retryBases[i]` = member i's refused batch bases.  Counters of removed members are gone with them, so
    /// `lo` counts the live members only, and `hi` every member ever.
    [[nodiscard]] Bounds totalBounds(Cat c, const std::vector<std::vector<int64_t>>& retryBases) const {
        Bounds b;
        const Kind want = kindOf(c);
        for (int64_t o = 0; o < end(); ++o) {
            const Rec& r = recs_[static_cast<std::size_t>(o)];
            bool someLive = false;
            for (std::size_t i = 0; i < members_.size(); ++i) {
                const Member& m = members_[i];
                if (o < batchBase(m.start) || evaluate(m, r).kind != want) continue;
                ++b.hi;
                someLive = someLive || m.live;
            }
            if (someLive) ++b.lo;
        }
        for (std::size_t i = 0; i < members_.size() && i < retryBases.size(); ++i) {
            const uint64_t extra = retryExtra(members_[i], retryBases[i], c);
            b.hi += extra;
            if (members_[i].live) b.lo += extra;   // a live member's refused batch is evaluated again, and that count stays
        }
        return b;
    }

private:
    [[nodiscard]] static int64_t valueOf(const Rec& r, Field f) noexcept {
        switch (f) {
            case Field::Player: return static_cast<int64_t>(r.playerId);
            case Field::World: return r.worldId ? static_cast<int64_t>(*r.worldId) : -1;
            case Field::Channel: return r.channel ? *r.channel : -1;
        }
        return -1;
    }
    [[nodiscard]] static Kind kindOf(Cat c) noexcept {
        switch (c) {
            case Cat::Absent: return Kind::Absent;
            case Cat::Unknown: return Kind::Unknown;
            case Cat::Null: return Kind::Null;
            case Cat::Malformed: return Kind::Malformed;
        }
        return Kind::Absent;
    }
    /// A refused batch is evaluated again: its records of the category count once more.
    [[nodiscard]] uint64_t retryExtra(const Member& m, const std::vector<int64_t>& bases, Cat c) const {
        uint64_t n = 0;
        const Kind want = kindOf(c);
        for (const int64_t base : bases) {
            auto it = std::lower_bound(batches_.begin(), batches_.end(), base,
                                       [](const Batch& b, int64_t v) { return b.base < v; });
            if (it == batches_.end() || it->base != base) continue;
            for (int64_t o = base; o < base + it->count; ++o)
                if (evaluate(m, recs_[static_cast<std::size_t>(o)]).kind == want) ++n;
        }
        return n;
    }

    uint16_t            known_;
    std::vector<Rec>    recs_;
    std::vector<Batch>  batches_;
    std::vector<Member> members_;
};

}  // namespace rune::node::filtermodel
