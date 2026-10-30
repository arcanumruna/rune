#pragma once
// CheckedWritesChecks.h - the invariants of a verdict source's log, shared by the matrix, the model check and the cluster stress.
#include "CheckedWritesModel.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace rune::node::cw {

/// Markers may overlap (a new activation re-marks from W while an uncommitted marker sits above it): in an overlap the entries
/// must be identical, and together the markers must cover [firstChange + 1, ...) without a gap.  Returns the problem, or "".
/// `reach` receives the end of the union.
inline std::string markerUnionProblem(const std::vector<VerdictRig::RigMarker>& markers, int64_t firstChange, int64_t* reach = nullptr) {
    std::map<int64_t, std::pair<uint16_t, uint32_t>> seen;
    int64_t end = firstChange + 1;
    for (const auto& m : markers) {
        const int64_t from = m.header.fromOffset, through = m.header.finalThrough;
        if (from > end) return "markers leave a gap at [" + std::to_string(end) + ", " + std::to_string(from) + ")";
        const int64_t overlapTo = std::min(through, end);
        std::map<int64_t, std::pair<uint16_t, uint32_t>> mine;
        for (const auto& [o, code, cid] : m.entries) mine[o] = {code, cid};
        for (auto it = seen.lower_bound(from); it != seen.end() && it->first < overlapTo; ++it) {
            const auto f = mine.find(it->first);
            if (f == mine.end() || f->second != it->second) return "marker at " + std::to_string(m.offset) + " disagrees with an earlier one at offset " + std::to_string(it->first);
        }
        for (const auto& [o, v] : mine) {
            if (o < overlapTo) {
                const auto f = seen.find(o);
                if (f == seen.end() || f->second != v) return "marker at " + std::to_string(m.offset) + " lists offset " + std::to_string(o) + " that an earlier one does not";
            }
            seen[o] = v;
        }
        end = std::max(end, through);
    }
    if (reach != nullptr) *reach = end;
    return {};
}


// ---- the invariants of the matrix and the model check, over one rig and the reference model --------------------------------------
// `present` (where a function takes one): the offsets the log still holds.  A compaction removes superseded and rejected records, so
// the model, built from the history, may name offsets the log no longer has; with `present` null the log is the whole history.

/// (a) The worker's rejected-offset index is the model's verdicts.
inline void checkIndex(VerdictRig& r, const CheckedWritesModel& model, const std::set<int64_t>* present = nullptr) {
    std::map<int64_t, ModelVerdict> got;
    r.writeState()->rejected.forEachIn(0, INT64_MAX, [&](const rune::storage::RejectedOffsetIndex::Entry& e) { got[e.offset] = {e.code, e.constraintId}; });
    if (present == nullptr) {
        EXPECT_EQ(got, model.verdicts()) << "(a) the rejected offsets";
        return;
    }
    for (const auto& [off, verdict] : got) {
        const auto it = model.verdicts().find(off);
        EXPECT_TRUE(it != model.verdicts().end() && it->second == verdict) << "(a) index entry " << off << " is not the model's verdict";
    }
    for (const auto& [off, verdict] : model.verdicts())
        if (present->count(off) != 0) EXPECT_TRUE(got.count(off) != 0) << "(a) rejected offset " << off << " is in the log but not in the index";
    EXPECT_EQ(r.writeState()->rejected.mismatches(), 0u) << "(a) the index counted mismatches";
}

/// (b) The markers carry the model's verdicts; unless a compaction removed superseded markers they cover the log without a gap.
inline void checkMarkers(const CheckedWritesModel& model, const VerdictRig::RigLog& log, int64_t target, bool compacted = false) {
    ASSERT_FALSE(log.markers.empty());
    ASSERT_FALSE(log.changes.empty());
    int64_t reach = 0;
    const std::string problem = markerUnionProblem(log.markers, log.changes.front().offset, &reach);
    if (!compacted) EXPECT_EQ(problem, "") << "(b) markers cover the log without a gap and agree where they overlap";
    std::map<int64_t, ModelVerdict> union_;
    for (const auto& m : log.markers) {
        for (const auto& [o, code, cid] : m.entries) {
            EXPECT_GE(o, m.header.fromOffset);
            EXPECT_LT(o, m.header.finalThrough);
            union_[o] = {code, cid};
        }
    }
    if (!compacted) {
        EXPECT_GE(reach, target);
        EXPECT_EQ(union_, model.verdicts()) << "(b) the markers carry every verdict";
        return;
    }
    for (const auto& [off, verdict] : union_) {
        const auto it = model.verdicts().find(off);
        EXPECT_TRUE(it != model.verdicts().end() && it->second == verdict) << "(b) marker entry " << off << " is not the model's verdict";
    }
}

/// (c)(d) The projection and the count views hold the model's live keys.
inline void checkViews(VerdictRig& r, const CheckedWritesModel& model, const VerdictRig::RigLog& log, int32_t projView, int32_t countView) {
    std::set<uint64_t> ids;
    for (const auto& d : log.data) ids.insert(d.id);
    const auto proj = model.projection();
    for (const uint64_t id : ids) {
        const auto row = r.idRow(projView, id);
        const auto it = proj.find(id);
        if (it == proj.end()) { EXPECT_FALSE(row) << "(c)(d) key " << id << " is not live"; continue; }
        ASSERT_TRUE(row) << "(d) key " << id;
        EXPECT_EQ(row->first, it->second.first) << "key " << id;
        EXPECT_EQ(row->second, it->second.second) << "key " << id;
    }
    const auto counts = model.countById();
    for (const uint64_t id : ids) {
        const auto it = counts.find(id);
        const int64_t want = it == counts.end() ? 0 : it->second, got = r.countRow(countView, id).value_or(0);
        EXPECT_EQ(got, want) << "count of key " << id;
    }
}

/// (c) A subscription delivered exactly the accepted records (Filtered: the live tag-1 ones).
inline void checkDelivered(VerdictRig& r, const CheckedWritesModel& model, int64_t sub, bool filtered, const std::set<int64_t>* present = nullptr) {
    const auto& box = r.collect(sub);
    std::vector<std::tuple<int64_t, uint64_t, uint16_t>> got, want;
    for (const auto& d : box.records) got.emplace_back(d.offset, d.id, d.tag);
    for (const auto& a : model.acceptedRecords()) {
        if (filtered && (a.del || a.tag != 1)) continue;
        if (present != nullptr && present->count(a.offset) == 0) continue;
        want.emplace_back(a.offset, a.id, a.del ? kTombstoneTag : a.tag);
    }
    EXPECT_EQ(got, want) << "(c) the delivered records are the accepted ones";
}

/// (e)(g) One cleaner pass keeps everything at or above the verdict bound and every key's latest accepted record.
inline void checkCompaction(VerdictRig& r, const CheckedWritesModel& model, const VerdictRig::RigLog& before) {
    const auto cp = r.compact();
    const auto after = r.readLog();
    std::set<int64_t> kept;
    for (const auto& d : after.data) kept.insert(d.offset);
    for (const auto& d : before.data)
        if (d.offset >= cp.bound) EXPECT_TRUE(kept.count(d.offset)) << "(g) offset " << d.offset << " is at or above the verdict bound " << cp.bound;
    for (const auto& [id, latest] : model.latestAcceptedByKey()) {
        if (latest.del) continue;
        EXPECT_TRUE(kept.count(latest.offset)) << "(e) key " << id << " lost its latest accepted record";
    }
}

}  // namespace rune::node::cw
