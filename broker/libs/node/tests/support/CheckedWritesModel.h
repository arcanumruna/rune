#pragma once
// CheckedWritesModel.h - the reference judge of the checked-writes matrix and the model checks.  Written independently of
// VerdictSource: it reads the data records and the constraint-set change records of a log and judges them in offset order with
// its own copy of the TestReject rules.  Every non-rejected record reaches every member's table (a joining member builds from the
// start of the log unjudged), so all members' live keys are the live keys of the accepted records.
#include "CheckedWritesRig.h"

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
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
    static constexpr uint16_t kConstraintViolation = 55;   // ErrorCode::ConstraintViolation

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

    [[nodiscard]] static uint32_t constraintIdOf(uint64_t viewObjectId) noexcept { return static_cast<uint32_t>((viewObjectId << 16) | 1u); }

    /// Judges `log` (data records and change records, ascending offsets) from scratch.
    void reset(const VerdictRig::RigLog& log) {
        verdicts_.clear(); accepted_.clear(); latest_.clear(); live_.clear();
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
            if (!rec.del) {
                for (const auto& [objectId, fingerprint] : set) {
                    verdict = judge_(objectId, fingerprint, rec);
                    if (verdict) break;
                }
            }
            if (verdict) { verdicts_[rec.offset] = *verdict; continue; }
            accepted_.push_back(rec);
            latest_[rec.id] = rec;
            if (rec.del) live_.erase(rec.id); else live_[rec.id] = rec;
        }
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
    std::optional<ModelVerdict> judge_(uint64_t objectId, uint64_t fingerprint, const LogRec& rec) const {
        if (const auto x = extra_.find(objectId); x != extra_.end()) return x->second(rec, *this);
        const auto it = rules_.find({objectId, fingerprint});
        if (it == rules_.end()) return std::nullopt;
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
};

}  // namespace rune::node::cw
