// FilterStressRegressionTest.cpp — Phase 2c Task 15: seeded, model-checked stress of the filter engine and the index.
//
// 64 seeds x 600 steps (RUNE_FILTER_STRESS_SEEDS widens the seed count) over ONE FilterGroup fed by a FakeSource
// and a FakeSink.  Each step is one of: add a member (Eq / In on 1-2 of playerId, worldId, channel; a random start
// offset), remove one, append a batch (1-64 records, a known version, 2% null, 1% short values), ALTER (the schema
// gains guild, drops channel, gains level), append records of a not-yet-known version (the schema arrives k steps
// later, or never), refuse the next N sends of a member, advance the clock (note intervals, or a whole version
// wait), or run pass().
//
// The model (support/FilterModel.h) is independent of the engine: records as structs, members as plain terms, brute
// force matching.  After EVERY pass() each live member's delivered offsets must equal the model's matching offsets
// below the member's position (so: a prefix of the model's list, in order, no duplicate, nothing missed).  At the
// end, after draining: exactly the model's set, every position at the log end, the counters inside the model's
// ranges (see FilterModel.h), and 0 heap allocations inside the measured passes (after a warm-up).
//
// Time is deterministic.  The version wait is only ever crossed on purpose: a clock advance never carries the
// clock a whole wait past the append of a record whose version is still unknown, except in expire(), which first
// lets every member and the scan reach the stall, then advances a whole wait, and repeats until all have passed
// every unknown record (those are then lost for the members live at that moment, as the model says).
//
// A failure prints the seed, the replay command and the step log.
#include "rune/node/filter/FilterGroup.h"

#include "support/AllocWindow.h"
#include "support/FilterFakes.h"
#include "support/FilterModel.h"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

using namespace rune::node::filter;
using namespace rune::node::filter::fakes;
namespace mm = rune::node::filtermodel;
using rune::Match;
using rune::anyOf;
using rune::codec::CompiledFilter;
using rune::codec::FilterBytes;
using rune::codec::FilterSchema;
using rune::codec::FilterWriter;
using rune::codec::PrimType;

namespace {

constexpr uint64_t kSrc = 0x57AE5501;
constexpr int64_t  kT0 = 1'000'000'000'000;
constexpr int      kSteps = 600, kWarmup = 100, kMaxLive = 24;
constexpr uint32_t kMaxSubs = 512;
constexpr int64_t  kNoteNs = 100'000, kWaitNs = 10'000'000;
constexpr uint16_t kMaxBuildableVersion = 4;   // playerEvent(): v2 +guild, v3 -channel, v4 +level
constexpr std::size_t kBigBudget = std::size_t{1} << 30;

struct StressFailure { std::string what; };

/// Wraps FakeSink (alloc-free: no frame recording) and keeps what the checks need in storage reserved up front, so
/// that the sink adds nothing to the measured allocations: the offsets of every sent batch, the refused batch
/// bases.  The harness drains the buffers after every pass.
class StressSink {
public:
    struct Sent { uint32_t sub; int64_t off; };
    struct Refused { uint32_t sub; int64_t base; };

    explicit StressSink(FakeSink& f) : f_(f) {
        sent_.reserve(kSentCap);
        refused_.reserve(kRefusedCap);
    }
    SendOutcome sendBatch(uint32_t sub, int64_t base, std::span<const uint8_t> b) {
        const SendOutcome o = f_.sendBatch(sub, base, b);
        if (o == SendOutcome::Sent) {
            rune::protocol::RecordBatchReader r(b.data(), b.size());
            r.forEachRecord([&](std::string_view, std::string_view, int64_t off, int64_t) {
                if (sent_.size() < sent_.capacity()) sent_.push_back({sub, off}); else overflow_ = true;
            });
        } else if (o == SendOutcome::Retry) {
            if (refused_.size() < refused_.capacity()) refused_.push_back({sub, base}); else overflow_ = true;
        }
        return o;
    }
    SendOutcome sendNote(uint32_t sub, int64_t c) {
        return f_.sendNote(sub, c);
    }
    int64_t& cursor(uint32_t sub) { return f_.cursor(sub); }
    bool deliverable(uint32_t sub) { return f_.deliverable(sub); }

    std::vector<Sent>    sent_;
    std::vector<Refused> refused_;
    bool                 overflow_ = false;

private:
    static constexpr std::size_t kSentCap = std::size_t{1} << 21, kRefusedCap = std::size_t{1} << 16;
    FakeSink& f_;
};
static_assert(FilterSink<StressSink>);

CompiledFilter compileTerms(const FilterSchema& s, const std::vector<mm::Term>& terms) {
    FilterBytes fb;
    FilterWriter w(std::span<uint8_t>(fb.bytes.data(), fb.bytes.size()));
    auto matchOf = []<class T>(const std::vector<int64_t>& vals) {
        std::vector<T> v;
        for (const int64_t x : vals) v.push_back(static_cast<T>(x));
        if (v.size() == 1) return Match<T>(v[0]);
        return Match<T>(anyOf(std::span<const T>(v)));
    };
    for (const mm::Term& t : terms) {   // tag order: playerId 0, worldId 1, channel 2
        switch (t.field) {
            case mm::Field::Player: w.term(0, PrimType::UInt64, 8, matchOf.template operator()<uint64_t>(t.values)); break;
            case mm::Field::World: w.term(1, PrimType::UInt32, 4, matchOf.template operator()<uint32_t>(t.values)); break;
            case mm::Field::Channel: w.term(2, PrimType::Int32, 4, matchOf.template operator()<int32_t>(t.values)); break;
        }
    }
    auto n = w.finish();
    if (!n || *n == 0) throw StressFailure{"filter encode failed"};
    auto c = CompiledFilter::compile(std::span<const uint8_t>(fb.bytes.data(), *n), s);
    if (!c) throw StressFailure{"filter compile failed"};
    return std::move(*c);
}

const char* fieldName(mm::Field f) { return f == mm::Field::Player ? "playerId" : f == mm::Field::World ? "worldId" : "channel"; }
std::string termsText(const std::vector<mm::Term>& terms) {
    std::ostringstream o;
    for (std::size_t i = 0; i < terms.size(); ++i) {
        o << (i ? " & " : "") << fieldName(terms[i].field) << (terms[i].values.size() == 1 ? "=" : " in {");
        for (std::size_t j = 0; j < terms[i].values.size(); ++j) o << (j ? "," : "") << terms[i].values[j];
        if (terms[i].values.size() != 1) o << "}";
    }
    return o.str();
}

class Seed {
public:
    explicit Seed(uint32_t seed)
        : seed_(seed), rng_(seed), fake_(), sink_(fake_), schema_(playerSchema(1)), model_(1) {
        solo_ = seed % 4 == 0;
        cfg_.noteIntervalNs = kNoteNs;
        cfg_.versionWaitNs = kWaitNs;
        cfg_.matchScratch = rng_() % 4 == 0 ? 6 : 65'536;           // some seeds overflow the match list constantly
        cfg_.catchUpSharePercent = static_cast<uint32_t>(10 + rng_() % 90);
        g_.init(1, 0, cfg_, kSrc);
        sc_.reserve(cfg_, 256);
        sc_.rebuild.resize(16'384);   // the engine's one-time growth, done in the warm-up (a 64-record batch is < 9 KB)
        fake_.recordFrames = false;
        fake_.reserveSubs(kMaxSubs);
        now_ = kT0;
    }

    /// Runs the seed; an empty string = pass, else the failure report.
    std::string run() {
        try {
            body_();
        } catch (const StressFailure& f) {
            std::ostringstream o;
            o << "FilterStressRegression FAILED  seed " << seed_ << ": " << f.what << "\n  replay: RUNE_FILTER_STRESS_SEEDS=" << seed_
              << " rune_filter_engine_tests --gtest_filter='*FilterStressRegression*/" << (seed_ - 1) << "'"
              << "  (the parameter index of seed N is N-1 in a default run)\nstep log (" << steps_.size() << " lines):\n";
            for (const auto& l : steps_) o << "  " << l << "\n";
            return o.str();
        }
        return {};
    }

    // What the test reports about a run.
    uint64_t measuredPasses() const { return measuredPasses_; }
    uint64_t allocations() const { return allocs_; }
    uint64_t measuredDeliveries() const { return measuredDeliveries_; }
    std::string stats() const {
        std::ostringstream o;
        const FilterCounters& c = g_.groupCounters();
        o << (solo_ ? "SOLO " : "") << "members " << mem_.size() << " records " << model_.end() << " deliveries(measured) " << measuredDeliveries_ << " passes(measured) "
          << measuredPasses_ << " expiries " << expiries_ << " zeroCopy " << c.zeroCopy << " rebuilt " << c.rebuilt << " notes " << c.notes
          << " fellBehind " << c.fellBehind << " rejoined " << c.rejoined << " overflowBatches " << c.overflowBatches << " counter ranges "
          << rangesChecked_ << " (points " << pointsChecked_ << ", nonzero " << nonZeroChecked_ << ")";
        return o.str();
    }

private:
    struct Mem {
        std::size_t mi = 0;
        uint32_t    sub = 0, gid = 0;
        int64_t     start = 0;
        bool        live = true;
        std::vector<int64_t> got;        // delivered offsets at or after `start`, in arrival order
        std::vector<int64_t> refusedBases;
        std::string desc;
    };

    [[noreturn]] void fail_(const std::string& what) { throw StressFailure{what}; }
    void log_(const std::string& s) { steps_.push_back("step " + std::to_string(step_) + ": " + s); }

    // ---- the drive ----
    void body_() {
        // Warm-up starts a few members; the first step already has someone to serve.
        if (!solo_) for (int i = 0; i < 2; ++i) addMember_();
        for (step_ = 0; step_ < kSteps; ++step_) {
            measuring_ = step_ >= kWarmup;
            while (!scheduled_.empty() && scheduled_.front().due <= step_) {
                const uint16_t v = scheduled_.front().version;
                scheduled_.erase(scheduled_.begin());
                while (model_.known() < v) alter_("scheduled arrival");
            }
            const uint64_t a = rng_() % 100;
            if (a < 8) addMember_();
            else if (a < 12) removeMember_();
            else if (a < 41) appendBatch_();
            else if (a < 44) alter_("random ALTER");
            else if (a < 48) appendUnknown_();
            else if (a < 54) refuse_();
            else if (a < 64) clock_();
            else pass_();
        }
        drain_();
    }

    void addMember_() {
        if (liveCount_() >= kMaxLive || nextSub_ >= kMaxSubs || (solo_ && !mem_.empty())) return;
        std::vector<mm::Field> fields{mm::Field::Player, mm::Field::World, mm::Field::Channel};
        std::shuffle(fields.begin(), fields.end(), rng_);
        fields.resize(1 + rng_() % 2);
        std::sort(fields.begin(), fields.end());
        std::vector<mm::Term> terms;
        for (const mm::Field f : fields) {
            const int64_t domain = f == mm::Field::Player ? 10 : f == mm::Field::World ? 6 : 4;
            mm::Term t;
            t.field = f;
            if (rng_() % 2 == 0) {
                t.values = {static_cast<int64_t>(rng_() % static_cast<uint64_t>(domain))};
            } else {
                std::set<int64_t> s;
                for (uint64_t k = 2 + rng_() % 2; k > 0; --k) s.insert(static_cast<int64_t>(rng_() % static_cast<uint64_t>(domain)));
                if (s.size() < 2) s.insert((*s.begin() + 1) % domain);
                t.values.assign(s.begin(), s.end());
            }
            terms.push_back(std::move(t));
        }
        int64_t start = static_cast<int64_t>(rng_() % static_cast<uint64_t>(model_.end() + 1));
        start = std::max(start, expiredClamp_());   // a record whose wait already ran out for others must not stall a new member
        Mem m;
        m.mi = model_.addMember(terms, start);
        m.sub = nextSub_++;
        m.start = start;
        fake_.cursor(m.sub) = start;
        m.gid = g_.add(m.sub, compileTerms(schema_, terms), schema_, kSrc);
        if (m.gid == kNil) fail_("FilterGroup::add refused a member (cap)");
        m.desc = termsText(terms) + " from " + std::to_string(start);
        log_("add member #" + std::to_string(mem_.size()) + " sub " + std::to_string(m.sub) + ": " + m.desc);
        mem_.push_back(std::move(m));
    }

    void removeMember_() {
        const auto live = liveIdx_();
        if (live.empty() || solo_) return;
        Mem& m = mem_[live[rng_() % live.size()]];
        g_.remove(m.gid);
        model_.removeMember(m.mi);
        m.live = false;
        log_("remove member #" + std::to_string(m.mi));
    }

    std::vector<PlayerRow> rows_(std::size_t n, uint16_t version, std::vector<mm::Rec>& recs) {
        std::vector<PlayerRow> rows;
        for (std::size_t i = 0; i < n; ++i) {
            PlayerRow r;
            r.playerId = rng_() % 10;
            r.worldId = static_cast<uint32_t>(rng_() % 6);
            r.channel = static_cast<int32_t>(rng_() % 4);
            mm::Rec rec;
            rec.version = version;
            rec.playerId = r.playerId;
            rec.worldId = r.worldId;
            if (version <= 2) rec.channel = r.channel;
            const uint64_t k = rng_() % 100;
            if (k < 2) { r.tombstone = true; rec = mm::Rec{}; rec.version = version; rec.null = true; }
            else if (k < 3) { r.shortValue = static_cast<int>(rng_() % 4); rec = mm::Rec{}; rec.version = version; rec.shortValue = true; }
            rows.push_back(r);
            recs.push_back(rec);
        }
        return rows;
    }

    void publish_(uint16_t version, std::size_t n) {
        std::vector<mm::Rec> recs;
        const auto rows = rows_(n, version, recs);
        src_.append(makePlayerBatch(model_.end(), version, rows));
        src_.publishAll();
        model_.appendBatch(std::move(recs));
    }

    void appendBatch_() {
        const uint16_t v = static_cast<uint16_t>(1 + rng_() % model_.known());
        const std::size_t n = 1 + rng_() % 64;
        log_("append batch v" + std::to_string(v) + " x" + std::to_string(n) + " at " + std::to_string(model_.end()));
        publish_(v, n);
    }

    void appendUnknown_() {
        std::vector<uint16_t> cand;
        for (uint16_t v = static_cast<uint16_t>(model_.known() + 1); v <= 6; ++v)
            if (expired_.count(v) == 0) cand.push_back(v);
        if (cand.empty()) return;
        const bool scheduled = model_.known() < kMaxBuildableVersion && rng_() % 3 != 0;
        const uint16_t v = scheduled ? static_cast<uint16_t>(model_.known() + 1 + rng_() % (kMaxBuildableVersion - model_.known()))
                                     : cand.back();   // 5 or 6: no schema build ever carries it
        if (expired_.count(v) != 0) return;
        const std::size_t n = 1 + rng_() % 3;
        if (!pendingSince_.count(v)) pendingSince_[v] = now_;
        std::ostringstream o;
        o << "append UNKNOWN v" << v << " x" << n << " at " << model_.end();
        if (v <= kMaxBuildableVersion) {
            const int due = step_ + 1 + static_cast<int>(rng_() % 40);
            scheduled_.push_back({due, v});
            std::sort(scheduled_.begin(), scheduled_.end(), [](const Arrival& a, const Arrival& b) { return a.due < b.due; });
            o << " (schema arrives at step " << due << ")";
        } else {
            o << " (schema never arrives)";
        }
        log_(o.str());
        publish_(v, n);
    }

    void alter_(const char* why) {
        if (model_.known() >= kMaxBuildableVersion) return;
        const uint16_t v = static_cast<uint16_t>(model_.known() + 1);
        if (schema_.extendFrom(playerSchema(v)) != FilterSchema::ExtendResult::Extended) fail_("schema extendFrom did not extend");
        model_.schemaArrived();
        for (auto it = pendingSince_.begin(); it != pendingSince_.end();) it = it->first <= v ? pendingSince_.erase(it) : std::next(it);
        for (auto it = expired_.begin(); it != expired_.end();) it = *it <= v ? expired_.erase(it) : std::next(it);
        log_(std::string("ALTER -> v") + std::to_string(v) + " (" + why + ")");
    }

    void refuse_() {
        const auto live = liveIdx_();
        if (live.empty()) return;
        const Mem& m = mem_[live[rng_() % live.size()]];
        const uint32_t n = 1 + static_cast<uint32_t>(rng_() % 4);
        fake_.refuse(m.sub, n);
        log_("refuse next " + std::to_string(n) + " sends of member #" + std::to_string(m.mi));
    }

    void clock_() {
        if (rng_() % 10 >= 8) { expire_(); return; }
        int64_t next = now_ + static_cast<int64_t>(1 + rng_() % 5) * kNoteNs;
        for (const auto& [v, since] : pendingSince_) next = std::min(next, since + kWaitNs - 1);
        next = std::max(next, now_);
        log_("clock +" + std::to_string(next - now_) + " ns");
        now_ = next;
    }

    /// A whole version wait.  With nothing unknown pending it is a plain advance; otherwise it is the controlled
    /// expiry described in the file header.
    void expire_() {
        if (pendingSince_.empty()) {
            now_ += kWaitNs;
            log_("clock +version wait (nothing pending)");
            return;
        }
        settle_();   // every stall is observed at the current clock
        ++expiries_;
        model_.expireUnresolved();
        for (const auto& [v, since] : pendingSince_) expired_.insert(v);
        pendingSince_.clear();
        std::ostringstream o;
        o << "EXPIRE: the version wait runs out for unknown versions {";
        for (const uint16_t v : expired_) o << " v" << v;
        o << " }";
        log_(o.str());
        for (int round = 0; round < 16; ++round) {
            now_ += kWaitNs;
            settle_();
            if (allAtEnd_()) return;
        }
        fail_("expire: a member or the scan never passed the unknown-version records");
    }

    void pass_() {
        static constexpr std::size_t kBudgets[] = {256, 2'048, 16'384, std::size_t{1} << 20, kBigBudget};
        const std::size_t b = kBudgets[rng_() % 5];
        log_("pass (budget " + std::to_string(b) + ")");
        doPass_(b);
    }

    // ---- passes and checks ----
    int doPass_(std::size_t budgetBytes) {
        std::size_t budget = budgetBytes;
        int work;
        if (measuring_) {
            rune::testing::AllocWindow w;
            work = g_.pass(src_, sink_, schema_, sc_, now_, budget, true);
            allocs_ += w.count();
            ++measuredPasses_;
        } else {
            work = g_.pass(src_, sink_, schema_, sc_, now_, budget, true);
        }
        verify_();
        return work;
    }

    /// Passes at a fixed clock until three in a row neither work nor attempt a send (a refused send is an attempt).
    void settle_() {
        int quiet = 0;
        for (int i = 0; i < 6000; ++i) {
            const uint64_t calls = fake_.calls;
            const int work = doPass_(kBigBudget);
            quiet = (work == 0 && fake_.calls == calls) ? quiet + 1 : 0;
            if (quiet >= 3) return;
        }
        fail_("settle: still active after 6000 passes (a hang)");
    }

    [[nodiscard]] bool allAtEnd_() {
        for (const Mem& m : mem_)
            if (m.live && g_.cursorOf(m.gid, fake_.cursor(m.sub)) != model_.end()) return false;
        return true;
    }

    void verify_() {
        if (sink_.overflow_) fail_("harness: the sink buffers overflowed (raise their capacity)");
        // 1. Fold the pass's deliveries into the members, checking order and content as they arrive.
        for (const auto& s : sink_.sent_) {
            Mem* m = memOfSub_(s.sub);
            if (m == nullptr || !m->live) fail_("a frame went to sub " + std::to_string(s.sub) + ", which has no live member");
            if (measuring_) ++measuredDeliveries_;
            if (s.off < m->start) continue;   // the straddling first batch: records before the member's start
            if (!m->got.empty() && s.off <= m->got.back())
                fail_("member #" + std::to_string(m->mi) + " (" + m->desc + "): duplicate or reordered offset " + std::to_string(s.off) +
                      " after " + std::to_string(m->got.back()));
            const auto& exp = model_.expected(m->mi);
            if (m->got.size() >= exp.size() || exp[m->got.size()] != s.off)
                fail_("member #" + std::to_string(m->mi) + " (" + m->desc + "): got offset " + std::to_string(s.off) + ", the model expects " +
                      (m->got.size() < exp.size() ? std::to_string(exp[m->got.size()]) : std::string("nothing more")) + " as delivery #" +
                      std::to_string(m->got.size()));
            m->got.push_back(s.off);
        }
        sink_.sent_.clear();
        for (const auto& r : sink_.refused_) if (Mem* m = memOfSub_(r.sub)) m->refusedBases.push_back(r.base);
        sink_.refused_.clear();
        // 2. Prefix: everything the model expects below a member's position has been delivered.
        for (Mem& m : mem_) {
            if (!m.live) continue;
            const int64_t pos = g_.cursorOf(m.gid, fake_.cursor(m.sub));
            const auto& exp = model_.expected(m.mi);
            const std::size_t due = static_cast<std::size_t>(std::lower_bound(exp.begin(), exp.end(), pos) - exp.begin());
            if (m.got.size() != due)
                fail_("member #" + std::to_string(m.mi) + " (" + m.desc + "): position " + std::to_string(pos) + " but " +
                      std::to_string(m.got.size()) + " deliveries, the model has " + std::to_string(due) + " matching records below it");
        }
    }

    // ---- the end ----
    void drain_() {
        step_ = kSteps;
        measuring_ = true;
        log_("DRAIN: remaining schema arrivals, settle, final expiry, a note interval");
        while (!scheduled_.empty()) {
            const uint16_t v = scheduled_.front().version;
            scheduled_.erase(scheduled_.begin());
            while (model_.known() < v) alter_("final arrival");
        }
        settle_();
        if (!pendingSince_.empty()) expire_();
        now_ += 3 * kNoteNs;
        settle_();
        // 1. Positions, deliveries.
        for (const Mem& m : mem_) {
            if (!m.live) continue;
            const int64_t pos = g_.cursorOf(m.gid, fake_.cursor(m.sub));
            if (pos != model_.end())
                fail_("member #" + std::to_string(m.mi) + " (" + m.desc + "): position " + std::to_string(pos) + " never reached the end " + std::to_string(model_.end()));
            const auto& exp = model_.expected(m.mi);
            if (m.got.size() != exp.size() || !std::equal(m.got.begin(), m.got.end(), exp.begin()))
                fail_("member #" + std::to_string(m.mi) + " (" + m.desc + "): delivered " + std::to_string(m.got.size()) + " records, the model expects " +
                      std::to_string(exp.size()));
        }
        // 2. Counters: inside the model's ranges, equal when the range is a point.
        std::vector<std::vector<int64_t>> retries;
        for (const Mem& m : mem_) retries.push_back(m.refusedBases);
        auto inRange = [&](const char* what, uint64_t got, mm::Bounds b, bool exact = false) {
            if (exact && (b.lo != b.hi || got != b.lo))
                fail_(std::string("counter ") + what + " = " + std::to_string(got) + ", the model's exact value is " + std::to_string(b.lo) +
                      (b.lo != b.hi ? " (model range not a point: " + std::to_string(b.hi) + ")" : std::string()));
            if (got < b.lo || got > b.hi)
                fail_(std::string("counter ") + what + " = " + std::to_string(got) + ", the model's range is [" + std::to_string(b.lo) + ", " +
                      std::to_string(b.hi) + "]");
            ++rangesChecked_;
            if (got != 0) ++nonZeroChecked_;
            if (b.lo == b.hi) ++pointsChecked_;
        };
        for (const Mem& m : mem_)
            if (m.live) inRange(("absent of member #" + std::to_string(m.mi) + " (" + m.desc + ")").c_str(), g_.countersOf(m.gid).absentExcluded,
                                model_.absentBounds(m.mi, m.refusedBases));
        auto total = [&](auto field) {
            uint64_t t = field(g_.groupCounters());
            for (const Mem& m : mem_) if (m.live) t += field(g_.countersOf(m.gid));
            return t;
        };
        inRange("unknownVersion", total([](const FilterCounters& c) { return c.unknownVersionExcluded; }), model_.totalBounds(mm::Cat::Unknown, retries), solo_);
        inRange("null", total([](const FilterCounters& c) { return c.nullValueExcluded; }), model_.totalBounds(mm::Cat::Null, retries), solo_);
        inRange("malformed", total([](const FilterCounters& c) { return c.malformed; }), model_.totalBounds(mm::Cat::Malformed, retries), solo_);
        // 3. The measured passes allocated nothing, and there were some.
        if (allocs_ != 0) fail_(std::to_string(allocs_) + " heap allocations inside " + std::to_string(measuredPasses_) + " measured passes");
        if (measuredPasses_ == 0) fail_("no measured pass ran");
    }

    // ---- helpers ----
    [[nodiscard]] std::size_t liveCount_() const {
        std::size_t n = 0;
        for (const Mem& m : mem_) n += m.live ? 1 : 0;
        return n;
    }
    [[nodiscard]] std::vector<std::size_t> liveIdx_() const {
        std::vector<std::size_t> v;
        for (std::size_t i = 0; i < mem_.size(); ++i) if (mem_[i].live) v.push_back(i);
        return v;
    }
    Mem* memOfSub_(uint32_t sub) {
        for (Mem& m : mem_) if (m.sub == sub) return &m;
        return nullptr;
    }
    /// The end of the last batch holding a record of a still-unknown version whose wait already ran out (a new member
    /// starts at or after it: it would read that batch whole and stall afresh).
    [[nodiscard]] int64_t expiredClamp_() const {
        int64_t c = 0;
        for (const auto& r : model_.records())
            if (model_.unresolved(r) && expired_.count(r.version) != 0) c = model_.batchEnd(r.offset);
        return c;
    }

    struct Arrival { int due; uint16_t version; };

    uint32_t        seed_;
    std::mt19937_64 rng_;
    FilterConfig    cfg_;
    FilterGroup     g_;
    FakeSource      src_;
    FakeSink        fake_;
    StressSink      sink_;
    FilterScratch   sc_;
    FilterSchema    schema_;
    mm::Model       model_;
    int64_t         now_ = kT0;
    int             step_ = 0;
    bool            measuring_ = false;
    uint32_t        nextSub_ = 0;
    bool            solo_ = false;   // one member for the whole run: every counter but "absent" must then be exact
    std::vector<Mem> mem_;
    std::vector<Arrival> scheduled_;
    std::map<uint16_t, int64_t> pendingSince_;   // unknown versions, not yet waited out: version -> earliest append
    std::set<uint16_t> expired_;                 // unknown versions whose wait ran out (no more appends of them)
    std::vector<std::string> steps_;
    uint64_t allocs_ = 0, measuredPasses_ = 0, measuredDeliveries_ = 0, rangesChecked_ = 0, pointsChecked_ = 0, nonZeroChecked_ = 0, expiries_ = 0;
};

std::vector<uint32_t> seeds() {
    const char* v = std::getenv("RUNE_FILTER_STRESS_SEEDS");
    const int n = v != nullptr && *v != '\0' ? std::max(1, std::atoi(v)) : 64;
    std::vector<uint32_t> s;
    for (int i = 1; i <= n; ++i) s.push_back(static_cast<uint32_t>(i));
    return s;
}

struct QuietLogs {   // the engine's cold first-occurrence log lines allocate; the measured passes are about the record path
    spdlog::level::level_enum old = spdlog::get_level();
    QuietLogs() { spdlog::set_level(spdlog::level::off); }
    ~QuietLogs() { spdlog::set_level(old); }
};

}  // namespace

class FilterStressRegression : public ::testing::TestWithParam<uint32_t> {};

TEST_P(FilterStressRegression, SeededRunsMatchTheModelRegression) {
    QuietLogs quiet;
    Seed s(GetParam());
    const std::string failure = s.run();
    ASSERT_TRUE(failure.empty()) << failure;
    if (std::getenv("RUNE_FILTER_STRESS_STATS") != nullptr) std::printf("[stress seed %u] %s\n", GetParam(), s.stats().c_str());
    EXPECT_GT(s.measuredPasses(), 100u) << "seed " << GetParam();
    EXPECT_EQ(s.allocations(), 0u) << "seed " << GetParam();
}

INSTANTIATE_TEST_SUITE_P(Seeds, FilterStressRegression, ::testing::ValuesIn(seeds()));
