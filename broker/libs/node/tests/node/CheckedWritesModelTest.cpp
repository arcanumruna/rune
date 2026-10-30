// Checked writes Part 04 Task 11 (rig slice): a seeded random history of produces, leader changes and restarts against an
// independent model.  The model knows only the rule (tag 9 is rejected, from the first produce on) and the produced order.
// After every step the worker must show exactly the model's accepted records to a consumer and exactly its accepted keys in
// an ordinary view.  The cluster stress (ThreeNodes) and the 585-cell matrix of the plan are not part of this file.
#include "CheckedWritesRig.h"

#include <algorithm>
#include <map>
#include <random>
#include <set>

using namespace rune::node;
using namespace rune::node::cw;

namespace {
constexpr int32_t kRej = 0x40000601, kProj = 0x40000602;
constexpr uint64_t kObj = 7;

struct Model {
    std::vector<std::pair<uint64_t, uint16_t>> accepted;   // (id, tag) in produced order
    std::set<uint64_t> keys;
    void produce(const std::vector<Rec>& recs) {
        for (const auto& r : recs)
            if (r.tag != 9) { accepted.emplace_back(r.id, r.tag); keys.insert(r.id); }
    }
};

struct History {
    VerdictRig rig;
    Model model;
    std::string trace;
    uint64_t hash = 1469598103934665603ull;
    void note(const std::string& s) {
        trace += s + ";";
        for (const char c : s) hash = (hash ^ static_cast<uint8_t>(c)) * 1099511628211ull;
    }
};

std::string check(History& run, int64_t& cursorSub) {
    auto& rig = run.rig;
    rig.passes(220);   // the judge, the refold and the markers settle
    const auto& box = rig.collect(cursorSub);
    std::vector<std::pair<uint64_t, uint16_t>> got;
    for (const auto& d : box.records) got.emplace_back(d.id, d.tag);
    if (got != run.model.accepted) return "consumer saw " + std::to_string(got.size()) + " records, model accepted " + std::to_string(run.model.accepted.size());
    for (uint64_t k = 1; k <= 20; ++k)
        if (rig.hasRow(kProj, k) != (run.model.keys.count(k) != 0)) return "projection row " + std::to_string(k) + " disagrees with the model";
    for (const auto t : rig.controlTypes()) (void)t;
    return {};
}
}  // namespace

TEST(CheckedWritesModelRegression, SeededRigMatchesTheModel) {
    for (uint64_t seed = 1; seed <= 200; ++seed) {
        std::mt19937_64 rng(seed);
        History run;
        run.rig.typed();
        TestRejectRule rule;
        rule.mode = TestRejectRule::Mode::TagEquals;
        rule.tag = 9;
        run.rig.installRejectView(kRej, kObj, rule);
        run.rig.installProjection(kProj);
        run.rig.passes(60);
        int64_t sub = run.rig.subscribe(SubKind::PushRead, 0);
        for (int step = 0; step < 20; ++step) {
            SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step) + " trace=" + run.trace);
            const int action = static_cast<int>(rng() % 10);
            if (action < 7) {
                std::vector<Rec> recs;
                const int n = 1 + static_cast<int>(rng() % 6);
                for (int i = 0; i < n; ++i) recs.push_back({1 + rng() % 20, static_cast<int64_t>(rng() % 100), static_cast<uint8_t>(rng() % 3 == 0 ? 9 : 1 + rng() % 2)});
                ASSERT_EQ(run.rig.produce(recs), 0);
                run.model.produce(recs);
                run.note("produce" + std::to_string(n));
            } else if (action < 9) {
                run.rig.demotePromote();
                run.note("failover");
            } else {
                run.rig.restart(true);
                run.note("restart");
            }
            const std::string why = check(run, sub);
            ASSERT_TRUE(why.empty()) << why;
        }
    }
}

TEST(CheckedWritesModelRegression, SameSeedSameTrace) {
    uint64_t h[2] = {0, 0};
    for (int pass = 0; pass < 2; ++pass) {
        std::mt19937_64 rng(3);
        History run;
        for (int step = 0; step < 20; ++step) run.note("a" + std::to_string(rng() % 10));
        h[pass] = run.hash;
    }
    EXPECT_EQ(h[0], h[1]);
}
