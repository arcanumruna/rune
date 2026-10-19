// StorageGateTest.cpp - the storage gate's transition table (ISR design v3, section 3.6, Step 9).
#include <gtest/gtest.h>

#include <rune/metadata/MetadataExtensions.h>
#include <rune/metadata/MetadataStateMachine.h>
#include <rune/node/StorageGate.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>

using namespace rune::node;
namespace fs = std::filesystem;
using Origin = StorageIdentity::Origin;
using Mode   = StorageIdentity::Mode;
using Change = StorageGate::Change;

namespace {

struct GateDir {
    fs::path path;
    explicit GateDir(const std::string& tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_gate_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~GateDir() { std::error_code ec; fs::remove_all(path, ec); }
    std::string meta() const {
        std::ifstream in(path / "storage.meta");
        std::stringstream ss;
        ss << in.rdbuf();
        return ss.str();
    }
};

constexpr int32_t kSelf = 2;
constexpr int64_t kId   = 0x1111;

/// An MSM that committed `NodeStorageRecord{kSelf, c}` (nothing when c == 0).
rune::metadata::MetadataStateMachine msmWith(int64_t c) {
    rune::metadata::MetadataStateMachine msm;
    if (c != 0) {
        uint8_t b[32];
        rune::metadata::NodeStorageRecord r;
        r.node = kSelf; r.storageId = c;
        const std::size_t n = r.encode(b, sizeof b);
        msm.apply(b, n, 0);
        msm.commit(0);
    }
    return msm;
}

enum class C { Zero, Own, Other };

struct Row {
    const char* name;
    Origin      origin;
    Mode        mode;
    C           committed;
    Evidence    ev;
    Change      expect;
    int         stateCode;        // 0 Confirmed, 1 Unconfirmed, 2 Undecided
    const char* originOnDisk;     // the `origin=` value storage.meta must say afterwards
};

void PrintTo(const Row& r, std::ostream* os) { *os << r.name; }

class StorageGateRegression : public ::testing::TestWithParam<Row> {};

TEST_P(StorageGateRegression, TransitionRowsRegression) {
    const Row& r = GetParam();
    GateDir d(r.name);
    StorageIdentity::Loaded l{kId, r.origin, r.origin == Origin::Undecided ? r.mode : Mode::None, 0};
    StorageIdentity::write_(d.path.string(), l);
    StorageGate gate(l, d.path.string(), kSelf, 3);
    const int64_t c = r.committed == C::Zero ? 0 : r.committed == C::Own ? kId : 0x2222;
    const auto msm = msmWith(c);
    EXPECT_EQ(gate.evaluate(msm, r.ev, 1'000'000'000), r.expect);
    EXPECT_EQ(gate.stateCode(), r.stateCode);
    EXPECT_EQ(gate.mayStand(), r.stateCode == 0) << "only a CONFIRMED storage may stand";
    EXPECT_EQ(gate.mayVote(), r.stateCode == 0) << "only a CONFIRMED storage may vote";
    EXPECT_NE(d.meta().find(std::string("origin=") + r.originOnDisk), std::string::npos) << d.meta();
}

Evidence ev(int fresh, int members, bool history = false, int64_t maxTerm = 0) {
    Evidence e;
    e.heardFresh = fresh; e.members = members; e.anyHistory = history; e.maxPeerTerm = maxTerm;
    e.peerWithHistory = history || maxTerm > 0 ? 1 : -1;
    return e;
}

INSTANTIATE_TEST_SUITE_P(Table, StorageGateRegression, ::testing::Values(
    Row{"undecided_auto_nobody_heard",      Origin::Undecided, Mode::Auto, C::Zero,  ev(0, 3),           Change::none,        2, "undecided"},
    Row{"undecided_new_one_fresh_founds",   Origin::Undecided, Mode::New,  C::Zero,  ev(1, 3),           Change::founded,     0, "founder"},
    Row{"undecided_auto_history_joins",     Origin::Undecided, Mode::Auto, C::Zero,  ev(0, 3, true),     Change::joined,      1, "joiner"},
    Row{"undecided_new_history_joins",      Origin::Undecided, Mode::New,  C::Zero,  ev(0, 3, true),     Change::joined,      1, "joiner"},
    Row{"undecided_auto_peer_term_joins",   Origin::Undecided, Mode::Auto, C::Zero,  ev(2, 3, false, 4), Change::joined,      1, "joiner"},
    Row{"joiner_nothing_committed",         Origin::Joiner,    Mode::None, C::Zero,  ev(0, 3),           Change::none,        1, "joiner"},
    Row{"joiner_own_committed_confirms",    Origin::Joiner,    Mode::None, C::Own,   ev(0, 3),           Change::confirmed,   0, "joiner"},
    Row{"founder_nothing_committed",        Origin::Founder,   Mode::None, C::Zero,  ev(0, 3),           Change::none,        0, "founder"},
    Row{"founder_other_committed",          Origin::Founder,   Mode::None, C::Other, ev(0, 3),           Change::unconfirmed, 1, "founder"}
));

// A Legacy storage is trusted on first use like a Founder; a failed persist leaves the gate undecided.
TEST(StorageGateRegression, LegacyIsConfirmedWithNothingCommittedRegression) {
    GateDir d("legacy");
    StorageIdentity::Loaded l{kId, Origin::Legacy, Mode::None, 0};
    StorageIdentity::write_(d.path.string(), l);
    StorageGate gate(l, d.path.string(), kSelf, 3);
    EXPECT_TRUE(gate.open());
    EXPECT_EQ(gate.originWireCode(), 2);
    EXPECT_EQ(gate.evaluate(msmWith(0), ev(0, 3), 1), Change::none);
}

// The origin is persisted BEFORE the state flips: an unwritable dir leaves the gate Undecided and the object unchanged.
TEST(StorageGateRegression, OriginIsPersistedBeforeTheStateFlipsRegression) {
    GateDir d("persist");
    StorageIdentity::Loaded l{kId, Origin::Undecided, Mode::New, 0};
    StorageIdentity::write_(d.path.string(), l);
    StorageGate gate(l, (d.path / "gone").string(), kSelf, 3);   // the dir does not exist: the write fails
    EXPECT_EQ(gate.evaluate(msmWith(0), ev(1, 3), 1), Change::none);
    EXPECT_EQ(gate.stateCode(), 2);
    EXPECT_EQ(l.origin, Origin::Undecided);
}

// C1 (CP2 review): a fresh quorum must persist for the founding dwell before the gate founds; history seen inside the
// dwell wins; losing the quorum resets the timer.  RED: found at once (no dwell).
TEST(StorageGateRegression, FoundingWaitsAFullDwellRegression) {
    constexpr int64_t ms = 1'000'000;
    const auto mk = [&](const char* tag, GateDir& d, StorageIdentity::Loaded& l) {
        l = StorageIdentity::Loaded{kId, Origin::Undecided, Mode::Auto, 0};
        StorageIdentity::write_(d.path.string(), l);
        return std::make_unique<StorageGate>(l, d.path.string(), kSelf, 3, 300 * ms);
        (void)tag;
    };
    const auto msm = msmWith(0);
    {
        GateDir d("dwell1"); StorageIdentity::Loaded l; auto g = mk("a", d, l);
        EXPECT_EQ(g->evaluate(msm, ev(1, 3), 1 * ms), Change::none);
        EXPECT_EQ(g->evaluate(msm, ev(1, 3), 200 * ms), Change::none);
        EXPECT_EQ(g->stateCode(), 2);
        EXPECT_EQ(g->evaluate(msm, ev(1, 3), 302 * ms), Change::founded);
    }
    {
        GateDir d("dwell2"); StorageIdentity::Loaded l; auto g = mk("b", d, l);
        EXPECT_EQ(g->evaluate(msm, ev(1, 3), 1 * ms), Change::none);
        EXPECT_EQ(g->evaluate(msm, ev(1, 3, true), 100 * ms), Change::joined);
    }
    {
        GateDir d("dwell3"); StorageIdentity::Loaded l; auto g = mk("c", d, l);
        EXPECT_EQ(g->evaluate(msm, ev(1, 3), 1 * ms), Change::none);
        EXPECT_EQ(g->evaluate(msm, ev(0, 3), 100 * ms), Change::none);   // quorum lost: the timer resets
        EXPECT_EQ(g->evaluate(msm, ev(1, 3), 150 * ms), Change::none);
        EXPECT_EQ(g->evaluate(msm, ev(1, 3), 400 * ms), Change::none);   // only 250 ms since the reset
        EXPECT_EQ(g->evaluate(msm, ev(1, 3), 451 * ms), Change::founded);
    }
}

} // namespace
