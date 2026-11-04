// NamespaceTestHygieneRegressionTest.cpp - issue #648: the namespace rigs and the stress test wait on the awaited event
// (a listing, an offset, an erased id), never on a timer, and never resend a probe whose answer is missing.  A source
// scan, no broker: it fails on a `pollFor(<window>, [] { return false; })` used as a sleep and on a `withRetry` resend.
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#ifndef RUNE_NODE_TESTS_DIR
#error "RUNE_NODE_TESTS_DIR must point at broker/libs/node/tests"
#endif

namespace {

std::string slurp(const std::string& relative) {
    const std::filesystem::path p = std::filesystem::path(RUNE_NODE_TESTS_DIR) / relative;
    std::ifstream in(p);
    EXPECT_TRUE(in.good()) << "cannot read " << p.string();
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

std::size_t lineOf(const std::string& text, std::size_t pos) {
    return 1 + static_cast<std::size_t>(std::count(text.begin(), text.begin() + static_cast<std::ptrdiff_t>(pos), '\n'));
}

/// Every match of `re` in `text`, as "file:line: <match>".
std::vector<std::string> findAll(const std::string& file, const std::string& text, const std::regex& re) {
    std::vector<std::string> out;
    for (auto it = std::sregex_iterator(text.begin(), text.end(), re); it != std::sregex_iterator(); ++it)
        out.push_back(file + ":" + std::to_string(lineOf(text, static_cast<std::size_t>(it->position()))) + ": " + it->str());
    return out;
}

/// `pollFor(<window>, [] { return false; })`: a poll whose predicate never holds is a sleep.
const std::regex& sleepPoll() {
    static const std::regex re(R"(pollFor\([^;]*?\[\s*\]\s*\{\s*return\s+false\s*;\s*\}\s*\))");
    return re;
}
const std::regex& resend() {
    static const std::regex re(R"(\bwithRetry\b)");
    return re;
}

}  // namespace

TEST(NamespaceTestHygieneRegression, NoSleepPollsOrResendsRegression) {
    // the scan itself is not vacuous: it flags the bad shapes
    EXPECT_EQ(findAll("sample", "w.r.c().pollFor(std::chrono::milliseconds(25), [] { return false; });", sleepPoll()).size(), 1u);
    EXPECT_EQ(findAll("sample", "pollFor(10ms,\n   []{ return false ; })", sleepPoll()).size(), 1u);
    EXPECT_EQ(findAll("sample", "pollFor(10s, [&] { return found; })", sleepPoll()).size(), 0u);
    EXPECT_EQ(findAll("sample", "Answer a = withRetry(user, f);", resend()).size(), 1u);

    for (const char* file : {"support/NamespaceRig.h", "node/NamespaceStressTest.cpp"}) {
        const std::string text = slurp(file);
        ASSERT_FALSE(text.empty()) << file;
        for (const std::string& hit : findAll(file, text, sleepPoll()))
            ADD_FAILURE() << "a pollFor used as a sleep (poll on the awaited event instead): " << hit;
        for (const std::string& hit : findAll(file, text, resend()))
            ADD_FAILURE() << "a probe resent after a missing answer (a missing answer fails the step): " << hit;
    }
}

// #711: the lifecycle rig and the stress test wait with the client's pollFor, never with a wall-clock sleep.  (The
// matrix `waitFor` and the trio World `call`/`burst` pace external client threads and keep their bounded sleeps.)
TEST(NamespaceTestHygieneRegression, NoSleepForInLifecycleTestsRegression) {
    const std::regex sleeps(R"(\bsleep_for\s*\()");
    EXPECT_EQ(findAll("sample", "std::this_thread::sleep_for(2ms);", sleeps).size(), 1u);
    for (const char* file : {"support/NamespaceLifecycleRig.h", "support/NamespaceRig.h", "node/NamespaceStressTest.cpp"}) {
        const std::string text = slurp(file);
        ASSERT_FALSE(text.empty()) << file;
        for (const std::string& hit : findAll(file, text, sleeps))
            ADD_FAILURE() << "a wall-clock sleep (poll on the awaited event with pollFor): " << hit;
    }
}
