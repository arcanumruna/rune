#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <random>

#include "rune/node/GrantHoldMarks.h"

using rune::node::GrantHoldMarks;
using V = GrantHoldMarks::Verdict;

TEST(GrantHoldMarksRegression, ADrainedRingAnswersAtOnceRegression) {
    GrantHoldMarks m;
    EXPECT_EQ(m.onDenied(1, 10, 100, 100), V::Answer);
    EXPECT_EQ(m.size(), 0u);
}

TEST(GrantHoldMarksRegression, AHeldFrameIsAnsweredOnceItsOwnBacklogIsAppliedRegression) {
    GrantHoldMarks m;
    EXPECT_EQ(m.onDenied(1, 10, 200, 100), V::Hold);
    EXPECT_EQ(m.size(), 1u);
    EXPECT_EQ(m.onDenied(1, 10, 900, 200), V::Answer);   // the ring is busy again, but what preceded the frame is applied
    EXPECT_EQ(m.size(), 0u);
}

TEST(GrantHoldMarksRegression, AHeldFrameStaysHeldUntilItsBacklogIsAppliedRegression) {
    GrantHoldMarks m;
    EXPECT_EQ(m.onDenied(1, 10, 200, 100), V::Hold);
    EXPECT_EQ(m.onDenied(1, 10, 300, 199), V::Hold);
    EXPECT_EQ(m.onDenied(1, 10, 300, 200), V::Answer);
}

TEST(GrantHoldMarksRegression, AnotherFrameOfTheSessionStartsItsOwnMarkRegression) {
    GrantHoldMarks m;
    EXPECT_EQ(m.onDenied(1, 10, 200, 100), V::Hold);
    EXPECT_EQ(m.onDenied(1, 11, 500, 300), V::Hold);   // its own target 500, although 300 >= 200
    EXPECT_EQ(m.onDenied(1, 11, 600, 500), V::Answer);
}

TEST(GrantHoldMarksRegression, AFullTableFallsBackToTheOldRuleRegression) {
    GrantHoldMarks m;
    for (int64_t s = 1; s <= 64; ++s) EXPECT_EQ(m.onDenied(s, 10 + s, 200, 100), V::Hold);
    EXPECT_EQ(m.size(), 64u);
    EXPECT_EQ(m.onDenied(65, 75, 200, 100), V::Hold);   // nothing due, nothing replaceable
    EXPECT_EQ(m.unmarkedHolds(), 1u);
    EXPECT_EQ(m.size(), 64u);
    EXPECT_EQ(m.onDenied(65, 75, 300, 200), V::Hold);   // replaces a due mark
    EXPECT_EQ(m.size(), 64u);
    EXPECT_EQ(m.onDenied(65, 75, 400, 300), V::Answer);
    // Session 1's mark was the first due one (swap-erase kept order for the scan), so it was the one replaced.
    EXPECT_EQ(m.onDenied(1, 11, 400, 300), V::Hold);
    EXPECT_EQ(m.onDenied(1, 11, 500, 400), V::Answer);
}

TEST(GrantHoldMarksRegression, ModelCheckedAgainstFirstSightPositionsRegression) {
    GrantHoldMarks m;
    std::mt19937_64 rng(0x201);
    struct Seen { int64_t corr; int64_t firstProducer; };
    std::map<int64_t, Seen> model;
    int64_t producer = 0, consumer = 0;
    for (int step = 0; step < 20000; ++step) {
        producer += static_cast<int64_t>(rng() % 3);
        if (consumer < producer) consumer += static_cast<int64_t>(rng() % 3);
        if (consumer > producer) consumer = producer;
        const int64_t sid = static_cast<int64_t>(rng() % 40);
        const int64_t corr = static_cast<int64_t>(rng() % 3);
        bool expectAnswer;
        auto it = model.find(sid);
        if (it != model.end() && it->second.corr == corr) {
            expectAnswer = consumer >= it->second.firstProducer;
            if (expectAnswer) model.erase(it);
        } else {
            if (it != model.end()) model.erase(it);
            expectAnswer = producer <= consumer;
            if (!expectAnswer) model[sid] = Seen{corr, producer};
        }
        ASSERT_EQ(m.onDenied(sid, corr, producer, consumer) == V::Answer, expectAnswer) << "step " << step;
    }
    EXPECT_EQ(m.unmarkedHolds(), 0u);
}
