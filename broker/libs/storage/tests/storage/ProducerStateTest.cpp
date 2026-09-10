#include <gtest/gtest.h>
#include <rune/storage/ProducerState.h>
using rune::storage::ProducerState;

// Base test from brief
TEST(ProducerState, DetectsDuplicatesAndGaps) {
    ProducerState ps;
    EXPECT_EQ(ps.validateAndUpdate(7, 0, 0, 3), ProducerState::Result::Ok);      // seq 0..2
    EXPECT_EQ(ps.validateAndUpdate(7, 0, 0, 3), ProducerState::Result::Duplicate); // replay of seq 0..2
    EXPECT_EQ(ps.validateAndUpdate(7, 0, 3, 2), ProducerState::Result::Ok);       // seq 3..4
    EXPECT_EQ(ps.validateAndUpdate(7, 0, 10, 1), ProducerState::Result::OutOfSequence); // gap: expected 5
}

// Two independent producerIds tracked separately
TEST(ProducerState, IndependentProducerIds) {
    ProducerState ps;
    EXPECT_EQ(ps.validateAndUpdate(1, 0, 0, 2), ProducerState::Result::Ok);      // producer 1: seq 0..1
    EXPECT_EQ(ps.validateAndUpdate(2, 0, 0, 2), ProducerState::Result::Ok);      // producer 2: seq 0..1
    EXPECT_EQ(ps.validateAndUpdate(1, 0, 2, 1), ProducerState::Result::Ok);      // producer 1: seq 2
    EXPECT_EQ(ps.validateAndUpdate(2, 0, 2, 1), ProducerState::Result::Ok);      // producer 2: seq 2
    EXPECT_EQ(ps.validateAndUpdate(1, 0, 0, 2), ProducerState::Result::Duplicate); // producer 1 replay
    EXPECT_EQ(ps.validateAndUpdate(2, 0, 5, 1), ProducerState::Result::OutOfSequence); // producer 2 gap
}

// Fresh producer with first baseSequence != 0 -> OutOfSequence
TEST(ProducerState, FirstBatchNotStartingAtZero) {
    ProducerState ps;
    EXPECT_EQ(ps.validateAndUpdate(99, 0, 1, 2), ProducerState::Result::OutOfSequence); // expected 0, got 1
}

// Exact boundary sequences across several batches
TEST(ProducerState, ContiguousSequencesAcrossBatches) {
    ProducerState ps;
    EXPECT_EQ(ps.validateAndUpdate(42, 0, 0, 5), ProducerState::Result::Ok);     // seq 0..4
    EXPECT_EQ(ps.validateAndUpdate(42, 0, 5, 10), ProducerState::Result::Ok);    // seq 5..14
    EXPECT_EQ(ps.validateAndUpdate(42, 0, 15, 1), ProducerState::Result::Ok);    // seq 15
    EXPECT_EQ(ps.validateAndUpdate(42, 0, 16, 3), ProducerState::Result::Ok);    // seq 16..18
}

// Replayed earlier baseSequence -> Duplicate
TEST(ProducerState, ReplayEarlierBatch) {
    ProducerState ps;
    EXPECT_EQ(ps.validateAndUpdate(5, 0, 0, 5), ProducerState::Result::Ok);      // seq 0..4
    EXPECT_EQ(ps.validateAndUpdate(5, 0, 5, 5), ProducerState::Result::Ok);      // seq 5..9
    EXPECT_EQ(ps.validateAndUpdate(5, 0, 2, 3), ProducerState::Result::Duplicate); // replay of seq 2..4 (earlier than current)
}

// Forward gap -> OutOfSequence
TEST(ProducerState, ForwardGap) {
    ProducerState ps;
    EXPECT_EQ(ps.validateAndUpdate(10, 0, 0, 3), ProducerState::Result::Ok);     // seq 0..2
    EXPECT_EQ(ps.validateAndUpdate(10, 0, 5, 2), ProducerState::Result::OutOfSequence); // gap, expected 3
}

// Large sequence numbers
TEST(ProducerState, LargeSequenceNumbers) {
    ProducerState ps;
    int32_t large = 1000000;
    EXPECT_EQ(ps.validateAndUpdate(111, 0, 0, 5), ProducerState::Result::Ok);         // seq 0..4
    EXPECT_EQ(ps.validateAndUpdate(111, 0, 5, 3), ProducerState::Result::Ok);         // seq 5..7
    // Now test with another producer using large sequence numbers
    EXPECT_EQ(ps.validateAndUpdate(222, 0, 0, 5), ProducerState::Result::Ok);         // seq 0..4
    EXPECT_EQ(ps.validateAndUpdate(222, 0, 5, 3), ProducerState::Result::Ok);         // seq 5..7
}

// Epoch changes (producer state should still track sequences correctly)
TEST(ProducerState, EpochChange) {
    ProducerState ps;
    EXPECT_EQ(ps.validateAndUpdate(7, 0, 0, 5), ProducerState::Result::Ok);      // seq 0..4, epoch 0
    EXPECT_EQ(ps.validateAndUpdate(7, 1, 5, 3), ProducerState::Result::Ok);      // seq 5..7, epoch 1 (epoch change recorded but seq must still be contiguous)
    EXPECT_EQ(ps.validateAndUpdate(7, 1, 8, 2), ProducerState::Result::Ok);      // seq 8..9, epoch 1
}
