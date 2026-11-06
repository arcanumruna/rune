// Part 07 Task 7: the checkpoint file splitter knows v3 records.
#include <gtest/gtest.h>

#include <array>
#include <vector>

#include <rune/node/ViewCheckpointScan.h>
#include <rune/view/ViewCheckpoint.h>

using namespace rune::view;

namespace {

std::vector<uint8_t> v1Record(int64_t O) {
    const uint8_t row[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    const DirtyEntry e{0, false, 5, row};
    std::vector<uint8_t> out;
    encodeDelta({&e, 1}, O, 8, out);
    return out;
}

std::vector<uint8_t> v3Record(int64_t O) {
    const uint8_t row[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    const std::array<uint8_t, 24> key{7};
    const KeyedDirtyEntry up{0, false, key, row};
    const KeyedDirtyEntry del{1, true, key, nullptr};
    const KeyedDirtyEntry es[] = {del, up};
    const KeyedTableDelta tds[] = {{0, 24, 8, es}, {1, 24, 8, {}}};
    std::vector<uint8_t> out;
    encodeKeyedDelta(tds, O, out);
    return out;
}

}  // namespace

TEST(CheckedWritesViewSet, SplitterAcceptsV3) {
    const auto a = v1Record(1), b = v3Record(2), c = v3Record(3);
    std::vector<uint8_t> file;
    file.insert(file.end(), a.begin(), a.end());
    file.insert(file.end(), b.begin(), b.end());
    file.insert(file.end(), c.begin(), c.end() - 3);   // the last v3 record is torn

    const auto recs = rune::node::splitViewCheckpointRecords(file);
    ASSERT_EQ(recs.size(), 2u);
    EXPECT_EQ(recs[0].size(), a.size());
    EXPECT_EQ(recs[1].size(), b.size());
    EXPECT_TRUE(std::equal(recs[1].begin(), recs[1].end(), b.begin()));

    const auto scan = rune::node::scanViewCheckpoint(file);
    EXPECT_TRUE(std::holds_alternative<rune::node::ViewCheckpointTornTail>(scan.tail));
}
