// CorruptSegmentBrokerRegressionTest.cpp - #839: a REAL client against a REAL BrokerNode (the shipped shard runtime,
// real disk, real Aeron UDP) whose log has a damaged sealed segment.  The broker is stopped, one byte inside the 2nd batch
// of the lowest-base segment is flipped, and it is started again: boot recovery keeps the bytes, flags the segment and
// counts it (the gauge's value).  A subscription from 0 gets every record below the damage, then ends CorruptBatch
// (StorageError on the wire, naming the subscription); a subscription from the next segment reads to the end; the log
// still appends; and a second restart sees the same damage.  Every wait has a hard cap.
#include "client/OneBroker.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
namespace rc = rune::client;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

constexpr int kRecords = 200;

// Thread-runner handlers run off the test thread: the collected offsets are guarded.
struct Collected {
    std::mutex           mu;
    std::vector<int64_t> offsets;
    std::vector<std::string> errors;
    [[nodiscard]] std::vector<int64_t> snapshot() {
        std::lock_guard<std::mutex> g(mu);
        return offsets;
    }
    [[nodiscard]] bool heardError(std::string_view needle) {
        std::lock_guard<std::mutex> g(mu);
        for (const auto& e : errors)
            if (e.find(needle) != std::string::npos) return true;
        return false;
    }
};

std::vector<std::size_t> batchStarts(const fs::path& logPath) {
    std::ifstream in(logPath, std::ios::binary);
    std::vector<uint8_t> buf((std::istreambuf_iterator<char>(in)), {});
    std::vector<std::size_t> out;
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= buf.size()) {
        rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        const int32_t total = r.totalLength();
        if (total < rune::protocol::batch::kRecordBatchOverhead || pos + static_cast<std::size_t>(total) > buf.size()) break;
        out.push_back(pos);
        pos += static_cast<std::size_t>(total);
    }
    return out;
}

int64_t batchBaseAt(const fs::path& logPath, std::size_t p) {
    std::ifstream in(logPath, std::ios::binary);
    in.seekg(static_cast<std::streamoff>(p));
    std::vector<uint8_t> hdr(static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead));
    in.read(reinterpret_cast<char*>(hdr.data()), static_cast<std::streamsize>(hdr.size()));
    return rune::protocol::RecordBatchReader(hdr.data(), hdr.size()).baseOffset();
}

struct Damage {
    int64_t D        = -1;   // the first offset of segment 0's 2nd batch (the damage)
    int64_t nextBase = -1;   // the base offset of the second segment
};

/// The partition directory of `topic`: the directory under `root` whose path names it and that holds >= 3 .log files.
fs::path findPartitionDir(const fs::path& root, std::string_view topic) {
    for (const auto& e : fs::recursive_directory_iterator(root)) {
        if (!e.is_directory() || e.path().string().find(topic) == std::string::npos) continue;
        std::size_t logs = 0;
        for (const auto& f : fs::directory_iterator(e.path())) logs += (f.path().extension() == ".log");
        if (logs >= 3) return e.path();
    }
    return {};
}

/// Flip one byte inside the 2nd batch of the lowest-base segment of the partition; fills `out`.
bool damageFirstSegment(const fs::path& dataDir, std::string_view topic, Damage& out) {
    const fs::path part = findPartitionDir(dataDir, topic);
    if (part.empty()) return false;
    std::vector<fs::path> segs;
    for (const auto& f : fs::directory_iterator(part))
        if (f.path().extension() == ".log") segs.push_back(f.path());
    std::sort(segs.begin(), segs.end());
    if (segs.size() < 3) return false;
    const auto starts = batchStarts(segs[0]);
    if (starts.size() < 2) return false;
    out.D        = batchBaseAt(segs[0], starts[1]);
    out.nextBase = batchBaseAt(segs[1], 0);
    std::fstream f(segs[0], std::ios::in | std::ios::out | std::ios::binary);
    const std::size_t at = starts[1] + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
    f.seekg(static_cast<std::streamoff>(at));
    char c = 0;
    f.read(&c, 1);
    c = static_cast<char>(c ^ 0x5A);
    f.seekp(static_cast<std::streamoff>(at));
    f.write(&c, 1);
    return f.good();
}

std::unique_ptr<rc::Client> openClient(OneBroker& broker, const rc::TopicSpec& topic, Collected& col) {
    rc::ClientConfig c = broker.clientConfig();
    c.topics.push_back(topic);
    auto client = std::make_unique<rc::Client>(c);
    client->onError([&col](const std::string& m) {
        std::lock_guard<std::mutex> g(col.mu);
        col.errors.push_back(m);
    });
    client->connectAndWait(30s);
    return client;
}

/// One record at a time (one batch each: segment 0 holds several batches); returns its base offset or -1.
int64_t produceOne(rc::Client& client, rc::Producer& prod, rc::Completions& cq, const rc::TopicHandle& topic, int k) {
    const std::string v = std::string(64, static_cast<char>('a' + k % 26));
    rc::SendResult r = rc::SendResult::BackPressured;
    for (int spin = 0; spin < 100'000 && r == rc::SendResult::BackPressured; ++spin) {
        r = prod.trySend(topic, rc::Key(static_cast<uint64_t>(k)),
                         std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()), cq,
                         static_cast<rc::Token>(k + 1));
        if (r == rc::SendResult::BackPressured) std::this_thread::yield();
    }
    if (r != rc::SendResult::Ok) return -1;
    prod.flush();
    int64_t base = -1;
    bool got = false;
    for (int i = 0; i < 20'000 && !got; ++i) {
        (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) {
            if (d.token == static_cast<rc::Token>(k + 1)) { got = true; base = d.status == rc::OpStatus::Ok ? d.value : -1; }
        });
        if (!got) std::this_thread::sleep_for(1ms);
    }
    (void)client;
    return got ? base : -1;
}

}  // namespace

TEST(CorruptSegmentBrokerRegression, RealClientFailsCorruptBatchAndReadsPastTheDamagedSegmentRegression) {
    OneBroker broker("corrupt_seg", OneBroker::Runtime::Shard);   // the shipped default runtime
    constexpr std::string_view kTopic = "dmg.t";
    int32_t topicId = -1;
    {
        rc::Client* admin = broker.connectClient();
        rc::TopicOptions opts;
        opts.config.segmentRollBytes = 4096;
        const auto r = admin->defineTopicAndWait(kTopic, 1, "", {}, rc::Persistence::Disk, opts, {}, 30s);
        ASSERT_EQ(r.errorCode, 0) << r.detail;
        topicId = r.topicId;
    }
    const rc::TopicSpec spec{std::string(kTopic), topicId, 1};
    Collected none;
    {
        auto producerClient = openClient(broker, spec, none);
        rc::ProducerConfig pc;
        pc.acks     = rc::Acks::Leader;
        pc.lingerUs = 0;
        rc::Producer prod = producerClient->producer(pc);
        rc::Completions cq(*producerClient);
        const rc::TopicHandle topic = producerClient->topic(spec.name);
        for (int k = 0; k < kRecords; ++k)
            ASSERT_EQ(produceOne(*producerClient, prod, cq, topic, k), k) << "record " << k << " was not acked at its offset";
        prod.close();
        producerClient->close();
    }

    Damage dmg;
    bool damaged = false;
    broker.restartWhileStopped([&](const fs::path& dataDir) { damaged = damageFirstSegment(dataDir, kTopic, dmg); });
    ASSERT_TRUE(damaged) << "no partition directory with >= 3 segments (or < 2 batches in the first)";
    ASSERT_GT(dmg.D, 0);
    ASSERT_GT(dmg.nextBase, dmg.D);
    ASSERT_LT(dmg.nextBase, kRecords);
    EXPECT_EQ(broker.node().bootDamagedSegmentsForTest(), 1) << "boot recovery must keep and count the damaged segment";

    Collected col;
    auto reader = openClient(broker, spec, col);
    {   // from 0: the records below the damage, then CorruptBatch
        rc::SubscribeOptions o;
        o.from = rc::From::earliest();
        rc::Subscription sub = reader->subscribe(reader->topic(spec.name), 0, o, [&col](const rc::Record& r) {
            std::lock_guard<std::mutex> g(col.mu);
            col.offsets.push_back(r.offset);
        });
        for (int i = 0; i < 20'000 && !sub.finished(); ++i) std::this_thread::sleep_for(1ms);
        ASSERT_TRUE(sub.finished()) << "the subscription never ended at the damage (a hang)";
        EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::CorruptBatch);
        const auto got = col.snapshot();
        ASSERT_EQ(static_cast<int64_t>(got.size()), dmg.D) << "exactly the records below the damage";
        for (int64_t i = 0; i < dmg.D; ++i) EXPECT_EQ(got[static_cast<std::size_t>(i)], i) << "gap or reorder at " << i;
        EXPECT_TRUE(col.heardError("corrupt")) << "onError never heard the broker's message";
    }
    {   // from the next segment: everything to the end
        Collected past;
        rc::SubscribeOptions o;
        o.from = rc::From::offset(dmg.nextBase);
        rc::Subscription sub = reader->subscribe(reader->topic(spec.name), 0, o, [&past](const rc::Record& r) {
            std::lock_guard<std::mutex> g(past.mu);
            past.offsets.push_back(r.offset);
        });
        const std::size_t want = static_cast<std::size_t>(kRecords - dmg.nextBase);
        for (int i = 0; i < 20'000 && past.snapshot().size() < want && !sub.finished(); ++i) std::this_thread::sleep_for(1ms);
        const auto got = past.snapshot();
        ASSERT_EQ(got.size(), want) << "a reader past the damaged segment must get every record";
        for (std::size_t i = 0; i < got.size(); ++i) EXPECT_EQ(got[i], dmg.nextBase + static_cast<int64_t>(i));
        EXPECT_FALSE(sub.finished());
    }
    {   // the log still appends
        rc::ProducerConfig pc;
        pc.acks     = rc::Acks::Leader;
        pc.lingerUs = 0;
        rc::Producer prod = reader->producer(pc);
        rc::Completions cq(*reader);
        EXPECT_EQ(produceOne(*reader, prod, cq, reader->topic(spec.name), kRecords), kRecords)
            << "the next record lands at the old log end";
        prod.close();
    }
    reader->close();
    reader.reset();

    broker.restart();
    EXPECT_EQ(broker.node().bootDamagedSegmentsForTest(), 1) << "the damage is still kept after a second restart";
}

// #872: the same damage, reached by a GROUP member (the shipped group path against a real broker).
TEST(CorruptSegmentBrokerRegression, RealGroupMemberFailsCorruptBatchAtTheDamageRegression) {
    OneBroker broker("corrupt_seg_group", OneBroker::Runtime::Shard);
    constexpr std::string_view kTopic = "dmg.g";
    int32_t topicId = -1;
    {
        rc::Client* admin = broker.connectClient();
        rc::TopicOptions opts;
        opts.config.segmentRollBytes = 4096;
        const auto r = admin->defineTopicAndWait(kTopic, 1, "", {}, rc::Persistence::Disk, opts, {}, 30s);
        ASSERT_EQ(r.errorCode, 0) << r.detail;
        topicId = r.topicId;
    }
    const rc::TopicSpec spec{std::string(kTopic), topicId, 1};
    Collected none;
    {
        auto producerClient = openClient(broker, spec, none);
        rc::ProducerConfig pc;
        pc.acks     = rc::Acks::Leader;
        pc.lingerUs = 0;
        rc::Producer prod = producerClient->producer(pc);
        rc::Completions cq(*producerClient);
        const rc::TopicHandle topic = producerClient->topic(spec.name);
        for (int k = 0; k < kRecords; ++k)
            ASSERT_EQ(produceOne(*producerClient, prod, cq, topic, k), k) << "record " << k << " was not acked at its offset";
        prod.close();
        producerClient->close();
    }

    Damage dmg;
    bool damaged = false;
    broker.restartWhileStopped([&](const fs::path& dataDir) { damaged = damageFirstSegment(dataDir, kTopic, dmg); });
    ASSERT_TRUE(damaged) << "no partition directory with >= 3 segments (or < 2 batches in the first)";
    ASSERT_GT(dmg.D, 0);
    ASSERT_GT(dmg.nextBase, dmg.D);
    ASSERT_LT(dmg.nextBase, kRecords);
    EXPECT_EQ(broker.node().bootDamagedSegmentsForTest(), 1) << "boot recovery must keep and count the damaged segment";

    Collected col;
    auto reader = openClient(broker, spec, col);
    // A new group with no committed offset starts at the log start (GroupConfig's default): no setting is needed.
    rc::GroupConfig gcfg;
    gcfg.autoCommit       = false;
    gcfg.sessionTimeoutMs = 10'000;
    rc::GroupConsumer gc = reader->subscribeGroup("g-dmg", {std::string(kTopic)}, gcfg);
    gc.onRecords([&](const rc::RecordSet& rs) {
        rs.forEachRecord([&](std::string_view, std::string_view, int64_t offset, int64_t) {
            std::lock_guard<std::mutex> g(col.mu);
            col.offsets.push_back(offset);
        });
    });
    const auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < 40'000 && !gc.finished(); ++i) std::this_thread::sleep_for(1ms);
    const auto took = std::chrono::steady_clock::now() - t0;
    ASSERT_TRUE(gc.finished()) << "the group consumer never ended at the damage (a hang)";
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::CorruptBatch);
    const auto got = col.snapshot();
    ASSERT_EQ(static_cast<int64_t>(got.size()), dmg.D) << "exactly the records below the damage";
    for (int64_t i = 0; i < dmg.D; ++i) EXPECT_EQ(got[static_cast<std::size_t>(i)], i) << "gap or reorder at " << i;
    EXPECT_TRUE(col.heardError("corrupt")) << "onError never heard the broker's message";
    gc.close();

    if (took < 5s) {   // a member of a NEW group fails the same way: not a one-off (one GroupConsumer per Client)
        Collected col2;
        auto reader2 = openClient(broker, spec, col2);
        rc::GroupConsumer gc2 = reader2->subscribeGroup("g-dmg-2", {std::string(kTopic)}, gcfg);
        gc2.onRecords([&](const rc::RecordSet& rs) {
            rs.forEachRecord([&](std::string_view, std::string_view, int64_t offset, int64_t) {
                std::lock_guard<std::mutex> g(col2.mu);
                col2.offsets.push_back(offset);
            });
        });
        for (int i = 0; i < 40'000 && !gc2.finished(); ++i) std::this_thread::sleep_for(1ms);
        ASSERT_TRUE(gc2.finished()) << "the second group's consumer never ended at the damage (a hang)";
        EXPECT_EQ(gc2.failReason(), rc::GroupFailReason::CorruptBatch);
        EXPECT_EQ(static_cast<int64_t>(col2.snapshot().size()), dmg.D);
        gc2.close();
        reader2->close();
    }
    reader->close();
    reader.reset();
}
