// tests/subscriber/CheckedWritesRecordOpTest.cpp - checked writes Part 05 Task 3: a delivered Record carries its
// attributes byte, and op() classifies the write (P5-11).
#include <rune/client/codec/DataCodec.h>
#include <rune/client/detail/HubCommand.h>
#include <rune/client/subscriber/SubscriberHub.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace rc  = rune::client;
namespace rcd = rune::client::detail;
using rune::client::subscriber::SubscriberHub;

TEST(CheckedWritesRecordOp, OpFollowsAttributesAndValue) {
    constexpr int64_t kSession = 7;
    SubscriberHub     hub;
    auto              impl = std::make_unique<rc::SubscriptionImpl>();
    impl->correlationId    = 11;
    impl->topicId          = 3;
    impl->partition        = 0;
    std::vector<rc::RecordOp> ops;
    std::vector<uint8_t>      attributes;
    impl->handler = [&](const rc::Record& r) {
        ops.push_back(r.op());
        attributes.push_back(r.attributes);
    };
    auto send = [](int32_t, const uint8_t*, std::size_t) { return true; };
    auto req  = SubscriberHub::makeSubscribeRequest(impl.get(), kSession, 1, -2, 0, {}, 3'600'000'000'000LL);
    rcd::HubCommand c;
    c.op     = rcd::HubCommand::Op::SubscribeAdd;
    c.handle = reinterpret_cast<uint64_t>(req.release());
    hub.applyCommand(c, 0, kSession, send, 0);
    rc::codec::SubscribeResponseView sr;
    sr.sessionId      = kSession;
    sr.correlationId  = 11;
    sr.subscriptionId = 101;
    hub.onSubscribeResponse(sr, 0, kSession, send);

    rune::protocol::RecordBatchBuilder b(0, 1'000'000);
    b.append("k", "v", 1'000'000, 0);
    b.append("k", "v", 1'000'000, 1);
    b.append("k", "v", 1'000'000, 2);
    b.append("k", "", 1'000'000, 0);
    b.append("k", "", 1'000'000, 2);
    b.append("k", "v", 1'000'000, 3);
    const auto batch = b.build();

    rc::codec::RecordsView rv;
    rv.sessionId      = kSession;
    rv.subscriptionId = 101;
    rv.topicId        = 3;
    rv.partition      = 0;
    rv.baseOffset     = 0;
    rv.recordBatch    = batch;
    hub.onRecords(rv);

    using rc::RecordOp;
    EXPECT_EQ(ops, (std::vector<RecordOp>{RecordOp::Upsert, RecordOp::Insert, RecordOp::Update, RecordOp::Delete,
                                          RecordOp::Delete, RecordOp::Upsert}));
    EXPECT_EQ(attributes, (std::vector<uint8_t>{0, 1, 2, 0, 2, 3}));
}

// ---- Task 14a: lease records ----

namespace {
std::string leaseValue(int64_t version, std::size_t n = 36) {
    std::string v(n, '\0');
    if (n >= 12) std::memcpy(v.data() + 4, &version, 8);
    return v;
}
}  // namespace

TEST(LeaseOpClassificationRegression, LeaseOpAndOpFollowTheLeaseBits) {
    using rc::LeaseRecordOp;
    using rc::RecordOp;
    struct Row {
        uint8_t     attrs;
        std::string value;
        int64_t     offset;
        LeaseRecordOp leaseOp;
        RecordOp      op;
    };
    const Row rows[] = {
        {0x01, leaseValue(40), 40, LeaseRecordOp::Granted, RecordOp::Insert},     // Put|Insert, version == offset
        {0x02, leaseValue(40), 41, LeaseRecordOp::Updated, RecordOp::Update},     // Put|Update, version != offset
        {0x01, leaseValue(40, 11), 40, LeaseRecordOp::Updated, RecordOp::Insert}, // value too short to hold the version
        {0x01, leaseValue(40, 12), 40, LeaseRecordOp::Granted, RecordOp::Insert}, // exactly 12 bytes is enough
        {0x06, leaseValue(40), 40, LeaseRecordOp::Revoked, RecordOp::Delete},     // Revoke|Update: the row stays in the value
        {0x08, leaseValue(40), 40, LeaseRecordOp::Expired, RecordOp::Delete},     // Expire (broker-written)
        {0x0C, leaseValue(40), 40, LeaseRecordOp::Checkpoint, RecordOp::Upsert},  // Checkpoint is not a delete
        {0x01, "", 40, LeaseRecordOp::Updated, RecordOp::Delete},                 // empty value
    };
    for (const Row& row : rows) {
        rc::Record r{};
        r.offset     = row.offset;
        r.attributes = row.attrs;
        r.value      = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(row.value.data()), row.value.size());
        EXPECT_EQ(r.leaseOp(), row.leaseOp) << "attrs " << int(row.attrs) << " len " << row.value.size();
        EXPECT_EQ(r.op(), row.op) << "attrs " << int(row.attrs) << " len " << row.value.size();
    }
}

TEST(LeaseOpClassificationRegression, CheckpointDroppedRegression) {
    constexpr int64_t kSession = 7;
    SubscriberHub     hub;
    auto              impl = std::make_unique<rc::SubscriptionImpl>();
    impl->correlationId    = 11;
    impl->topicId          = 3;
    impl->partition        = 0;
    std::vector<rc::LeaseRecordOp> seen;
    std::vector<int64_t>           offsets;
    impl->handler = [&](const rc::Record& r) {
        seen.push_back(r.leaseOp());
        offsets.push_back(r.offset);
    };
    rc::SubscriptionImpl* raw = impl.get();
    auto send = [](int32_t, const uint8_t*, std::size_t) { return true; };
    auto req  = SubscriberHub::makeSubscribeRequest(impl.get(), kSession, 1, -2, 0, {}, 3'600'000'000'000LL);
    rcd::HubCommand c;
    c.op     = rcd::HubCommand::Op::SubscribeAdd;
    c.handle = reinterpret_cast<uint64_t>(req.release());
    hub.applyCommand(c, 0, kSession, send, 0);
    rc::codec::SubscribeResponseView sr;
    sr.sessionId      = kSession;
    sr.correlationId  = 11;
    sr.subscriptionId = 101;
    hub.onSubscribeResponse(sr, 0, kSession, send);

    rune::protocol::RecordBatchBuilder b(0, 1'000'000);
    b.append("k", leaseValue(0), 1'000'000, 0x01);   // Put (offset 0: a grant)
    b.append("k", leaseValue(0), 1'000'000, 0x0C);   // Checkpoint
    b.append("k", leaseValue(0), 1'000'000, 0x08);   // Expire
    const auto batch = b.build();
    rc::codec::RecordsView rv;
    rv.sessionId      = kSession;
    rv.subscriptionId = 101;
    rv.topicId        = 3;
    rv.partition      = 0;
    rv.baseOffset     = 0;
    rv.recordBatch    = batch;
    hub.onRecords(rv);

    EXPECT_EQ(seen, (std::vector<rc::LeaseRecordOp>{rc::LeaseRecordOp::Granted, rc::LeaseRecordOp::Expired}));
    EXPECT_EQ(offsets, (std::vector<int64_t>{0, 2}));
    EXPECT_EQ(raw->nextOffset, 3) << "the position moves past the dropped checkpoint and the records after it";

    // A batch that ENDS with a checkpoint: the position still passes it (nothing after it to carry it).
    rune::protocol::RecordBatchBuilder b2(3, 1'000'000);
    b2.append("k", leaseValue(3), 1'000'000, 0x01);
    b2.append("k", leaseValue(3), 1'000'000, 0x0C);
    const auto batch2 = b2.build();
    rv.baseOffset  = 3;
    rv.recordBatch = batch2;
    hub.onRecords(rv);
    EXPECT_EQ(seen.size(), 3u);
    EXPECT_EQ(raw->nextOffset, 5);
}

// Spec A.3: a Put with intent 0 on a lease topic is a broker Carry, whatever its version; op() of it stays Upsert.
TEST(LeaseRecordOpRegression, CarriedRegression) {
    using rc::LeaseRecordOp;
    using rc::RecordOp;
    struct Row {
        uint8_t        attrs;
        LeaseRecordOp  leaseOp;
        RecordOp       op;
        const char*    what;
        int64_t        offset = 40;
    };
    const Row rows[] = {
        {0x00, LeaseRecordOp::Carried, RecordOp::Upsert, "Put intent 0, version == offset"},
        {0x01, LeaseRecordOp::Granted, RecordOp::Insert, "Put intent 1, version == offset"},
        {0x02, LeaseRecordOp::Updated, RecordOp::Update, "Put intent 2 (version != offset)", 41},
        {0x0C, LeaseRecordOp::Checkpoint, RecordOp::Upsert, "Checkpoint"},
    };
    const std::string v = leaseValue(40);
    for (const Row& row : rows) {
        rc::Record r{};
        r.offset     = row.offset;
        r.attributes = row.attrs;
        r.value      = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size());
        EXPECT_EQ(r.leaseOp(), row.leaseOp) << row.what;
        EXPECT_EQ(r.op(), row.op) << row.what;
    }
    // A Carried record with a version != offset is also Carried (the intent decides, not the version).
    rc::Record r{};
    r.offset     = 41;
    r.attributes = 0x00;
    r.value      = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size());
    EXPECT_EQ(r.leaseOp(), LeaseRecordOp::Carried);
}
