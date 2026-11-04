// tests/subscriber/CheckedWritesRecordOpTest.cpp - checked writes Part 05 Task 3: a delivered Record carries its
// attributes byte, and op() classifies the write (P5-11).
#include <rune/client/codec/DataCodec.h>
#include <rune/client/detail/HubCommand.h>
#include <rune/client/subscriber/SubscriberHub.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <memory>
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
