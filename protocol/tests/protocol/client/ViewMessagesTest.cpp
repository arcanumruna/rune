// tests/protocol/client/ViewMessagesTest.cpp
//
// Round-trip tests for the view-engine wire messages (Task 18):
//   CreateView / CreateViewResponse   (SchemaMessages.h)
//   GetViewRow / ViewRowResponse      (DataMessages.h)
//
// Convention mirrors SchemaMessagesTest.cpp: encode -> decode -> field
// equality, plus a short/truncated-buffer -> nullopt safety check per
// message.

#include <gtest/gtest.h>
#include <vector>
#include <cstring>
#include "rune/protocol/client/SchemaMessages.h"
#include "rune/protocol/client/DataMessages.h"
#include "rune/protocol/client/Dispatch.h"

using namespace rune::protocol::client;

TEST(ViewMessages, CreateViewRoundTrip) {
    std::vector<uint8_t> blob = {1, 2, 3, 4, 5, 6};  // opaque authored ViewPlan bytes
    CreateView in;
    in.sessionId = 7; in.correlationId = 99;
    in.viewName = "char_by_region";
    in.keyField = "regionId";
    in.persistence = 1;
    in.planBlob = blob.data();
    in.planLen = static_cast<uint32_t>(blob.size());

    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    ASSERT_EQ(n, in.encodedSize());
    EXPECT_EQ(peekTemplateId(buf.data(), n),
              static_cast<uint16_t>(TemplateId::CreateView));

    auto out = CreateView::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->sessionId, 7);
    EXPECT_EQ(out->correlationId, 99);
    EXPECT_EQ(out->viewName, "char_by_region");
    EXPECT_EQ(out->keyField, "regionId");
    EXPECT_EQ(out->persistence, 1);
    ASSERT_EQ(out->planLen, blob.size());
    EXPECT_EQ(0, std::memcmp(out->planBlob, blob.data(), blob.size()));
}

TEST(ViewMessages, CreateViewDecodeRejectsTruncated) {
    std::vector<uint8_t> blob = {1, 2, 3};
    CreateView in; in.viewName = "v"; in.keyField = "k"; in.persistence = 0;
    in.planBlob = blob.data(); in.planLen = static_cast<uint32_t>(blob.size());
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    EXPECT_FALSE(CreateView::decode(buf.data(), buf.size() - 1).has_value());
}

TEST(ViewMessagesRegression, CreateViewUnknownPersistenceValueIsRejected) {
    std::vector<uint8_t> blob = {1, 2, 3};
    CreateView in; in.viewName = "v"; in.keyField = "k"; in.persistence = 1;
    in.planBlob = blob.data(); in.planLen = static_cast<uint32_t>(blob.size());
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    ASSERT_TRUE(CreateView::decode(buf.data(), buf.size()).has_value());
    buf[MessageHeader::ENCODED_SIZE] = 2;   // one past Transient: never cast
    EXPECT_FALSE(CreateView::decode(buf.data(), buf.size()).has_value());
}

TEST(ViewMessages, CreateViewResponseRoundTrip) {
    CreateViewResponse in;
    in.sessionId = 7; in.correlationId = 99;
    in.viewTopicId = 0x40000001;
    in.outputFingerprint = 0x0102030405060708ULL;
    in.errorCode = static_cast<uint16_t>(ErrorCode::OK);
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    ASSERT_EQ(n, in.encodedSize());
    EXPECT_EQ(peekTemplateId(buf.data(), n),
              static_cast<uint16_t>(TemplateId::CreateViewResponse));

    auto out = CreateViewResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->viewTopicId, 0x40000001);
    EXPECT_EQ(out->outputFingerprint, 0x0102030405060708ULL);
    EXPECT_EQ(out->errorCode, 0);
}

TEST(ViewMessages, CreateViewResponseRoundTripViewExistsError) {
    CreateViewResponse in;
    in.sessionId = 1; in.correlationId = 2;
    in.viewTopicId = -1;
    in.outputFingerprint = 0;
    in.errorCode = static_cast<uint16_t>(ErrorCode::ViewExists);
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    auto out = CreateViewResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->errorCode, static_cast<uint16_t>(ErrorCode::ViewExists));
}

TEST(ViewMessages, CreateViewResponseDecodeRejectsTruncated) {
    CreateViewResponse in; in.viewTopicId = 1; in.outputFingerprint = 2; in.errorCode = 0;
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    EXPECT_FALSE(CreateViewResponse::decode(buf.data(), buf.size() - 1).has_value());
}

TEST(ViewMessages, GetViewRowRoundTrip) {
    GetViewRow in;
    in.sessionId = 3; in.correlationId = 4;
    in.viewTopicId = 0x7FFFFFFF;
    in.keyU64 = 0xAABBCCDDEEFF0011ULL;
    in.requestedFingerprint = 0x0102030405060708ULL;

    std::vector<uint8_t> buf(static_cast<std::size_t>(GetViewRow::ENCODED_SIZE));
    const std::size_t n = in.encode(buf.data());
    ASSERT_EQ(n, GetViewRow::ENCODED_SIZE);
    EXPECT_EQ(peekTemplateId(buf.data(), n),
              static_cast<uint16_t>(TemplateId::GetViewRow));

    auto out = GetViewRow::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->sessionId, 3);
    EXPECT_EQ(out->correlationId, 4);
    EXPECT_EQ(out->viewTopicId, 0x7FFFFFFF);
    EXPECT_EQ(out->keyU64, 0xAABBCCDDEEFF0011ULL);
    EXPECT_EQ(out->requestedFingerprint, 0x0102030405060708ULL);
}

TEST(ViewMessages, GetViewRowRoundTripLatestFingerprint) {
    // requestedFingerprint = 0 means "latest" — must also survive round-trip.
    GetViewRow in; in.viewTopicId = 1; in.keyU64 = 42; in.requestedFingerprint = 0;
    std::vector<uint8_t> buf(static_cast<std::size_t>(GetViewRow::ENCODED_SIZE));
    const std::size_t n = in.encode(buf.data());
    auto out = GetViewRow::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->requestedFingerprint, 0u);
}

TEST(ViewMessages, GetViewRowDecodeRejectsTruncated) {
    GetViewRow in; in.viewTopicId = 1; in.keyU64 = 2; in.requestedFingerprint = 3;
    std::vector<uint8_t> buf(static_cast<std::size_t>(GetViewRow::ENCODED_SIZE));
    in.encode(buf.data());
    EXPECT_FALSE(GetViewRow::decode(buf.data(), buf.size() - 1).has_value());
}

TEST(ViewMessages, ViewRowResponseRoundTrip) {
    std::vector<uint8_t> row = {0xDE, 0xAD, 0xBE, 0xEF};
    ViewRowResponse in;
    in.sessionId = 5; in.correlationId = 6;
    in.status = 0;  // OK
    in.rowLen = static_cast<uint32_t>(row.size());
    in.rowBytes = row.data();
    in.rowBytesLen = static_cast<uint32_t>(row.size());

    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    ASSERT_EQ(n, in.encodedSize());
    EXPECT_EQ(peekTemplateId(buf.data(), n),
              static_cast<uint16_t>(TemplateId::ViewRowResponse));

    auto out = ViewRowResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->sessionId, 5);
    EXPECT_EQ(out->correlationId, 6);
    EXPECT_EQ(out->status, 0);
    EXPECT_EQ(out->rowLen, row.size());
    ASSERT_EQ(out->rowBytesLen, row.size());
    EXPECT_EQ(0, std::memcmp(out->rowBytes, row.data(), row.size()));
}

TEST(ViewMessages, ViewRowResponseRoundTripAbsentKey) {
    // AbsentKey/NotReady/etc: no row payload.
    ViewRowResponse in;
    in.sessionId = 1; in.correlationId = 2;
    in.status = 1;  // AbsentKey
    in.rowLen = 0;
    in.rowBytes = nullptr;
    in.rowBytesLen = 0;

    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    auto out = ViewRowResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->status, 1);
    EXPECT_EQ(out->rowLen, 0u);
    EXPECT_EQ(out->rowBytesLen, 0u);
}

TEST(ViewMessages, ViewRowResponseDecodeRejectsTruncated) {
    std::vector<uint8_t> row = {1, 2, 3};
    ViewRowResponse in; in.status = 0; in.rowLen = 3; in.rowBytes = row.data(); in.rowBytesLen = 3;
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    EXPECT_FALSE(ViewRowResponse::decode(buf.data(), buf.size() - 1).has_value());
}

// U1: ErrorCode values are append-only wire constants.
TEST(ViewMessagesRegression, ViewErrorCodesArePinned) {
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::ViewSourceTransient), 37u);
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::ViewNotAlterable), 38u);
}

// No-hang wave: InvalidRequest (39) answers a header-decodable request whose
// body did not decode; ViewRowResponse gains InvalidRequest (5) and Busy (6).
// Append-only wire constants.
TEST(ViewMessagesRegression, InvalidRequestAndViewRowStatusesArePinnedRegression) {
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::InvalidRequest), 39u);
    EXPECT_EQ(static_cast<uint16_t>(ViewRowResponse::kInvalidRequest), 5u);
    EXPECT_EQ(static_cast<uint16_t>(ViewRowResponse::kBusy), 6u);
    EXPECT_EQ(static_cast<uint16_t>(ViewRowResponse::kUnknownView), 4u);
    ViewRowResponse in; in.sessionId = 3; in.correlationId = 9; in.status = ViewRowResponse::kBusy;
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    const auto out = ViewRowResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->status, ViewRowResponse::kBusy);
    EXPECT_EQ(out->correlationId, 9);
}

// DDL conformance: a DefineTopic/CreateView name that fails the broker's name rule
// (empty, > 249 bytes, "." / "..", a byte outside [A-Za-z0-9._-], or the reserved
// "__" prefix) is answered InvalidTopicName.  Append-only wire constant.
TEST(ViewMessagesRegression, InvalidTopicNameCodeIsPinned) {
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::InvalidTopicName), 40u);
}

// DDL conformance (user decision D3b): a CreateView whose source is another view's
// output is rejected (a view sink carries no change events until Phase 3, so the
// view would stay silently empty).  Append-only wire constant.
TEST(ViewMessagesRegression, ViewSourceIsViewCodeIsPinned) {
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::ViewSourceIsView), 41u);
}

// No-hang fix round 2: kRowTooLarge (7) appended; client version 5 is the
// InvalidRequest gate.
TEST(ViewMessagesRegression, RowTooLargeStatusAndInvalidRequestClientGateArePinnedRegression) {
    EXPECT_EQ(static_cast<uint16_t>(ViewRowResponse::kRowTooLarge), 7u);
    EXPECT_EQ(kClientVersionInvalidRequest, 5);
    EXPECT_EQ(kClientVersion, 8);   // v8: catalog history DDL (v7: the transient lineage note)
    EXPECT_FALSE(clientUnderstandsInvalidRequest(4));
    EXPECT_TRUE(clientUnderstandsInvalidRequest(5));
}
