// conformance/gen_vectors.cpp
// Generator for the golden .bin files in conformance/vectors/ (not the cg1_* / v0 /
// record_batch ones, which have their own generators or are hand-pinned).
// Run once, commit the output, then delete (or keep) this file.
// The harness (ConformanceTest.cpp) only READS the committed vectors.
//
// Compile & run (from rune-protocol root):
//   c++ -std=c++23 -I include conformance/gen_vectors.cpp -o /tmp/rune_gen_vectors
//   /tmp/rune_gen_vectors conformance/vectors

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "rune/protocol/client/MessageHeader.h"
#include "rune/protocol/client/ControlMessages.h"
#include "rune/protocol/client/DataMessages.h"
#include "rune/protocol/client/PingMessages.h"
#include "rune/protocol/client/SchemaMessages.h"

namespace rpc = rune::protocol::client;

static void write_vec(const char* dir, const char* name,
                      const uint8_t* buf, std::size_t len) {
    std::string path = std::string(dir) + "/" + name;
    FILE* f = std::fopen(path.c_str(), "wb");
    if (!f) { std::perror(path.c_str()); std::exit(1); }
    if (std::fwrite(buf, 1, len, f) != len) {
        std::perror(path.c_str()); std::fclose(f); std::exit(1);
    }
    std::fclose(f);
    std::printf("wrote %s (%zu bytes)\n", path.c_str(), len);
}

int main(int argc, char** argv) {
    const char* dir = (argc > 1) ? argv[1] : "conformance/vectors";

    // Common session/correlation IDs used for all vectors.
    static constexpr int64_t kSessionId     = static_cast<int64_t>(0x1122334455667788ULL);
    static constexpr int64_t kCorrelationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);

    uint8_t buf[1024];
    std::size_t n;

    // -----------------------------------------------------------------------
    // 1. connect_request.bin
    // -----------------------------------------------------------------------
    {
        rpc::ConnectRequest msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.egressStreamId = 1001;
        msg.clientVersion  = 1;
        msg.egressChannel  = "aeron:udp?endpoint=10.0.0.5:40456";
        n = msg.encode(buf);
        write_vec(dir, "connect_request.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 2. connect_request_empty_channel.bin
    // -----------------------------------------------------------------------
    {
        rpc::ConnectRequest msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.egressStreamId = 1001;
        msg.clientVersion  = 1;
        msg.egressChannel  = "";
        n = msg.encode(buf);
        write_vec(dir, "connect_request_empty_channel.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // connect_request_v16_response.bin (NAT-safe egress)
    // -----------------------------------------------------------------------
    {
        rpc::ConnectRequest msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.egressStreamId = 1001;
        msg.clientVersion  = 16;
        msg.egressChannel  = "aeron:udp?endpoint=localhost:40200";
        msg.responseCorrelated = true;
        n = msg.encode(buf);
        write_vec(dir, "connect_request_v16_response.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 3. connect_response.bin
    // -----------------------------------------------------------------------
    {
        rpc::ConnectResponse msg;
        msg.sessionId         = kSessionId;
        msg.correlationId     = kCorrelationId;
        msg.code              = static_cast<int16_t>(rpc::ErrorCode::OK);   // 0
        msg.leaderMemberId    = 2;
        msg.assignedSessionId = static_cast<int64_t>(0x00000000DEADBEEFULL);
        // The golden vector is the v0 (pre-brokerVersion, 38-byte) form — every
        // decoder must keep accepting it; the 42-byte form only appends
        // brokerVersion (int32 at [38..41]).
        msg.encode(buf);
        rune::protocol::client::detail::putLE16(buf, 0, rpc::ConnectResponse::BODY_FIXED_V0);
        n = rpc::ConnectResponse::ENCODED_SIZE_V0;
        write_vec(dir, "connect_response.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 4. connect_response_redirect.bin
    // -----------------------------------------------------------------------
    {
        rpc::ConnectResponse msg;
        msg.sessionId         = kSessionId;
        msg.correlationId     = kCorrelationId;
        msg.code              = static_cast<int16_t>(rpc::ErrorCode::Redirect); // 3
        msg.leaderMemberId    = 1;
        msg.assignedSessionId = 0;
        // The golden vector is the v0 (pre-brokerVersion, 38-byte) form — every
        // decoder must keep accepting it; the 42-byte form only appends
        // brokerVersion (int32 at [38..41]).
        msg.encode(buf);
        rune::protocol::client::detail::putLE16(buf, 0, rpc::ConnectResponse::BODY_FIXED_V0);
        n = rpc::ConnectResponse::ENCODED_SIZE_V0;
        write_vec(dir, "connect_response_redirect.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 4b. connect_response_v3.bin — the 42-byte form (brokerVersion = 3) every
    //     broker >= version 3 sends.
    // -----------------------------------------------------------------------
    {
        rpc::ConnectResponse msg;
        msg.sessionId         = kSessionId;
        msg.correlationId     = kCorrelationId;
        msg.code              = static_cast<int16_t>(rpc::ErrorCode::OK);
        msg.leaderMemberId    = 2;
        msg.assignedSessionId = static_cast<int64_t>(0x00000000DEADBEEFULL);
        msg.brokerVersion     = 3;
        n = msg.encode(buf);
        write_vec(dir, "connect_response_v3.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 5. keepalive.bin
    // -----------------------------------------------------------------------
    {
        rpc::Keepalive msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        n = msg.encode(buf);
        write_vec(dir, "keepalive.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 6. disconnect.bin
    // -----------------------------------------------------------------------
    {
        rpc::Disconnect msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        msg.reason        = -1;
        n = msg.encode(buf);
        write_vec(dir, "disconnect.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 7. error_response.bin
    // -----------------------------------------------------------------------
    {
        static constexpr char kMsg[] = "session 0x1122... expired";
        rpc::ErrorResponse msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        msg.code          = static_cast<int16_t>(rpc::ErrorCode::SessionExpired); // 4
        msg.message       = kMsg;
        n = msg.encode(buf);
        write_vec(dir, "error_response.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 8. error_response_empty.bin
    // -----------------------------------------------------------------------
    {
        rpc::ErrorResponse msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        msg.code          = static_cast<int16_t>(rpc::ErrorCode::Internal); // 6
        msg.message       = "";
        n = msg.encode(buf);
        write_vec(dir, "error_response_empty.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 9. ping.bin
    // -----------------------------------------------------------------------
    {
        static const uint8_t kEcho[] = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00};
        rpc::Ping msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        msg.echo          = std::string_view(reinterpret_cast<const char*>(kEcho), sizeof(kEcho));
        n = msg.encode(buf);
        write_vec(dir, "ping.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 10. pong.bin
    // -----------------------------------------------------------------------
    {
        static const uint8_t kEcho[] = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00};
        rpc::Pong msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        msg.echo          = std::string_view(reinterpret_cast<const char*>(kEcho), sizeof(kEcho));
        n = msg.encode(buf);
        write_vec(dir, "pong.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 11. produce_explicit_partition.bin
    //
    // Produce{ topicId=1001, partition=7, partitionKnown=1, acks=0, key=empty,
    //          recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02} }
    // Total = 24 + 10 + 2 + 0 + 2 + 8 = 46 bytes
    // -----------------------------------------------------------------------
    {
        static constexpr uint8_t kBatch[] = {
            0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x01, 0x02
        };
        rpc::Produce msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.topicId        = 1001;
        msg.partition      = 7;
        msg.partitionKnown = 1;
        msg.key            = std::span<const uint8_t>{};
        msg.recordBatch    = std::span<const uint8_t>(kBatch, sizeof(kBatch));

        std::vector<uint8_t> vbuf(msg.encodedSize());
        n = msg.encode(vbuf.data());
        write_vec(dir, "produce_explicit_partition.bin", vbuf.data(), n);
    }

    // -----------------------------------------------------------------------
    // 12. produce_key_only.bin
    //
    // Produce{ topicId=2002, partition=0, partitionKnown=0, acks=0, key="ABC",
    //          recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02} }
    // Total = 24 + 10 + 2 + 3 + 2 + 8 = 49 bytes
    // -----------------------------------------------------------------------
    {
        static constexpr uint8_t kBatch[] = {
            0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x01, 0x02
        };
        static constexpr uint8_t kKey[] = {0x41, 0x42, 0x43}; // "ABC"
        rpc::Produce msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.topicId        = 2002;
        msg.partition      = 0;
        msg.partitionKnown = 0;
        msg.key            = std::span<const uint8_t>(kKey, sizeof(kKey));
        msg.recordBatch    = std::span<const uint8_t>(kBatch, sizeof(kBatch));

        std::vector<uint8_t> vbuf(msg.encodedSize());
        n = msg.encode(vbuf.data());
        write_vec(dir, "produce_key_only.bin", vbuf.data(), n);
    }

    // -----------------------------------------------------------------------
    // 13. produce_response.bin
    //
    // ProduceResponse{ errorCode=0(OK), partition=7, baseOffset=123456,
    //                  logStartOffset=-1 (not carried) }
    // Total = 24 + 22 = 46 bytes (D6: logStartOffset appended; the pre-D6
    // 38-byte form is kept as produce_response_v0.bin — a compat vector, not
    // regenerated here).
    // -----------------------------------------------------------------------
    {
        rpc::ProduceResponse msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        msg.errorCode     = 0;  // OK
        msg.partition     = 7;
        msg.baseOffset    = 123456LL;
        n = msg.encode(buf);
        write_vec(dir, "produce_response.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 13b. produce_response_unknown_pid.bin (D6)
    //
    // ProduceResponse{ errorCode=28(UnknownProducerId), partition=7,
    //                  baseOffset=0, logStartOffset=500 }
    // Total = 46 bytes
    // -----------------------------------------------------------------------
    {
        rpc::ProduceResponse msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.errorCode      = static_cast<int16_t>(rpc::ErrorCode::UnknownProducerId);
        msg.partition      = 7;
        msg.baseOffset     = 0;
        msg.logStartOffset = 500;
        n = msg.encode(buf);
        write_vec(dir, "produce_response_unknown_pid.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // 14. subscribe_explicit.bin
    //
    // Subscribe{ topicId=3001, partition=5, partitionKnown=1, fromOffset=9999,
    //            isolation=1, key=empty }
    // Total = 24 + 18 + 2 = 44 bytes
    // -----------------------------------------------------------------------
    {
        rpc::Subscribe msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.topicId        = 3001;
        msg.partition      = 5;
        msg.partitionKnown = 1;
        msg.fromOffset     = 9999LL;
        msg.isolation      = 1;
        msg.key            = std::span<const uint8_t>{};

        std::vector<uint8_t> vbuf(msg.encodedSize());
        n = msg.encode(vbuf.data());
        write_vec(dir, "subscribe_explicit.bin", vbuf.data(), n);
    }

    // -----------------------------------------------------------------------
    // 15. subscribe_key_only.bin
    //
    // Subscribe{ topicId=4004, partition=0, partitionKnown=0, fromOffset=0,
    //            isolation=0, key="ABC" }
    // Total = 24 + 18 + 2 + 3 = 47 bytes
    // -----------------------------------------------------------------------
    {
        static constexpr uint8_t kKey[] = {0x41, 0x42, 0x43}; // "ABC"
        rpc::Subscribe msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.topicId        = 4004;
        msg.partition      = 0;
        msg.partitionKnown = 0;
        msg.fromOffset     = 0LL;
        msg.isolation      = 0;
        msg.key            = std::span<const uint8_t>(kKey, sizeof(kKey));

        std::vector<uint8_t> vbuf(msg.encodedSize());
        n = msg.encode(vbuf.data());
        write_vec(dir, "subscribe_key_only.bin", vbuf.data(), n);
    }

    // -----------------------------------------------------------------------
    // 16. records_push.bin
    //
    // Records{ subscriptionId=0x0102030405060708, topicId=3001, partition=5,
    //          baseOffset=100000, recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02} }
    // Total = 24 + 24 + 2 + 8 = 58 bytes
    // -----------------------------------------------------------------------
    {
        static constexpr uint8_t kBatch[] = {
            0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x01, 0x02
        };
        rpc::Records msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.subscriptionId = static_cast<int64_t>(0x0102030405060708ULL);
        msg.topicId        = 3001;
        msg.partition      = 5;
        msg.baseOffset     = 100000LL;
        msg.recordBatch    = std::span<const uint8_t>(kBatch, sizeof(kBatch));

        std::vector<uint8_t> vbuf(msg.encodedSize());
        n = msg.encode(vbuf.data());
        write_vec(dir, "records_push.bin", vbuf.data(), n);
    }

    // -----------------------------------------------------------------------
    // 17. unsubscribe.bin
    //
    // Unsubscribe{ subscriptionId=0x0102030405060708 }
    // Total = 24 + 8 = 32 bytes
    // -----------------------------------------------------------------------
    {
        rpc::Unsubscribe msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.subscriptionId = static_cast<int64_t>(0x0102030405060708ULL);
        n = msg.encode(buf);
        write_vec(dir, "unsubscribe.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // Catalog history (client version 8) — CatalogDdlMessagesTest.cpp builds the
    // same frames (CatalogDdlConformance.*Golden).
    // -----------------------------------------------------------------------
    // 18. define_topic_v8.bin — DefineTopic + persistence + rf + config + ext
    {
        rpc::DefineTopic m;
        m.sessionId = kSessionId; m.correlationId = kCorrelationId;
        m.topicName = "events"; m.partitionCount = 4; m.persistence = 0; m.replicationFactor = 3;
        m.config.retentionMs = 60'000; m.config.cleanupPolicy = 1; m.config.partitionKey = "k";
        m.hasExt = true; m.ext.statement = "topic events partitions 4 replicas 3;"; m.ext.clientName = "cli";
        m.ext.requestId = {.hi = 0xC11E, .lo = 77};
        m.ext.hasReplayedFrom = 1; m.ext.replayedClusterId = "src"; m.ext.replayedOffset = 5;
        n = m.encode(buf);
        if (n == 0) { std::fprintf(stderr, "define_topic_v8: encode failed\n"); return 1; }
        write_vec(dir, "define_topic_v8.bin", buf, n);
    }
    // 19. drop_topic.bin
    {
        rpc::DropTopic m;
        m.sessionId = kSessionId; m.correlationId = kCorrelationId; m.topicName = "t";
        m.ext.statement = "drop topic t;"; m.ext.clientName = "cli"; m.ext.requestId = {.hi = 1, .lo = 9};
        n = m.encode(buf);
        if (n == 0) { std::fprintf(stderr, "drop_topic: encode failed\n"); return 1; }
        write_vec(dir, "drop_topic.bin", buf, n);
    }
    // 20. alter_topic_config.bin — one change: retention.ms (key 1) = 60000
    {
        const rpc::ConfigChangeWire change{1, 60'000};
        uint8_t changes[rpc::configChangesEncodedSize(1)];
        rpc::putConfigChanges(changes, std::span<const rpc::ConfigChangeWire>(&change, 1));
        rpc::AlterTopicConfig m;
        m.sessionId = kSessionId; m.correlationId = kCorrelationId;
        m.topicName = "events"; m.expectedVersion = 2; m.changes = changes;
        m.ext.statement = "ALTER TOPIC events SET (retention.ms=60000)"; m.ext.clientName = "cli";
        m.ext.requestId = {.hi = 1, .lo = 10};
        n = m.encode(buf);
        if (n == 0) { std::fprintf(stderr, "alter_topic_config: encode failed\n"); return 1; }
        write_vec(dir, "alter_topic_config.bin", buf, n);
    }
    // 21. drop_view.bin
    {
        rpc::DropView m;
        m.sessionId = kSessionId; m.correlationId = kCorrelationId; m.viewName = "v";
        m.ext.statement = "drop view v;"; m.ext.clientName = "cli"; m.ext.requestId = {.hi = 1, .lo = 11};
        n = m.encode(buf);
        if (n == 0) { std::fprintf(stderr, "drop_view: encode failed\n"); return 1; }
        write_vec(dir, "drop_view.bin", buf, n);
    }
    // 22. alter_view.bin — plan blob DE AD BE EF
    {
        static constexpr uint8_t kPlan[] = {0xDE, 0xAD, 0xBE, 0xEF};
        rpc::AlterView m;
        m.sessionId = kSessionId; m.correlationId = kCorrelationId;
        m.viewName = "v"; m.keyField = "id"; m.persistence = 0; m.expectedVersion = 1;
        m.planBlob = kPlan; m.planLen = sizeof(kPlan);
        m.ext.statement = "alter view v;"; m.ext.clientName = "cli"; m.ext.requestId = {.hi = 1, .lo = 12};
        n = m.encode(buf);
        if (n == 0) { std::fprintf(stderr, "alter_view: encode failed\n"); return 1; }
        write_vec(dir, "alter_view.bin", buf, n);
    }
    // 23. ddl_response.bin
    {
        rpc::DdlResponse r;
        r.sessionId = kSessionId; r.correlationId = kCorrelationId;
        r.errorCode = 39; r.objectId = 12; r.newVersion = 3; r.gateTopicId = 5;
        r.detail = "replicationFactor 5 > broker count 3";
        n = r.encode(buf);
        if (n == 0) { std::fprintf(stderr, "ddl_response: encode failed\n"); return 1; }
        write_vec(dir, "ddl_response.bin", buf, n);
    }

    // -----------------------------------------------------------------------
    // Replication schema 112, v3 vs v4 (ISR protocol) — ReplicationSchemaV4Test.cpp
    // decodes these with the generated SBE codecs. Hand layout, little-endian.
    // v3 frames carry the v3 block and version 3; v4 frames the v4 block and version 4.
    // Field values: see manifest.md section "repl_*".
    // -----------------------------------------------------------------------
    {
        struct Le {
            std::vector<uint8_t> b;
            void u16(uint16_t v) { for (int i = 0; i < 2; ++i) b.push_back(uint8_t(v >> (8 * i))); }
            void i32(int32_t v)  { const uint32_t u = uint32_t(v); for (int i = 0; i < 4; ++i) b.push_back(uint8_t(u >> (8 * i))); }
            void i64(int64_t v)  { const uint64_t u = uint64_t(v); for (int i = 0; i < 8; ++i) b.push_back(uint8_t(u >> (8 * i))); }
            void hdr(uint16_t block, uint16_t tmpl, uint16_t ver) { u16(block); u16(tmpl); u16(112); u16(ver); }
        };
        auto isr16 = [](Le& w, std::initializer_list<int32_t> v) { int n = 0; for (int32_t x : v) { w.i32(x); ++n; } for (; n < 16; ++n) w.i32(0); };
        auto sto16 = [](Le& w, std::initializer_list<int64_t> v) { int n = 0; for (int64_t x : v) { w.i64(x); ++n; } for (; n < 16; ++n) w.i64(0); };
        for (const bool v4 : {false, true}) {
            {   // repl_alter_isr_v3.bin / _v4.bin (template 7)
                Le w; w.hdr(v4 ? 232 : 92, 7, v4 ? 4 : 3);
                w.i32(1); w.i32(2); w.i32(3); w.i64(4); w.i32(2); isr16(w, {3, 5}); w.i32(9);
                if (v4) { w.i64(77); sto16(w, {11, 22}); w.i32(3); }
                write_vec(dir, v4 ? "repl_alter_isr_v4.bin" : "repl_alter_isr_v3.bin", w.b.data(), w.b.size());
            }
            {   // repl_alter_isr_response_v3.bin / _v4.bin (template 8)
                Le w; w.hdr(v4 ? 46 : 26, 8, v4 ? 4 : 3);
                w.i32(1); w.i32(2); w.i32(3); w.i64(4); w.i32(9); w.u16(2);
                if (v4) { w.i64(77); w.i64(8); w.i32(3); }
                write_vec(dir, v4 ? "repl_alter_isr_response_v4.bin" : "repl_alter_isr_response_v3.bin", w.b.data(), w.b.size());
            }
            {   // repl_fetch_request_v3.bin / _v4.bin (template 1)
                Le w; w.hdr(v4 ? 60 : 44, 1, v4 ? 4 : 3);
                w.i64(5); w.i32(1); w.i32(3); w.i64(100); w.i32(4096); w.i32(7); w.i32(4); w.i64(0x1122);
                if (v4) { w.i64(0x1234); w.i64(90); }
                write_vec(dir, v4 ? "repl_fetch_request_v4.bin" : "repl_fetch_request_v3.bin", w.b.data(), w.b.size());
            }
        }
    }
    // Replication schema 112 v5 (#789): FetchResponse appends fromOffset after topicId.
    // ReplicationSchemaV5Test.cpp decodes these. Records: 3 bytes {01 02 03}.
    {
        struct Le {
            std::vector<uint8_t> b;
            void u16(uint16_t v) { for (int i = 0; i < 2; ++i) b.push_back(uint8_t(v >> (8 * i))); }
            void i32(int32_t v)  { const uint32_t u = uint32_t(v); for (int i = 0; i < 4; ++i) b.push_back(uint8_t(u >> (8 * i))); }
            void i64(int64_t v)  { const uint64_t u = uint64_t(v); for (int i = 0; i < 8; ++i) b.push_back(uint8_t(u >> (8 * i))); }
            void hdr(uint16_t block, uint16_t tmpl, uint16_t ver) { u16(block); u16(tmpl); u16(112); u16(ver); }
        };
        for (const bool v5 : {false, true}) {
            // repl_fetch_response_v4.bin / _v5.bin (template 2)
            Le w; w.hdr(v5 ? 42 : 34, 2, v5 ? 5 : 4);
            w.i64(5); w.i32(1); w.i64(200); w.i64(0); w.u16(0); w.i32(7);
            if (v5) w.i64(1047);
            w.i32(3); w.b.push_back(1); w.b.push_back(2); w.b.push_back(3);
            write_vec(dir, v5 ? "repl_fetch_response_v5.bin" : "repl_fetch_response_v4.bin", w.b.data(), w.b.size());
        }
    }

    std::printf("Done. 33 vectors written to %s/\n", dir);
    return 0;
}
