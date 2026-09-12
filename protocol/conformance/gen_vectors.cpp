// conformance/gen_vectors.cpp
// One-shot generator: produces all 13 golden .bin files in conformance/vectors/.
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
    // 3. connect_response.bin
    // -----------------------------------------------------------------------
    {
        rpc::ConnectResponse msg;
        msg.sessionId         = kSessionId;
        msg.correlationId     = kCorrelationId;
        msg.code              = static_cast<int16_t>(rpc::ErrorCode::OK);   // 0
        msg.leaderMemberId    = 2;
        msg.assignedSessionId = static_cast<int64_t>(0x00000000DEADBEEFULL);
        n = msg.encode(buf);
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
        n = msg.encode(buf);
        write_vec(dir, "connect_response_redirect.bin", buf, n);
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
    // Produce{ topicId=1001, partition=7, partitionKnown=1, key=empty,
    //          recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02} }
    // Total = 24 + 9 + 2 + 0 + 2 + 8 = 45 bytes
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
    // Produce{ topicId=2002, partition=0, partitionKnown=0, key="ABC",
    //          recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02} }
    // Total = 24 + 9 + 2 + 3 + 2 + 8 = 48 bytes
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
    // ProduceResponse{ errorCode=0(OK), partition=7, baseOffset=123456 }
    // Total = 24 + 14 = 38 bytes
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

    std::printf("Done. 17 vectors written to %s/\n", dir);
    return 0;
}
