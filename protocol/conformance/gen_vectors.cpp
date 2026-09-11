// conformance/gen_vectors.cpp
// One-shot generator: produces all 10 golden .bin files in conformance/vectors/.
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
#include <string>
#include <string_view>

#include "rune/protocol/client/MessageHeader.h"
#include "rune/protocol/client/ControlMessages.h"
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

    std::printf("Done. 10 vectors written to %s/\n", dir);
    return 0;
}
