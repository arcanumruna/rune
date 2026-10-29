// ResponseChannelRegression — NAT-safe egress (spec D2/D7): the pure response-channel helpers.
#include <rune/client/transport/ResponseChannel.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using rune::client::transport::responseControlChannelFor;
using rune::client::transport::splitRedirectTarget;
using rune::client::transport::withResponseCorrelation;

namespace {
struct ChanCase {
    const char* name;
    std::string channel;
    int32_t     port;
    const char* expect;   // nullptr = nullopt
};
} // namespace

TEST(ResponseChannelRegression, ResponseControlChannelTable) {
    const std::vector<ChanCase> cases = {
        {"name",        "aeron:udp?endpoint=gw.example.test:40100", 40163, "aeron:udp?control=gw.example.test:40163|control-mode=response"},
        {"ipv4",        "aeron:udp?endpoint=127.0.0.1:40100", 40163, "aeron:udp?control=127.0.0.1:40163|control-mode=response"},
        {"ipv6",        "aeron:udp?endpoint=[::1]:40100", 40163, "aeron:udp?control=[::1]:40163|control-mode=response"},
        {"extra params", "aeron:udp?term-length=65536|endpoint=127.0.0.1:40100|mtu=1408", 7, "aeron:udp?control=127.0.0.1:7|control-mode=response"},
        {"ipc",         "aeron:ipc", 40163, nullptr},
        {"no endpoint", "aeron:udp?term-length=65536", 40163, nullptr},
        {"no query",    "aeron:udp", 40163, nullptr},
        {"no port in endpoint", "aeron:udp?endpoint=hostonly", 40163, nullptr},
        {"empty host",  "aeron:udp?endpoint=:40100", 40163, nullptr},
        {"bare host:port", "127.0.0.1:40100", 40163, nullptr},
        {"port 0",      "aeron:udp?endpoint=127.0.0.1:40100", 0, nullptr},
        {"port 1",      "aeron:udp?endpoint=127.0.0.1:40100", 1, "aeron:udp?control=127.0.0.1:1|control-mode=response"},
        {"port 65535",  "aeron:udp?endpoint=127.0.0.1:40100", 65535, "aeron:udp?control=127.0.0.1:65535|control-mode=response"},
        {"port 65536",  "aeron:udp?endpoint=127.0.0.1:40100", 65536, nullptr},
        {"port -1",     "aeron:udp?endpoint=127.0.0.1:40100", -1, nullptr},
    };
    for (const auto& c : cases) {
        const auto got = responseControlChannelFor(c.channel, c.port);
        if (c.expect == nullptr) {
            EXPECT_FALSE(got.has_value()) << c.name;
        } else {
            ASSERT_TRUE(got.has_value()) << c.name;
            EXPECT_EQ(*got, c.expect) << c.name;
        }
    }
}

TEST(ResponseChannelRegression, WithResponseCorrelation) {
    EXPECT_EQ(withResponseCorrelation("aeron:udp?endpoint=h:1", 42), "aeron:udp?endpoint=h:1|response-correlation-id=42");
    EXPECT_EQ(withResponseCorrelation("aeron:udp", 7), "aeron:udp?response-correlation-id=7");
    EXPECT_EQ(withResponseCorrelation("aeron:udp?endpoint=h:1", 9000000000000LL),
              "aeron:udp?endpoint=h:1|response-correlation-id=9000000000000");
}

TEST(ResponseChannelRegression, SplitRedirectTargetTable) {
    struct Case { const char* msg; const char* endpoint; int32_t port; };
    const Case cases[] = {
        {"h:p", "h:p", 0},
        {"h:p#7", "h:p", 7},
        {"aeron:udp?endpoint=h:p#7", "aeron:udp?endpoint=h:p", 7},
        {"h:p#", "h:p", 0},
        {"#7", "", 7},
        {"h:p#abc", "h:p", 0},
        {"h:p#0", "h:p", 0},
        {"h:p#65535", "h:p", 65535},
        {"h:p#65536", "h:p", 0},
        {"h:p#-1", "h:p", 0},
        {"h:p#7#8", "h:p#7", 8},
        {"h:p#123456789012", "h:p", 0},
        {"", "", 0},
    };
    for (const auto& c : cases) {
        const auto t = splitRedirectTarget(c.msg);
        EXPECT_EQ(std::string(t.endpoint), c.endpoint) << c.msg;
        EXPECT_EQ(t.responsePort, c.port) << c.msg;
    }
}
