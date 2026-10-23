// tests/support/GroupRig.h — the consumer-group test rig shared by GroupRoutingRegressionTest and
// GroupSeekTest: one Invoker client + one InvokerGateway modelling several broker nodes (endpoint 0 =
// node 0 = the seed / group coordinator).  Everything is single-threaded and condition-driven: no
// sleeps; every wait is a bounded pump loop.  Defines its names in an ANONYMOUS namespace: include it
// once per translation unit.
#pragma once

#include <filesystem>
#include "InvokerGateway.h"
#include "TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/group/GroupConsumer.h>

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <map>
#include <string>
#include <utility>

namespace rc = rune::client;

namespace {
int g_ctr = 0;  // test thread only

std::string udp(int port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

// MEDIUM-6: never a fixed port — the kernel picks a free one.
int freePort() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{};
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port        = 0;
    (void)::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
    socklen_t len = sizeof a;
    (void)::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    ::close(fd);
    return ntohs(a.sin_port);
}

struct RigOptions {
    // Each fake node sends from its own UDP socket (a distinct
    // Image::sourceIdentity), like real broker nodes on distinct hosts; the
    // client's egress channel is UDP then.  Default: IPC egress (one source).
    bool    udpSources        = false;
    int64_t keepaliveNs       = 0;  // 0 = agent defaults
    int64_t responseTimeoutNs = 0;
    int64_t connectTimeoutMs  = 0;  // 0 = the client default
    bool    reconnectForever  = false;
    int64_t pingTimeoutMs     = 0;  // 0 = the client default
};

struct GroupRig {
    RigOptions                   opt;
    std::string                  egress;
    rc::ClientConfig             c;
    rc::Client                   client;
    test_support::InvokerGateway gw;
    rc::Completions              cq{client};
    rc::Token                    nextToken = 1;

    // Node 0 (the seed + group coordinator) on a free port.  A real clock: UDP
    // sessions need the SessionMachine's connect retry (duty clock).
    GroupRig(int32_t in, int32_t eg, RigOptions o = {})
        : opt(o),
          egress(o.udpSources ? udp(freePort()) : std::string("aeron:ipc")),
          c(makeCfg(in, eg, udp(freePort()), egress, o)),
          client(c),
          gw(c.dataDir + "/aeron", c.gatewayChannel, in, egress, eg, pubChannel()) {
        const std::string& seed = c.gatewayChannel;
        gw.brokerHost       = "127.0.0.1";
        gw.brokerPort       = std::stoi(seed.substr(seed.rfind(':') + 1));
        gw.uniqueSessionIds = true;   // each node session gets its own id
        gw.groupProtocol    = true;
        gw.holdJoins        = true;   // the test releases the join once routes are known
        gw.replyPongs       = true;   // keepalives answered (each node answers its own)
        gw.replyFromArrival = true;   // each node answers / pushes from its own publication
    }
    // The reply publication of the next fake node (a distinct source when udpSources).
    std::string pubChannel() const {
        return opt.udpSources ? egress + "|interface=127.0.0.1:" + std::to_string(freePort()) : std::string();
    }
    /// Adds a fake broker node on a free port; returns (endpoint index, port).
    std::pair<int, int> addNode(int32_t stream) {
        const int port = freePort();
        gw.nextPubChannel = pubChannel();
        const int ep = gw.addEndpoint(udp(port), stream);
        gw.nodeOfEndpoint[ep] = ep;   // a broker of its own: its own session table (strict sessions)
        return {ep, port};
    }
    static rc::ClientConfig makeCfg(int32_t in, int32_t eg, const std::string& seed, const std::string& egress,
                                    const RigOptions& o) {
        rc::ClientConfig c;
        c.gatewayChannel  = seed;
        c.gatewayStreamId = in;
        c.egressChannel   = egress;
        c.egressStreamId  = eg;
        c.dataDir = std::filesystem::temp_directory_path().string() + "/group_routing_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
        c.runnerMode = rc::RunnerMode::Invoker;
        c.keepaliveIntervalNs = o.keepaliveNs;
        c.responseTimeoutNs   = o.responseTimeoutNs;
        if (o.connectTimeoutMs > 0) c.connectTimeoutMs = o.connectTimeoutMs;
        c.reconnectForeverAfterLive = o.reconnectForever;
        if (o.pingTimeoutMs > 0) c.pingTimeoutMs = o.pingTimeoutMs;
        rc::TopicSpec ts;
        ts.name           = "t";
        ts.topicId        = 1;
        ts.partitionCount = 2;
        c.topics.push_back(ts);
        rc::TopicSpec tb;
        tb.name           = "b";
        tb.topicId        = 2;
        tb.partitionCount = 1;
        c.topics.push_back(tb);
        return c;
    }
    template <class P>
    bool drive(P&& pred, int cap = 400'000) {
        for (int i = 0; i < cap; ++i) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        const rc::Token t = nextToken++;
        ASSERT_EQ(client.connect(cq, t), rc::OpResult::Accepted);
        bool ok = false;
        ASSERT_TRUE(drive([&] {
            cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
                if (cc.token == t) {
                    ok      = cc.status == rc::OpStatus::Ok;
                    seedSid = cc.value;   // the seed session's broker id
                }
            });
            return ok;
        }));
    }
    int64_t seedSid = 0;  // set by connect()
    /// A subscription for partition `p` registered on endpoint `ep`, or nullptr.
    [[nodiscard]] const test_support::InvokerGateway::Subscribed* subAt(int32_t p, int ep) const {
        const auto* s = gw.latestSubscription(1, p);
        return (s != nullptr && s->endpoint == ep) ? s : nullptr;
    }
};
} // namespace
