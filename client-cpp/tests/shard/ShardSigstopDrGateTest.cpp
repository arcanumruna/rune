// tests/shard/ShardSigstopDrGateTest.cpp — T15b Gate 4 (design note I7 / R2),
// run locally (in-process + fork; the k3s-node-01 run is a later DR pass).
//
// A client PROCESS (fork) runs an Agent ShardGroup (N = 2, UDP ingress + UDP
// egress base + i) under steady load against a fake broker in THIS process (an
// InvokerGateway on its own driver).  The client process is SIGSTOPped for 16 s
// — past the Aeron 10 s image / driver defaults and the 10 s session response
// timeout — then SIGCONTed.  Expected per shard (status events the child
// reports through a pipe): Live -> Reconnecting -> Live, never Failed; the
// shards' 1 h duty-thread liveness / driver timeouts (T15a I4/R2) keep both
// Aeron clients alive across the stall.  Exactly once: every record the child
// accepted completes once (acked Ok — the 30 s delivery timeout is never
// reached); the broker's (deduplicated) sequences per partition are exactly
// [0, acked).
//
// The 16 s is the gate's stimulus (how long the process is stopped); the
// broker keeps working the whole time.  Every other wait is progress-driven
// with a deadline.  The child is always killed + reaped (RAII).
#include "../support/InvokerGateway.h"

#include <rune/client/shard/ClientShard.h>

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <set>
#include <span>
#include <string>
#include <sys/select.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
namespace rs = rune::client::shard;
using namespace std::chrono_literals;

namespace {

constexpr const char* kGateway = "aeron:udp?endpoint=127.0.0.1:24890";
constexpr const char* kEgress  = "aeron:udp?endpoint=127.0.0.1:24900";
constexpr int32_t     kCtl     = 3950;
constexpr int32_t     kEg      = 3951;

void writeLine(int fd, const std::string& s) {
    const std::string l = s + "\n";
    (void)!::write(fd, l.data(), l.size());
}

struct ChildLedger {
    int64_t ok = 0, failed = 0, records = 0;
    static void on(void* ctx, const rc::BatchAck& a) noexcept {
        auto* self = static_cast<ChildLedger*>(ctx);
        self->records += a.recordCount;
        (a.errorCode == 0 ? self->ok : self->failed) += a.recordCount;
    }
};

[[noreturn]] void childMain(int out, int in, const std::string& dir) {
    try {
        rs::ShardGroupConfig g;
        g.base.gatewayChannel  = kGateway;
        g.base.gatewayStreamId = kCtl;
        g.base.egressChannel   = kEgress;
        g.base.egressStreamId  = kEg;
        g.base.dataDir         = dir;
        rc::TopicSpec t;
        t.name           = "t";
        t.topicId        = 1;
        t.partitionCount = 8;
        g.base.topics.push_back(t);
        g.shardCount        = 2;
        g.producer.acks     = rc::Acks::Leader;
        g.producer.lingerUs = 0;
        rs::ShardGroup grp(g);
        ChildLedger    l0, l1;
        grp.onBatchAck(0, &ChildLedger::on, &l0);
        grp.onBatchAck(1, &ChildLedger::on, &l1);
        grp.connectAll(30s);
        writeLine(out, "C");
        const auto                 topic = grp.shard(0).topic("t");
        const std::vector<uint8_t> val(64, 0x42);
        int64_t                    accepted = 0;
        uint64_t                   k        = 0;
        auto report = [&](const rs::ShardStatus& s) {
            writeLine(out, "E " + std::to_string(s.shard) + " " + rs::toString(s.state));
        };
        for (;;) {
            grp.pollEvents(report);
            fd_set rd;
            FD_ZERO(&rd);
            FD_SET(in, &rd);
            timeval tv{0, 0};
            if (::select(in + 1, &rd, nullptr, nullptr, &tv) == 1) break;  // 'q'
            int sent = 0;
            for (; sent < 16; ++sent) {
                if (grp.trySend(topic, rc::Key(k), val) != rc::SendResult::Ok) break;
                ++accepted;
                ++k;
            }
            if (sent == 0) std::this_thread::yield();  // back-pressured
        }
        const rc::OpStatus st = grp.close(30s);
        grp.pollEvents(report);
        writeLine(out, "S " + std::to_string(accepted) + " " + std::to_string(l0.records + l1.records) + " " +
                           std::to_string(l0.ok + l1.ok) + " " + std::to_string(l0.failed + l1.failed) + " " +
                           std::to_string(static_cast<int>(st)));
    } catch (const std::exception& e) {
        writeLine(out, std::string("X ") + e.what());
        ::_exit(2);
    }
    ::_exit(0);
}

// Kills + reaps the child whatever happens to the test.
struct ChildGuard {
    pid_t pid = -1;
    ~ChildGuard() {
        if (pid > 0) {
            ::kill(pid, SIGCONT);
            ::kill(pid, SIGKILL);
            int st = 0;
            ::waitpid(pid, &st, 0);
        }
    }
};

struct Lines {
    int                      fd = -1;
    std::string              buf;
    std::vector<std::string> lines;
    void poll() {
        for (;;) {
            fd_set rd;
            FD_ZERO(&rd);
            FD_SET(fd, &rd);
            timeval tv{0, 0};
            if (::select(fd + 1, &rd, nullptr, nullptr, &tv) != 1) return;
            char      tmp[4096];
            const auto n = ::read(fd, tmp, sizeof tmp);
            if (n <= 0) return;
            buf.append(tmp, static_cast<std::size_t>(n));
            std::size_t nl;
            while ((nl = buf.find('\n')) != std::string::npos) {
                lines.push_back(buf.substr(0, nl));
                buf.erase(0, nl + 1);
            }
        }
    }
};

} // namespace

TEST(ShardDrGateRegression, SigstopPast15sThenSigcontRecoversLiveExactlyOnce) {
    const std::string base = "/tmp/shard_drgate_" + std::to_string(::getpid());
    int toParent[2], toChild[2];
    ASSERT_EQ(::pipe(toParent), 0);
    ASSERT_EQ(::pipe(toChild), 0);
    ChildGuard child;
    child.pid = ::fork();
    ASSERT_GE(child.pid, 0);
    if (child.pid == 0) {
        ::close(toParent[0]);
        ::close(toChild[1]);
        childMain(toParent[1], toChild[0], base + "/client");
    }
    ::close(toParent[1]);
    ::close(toChild[0]);

    // The broker: a driver host (an Invoker shard that never connects) + the
    // gateway on that driver; replies go to each session's announced egress.
    rs::ShardGroupConfig hostCfg;
    hostCfg.base.dataDir         = base + "/broker";
    hostCfg.base.gatewayStreamId = 3960;
    hostCfg.base.egressStreamId  = 3961;
    hostCfg.shardCount           = 1;
    hostCfg.mode                 = rs::ShardMode::Invoker;
    rs::ClientShard              host(hostCfg, 0);
    test_support::InvokerGateway gw(rs::shardClientConfig(hostCfg, 0).dataDir + "/aeron", kGateway, kCtl,
                                    kEgress, kEg);
    gw.replyOnAnnouncedEgress = true;
    gw.uniqueSessionIds       = true;
    Lines in;
    in.fd = toParent[0];
    auto pumpUntil = [&](auto&& pred, std::chrono::seconds limit) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < deadline) {
            host.doWork();
            if (gw.pump() == 0) std::this_thread::yield();
            in.poll();
            if (pred()) return true;
        }
        return false;
    };
    auto sawFrom = [&](std::size_t from, const std::string& line) {
        for (std::size_t i = from; i < in.lines.size(); ++i) {
            if (in.lines[i] == line) return true;
        }
        return false;
    };
    auto failedOrCrashed = [&] {
        for (const auto& l : in.lines) {
            if (l.rfind("X ", 0) == 0 || l.find("Failed") != std::string::npos) return true;
        }
        return false;
    };

    // 1. Both shards Live and records flowing.
    ASSERT_TRUE(pumpUntil([&] { return failedOrCrashed() || (sawFrom(0, "C") && sawFrom(0, "E 0 Live") &&
                                                             sawFrom(0, "E 1 Live") && gw.produced.size() > 200); },
                          30s))
        << "child never got Live under load";
    ASSERT_FALSE(failedOrCrashed()) << (in.lines.empty() ? "" : in.lines.back());

    // 2. SIGSTOP the whole client process for 16 s (the stimulus); the broker
    //    keeps running.
    ASSERT_EQ(::kill(child.pid, SIGSTOP), 0);
    const auto stoppedAt = std::chrono::steady_clock::now();
    // Frames already in flight at the stop still land; after 2 s nothing new.
    (void)pumpUntil([&] { return std::chrono::steady_clock::now() >= stoppedAt + 2s; }, 5s);
    const std::size_t producedAtStop = gw.produced.size();
    (void)pumpUntil([&] { return std::chrono::steady_clock::now() >= stoppedAt + 16s; }, 20s);
    EXPECT_EQ(gw.produced.size(), producedAtStop) << "a stopped process sends nothing";

    // 3. SIGCONT: every shard goes Reconnecting, then Live again; records flow.
    const std::size_t mark = in.lines.size();
    ASSERT_EQ(::kill(child.pid, SIGCONT), 0);
    ASSERT_TRUE(pumpUntil([&] { return failedOrCrashed() || (sawFrom(mark, "E 0 Live") && sawFrom(mark, "E 1 Live") &&
                                                             gw.produced.size() > producedAtStop + 200); },
                          60s))
        << "child did not return to Live after SIGCONT";
    EXPECT_FALSE(failedOrCrashed()) << "a stall past every Aeron default must never fail a shard";
    EXPECT_TRUE(sawFrom(mark, "E 0 Reconnecting"));
    EXPECT_TRUE(sawFrom(mark, "E 1 Reconnecting"));

    // 4. Stop the load; the child closes (drains) and reports its ledger.
    (void)!::write(toChild[1], "q", 1);
    std::string summary;
    ASSERT_TRUE(pumpUntil([&] {
        for (const auto& l : in.lines) {
            if (l.rfind("S ", 0) == 0 || l.rfind("X ", 0) == 0) summary = l;
        }
        return !summary.empty();
    }, 60s));
    int status = 0;
    ASSERT_EQ(::waitpid(child.pid, &status, 0), child.pid);
    child.pid = -1;
    ASSERT_EQ(summary.rfind("S ", 0), 0u) << summary;
    long long accepted = 0, records = 0, ok = 0, failed = 0;
    int       closeSt  = -1;
    ASSERT_EQ(std::sscanf(summary.c_str(), "S %lld %lld %lld %lld %d", &accepted, &records, &ok, &failed, &closeSt),
              5);
    EXPECT_EQ(records, accepted) << "every accepted record completed exactly once";
    EXPECT_EQ(ok, accepted) << "none failed: the stall stayed under the delivery timeout";
    EXPECT_EQ(failed, 0);
    EXPECT_EQ(closeSt, static_cast<int>(rc::OpStatus::Ok));
    EXPECT_FALSE(failedOrCrashed());

    // The broker side, deduplicated as an idempotent broker would: per partition
    // the union of the received sequences is exactly [0, n), and the n add up to
    // the acked records (re-offered frames are duplicates, nothing is missing).
    std::map<int32_t, std::set<int32_t>> seqs;
    std::size_t                          frames = 0;
    for (const auto& pr : gw.produced) {
        ++frames;
        for (int32_t s = 0; s < pr.records; ++s) seqs[pr.partition].insert(pr.baseSequence + s);
    }
    long long distinct = 0;
    for (const auto& [p, s] : seqs) {
        distinct += static_cast<long long>(s.size());
        ASSERT_FALSE(s.empty());
        EXPECT_EQ(*s.begin(), 0) << "partition " << p;
        EXPECT_EQ(*s.rbegin(), static_cast<int32_t>(s.size()) - 1) << "partition " << p << ": a sequence gap";
    }
    EXPECT_EQ(distinct, ok);
    std::printf("[ DR gate ] accepted=%lld acked=%lld frames=%zu distinct=%lld events=%zu\n", accepted, ok, frames,
                distinct, in.lines.size());
    for (const auto& l : in.lines) {
        if (l.rfind("E ", 0) == 0) std::printf("[ DR gate ] %s\n", l.c_str());
    }
    ::close(toParent[0]);
    ::close(toChild[1]);
    std::error_code ec;
    std::filesystem::remove_all(base, ec);
}
