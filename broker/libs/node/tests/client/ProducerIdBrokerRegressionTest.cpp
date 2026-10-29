// ProducerIdBrokerRegressionTest.cpp - Phase 2b F Task 4 (FD4): every acked producer gets its producer id from
// the BROKER (InitProducerId with an empty transactional id).  Two REAL clients, one producer each, against ONE
// real in-process BrokerNode over Aeron UDP: the ids are distinct and come from the broker's block (never the
// client-drawn random ones of the old code), and every record of both topics lands.  No broker change was needed:
// the broker already answers an empty-id InitProducerId (idempotent-only producers).
//
// "Consume back" reads the partition logs on each partition's owner thread; every wait is a deadline loop.
#include "client/OneBroker.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/producer/Producer.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {

constexpr int32_t kParts   = 2;
constexpr int     kRecords = 40;

/// The values stored in one partition, in offset order (read on the partition's owner thread), and the producer
/// ids the stored batches carry.
struct Stored {
    std::vector<std::string> values;
    std::set<int64_t>        producerIds;
};

Stored readPartition(BrokerNode& n, int32_t topicId, int32_t partition) {
    return n.onOwnerThreadForTest(topicId, partition, [](rune::replication::PartitionReplica& r) {
        Stored out;
        rune::storage::Log& log = *r.log();
        int64_t next = 0;
        const int64_t leo = log.logEndOffset();
        while (next < leo) {
            const std::vector<uint8_t> data = log.read(next, 1 << 22);
            if (data.empty()) break;
            const int64_t before = next;
            std::size_t pos = 0;
            while (pos < data.size()) {
                rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                if (!reader.ensureValid()) break;
                const auto tl = static_cast<std::size_t>(reader.totalLength());
                if (!reader.isControl()) {
                    out.producerIds.insert(reader.producerId());
                    reader.forEachRecord([&](std::string_view, std::string_view v, int64_t off, int64_t) {
                        if (off < next) return;
                        out.values.emplace_back(v);
                        next = off + 1;
                    });
                }
                if (tl == 0) break;
                pos += tl;
            }
            if (next == before) break;
        }
        return out;
    });
}

struct Side {
    std::unique_ptr<rc::Client> client;
    int32_t                     topicId = -1;
    std::string                 name;
};

}  // namespace

TEST(ProducerIdBrokerRegression, TwoPlainProducersGetDistinctBrokerIdsRegression) {
    OneBroker broker("pid_two");
    Side sides[2];
    // Topics first (the Client takes its topic list at construction).
    {
        rc::Client* admin = broker.connectClient();
        for (int i = 0; i < 2; ++i) {
            sides[i].name = "pid.t" + std::to_string(i);
            const auto r  = admin->defineTopicAndWait(sides[i].name, kParts, "", {}, rc::Persistence::Disk, {}, {}, 30s);
            ASSERT_EQ(r.errorCode, 0) << r.detail;
            sides[i].topicId = r.topicId;
        }
    }
    const std::filesystem::path base = std::filesystem::temp_directory_path() / ("rune_pidbroker_" + std::to_string(::getpid()));
    for (int i = 0; i < 2; ++i) {
        rc::ClientConfig c;
        c.gatewayChannel  = broker.clientChannel();
        c.gatewayStreamId = 1;
        c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        c.egressStreamId  = 2;
        c.dataDir         = (base / ("c" + std::to_string(i))).string();
        c.topics.push_back(rc::TopicSpec{sides[i].name, sides[i].topicId, kParts});
        sides[i].client = std::make_unique<rc::Client>(c);
        sides[i].client->connectAndWait(30s);
    }

    rc::ProducerConfig pc;
    pc.acks     = rc::Acks::Leader;
    pc.lingerUs = 0;
    rc::Producer prod[2] = {sides[0].client->producer(pc), sides[1].client->producer(pc)};
    rc::Completions cq0(*sides[0].client), cq1(*sides[1].client);
    rc::Completions* cqs[2] = {&cq0, &cq1};

    std::map<rc::Token, rc::Completion> done[2];
    for (int i = 0; i < 2; ++i) {
        const rc::TopicHandle topic = sides[i].client->topic(sides[i].name);
        for (int k = 0; k < kRecords; ++k) {
            const std::string v = "t" + std::to_string(i) + "-" + std::to_string(k);
            rc::SendResult r = rc::SendResult::BackPressured;
            for (int spin = 0; spin < 1'000'000 && r == rc::SendResult::BackPressured; ++spin) {
                r = prod[i].trySend(topic, rc::Key(static_cast<uint64_t>(k)),
                                    std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()),
                                    *cqs[i], static_cast<rc::Token>(k + 1));
                if (r == rc::SendResult::BackPressured) std::this_thread::yield();
            }
            ASSERT_EQ(r, rc::SendResult::Ok) << "side " << i << " record " << k;
        }
        prod[i].flush();
    }
    for (int i = 0; i < 2; ++i) {
        const auto deadline = std::chrono::steady_clock::now() + 60s;
        while (done[i].size() < static_cast<std::size_t>(kRecords) && std::chrono::steady_clock::now() < deadline) {
            (void)cqs[i]->poll([&](const rc::Completion& d, std::span<const uint8_t>) { done[i][d.token] = d; });
            std::this_thread::yield();
        }
        ASSERT_EQ(done[i].size(), static_cast<std::size_t>(kRecords)) << "side " << i;
        for (const auto& [tok, d] : done[i]) {
            EXPECT_EQ(d.status, rc::OpStatus::Ok) << "side " << i << " token " << tok;
        }
    }

    // The handle reports the broker-assigned ids: valid, distinct, never the unit-rig id.
    const int64_t id0 = prod[0].producerId();
    const int64_t id1 = prod[1].producerId();
    EXPECT_GE(id0, 0);
    EXPECT_GE(id1, 0);
    EXPECT_NE(id0, id1) << "two producers (two clients) must not share a producer id";
    EXPECT_NE(id0, rune::client::producer::kRigProducerId);

    // The stored batches carry exactly those ids; every record of both topics landed.
    for (int i = 0; i < 2; ++i) {
        std::multiset<std::string> got;
        std::set<int64_t>          ids;
        for (int32_t p = 0; p < kParts; ++p) {
            const Stored s = readPartition(broker.node(), sides[i].topicId, p);
            got.insert(s.values.begin(), s.values.end());
            ids.insert(s.producerIds.begin(), s.producerIds.end());
        }
        std::multiset<std::string> want;
        for (int k = 0; k < kRecords; ++k) want.insert("t" + std::to_string(i) + "-" + std::to_string(k));
        EXPECT_EQ(got, want) << "topic " << i << ": every record exactly once";
        EXPECT_EQ(ids, (std::set<int64_t>{i == 0 ? id0 : id1})) << "topic " << i << ": batches stamped with the broker's id";
    }
    for (int i = 0; i < 2; ++i) {
        prod[i].close();
        sides[i].client->close();
    }
    std::error_code ec;
    std::filesystem::remove_all(base, ec);
}
