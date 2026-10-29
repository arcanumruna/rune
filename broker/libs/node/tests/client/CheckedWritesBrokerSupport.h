#pragma once
// CheckedWritesBrokerSupport.h — checked writes Part 02: helpers for the real-broker slice (typed define, a raw produce
// with a bounded wait, worker-state probes that run on the owning thread).
#include "client/OneBroker.h"
#include "support/CheckedWritesBatches.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/producer/Producer.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/Worker.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace rune::node::test::cw_broker {

using namespace std::chrono_literals;
namespace rc = rune::client;

/// A typed topic {id uint64 key, name string(16), bio string, blob bytes, kind enum} with `partitions` partitions.
inline rc::DefineTopicResult defineTyped(rc::Client& c, const std::string& name, uint32_t partitions) {
    const auto blob = rune::codec::serialize(rune::node::cw::matrixDescriptor());
    return c.defineTopicAndWait(name, partitions, "id", blob, false, rc::Persistence::Disk, {}, {}, 60s);
}

/// Worker state of a partition, read on the worker's own thread (the topic need not be built).
inline bool hasWriteState(BrokerNode& n, int32_t topicId, int32_t partition) {
    const int32_t w = rune::metadata::ShardRouter(n.workerCountForTest()).workerForPartition(topicId, partition);
    return n.onWorkerThreadForTest(w, [topicId, partition](Worker& wk) { return wk.writeStateForTest(topicId, partition) != nullptr; });
}

inline bool anyWorkerHasValidator(BrokerNode& n, int32_t topicId) {
    bool any = false;
    for (int32_t w = 0; w < n.workerCountForTest(); ++w)
        any = any || n.onWorkerThreadForTest(w, [topicId](Worker& wk) { return wk.hasValidatorForTest(topicId); });
    return any;
}

inline int32_t catalogTopicId(BrokerNode& n, const std::string& name) {
    return n.onAdminThreadForTest([&n, &name]() -> int32_t {
        const auto* t = n.adminWorkerAgentForTest()->worker().applier().state().topicByName(rune::catalog::kDefaultNsId, name);
        return t != nullptr ? t->topicId : -1;
    });
}

inline constexpr uint16_t kNotOk = 0xFFFF;   // a completion that is neither Ok nor a broker error

/// One raw produce of `value` (key = `key`) to partition-hashed `topicId` on a fresh client; the completion's broker
/// error code (0 = Ok).
inline uint16_t produceRaw(OneBroker& b, const std::string& topicName, int32_t topicId, int32_t partitions,
                           const std::string& value, const rc::Key& key) {
    rc::ClientConfig cfg = b.clientConfig();
    cfg.topics.push_back(rc::TopicSpec{topicName, topicId, partitions});
    rc::Client client(cfg);
    client.connectAndWait(30s);
    rc::ProducerConfig pc;
    pc.acks     = rc::Acks::All;
    pc.lingerUs = 0;
    rc::Producer prod = client.producer(pc);
    rc::Completions cq(client);
    const rc::TopicHandle topic = client.topic(topicName);
    rc::SendResult r = rc::SendResult::BackPressured;
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (r == rc::SendResult::BackPressured && std::chrono::steady_clock::now() < deadline) {
        r = prod.trySend(topic, key, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(value.data()), value.size()), cq, 1);
        if (r == rc::SendResult::BackPressured) std::this_thread::yield();
    }
    if (r != rc::SendResult::Ok) return kNotOk;
    prod.flush();
    uint16_t out = kNotOk;
    bool done = false;
    while (!done && std::chrono::steady_clock::now() < deadline) {
        (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) {
            done = true;
            out = d.status == rc::OpStatus::Ok ? uint16_t{0} : d.status == rc::OpStatus::BrokerError ? d.errorCode : kNotOk;
        });
        std::this_thread::yield();
    }
    prod.close();
    client.close();
    return out;
}

}  // namespace rune::node::test::cw_broker
