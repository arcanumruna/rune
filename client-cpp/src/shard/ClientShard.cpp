// src/shard/ClientShard.cpp — ClientShard / ShardGroup (plan Task 15, T15b).
#include <rune/client/shard/ClientShard.h>

#include <cstdio>
#include <stdexcept>
#include <string>
#include <utility>

namespace rune::client::shard {

namespace {

// The single-index form: an explicit count only (shardClientConfig(g, i)
// rejects AUTO and applies the core rules), validated before any driver starts.
ClientConfig validatedSingleShardConfig(const ShardGroupConfig& g, uint32_t index) {
    ClientConfig cc = shardClientConfig(g, index);
    validateShardGroupConfig(g, g.shardCount);
    return cc;
}

} // namespace

ClientShard::ClientShard(const ShardGroupConfig& g, uint32_t index)
    : ClientShard(Validated{}, g, index, g.shardCount, validatedSingleShardConfig(g, index)) {}

ClientShard::ClientShard(const ShardGroupConfig& g, uint32_t index, uint32_t count, int32_t runnerCore)
    : ClientShard(Validated{}, g, index, count, shardClientConfig(g, index, count, runnerCore)) {}

ClientShard::ClientShard(Validated, const ShardGroupConfig& g, uint32_t index, uint32_t count,
                         ClientConfig cc)
    : index_(index),
      count_(count),
      mode_(g.mode),
      client_(std::make_unique<Client>(std::move(cc))),
      producer_(client_->producer(shardProducerConfig(g))) {
    if (mode_ == ShardMode::Invoker) {
        // The constructing thread is the Invoker owner: it owns the queue too.
        // Credit (Producer::batchAcksTo): every send reserves 48 B until its
        // batch ack is polled; BatchAck records carry no payload.
        CompletionsConfig qc;
        qc.capacityBytes  = kBatchAckQueueBytes;
        qc.maxResultBytes = 0;
        qc.maxErrorText   = 0;
        acks_.emplace(*client_, qc);
        producer_.batchAcksTo(*acks_);  // setup phase: before the first send
    }
}

void ClientShard::send(TopicHandle topic, const Key& key, std::span<const uint8_t> value) {
    switch (trySend(topic, key, value)) {
    case SendResult::Ok:
        return;
    case SendResult::WrongShard:
        throw std::logic_error("ClientShard::send: partition owned by another shard");
    case SendResult::Closed:
        throw std::runtime_error("ClientShard::send: shard closed");
    case SendResult::QueueFull:
        throw std::runtime_error("ClientShard::send: batch-ack queue full — pollBatchAcks(), then retry");
    default:
        producer_.send(topic, key, value);  // backs off / pumps; throws on invalid input
    }
}

int ClientShard::doWork() {
    if (mode_ != ShardMode::Invoker) {
        throw std::logic_error("ClientShard::doWork(): invoker mode only");
    }
    const int work = client_->doWork();
    if (!closed_ && client_->transportClosed()) closed_ = true;
    return work;
}

ShardGroup::ShardGroup(ShardGroupConfig cfg) : cfg_(std::move(cfg)) {
    if (cfg_.mode != ShardMode::Agent) {
        throw std::invalid_argument(
            "ShardGroup: agent mode only; in invoker mode each app thread constructs its own "
            "ClientShard(cfg, i)");
    }
    // Control thread, before any driver starts (design §2a): N is resolved once
    // and fixed for the life of the group (shardFor needs a stable N).
    const CpuSet               allowed = platformAllowedCpus();
    const ShardCount           n       = resolveShardCount(cfg_, allowed, platformQuotaCpus());
    const std::vector<int32_t> cores   = assignShardCores(cfg_, n.count, allowed, platformPinningSupported());
    validateShardGroupConfig(cfg_, n.count);
    std::fprintf(stderr, "rune-client: shard group: %u shard(s) — %s\n", n.count, n.reason.c_str());
    for (const std::string& w : shardConfigWarnings(cfg_, n.count)) {
        std::fprintf(stderr, "rune-client: shard group warning: %s\n", w.c_str());
    }
    cfg_.shardCount = n.count;

    shards_.reserve(n.count);
    try {
        for (uint32_t i = 0; i < n.count; ++i) {
            shards_.push_back(std::make_unique<ClientShard>(cfg_, i, n.count, cores[i]));
        }
    } catch (...) {
        while (!shards_.empty()) shards_.pop_back();  // k-1 .. 0
        throw;
    }
}

ShardGroup::~ShardGroup() {
    while (!shards_.empty()) shards_.pop_back();
}

void ShardGroup::connectAll(std::chrono::nanoseconds timeout) {
    // Agent mode: the runners make progress by themselves; this thread owns the
    // queues and only polls them (Completions::waitFor, no pump).  reserve()
    // first: a registered Completions is never moved after an op is issued.
    std::vector<Completions> cqs;
    cqs.reserve(shards_.size());
    for (uint32_t i = 0; i < shards_.size(); ++i) {
        cqs.emplace_back(shards_[i]->client());
        const OpResult r = shards_[i]->connect(cqs.back(), i);
        if (r != OpResult::Accepted) {
            throw std::runtime_error("ShardGroup::connectAll: shard " + std::to_string(i) +
                                     " rejected connect (OpResult " +
                                     std::to_string(static_cast<int>(r)) + ")");
        }
    }
    for (uint32_t i = 0; i < shards_.size(); ++i) {
        OpStatus   st = OpStatus::Closed;
        const auto w  = cqs[i].waitFor(i, timeout,
                                       [&](const Completion& c, std::span<const uint8_t>) { st = c.status; });
        if (w != Completions::Wait::Done || st != OpStatus::Ok) {
            throw std::runtime_error("ShardGroup::connectAll: shard " + std::to_string(i) +
                                     (w == Completions::Wait::Done
                                          ? " did not connect (status " + std::to_string(static_cast<int>(st)) + ")"
                                          : std::string(" did not connect within the timeout")));
        }
    }
}

SendResult ShardGroup::trySend(TopicHandle topic, const Key& key, std::span<const uint8_t> value) noexcept {
    if (!topic.valid()) return SendResult::InvalidTopic;
    return owner(topic.topicId, key.partition(topic.partitionCount)).trySend(topic, key, value);
}

void ShardGroup::send(TopicHandle topic, const Key& key, std::span<const uint8_t> value) {
    if (!topic.valid()) throw std::invalid_argument("ShardGroup::send: invalid topic handle");
    owner(topic.topicId, key.partition(topic.partitionCount)).send(topic, key, value);
}

} // namespace rune::client::shard
