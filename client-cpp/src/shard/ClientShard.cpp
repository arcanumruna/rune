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

// ---------------------------------------------------------------------------
// ClientShard
// ---------------------------------------------------------------------------

ClientShard::ClientShard(const ShardGroupConfig& g, uint32_t index)
    : ClientShard(Validated{}, g, index, g.shardCount, validatedSingleShardConfig(g, index)) {}

ClientShard::ClientShard(const ShardGroupConfig& g, uint32_t index, uint32_t count, int32_t runnerCore)
    : ClientShard(Validated{}, g, index, count, shardClientConfig(g, index, count, runnerCore)) {}

ClientShard::ClientShard(Validated, const ShardGroupConfig& g, uint32_t index, uint32_t count,
                         ClientConfig cc)
    : index_(index),
      count_(count),
      mode_(g.mode),
      client_(std::make_unique<Client>([&] {
          cc.shardStatus = true;  // the Client derives + publishes this shard's status
          cc.shardIndex  = index;
          return std::move(cc);
      }())),
      producer_(client_->producer(shardProducerConfig(g))) {
    seen_.shard = index_;
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

ClientShard::~ClientShard() { closeImmediately(); }

void ClientShard::send(TopicHandle topic, const Key& key, std::span<const uint8_t> value) {
    if (!topic.valid()) throw std::invalid_argument("ClientShard::send: invalid topic handle");
    sendTo(topic, key.partition(topic.partitionCount), key, value);
}

void ClientShard::sendTo(TopicHandle topic, int32_t partition, const Key& key, std::span<const uint8_t> value) {
    switch (trySendTo(topic, partition, key, value)) {
    case SendResult::Ok:
        return;
    case SendResult::WrongShard:
        throw std::logic_error("ClientShard::send: partition owned by another shard");
    case SendResult::Closed:
        throw std::runtime_error("ClientShard::send: shard closed");
    case SendResult::QueueFull:
        throw std::runtime_error("ClientShard::send: batch-ack queue full — pollBatchAcks(), then retry");
    case SendResult::InvalidTopic:
        throw std::invalid_argument("ClientShard::send: invalid topic handle");
    case SendResult::InvalidArgument:
        throw std::invalid_argument("ClientShard::send: partition out of range");
    default:
        // Back-pressured: the producer backs off (Agent) or pumps (Invoker owner).
        // Review finding 10: the cycles it pumped may have failed the shard —
        // latch it here too (Invoker), whether the send succeeded or threw.
        try {
            producer_.sendTo(topic, partition, key, value);
        } catch (...) {
            if (mode_ == ShardMode::Invoker) latchClosed_();
            throw;
        }
        if (mode_ == ShardMode::Invoker) latchClosed_();
    }
}

int ClientShard::doWork() {
    if (mode_ != ShardMode::Invoker) {
        throw std::logic_error("ClientShard::doWork(): invoker mode only");
    }
    if (tornDown_) return 0;
    const int work = client_->doWork();
    latchClosed_();
    return work;
}

OpStatus ClientShard::close() { return closeImpl_(nullptr); }

OpStatus ClientShard::closeAndWait(std::chrono::nanoseconds timeout) { return closeImpl_(&timeout); }

OpStatus ClientShard::closeImpl_(const std::chrono::nanoseconds* timeout) {
    if (tornDown_) return OpStatus::Closed;
    // 1. Begin-close + wait for the drain (Closed at once when the intake is
    //    already closed: failed, or a begin-close / closeImmediately earlier).
    OpStatus st = OpStatus::Closed;
    try {
        st = timeout != nullptr ? producer_.closeAndWait(*timeout) : producer_.close();
    } catch (const std::logic_error&) {
        throw;  // from a duty-cycle callback: the caller's bug, nothing torn down
    } catch (...) {
        closeImmediately();  // the bounded wait passed: tear down anyway
        throw;
    }
    // 2. Teardown: Client::close() after a complete drain (its own close drain
    //    finds nothing — join / stop); otherwise closeImmediately.
    teardown_(st == OpStatus::Ok);
    return st;
}

void ClientShard::teardown_(bool drained) noexcept {
    if (tornDown_) return;
    if (drained) {
        try {
            client_->close();
        } catch (...) {
            client_->closeImmediately();  // e.g. the caller is a duty-cycle callback
        }
    } else {
        client_->closeImmediately();
    }
    finishTeardown_();
}

void ClientShard::closeImmediately() noexcept {
    if (tornDown_) return;
    // Client close completes every outstanding op and every accepted record
    // Closed (exactly once), a pending begin-close token Closed, and joins the
    // runner (Agent) — after it this thread is the ring's only producer.
    client_->closeImmediately();
    finishTeardown_();
}

void ClientShard::finishTeardown_() noexcept {
    tornDown_ = true;
    client_->publishShardClosed();  // no duty thread left: this thread publishes Closed
    if (mode_ == ShardMode::Invoker) closed_ = true;  // Agent: app threads may still be sending
}

// ---------------------------------------------------------------------------
// ShardGroup
// ---------------------------------------------------------------------------

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
    uint32_t i = 0;
    auto tearDownStarted = [&] {
        while (!shards_.empty()) {  // k-1 .. 0
            teardown_(static_cast<uint32_t>(shards_.size() - 1));
            shards_.pop_back();
        }
    };
    try {
        for (; i < n.count; ++i) {
            shards_.push_back(std::unique_ptr<ClientShard>(new ClientShard(cfg_, i, n.count, cores[i])));
        }
    } catch (const std::invalid_argument& e) {
        tearDownStarted();
        throw std::invalid_argument("ShardGroup: shard " + std::to_string(i) + ": " + e.what());
    } catch (const std::logic_error& e) {
        tearDownStarted();
        throw std::logic_error("ShardGroup: shard " + std::to_string(i) + ": " + e.what());
    } catch (const std::exception& e) {
        tearDownStarted();
        throw std::runtime_error("ShardGroup: shard " + std::to_string(i) + ": " + e.what());
    } catch (...) {
        tearDownStarted();
        throw;
    }
}

ShardGroup::~ShardGroup() {
    closeImmediately();
    while (!shards_.empty()) shards_.pop_back();
}

void ShardGroup::teardown_(uint32_t i, bool drained) noexcept {
    ClientShard& s = *shards_[i];
    if (s.tornDown()) return;
    if (cfg_.testTeardownHook != nullptr) cfg_.testTeardownHook(cfg_.testTeardownCtx, i);
    s.teardown_(drained);
}

ShardCompletions::ShardCompletions(ShardGroup& group, CompletionsConfig cfg) {
    queues_.reserve(group.size());
    for (uint32_t i = 0; i < group.size(); ++i) {
        queues_.push_back(std::make_unique<Completions>(group.shard(i).client(), cfg));
    }
}

OpResult ShardGroup::connectAll(ShardCompletions& q, Token base) {
    if (q.size() != shards_.size()) return OpResult::InvalidArgument;
    if (base > kMaxToken - shards_.size()) return OpResult::InvalidArgument;
    for (uint32_t i = 0; i < shards_.size(); ++i) {
        const OpResult r = shards_[i]->connect(q.at(i), base + i);
        if (r != OpResult::Accepted) return r;
    }
    return OpResult::Accepted;
}

void ShardGroup::connectAllAndWait(std::chrono::nanoseconds timeout) {
    // Agent mode: the runners make progress by themselves; this thread owns the
    // queues and only polls them (Completions::waitFor, no pump).
    ShardCompletions q(*this);
    const OpResult   r = connectAll(q, 0);
    if (r != OpResult::Accepted) {
        throw std::runtime_error("ShardGroup::connectAll: a shard rejected connect (OpResult " +
                                 std::to_string(static_cast<int>(r)) + ")");
    }
    // ONE deadline for the whole loop (review finding 7): never N x timeout.
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (uint32_t i = 0; i < shards_.size(); ++i) {
        auto left = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - std::chrono::steady_clock::now());
        if (left.count() < 0) left = std::chrono::nanoseconds{0};
        OpStatus   st = OpStatus::Closed;
        const auto w  = q.at(i).waitFor(i, left,
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
    const int32_t partition = partitionOf_(topic, key);  // the ONE hash (M3)
    return owner(topic.topicId, partition).trySendTo(topic, partition, key, value);
}

void ShardGroup::send(TopicHandle topic, const Key& key, std::span<const uint8_t> value) {
    if (!topic.valid()) throw std::invalid_argument("ShardGroup::send: invalid topic handle");
    const int32_t partition = partitionOf_(topic, key);  // the ONE hash (M3)
    owner(topic.topicId, partition).sendTo(topic, partition, key, value);
}

OpStatus ShardGroup::close(std::chrono::nanoseconds timeout) {
    bool any = false;
    for (const auto& s : shards_) any = any || !s->tornDown();
    if (!any) return OpStatus::Closed;
    // 1. Begin-close every intake first — each one synchronous, so no send is
    //    accepted after this loop — before waiting on any drain.
    std::vector<Completions> cqs;
    cqs.reserve(shards_.size());
    std::vector<bool> pending(shards_.size(), false);
    std::vector<bool> drainedOk(shards_.size(), false);
    OpStatus          result = OpStatus::Ok;
    for (uint32_t i = 0; i < shards_.size(); ++i) {
        ClientShard& s = *shards_[i];
        cqs.emplace_back(s.client());
        if (s.tornDown()) continue;
        const OpResult r = s.close(cqs.back(), i);
        if (r == OpResult::Accepted) {
            pending[i] = true;
        } else if (result == OpStatus::Ok) {
            result = OpStatus::Closed;  // already closing / failed: nothing to drain
        }
    }
    // 2. Every drain against ONE deadline.
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (uint32_t i = 0; i < shards_.size(); ++i) {
        if (!pending[i]) continue;
        auto left = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - std::chrono::steady_clock::now());
        if (left.count() < 0) left = std::chrono::nanoseconds{0};
        OpStatus   st = OpStatus::Timeout;
        const auto w  = cqs[i].waitFor(i, left,
                                       [&](const Completion& c, std::span<const uint8_t>) { st = c.status; });
        if (w != Completions::Wait::Done) st = OpStatus::Timeout;
        drainedOk[i] = st == OpStatus::Ok;
        if (st != OpStatus::Ok && result == OpStatus::Ok) result = st;
    }
    // 3. Teardown, reverse order: Client::close() for the drained ones (quick:
    //    nothing left), closeImmediately for the rest.
    for (uint32_t i = static_cast<uint32_t>(shards_.size()); i-- > 0;) {
        teardown_(i, drainedOk[i]);
    }
    return result;
}

void ShardGroup::closeImmediately() noexcept {
    for (uint32_t i = static_cast<uint32_t>(shards_.size()); i-- > 0;) teardown_(i);
}

} // namespace rune::client::shard
