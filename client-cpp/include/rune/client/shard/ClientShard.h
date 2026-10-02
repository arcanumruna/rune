// include/rune/client/shard/ClientShard.h — client shards (spec §4.1; plan Task 15,
// T15b; design note docs/superpowers/specs/2026-10-01-t15-clientshard-design.md).
//
// A shard is one Client — its own embedded driver (own dataDir, invoker mode,
// pumped by the shard's one duty thread, D1), own Aeron client, own sessions and
// ack (egress) subscription on its own egress port — plus its Producer.
// Partitions map deterministically to shards (shardFor); a shard only accepts
// its own partitions (two shards on one partition would collide on the shared
// idempotent producer id).  Configuration: ShardConfig.h (T15a).
//
//   ShardMode::Agent   (default): a runner thread per shard (optionally pinned);
//                      ShardGroup::send from any app thread goes to the owner
//                      shard's producer (its many-to-one ring).
//   ShardMode::Invoker (opt-in):  no threads; each app thread constructs its own
//                      ClientShard(cfg, i) and calls trySend()...doWork() in its
//                      own loop.  Records of one pass leave as one frame per
//                      partition (no linger timer, spec §4.2).  Batch acks are
//                      polled with pollBatchAcks() on the same thread.
//
// F8c: no futures, atomics, mutexes or shared_ptr handles.  Connect is the
// Completions/Token op (or connectAndWait sugar); every piece of shard state is
// owned by the shard's duty thread (Invoker: the constructing thread).
#pragma once

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/shard/ShardConfig.h>
#include <rune/client/shard/ShardMap.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::client::shard {

class ClientShard {
public:
    /// Shard `index` of an EXPLICIT g.shardCount (AUTO -> std::invalid_argument:
    /// in Invoker mode N is the number of driving app threads, which only the
    /// app knows).  The group config is validated (validateShardGroupConfig)
    /// before the shard's driver starts.  Invoker mode: the constructing thread
    /// becomes the owner — the only thread that may send, doWork(), poll or
    /// destroy the shard.
    ClientShard(const ShardGroupConfig& g, uint32_t index);

    /// Shard `index` of a layout already resolved and validated by a group
    /// (ShardGroup: count from resolveShardCount, core from assignShardCores).
    ClientShard(const ShardGroupConfig& g, uint32_t index, uint32_t count, int32_t runnerCore);

    ClientShard(const ClientShard&)            = delete;
    ClientShard& operator=(const ClientShard&) = delete;
    ClientShard(ClientShard&&)                 = delete;
    ClientShard& operator=(ClientShard&&)      = delete;

    /// F8c: exactly one Connect completion into `target` when Accepted (a
    /// Completions registered with client() and owned by the calling thread, or
    /// a CompletionHandler on the duty thread).  Invoker: owner thread only,
    /// then drive doWork() and poll.
    [[nodiscard]] OpResult connect(CompletionTarget target, Token token) {
        return client_->connect(target, token);
    }
    /// Blocking sugar: Invoker owner pumps doWork(); Agent polls under the idle
    /// strategy.  Throws std::runtime_error on any non-Ok outcome or timeout.
    void connectAndWait(std::chrono::nanoseconds timeout = Client::kDefaultWaitTimeout) {
        client_->connectAndWait(timeout);
    }

    /// Invoker batch-ack queue: 8 MiB = ~174k accepted-but-unpolled records
    /// (48 B credit each, Producer::batchAcksTo).
    static constexpr uint32_t kBatchAckQueueBytes = 8u << 20;

    /// Non-blocking; zero allocation when accepted.  WrongShard for a partition
    /// another shard owns (nothing accepted, no completion).  Invoker: QueueFull
    /// when the batch-ack queue has no credit — pollBatchAcks(), then retry.
    /// Agent: any app thread (the producer ring is many-to-one).
    [[nodiscard]] SendResult trySend(TopicHandle topic, const Key& key,
                                     std::span<const uint8_t> value) noexcept {
        if (closed_) return SendResult::Closed;
        if (!topic.valid()) return SendResult::InvalidTopic;
        if (!owns(topic.topicId, key.partition(topic.partitionCount))) return SendResult::WrongShard;
        return producer_.trySend(topic, key, value);
    }

    /// Never drops (Producer::send semantics).  WrongShard -> std::logic_error,
    /// Closed -> std::runtime_error, Invoker QueueFull -> std::runtime_error
    /// (only this thread can pollBatchAcks()); otherwise Producer::send's rules.
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value);

    /// Invoker mode: one shard duty cycle (driver, conductor, sessions, acks,
    /// producer).  Returns the work count.  Agent mode: std::logic_error.
    int doWork();

    /// Invoker mode: h(const BatchAck&) for every batch ack completed since the
    /// last call (F8c: OpKind::BatchAck completions in the shard-owned queue).
    /// Every accepted record is reported in exactly one BatchAck.  Returns the
    /// number of batch acks delivered.  Agent mode: 0 (no queue).
    template <class H>
    int pollBatchAcks(H&& h) {
        if (!acks_) return 0;
        return acks_->poll([&](const Completion& c, std::span<const uint8_t>) { h(batchAckOf(c)); });
    }

    [[nodiscard]] bool owns(int32_t topicId, int32_t partition) const noexcept {
        return shardFor(topicId, partition, count_) == index_;
    }
    /// Invoker: true once the shard's Aeron client closed itself (an owner stall
    /// past the configured liveness, a driver error); every later send is
    /// Closed.  Owner-thread state (set by doWork()); always false in Agent mode
    /// (closure reaches app threads through the producer's tail-bit close).
    [[nodiscard]] bool        closed() const noexcept { return closed_; }
    [[nodiscard]] uint32_t    index() const noexcept { return index_; }
    [[nodiscard]] uint32_t    count() const noexcept { return count_; }
    [[nodiscard]] ShardMode   mode() const noexcept { return mode_; }
    [[nodiscard]] TopicHandle topic(const std::string& name) const noexcept { return client_->topic(name); }
    [[nodiscard]] Client&     client() noexcept { return *client_; }
    [[nodiscard]] Producer&   producer() noexcept { return producer_; }

private:
    struct Validated {};
    ClientShard(Validated, const ShardGroupConfig& g, uint32_t index, uint32_t count, ClientConfig cc);

    uint32_t                   index_;
    uint32_t                   count_;
    ShardMode                  mode_;
    std::unique_ptr<Client>    client_;    // destroyed last
    std::optional<Completions> acks_;      // Invoker: batch acks; owner thread.  Never moved
                                           // (the producer holds its address); declared
                                           // before producer_, so it outlives it
    Producer                   producer_;  // move-only handle (F8c C7); destroyed first
    bool                       closed_ = false;  // Invoker owner thread only
};

class ShardGroup {
public:
    /// Agent mode only (Invoker: each app thread constructs its own
    /// ClientShard(cfg, i) -> std::invalid_argument).  On this (control) thread,
    /// before any driver starts: resolves the shard count once (AUTO by
    /// default: resolveShardCount over the calling thread's CPUs and the cgroup
    /// quota), assigns cores and validates the layout, logging the resolved
    /// count and any warnings once.  All-or-nothing: if shard k fails to start,
    /// shards k-1 .. 0 are torn down (in that order) and the error propagates.
    explicit ShardGroup(ShardGroupConfig cfg);
    /// Tears the shards down in reverse order (each = closeImmediately(); call
    /// connectAll ... producer().close() first when delivery matters).
    ~ShardGroup();

    ShardGroup(const ShardGroup&)            = delete;
    ShardGroup& operator=(const ShardGroup&) = delete;

    /// Control thread.  Issues every shard's connect (they proceed in parallel
    /// on the runners), then waits for each token; the broker side of each is
    /// bounded by base.connectTimeoutMs.  Throws std::runtime_error naming the
    /// first shard whose connect did not complete Ok within `timeout`.
    void connectAll(std::chrono::nanoseconds timeout = Client::kDefaultWaitTimeout);

    /// Any app thread: routed to the owning shard.
    [[nodiscard]] SendResult trySend(TopicHandle topic, const Key& key,
                                     std::span<const uint8_t> value) noexcept;
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value);

    [[nodiscard]] ClientShard& shard(uint32_t i) { return *shards_.at(i); }
    [[nodiscard]] ClientShard& owner(int32_t topicId, int32_t partition) noexcept {
        return *shards_[shardFor(topicId, partition, size())];
    }
    [[nodiscard]] uint32_t size() const noexcept { return static_cast<uint32_t>(shards_.size()); }
    /// The group config with shardCount resolved (never AUTO).
    [[nodiscard]] const ShardGroupConfig& config() const noexcept { return cfg_; }

private:
    ShardGroupConfig                          cfg_;
    std::vector<std::unique_ptr<ClientShard>> shards_;  // sized once; immutable after construction
};

} // namespace rune::client::shard
