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
// State (design §4, shard/ShardState.h): derived on the duty thread every duty
// cycle from the Client's sessions / transport / producer, published on change
// into this shard's StatusRing; pollEvents() drains it in order (the Invoker
// owner, or the Agent control thread — the ring's one consumer).  The failure
// path (the Aeron client closed, or the seed Rejected) is the Client's: the
// intake is tail-closed, every accepted record completes Closed exactly once,
// no further I/O; the shard is then Failed until it is closed.
//
// Threads (F8c: no futures, atomics, mutexes or shared_ptr handles):
//   Invoker: everything on the owner (constructing) thread.
//   Agent:   trySend / send from any app thread; connect, close, pollEvents,
//            status() and closed() on ONE control thread; the runner owns the
//            duty side.
#pragma once

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/shard/ShardConfig.h>
#include <rune/client/shard/ShardMap.h>
#include <rune/client/shard/ShardState.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::client::shard {

class ShardGroup;

class ClientShard {
public:
    /// Shard `index` of an EXPLICIT g.shardCount (AUTO -> std::invalid_argument:
    /// in Invoker mode N is the number of driving app threads, which only the
    /// app knows).  The group config is validated (validateShardGroupConfig)
    /// before the shard's driver starts.  Invoker mode: the constructing thread
    /// becomes the owner — the only thread that may send, doWork(), poll, close
    /// or destroy the shard.
    ClientShard(const ShardGroupConfig& g, uint32_t index);

    /// = closeImmediately(): never blocks.  Call close() first when delivery matters.
    ~ClientShard();

    ClientShard(const ClientShard&)            = delete;
    ClientShard& operator=(const ClientShard&) = delete;
    ClientShard(ClientShard&&)                 = delete;
    ClientShard& operator=(ClientShard&&)      = delete;

    // ---- connect ---------------------------------------------------------------

    /// F8c: exactly one Connect completion into `target` when Accepted (a
    /// Completions registered with client() and owned by the calling thread, or
    /// a CompletionHandler on the duty thread).  Invoker: owner thread only,
    /// then drive doWork() and poll.
    [[nodiscard]] OpResult connect(CompletionTarget target, Token token) {
        return client_->connect(target, token);
    }
    /// Blocking sugar: Invoker owner pumps doWork(); Agent polls under the idle
    /// strategy.  Throws std::runtime_error on any non-Ok outcome or timeout, and
    /// std::logic_error from a duty-cycle callback.
    void connectAndWait(std::chrono::nanoseconds timeout = Client::kDefaultWaitTimeout) {
        client_->connectAndWait(timeout);
    }

    // ---- send ------------------------------------------------------------------

    /// Invoker batch-ack queue: 8 MiB = ~174k accepted-but-unpolled records
    /// (48 B credit each, Producer::batchAcksTo).
    static constexpr uint32_t kBatchAckQueueBytes = 8u << 20;

    /// Non-blocking; zero allocation when accepted.  The partition is computed
    /// ONCE here and passed down (M3).  WrongShard for a partition another
    /// shard owns (nothing accepted, no completion); Closed once the shard
    /// failed or was closed.  Invoker: QueueFull when the batch-ack queue has no
    /// credit — pollBatchAcks(), then retry.  Agent: any app thread.
    [[nodiscard]] SendResult trySend(TopicHandle topic, const Key& key,
                                     std::span<const uint8_t> value) noexcept {
        if (!topic.valid()) return SendResult::InvalidTopic;
        return trySendTo(topic, key.partition(topic.partitionCount), key, value);
    }
    /// Router form: `partition` is the caller's key.partition(partitionCount)
    /// (ShardGroup computed it to route) — used as is, never re-hashed.
    /// InvalidArgument for a partition outside [0, partitionCount).
    [[nodiscard]] SendResult trySendTo(TopicHandle topic, int32_t partition, const Key& key,
                                       std::span<const uint8_t> value) noexcept {
        if (closed_) return SendResult::Closed;  // Invoker owner state; never set in Agent mode
        if (!topic.valid()) return SendResult::InvalidTopic;
        if (partition < 0 || partition >= topic.partitionCount) [[unlikely]] return SendResult::InvalidArgument;
        if (!owns(topic.topicId, partition)) return SendResult::WrongShard;
        return producer_.trySendTo(topic, partition, key, value);
    }

    /// Never drops (Producer::send semantics).  WrongShard -> std::logic_error,
    /// Closed -> std::runtime_error, Invoker QueueFull -> std::runtime_error
    /// (only this thread can pollBatchAcks()); otherwise Producer::send's rules.
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value);
    void sendTo(TopicHandle topic, int32_t partition, const Key& key, std::span<const uint8_t> value);

    // ---- duty (Invoker) ----------------------------------------------------------

    /// Invoker mode: one shard duty cycle (driver, conductor, sessions, acks,
    /// producer, state).  Returns the work count; 0 once closed.  Agent mode:
    /// std::logic_error.
    int doWork();

    /// A7 park-until-work (Invoker owner): the shard driver's socket-readiness fds (Client::wakeFds).
    [[nodiscard]] int wakeFds(int* out, int max) const noexcept { return client_->wakeFds(out, max); }

    /// Invoker mode: h(const BatchAck&) for every batch ack completed since the
    /// last call (F8c: OpKind::BatchAck completions in the shard-owned queue).
    /// Every accepted record is reported in exactly one BatchAck (a failed /
    /// closed record with errorCode BatchAck::kClientFailure).  Returns the
    /// number delivered.  Agent mode: 0 (use ShardGroup::onBatchAck /
    /// producer().onBatchAck).
    template <class H>
    int pollBatchAcks(H&& h) {
        if (!acks_) return 0;
        return acks_->poll([&](const Completion& c, std::span<const uint8_t>) { h(batchAckOf(c)); });
    }

    // ---- state -------------------------------------------------------------------

    /// h(const ShardStatus&) for every status published since the last call,
    /// in order (Invoker: the owner; Agent: the control thread — the ring's one
    /// consumer).  Returns the count.
    template <class H>
    int pollEvents(H&& h) {
        return client_->pollShardStatus([&](const ShardStatus& s) {
            seen_ = s;
            h(s);
        });
    }
    /// Invoker: the owner's current state (derived each doWork).  Agent: the
    /// latest status this control thread drained with pollEvents().
    [[nodiscard]] ShardStatus status() const noexcept {
        return mode_ == ShardMode::Invoker ? client_->shardStatusDuty() : seen_;
    }
    /// Invoker: true once the shard failed or closed (owner state).  Agent: the
    /// control thread's view (pollEvents) — app threads learn it as
    /// SendResult::Closed from the producer's tail-closed intake.
    [[nodiscard]] bool closed() const noexcept {
        if (mode_ == ShardMode::Invoker) return closed_;
        return seen_.state == ShardState::Failed || seen_.state == ShardState::Closed;
    }
    /// True once close() / closeImmediately() / the group tore this shard down.
    [[nodiscard]] bool tornDown() const noexcept { return tornDown_; }

    // ---- close (design §4) ---------------------------------------------------------

    /// Begin-close (non-blocking): the producer's intake closes SYNCHRONOUSLY
    /// (every later send is Closed), the shard is Draining, and `token`
    /// completes once every accepted record completed (Ok), at the delivery
    /// deadline (Timeout), or Closed by closeImmediately().  Accepted (one
    /// completion follows) or Closed (already closing / failed: none).  Follow
    /// with close() / closeImmediately() to tear the shard down.
    [[nodiscard]] OpResult close(const CompletionTarget& target, Token token) {
        return producer_.close(target, token);
    }
    /// Blocking close: begin-close + wait for the drain (Invoker owner pumps
    /// doWork(); Agent polls), then teardown, then Closed is published.
    /// Returns the drain's status (Ok / Timeout / Closed — Closed at once when
    /// the shard failed, or was already closing); a second close() is a no-op
    /// returning Closed.  From a duty-cycle callback: std::logic_error.
    OpStatus close();
    /// close() with a bound on the drain wait (std::runtime_error when it
    /// passes; the teardown then still runs — remaining records complete Closed).
    OpStatus closeAndWait(std::chrono::nanoseconds timeout);
    /// Tear down now: every outstanding op and accepted record completes Closed
    /// exactly once (a pending begin-close token Closed), no wait for the
    /// broker; then Closed is published.  Idempotent.  The destructor does this.
    void closeImmediately() noexcept;

    // ---- misc --------------------------------------------------------------------

    [[nodiscard]] bool owns(int32_t topicId, int32_t partition) const noexcept {
        return shardFor(topicId, partition, count_) == index_;
    }
    [[nodiscard]] uint32_t    index() const noexcept { return index_; }
    [[nodiscard]] uint32_t    count() const noexcept { return count_; }
    [[nodiscard]] ShardMode   mode() const noexcept { return mode_; }
    [[nodiscard]] TopicHandle topic(const std::string& name) const noexcept { return client_->topic(name); }
    [[nodiscard]] Client&     client() noexcept { return *client_; }
    [[nodiscard]] Producer&   producer() noexcept { return producer_; }

private:
    friend class ShardGroup;
    struct Validated {};
    /// ShardGroup's layout: count from resolveShardCount, core from
    /// assignShardCores, the group already validated.
    ClientShard(const ShardGroupConfig& g, uint32_t index, uint32_t count, int32_t runnerCore);
    ClientShard(Validated, const ShardGroupConfig& g, uint32_t index, uint32_t count, ClientConfig cc);

    OpStatus    closeImpl_(const std::chrono::nanoseconds* timeout);
    /// Teardown after a drain: Client::close() when the drain completed Ok
    /// (nothing left: join + stop), else closeImmediately (the rest completes
    /// Closed).  Then Closed is published.
    void        teardown_(bool drained) noexcept;
    void        finishTeardown_() noexcept;
    /// Invoker: latch closed_ from the duty-side status (owner thread).
    void        latchClosed_() noexcept {
        if (!closed_ && client_->shardStatusDuty().state == ShardState::Failed) closed_ = true;
    }

    uint32_t    index_;
    uint32_t    count_;
    ShardMode   mode_;
    bool        closed_   = false;  // Invoker owner state (never written in Agent mode)
    ShardStatus seen_{};            // consumer side: the latest drained status
    bool        tornDown_ = false;  // the tearing-down thread (owner / control)

    std::unique_ptr<Client>    client_;    // destroyed last
    std::optional<Completions> acks_;      // Invoker: batch acks; owner thread.  Never moved
                                           // (the producer holds its address); declared
                                           // before producer_, so it outlives it
    Producer                   producer_;  // move-only handle (F8c C7); destroyed first
};

/// Owner-thread bundle of one Completions per shard, each registered with that
/// shard's Client (a Completions is bound to ONE Client).  The token forms of
/// the group ops complete shard i's op into at(i).  Construct and use it on the
/// control thread; it must outlive the ops issued on it.
class ShardCompletions {
public:
    explicit ShardCompletions(ShardGroup& group, CompletionsConfig cfg = {});
    ShardCompletions(const ShardCompletions&)            = delete;
    ShardCompletions& operator=(const ShardCompletions&) = delete;

    [[nodiscard]] Completions& at(uint32_t shard) { return *queues_.at(shard); }
    [[nodiscard]] uint32_t     size() const noexcept { return static_cast<uint32_t>(queues_.size()); }
    /// h(uint32_t shard, const Completion&, std::span<const uint8_t> payload)
    /// for every completion in every shard's queue, shard by shard.
    template <class H>
    int poll(H&& h) {
        int n = 0;
        for (uint32_t i = 0; i < queues_.size(); ++i) {
            n += queues_[i]->poll([&](const Completion& c, std::span<const uint8_t> p) { h(i, c, p); });
        }
        return n;
    }

private:
    std::vector<std::unique_ptr<Completions>> queues_;  // never moved (registered addresses)
};

class ShardGroup {
public:
    /// Agent mode only (Invoker: each app thread constructs its own
    /// ClientShard(cfg, i) -> std::invalid_argument).  On this (control) thread,
    /// before any driver starts: resolves the shard count once (AUTO by
    /// default: resolveShardCount over the calling thread's CPUs and the cgroup
    /// quota), assigns cores and validates the layout, logging the resolved
    /// count and any warnings once.  All-or-nothing: if shard k fails to start,
    /// shards k-1 .. 0 are torn down (in that order) and the error propagates,
    /// prefixed "ShardGroup: shard k: " (same exception category).
    explicit ShardGroup(ShardGroupConfig cfg);
    /// closeImmediately(), in reverse shard order.
    ~ShardGroup();

    ShardGroup(const ShardGroup&)            = delete;
    ShardGroup& operator=(const ShardGroup&) = delete;

    /// Token form (F8c, control thread): shard i's connect is issued with token
    /// base + i and completes exactly once into q.at(i).  Accepted when every
    /// shard accepted; otherwise the first other OpResult (the shards before it
    /// were issued and will complete; none after it was issued).
    [[nodiscard]] OpResult connectAll(ShardCompletions& q, Token base);
    /// Blocking sugar: issues every shard's connect (they proceed in parallel on
    /// the runners), then waits for the tokens against ONE deadline (now +
    /// timeout); the broker side of each is bounded by base.connectTimeoutMs.
    /// Throws std::runtime_error naming the first shard whose connect did not
    /// complete Ok in time.
    void connectAllAndWait(std::chrono::nanoseconds timeout = Client::kDefaultWaitTimeout);
    /// = connectAllAndWait (the plan's name).
    void connectAll(std::chrono::nanoseconds timeout = Client::kDefaultWaitTimeout) { connectAllAndWait(timeout); }

    /// Setup phase (before the first send), control thread: shard i's per-batch
    /// acknowledgements to fn(ctx, ack) on ITS runner — one ctx per shard, never
    /// shared across runners.  std::out_of_range for a bad index.
    void onBatchAck(uint32_t shard, BatchAckFn fn, void* ctx) { shards_.at(shard)->producer().onBatchAck(fn, ctx); }

    /// Any app thread: routed to the owning shard; the key is hashed ONCE.
    [[nodiscard]] SendResult trySend(TopicHandle topic, const Key& key,
                                     std::span<const uint8_t> value) noexcept;
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value);

    /// Control thread: every shard's published statuses, shard by shard, each
    /// in order.  Returns the count.
    template <class H>
    int pollEvents(H&& h) {
        int n = 0;
        for (auto& s : shards_) n += s->pollEvents(h);
        return n;
    }

    /// Control thread (design §4): 1. begin-close EVERY shard's intake (each
    /// synchronous: no send is accepted after it), 2. wait for every drain
    /// against ONE deadline, 3. Client::close() each shard (reverse order; quick:
    /// already drained — a shard whose drain did not complete Ok is
    /// closeImmediately'd instead) — Closed published per shard.  Returns Ok when every drain completed Ok, else the
    /// first other status (Timeout: the deadline passed first — the remaining
    /// records complete Closed in the teardown).  Idempotent (then Closed).
    OpStatus close(std::chrono::nanoseconds timeout = Client::kDefaultWaitTimeout);
    /// Control thread: tear every shard down now (reverse order).  Idempotent.
    void closeImmediately() noexcept;

    [[nodiscard]] ClientShard& shard(uint32_t i) { return *shards_.at(i); }
    [[nodiscard]] ClientShard& owner(int32_t topicId, int32_t partition) noexcept {
        return *shards_[shardFor(topicId, partition, size())];
    }
    [[nodiscard]] uint32_t size() const noexcept { return static_cast<uint32_t>(shards_.size()); }
    /// The group config with shardCount resolved (never AUTO).
    [[nodiscard]] const ShardGroupConfig& config() const noexcept { return cfg_; }

private:
    void teardown_(uint32_t i, bool drained = false) noexcept;
    [[nodiscard]] int32_t partitionOf_(TopicHandle topic, const Key& key) const noexcept {
        if (cfg_.testPartitioner != nullptr) [[unlikely]] return cfg_.testPartitioner(key, topic.partitionCount);
        return key.partition(topic.partitionCount);
    }

    ShardGroupConfig                          cfg_;
    std::vector<std::unique_ptr<ClientShard>> shards_;  // sized once; immutable after construction
};

} // namespace rune::client::shard
