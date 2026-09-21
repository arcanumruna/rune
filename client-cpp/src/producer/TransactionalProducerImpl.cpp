// src/producer/TransactionalProducerImpl.cpp
//
// TX-5 (fix-round 1) — TransactionalProducerImpl: Tier-3 transactional produce
// + idempotent-by-default sequencing.
//
// Threading discipline (DR-BUG-1 / ProducerHub):
//   App thread: submitXxx() methods acquire cmdMtx_ / pendingSendsMtx_ and push;
//               returns a std::future immediately (zero blocking).
//   Agent thread: doWork() drains queues (under brief locks), then emits the
//               appropriate wire frames via sendFn.
//   No lock on the hot-path frame encode/send path (single-writer: agent).
//
// Idempotent sequence (CRITICAL 1 fix):
//   TxnPartitionState::nextBaseSequence advances monotonically per partition.
//   Produce frames are sent with a REAL correlationId and acks=cfg_.acks.
//   The encoded wire frame is retained in pendingProduceBatches_ keyed by
//   correlationId.  On ProduceResponse:
//     - OK: batch removed; sequence already advanced at send time.
//     - Retriable error (e.g. NotLeader): RE-SEND the stored wire frame
//       unchanged — same baseSequence, same bytes.  Broker deduplicates.
//     - OutOfOrderSequence: fence (epoch stale).
//     - InvalidProducerEpoch / ProducerFenced: fence.
//
// Fencing:
//   On ProducerFenced or InvalidProducerEpoch in any response, all pending ops
//   are failed and producerId_ is reset to kNoPid.

#include <rune/client/producer/TransactionalProducerImpl.h>
#include <rune/client/producer/TransactionalProducer.h>
#include <rune/client/codec/DataCodec.h>

#include <rune/protocol/client/TxnMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/Crc32c.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <stdexcept>

namespace rune::client::producer {

namespace rpc = rune::protocol::client;
namespace rb  = rune::protocol::batch;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

inline int64_t wallNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

TransactionalProducerImpl::TransactionalProducerImpl(
    std::string transactionalId,
    ProducerConfig cfg,
    std::atomic<int64_t>& nextCorrId,
    int32_t transactionTimeoutMs)
    : transactionalId_(std::move(transactionalId))
    , cfg_(cfg)
    , transactionTimeoutMs_(transactionTimeoutMs)
    , nextCorrId_(nextCorrId)
{
    encodeScratch_.reserve(4096);
    batchScratch_.reserve(4096);
}

// ---------------------------------------------------------------------------
// App-thread API
// ---------------------------------------------------------------------------

std::future<void> TransactionalProducerImpl::submitInitTransactions()
{
    std::promise<void> p;
    auto fut = p.get_future();

    std::lock_guard<std::mutex> lk(cmdMtx_);
    Command cmd;
    cmd.kind    = CmdKind::InitTransactions;
    cmd.promise = std::move(p);
    cmdQueue_.push_back(std::move(cmd));

    return fut;
}

void TransactionalProducerImpl::beginTransaction()
{
    txnActive_.store(true, std::memory_order_release);
}

void TransactionalProducerImpl::submitSend(int32_t topicId,
                                            int32_t partition,
                                            const Key& key,
                                            std::span<const uint8_t> value)
{
    // Queue the send record; agent processes it on doWork.
    // NOTE: Tier-3 sends are control-plane (require pid stamp) — app-thread
    // allocation here is acceptable per the brief (no void-path contract).
    SendRecord sr;
    sr.topicId   = topicId;
    sr.partition = partition;
    sr.key       = key;
    sr.value.assign(value.begin(), value.end());
    std::lock_guard<std::mutex> lk(pendingSendsMtx_);
    pendingSends_.push_back(std::move(sr));
}

std::future<void> TransactionalProducerImpl::submitSendOffsets(
    const std::map<rune::client::TopicPartitionKey, int64_t>& offsets,
    const std::string& groupId)
{
    std::promise<void> p;
    auto fut = p.get_future();

    std::lock_guard<std::mutex> lk(cmdMtx_);
    Command cmd;
    cmd.kind    = CmdKind::SendOffsets;
    cmd.groupId = groupId;
    cmd.offsets = offsets;
    cmd.promise = std::move(p);
    cmdQueue_.push_back(std::move(cmd));

    return fut;
}

std::future<void> TransactionalProducerImpl::submitCommit()
{
    std::promise<void> p;
    auto fut = p.get_future();

    std::lock_guard<std::mutex> lk(cmdMtx_);
    Command cmd;
    cmd.kind    = CmdKind::EndTxnCommit;
    cmd.promise = std::move(p);
    cmdQueue_.push_back(std::move(cmd));

    return fut;
}

std::future<void> TransactionalProducerImpl::submitAbort()
{
    std::promise<void> p;
    auto fut = p.get_future();

    std::lock_guard<std::mutex> lk(cmdMtx_);
    Command cmd;
    cmd.kind    = CmdKind::EndTxnAbort;
    cmd.promise = std::move(p);
    cmdQueue_.push_back(std::move(cmd));

    return fut;
}

// ---------------------------------------------------------------------------
// Agent-thread: doWork
// ---------------------------------------------------------------------------

int TransactionalProducerImpl::doWork(
    int64_t sessionId,
    std::function<bool(const uint8_t*, std::size_t)> sendFn)
{
    if (sessionId == 0) return 0;

    int work = 0;

    // TX-6 FIX (ordering): drain pending SENDS *before* the command queue.
    //
    // The command queue carries EndTxn(commit/abort).  On a single doWork cycle
    // where the app has both enqueued N transactional sends (into pendingSends_)
    // AND requested commit (into cmdQueue_) — the exact pattern the harness'
    // txn_produce mode uses: send×N then commitTransaction() — the EndTxn frame
    // MUST NOT be written to the ordered channel before the N Produce frames.
    // Aeron preserves publication order, so wire-order == broker-processing
    // order: if EndTxn is sent first the broker writes the commit control marker
    // and advances the LSO *before* the data batches arrive, so the data batches
    // land AFTER the marker and are never covered by the committed range — a
    // read_committed consumer then sees only the records that happened to drain
    // in an earlier cycle (typically just the first).  Draining sends first
    // guarantees every data batch precedes the EndTxn on the wire.
    //
    // InitProducerId ordering is unaffected: the API contract requires
    // initTransactions().get() to complete before any send(), so init is never
    // co-queued with sends in a way that could be reordered here (by the time a
    // send is enqueued the producerId is already set from a prior cycle).
    drainPendingSends(sessionId, sendFn, work);

    // Drain command queue (brief lock to swap).
    std::vector<Command> cmds;
    {
        std::lock_guard<std::mutex> lk(cmdMtx_);
        cmds.swap(cmdQueue_);
    }

    for (auto& cmd : cmds) {
        switch (cmd.kind) {
        case CmdKind::InitTransactions: {
            // Emit InitProducerId.
            const int64_t corrId =
                nextCorrId_.fetch_add(1, std::memory_order_relaxed);

            if (sendInitProducerId(sessionId, corrId, sendFn)) {
                // Register pending op.
                PendingTxnOp op;
                op.kind    = PendingTxnOp::Kind::InitProducerId;
                op.promise = std::move(cmd.promise);
                op.deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(cfg_.produceTimeoutMs);
                pendingOps_.emplace(corrId, std::move(op));
                ++work;
            } else {
                // Send failed — fail the future.
                try {
                    cmd.promise.set_exception(
                        std::make_exception_ptr(
                            std::runtime_error("TransactionalProducer: transport send failed")));
                } catch (...) {}
            }
            break;
        }
        case CmdKind::SendRecord:
            // SendRecord commands are processed from pendingSends_ below.
            // This case should not occur (submitSend doesn't use cmdQueue_).
            break;
        case CmdKind::SendOffsets: {
            // Emit AddOffsetsToTxn; store offsets + promise for deferred TxnOffsetCommit.
            if (producerId_.load(std::memory_order_acquire) == kNoPid) {
                try {
                    cmd.promise.set_exception(
                        std::make_exception_ptr(
                            std::runtime_error("sendOffsetsToTransaction: no producerId — call initTransactions first")));
                } catch (...) {}
                break;
            }
            const int64_t corrId =
                nextCorrId_.fetch_add(1, std::memory_order_relaxed);
            const auto grpSpan = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(cmd.groupId.data()),
                cmd.groupId.size());

            if (sendAddOffsetsToTxn(sessionId, corrId, grpSpan, sendFn)) {
                // Stash for deferred TxnOffsetCommit after AddOffsetsToTxnResponse.
                auto pop = std::make_unique<PendingOffsetOp>();
                pop->groupId = std::move(cmd.groupId);
                pop->offsets = std::move(cmd.offsets);
                pop->corrId  = corrId;
                pop->promise = std::move(cmd.promise);
                pendingOffsetOp_ = std::move(pop);
                ++work;
            } else {
                try {
                    cmd.promise.set_exception(
                        std::make_exception_ptr(
                            std::runtime_error("sendOffsetsToTransaction: transport send failed")));
                } catch (...) {}
            }
            break;
        }
        case CmdKind::EndTxnCommit:
        case CmdKind::EndTxnAbort: {
            if (producerId_.load(std::memory_order_acquire) == kNoPid) {
                try {
                    cmd.promise.set_exception(
                        std::make_exception_ptr(
                            std::runtime_error("EndTxn: no producerId — call initTransactions first")));
                } catch (...) {}
                break;
            }
            const bool commit = (cmd.kind == CmdKind::EndTxnCommit);
            const int64_t corrId =
                nextCorrId_.fetch_add(1, std::memory_order_relaxed);
            {
                std::lock_guard<std::mutex> lk(pendingSendsMtx_);
                fprintf(stderr, "CLIENTDBG EndTxn about-to-send commit=%d pendingSendsRemaining=%zu inFlightProduce=%zu\n",
                        commit ? 1 : 0, pendingSends_.size(), pendingProduceBatches_.size());
            }

            if (sendEndTxn(sessionId, corrId, (cmd.kind == CmdKind::EndTxnCommit), sendFn)) {
                PendingTxnOp op;
                op.kind    = PendingTxnOp::Kind::EndTxn;
                op.promise = std::move(cmd.promise);
                op.deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(cfg_.produceTimeoutMs);
                pendingOps_.emplace(corrId, std::move(op));
                // Transaction is no longer active (whether commit or abort).
                txnActive_.store(false, std::memory_order_release);
                // Clear the addedPartitions set for the next transaction.
                addedPartitions_.clear();
                ++work;
            } else {
                try {
                    cmd.promise.set_exception(
                        std::make_exception_ptr(
                            std::runtime_error("EndTxn: transport send failed")));
                } catch (...) {}
            }
            break;
        }
        } // switch
    }

    // Sweep expired pending ops (timeouts).
    sweepTimedOut();

    return work;
}

// ---------------------------------------------------------------------------
// Agent-thread: drain pending transactional sends (Produce frames)
//
// Extracted from doWork() (TX-6 ordering fix) so it can run BEFORE the command
// queue is processed, guaranteeing every data batch precedes an EndTxn on the
// ordered publication.  Accumulates work into `work`.
// ---------------------------------------------------------------------------

void TransactionalProducerImpl::drainPendingSends(
    int64_t sessionId,
    const std::function<bool(const uint8_t*, std::size_t)>& sendFn,
    int& work)
{
    // Process pending sends (auto-AddPartitionsToTxn + Produce).
    std::vector<SendRecord> sends;
    {
        std::lock_guard<std::mutex> lk(pendingSendsMtx_);
        sends.swap(pendingSends_);
    }

    for (auto& sr : sends) {
        const rune::client::TopicPartitionKey tpk{sr.topicId, sr.partition};

        // Auto AddPartitionsToTxn on first send to a partition within this txn.
        if (txnActive_.load(std::memory_order_acquire) &&
            addedPartitions_.find(tpk) == addedPartitions_.end())
        {
            const int64_t apCorrId =
                nextCorrId_.fetch_add(1, std::memory_order_relaxed);
            std::vector<rpc::TxnTopicPartition> parts;
            parts.push_back({sr.topicId, sr.partition});
            if (sendAddPartitionsToTxn(sessionId, apCorrId, parts, sendFn)) {
                addedPartitions_.insert(tpk);
                ++work;
            }
        }

        // Build RecordBatch and send Produce.
        TxnPartitionState& ps = getOrCreatePartition(sr.topicId, sr.partition);

        const int64_t pid    = producerId_.load(std::memory_order_acquire);
        const int16_t epoch  = producerEpoch_.load(std::memory_order_acquire);

        // Build a one-record RecordBatch.
        rune::protocol::RecordBatchBuilder builder(0, wallNs());
        const std::string_view kv(reinterpret_cast<const char*>(sr.key.bytes().data()),
                                   sr.key.size());
        const std::string_view vv(reinterpret_cast<const char*>(sr.value.data()),
                                   sr.value.size());
        builder.append(kv, vv, wallNs());

        // Stamp idempotent identity.
        builder.setProducerId(pid);
        builder.setProducerEpoch(epoch);

        // TX-6 FIX: mark the DATA batch transactional (attributes bit 0x10).
        // Without this the broker treats it as a plain idempotent batch: its
        // LsoTracker never records the open transaction and its read_committed
        // abort filter (which gates on the TRANSACTIONAL bit) is skipped, so
        // aborted records leak to read_committed consumers and read_committed
        // is indistinguishable from read_uncommitted.
        builder.setTransactional(true);

        // Snapshot baseSequence BEFORE advancing — this is what we stamp and retain.
        const int32_t baseSeq = ps.nextBaseSequence;
        builder.setBaseSequence(baseSeq);

        builder.buildInto(batchScratch_);

        // Advance sequence NOW (before send) so a duplicate submission from the
        // app layer cannot collide: the in-flight entry retains the wire frame for
        // the retry path, which re-sends the same baseSeq unchanged.
        ps.nextBaseSequence += builder.recordCount();

        // Assign a real correlationId so we receive a ProduceResponse.
        const int64_t prodCorrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);

        // Encode Produce frame with real acks (not fire-and-forget).
        rpc::Produce msg;
        msg.sessionId      = sessionId;
        msg.correlationId  = prodCorrId;
        msg.topicId        = sr.topicId;
        msg.partition      = sr.partition;
        msg.partitionKnown = 1;
        msg.acks           = (cfg_.acks != Acks::None) ? uint8_t{1} : uint8_t{0};
        msg.key            = {};
        // TX-6 FIX: stamp the idempotent/transactional identity onto the Produce
        // FRAME (not just the record batch).  The broker's handleProduce gates
        // ALL transactional tracking — ProducerStateManager dedup, LsoTracker
        // onProduceBatch (open-txn set), read_committed abort filtering — on
        // `msg.producerId >= 0`.  Left at the -1 default the batch is treated as
        // a legacy non-idempotent produce: LSO never tracks the open txn, so on
        // abort onControlMarker returns firstOffset=-1 and the aborted range is
        // never recorded → aborted records leak to read_committed consumers.
        msg.producerId     = pid;
        msg.producerEpoch  = epoch;
        msg.baseSequence   = baseSeq;
        msg.recordBatch    = std::span<const uint8_t>(batchScratch_);

        const std::size_t needed = msg.encodedSize();
        if (encodeScratch_.size() < needed) encodeScratch_.resize(needed);
        const std::size_t written = msg.encode(encodeScratch_.data());
        if (written > 0) {
            if (sendFn(encodeScratch_.data(), written)) {
                // Retain wire frame for idempotent retry: same bytes = same baseSeq.
                InFlightBatch ib;
                ib.topicId      = sr.topicId;
                ib.partition    = sr.partition;
                ib.baseSequence = baseSeq;
                ib.wireFrame.assign(encodeScratch_.data(), encodeScratch_.data() + written);
                ib.deadline     = std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds(cfg_.produceTimeoutMs);
                pendingProduceBatches_.emplace(prodCorrId, std::move(ib));
            }
            ++work;
        }
    }
}

// ---------------------------------------------------------------------------
// Agent-thread: deadline sweep
// ---------------------------------------------------------------------------

void TransactionalProducerImpl::sweepTimedOut()
{
    const auto now = std::chrono::steady_clock::now();

    // Sweep expired control-plane ops (InitProducerId, EndTxn, SendOffsets).
    std::vector<int64_t> expiredOps;
    for (const auto& [corrId, op] : pendingOps_) {
        if (now >= op.deadline) {
            expiredOps.push_back(corrId);
        }
    }
    for (const int64_t corrId : expiredOps) {
        auto it = pendingOps_.find(corrId);
        if (it == pendingOps_.end()) continue;
        try {
            it->second.promise.set_exception(
                std::make_exception_ptr(
                    std::runtime_error(
                        "TransactionalProducer: operation timed out (no response)")));
        } catch (...) {}
        pendingOps_.erase(it);
    }

    // Sweep expired in-flight produce batches.  These have no caller future to
    // fail (the produce path is fire-and-ack); silently drop them so they don't
    // accumulate indefinitely when a ProduceResponse is lost or never delivered.
    std::vector<int64_t> expiredBatches;
    for (const auto& [corrId, ib] : pendingProduceBatches_) {
        if (now >= ib.deadline) {
            expiredBatches.push_back(corrId);
        }
    }
    for (const int64_t corrId : expiredBatches) {
        pendingProduceBatches_.erase(corrId);
    }
}

// ---------------------------------------------------------------------------
// Agent-thread: response handlers
// ---------------------------------------------------------------------------

void TransactionalProducerImpl::onProduceResponse(
    const uint8_t* buf, std::size_t len,
    std::function<bool(const uint8_t*, std::size_t)> sendFn)
{
    auto opt = rpc::ProduceResponse::decode(buf, len);
    if (!opt) return;

    const int64_t corrId = opt->correlationId;
    auto it = pendingProduceBatches_.find(corrId);
    if (it == pendingProduceBatches_.end()) return; // not our produce

    const auto ec = static_cast<rpc::ErrorCode>(opt->errorCode);

    if (ec == rpc::ErrorCode::OK) {
        // Success: batch acknowledged; remove from in-flight.
        pendingProduceBatches_.erase(it);
        return;
    }

    // Fencing errors: fence the producer.
    if (ec == rpc::ErrorCode::OutOfOrderSequence ||
        ec == rpc::ErrorCode::InvalidProducerEpoch ||
        ec == rpc::ErrorCode::ProducerFenced) {
        pendingProduceBatches_.erase(it);
        handleFencing(opt->errorCode);
        return;
    }

    // Retriable error (NotLeader, etc.): re-send the SAME wire frame unchanged.
    // The frame already carries the original baseSequence — the broker will
    // deduplicate if it already accepted this batch.
    InFlightBatch& ib = it->second;
    if (ib.retryCount >= InFlightBatch::kMaxRetries) {
        // Exhausted retries — drop (EndTxn will fail or caller can retry the txn).
        pendingProduceBatches_.erase(it);
        return;
    }

    ++ib.retryCount;
    // Re-send: same bytes, same correlationId in the frame header.
    sendFn(ib.wireFrame.data(), ib.wireFrame.size());
}

void TransactionalProducerImpl::onInitProducerIdResponse(const uint8_t* buf,
                                                          std::size_t len)
{
    auto opt = rpc::InitProducerIdResponse::decode(buf, len);
    if (!opt) return;

    const auto ec = static_cast<rpc::ErrorCode>(opt->errorCode);
    const int64_t corrId = opt->correlationId;

    auto it = pendingOps_.find(corrId);
    if (it == pendingOps_.end()) return;

    if (ec == rpc::ErrorCode::OK) {
        // Store the producer identity.
        producerId_.store(opt->producerId, std::memory_order_release);
        producerEpoch_.store(opt->producerEpoch, std::memory_order_release);
        try { it->second.promise.set_value(); } catch (...) {}
    } else if (ec == rpc::ErrorCode::ProducerFenced ||
               ec == rpc::ErrorCode::InvalidProducerEpoch) {
        // handleFencing clears pendingOps_ (failing every pending promise including
        // 'it') — do NOT touch 'it' after this call (iterator is invalidated).
        handleFencing(opt->errorCode);
        return;
    } else {
        const std::string msg =
            "InitProducerId failed (errorCode=" + std::to_string(opt->errorCode) + ")";
        try {
            it->second.promise.set_exception(
                std::make_exception_ptr(std::runtime_error(msg)));
        } catch (...) {}
    }

    pendingOps_.erase(it);
}

void TransactionalProducerImpl::onAddPartitionsToTxnResponse(const uint8_t* buf,
                                                               std::size_t len)
{
    auto opt = rpc::AddPartitionsToTxnResponse::decode(buf, len);
    if (!opt) return;

    // Check for fencing errors in per-partition results.
    for (std::size_t i = 0; i < opt->results.size(); ++i) {
        const auto& r = opt->results[i];
        const auto ec = static_cast<rpc::ErrorCode>(r.errorCode);
        if (ec == rpc::ErrorCode::ProducerFenced ||
            ec == rpc::ErrorCode::InvalidProducerEpoch) {
            handleFencing(r.errorCode);
            return;
        }
    }
    // OK — no action needed (partition already in addedPartitions_ set).
}

void TransactionalProducerImpl::onAddOffsetsToTxnResponse(
    const uint8_t* buf, std::size_t len,
    int64_t sessionId,
    std::function<bool(const uint8_t*, std::size_t)> sendFn)
{
    auto opt = rpc::AddOffsetsToTxnResponse::decode(buf, len);
    if (!opt) return;

    const auto ec = static_cast<rpc::ErrorCode>(opt->errorCode);

    if (!pendingOffsetOp_) return;

    if (ec == rpc::ErrorCode::ProducerFenced ||
        ec == rpc::ErrorCode::InvalidProducerEpoch) {
        handleFencing(opt->errorCode);
        try {
            pendingOffsetOp_->promise.set_exception(
                std::make_exception_ptr(
                    std::runtime_error("AddOffsetsToTxnResponse: ProducerFenced")));
        } catch (...) {}
        pendingOffsetOp_.reset();
        return;
    }

    if (ec != rpc::ErrorCode::OK) {
        try {
            pendingOffsetOp_->promise.set_exception(
                std::make_exception_ptr(
                    std::runtime_error(
                        "AddOffsetsToTxnResponse failed (errorCode=" +
                        std::to_string(opt->errorCode) + ")")));
        } catch (...) {}
        pendingOffsetOp_.reset();
        return;
    }

    // OK — now send TxnOffsetCommit.
    const int64_t corrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);

    const auto grpSpan = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(pendingOffsetOp_->groupId.data()),
        pendingOffsetOp_->groupId.size());

    // Build entries.
    std::vector<rpc::TxnOffsetCommitEntry> entries;
    entries.reserve(pendingOffsetOp_->offsets.size());
    for (const auto& [tpk, off] : pendingOffsetOp_->offsets) {
        rpc::TxnOffsetCommitEntry e;
        e.topicId   = tpk.topicId;
        e.partition = tpk.partition;
        e.offset    = off;
        e.metadata  = {};
        entries.push_back(e);
    }

    if (sendTxnOffsetCommit(sessionId, corrId, grpSpan, entries, sendFn)) {
        // Register a pending op for TxnOffsetCommitResponse.
        PendingTxnOp op;
        op.kind    = PendingTxnOp::Kind::SendOffsets;
        op.promise = std::move(pendingOffsetOp_->promise);
        op.deadline = std::chrono::steady_clock::now() +
                      std::chrono::milliseconds(cfg_.produceTimeoutMs);
        pendingOps_.emplace(corrId, std::move(op));
    } else {
        try {
            pendingOffsetOp_->promise.set_exception(
                std::make_exception_ptr(
                    std::runtime_error("TxnOffsetCommit: transport send failed")));
        } catch (...) {}
    }

    pendingOffsetOp_.reset();
}

void TransactionalProducerImpl::onTxnOffsetCommitResponse(const uint8_t* buf,
                                                           std::size_t len)
{
    auto opt = rpc::TxnOffsetCommitResponse::decode(buf, len);
    if (!opt) return;

    const int64_t corrId = opt->correlationId;
    auto it = pendingOps_.find(corrId);
    if (it == pendingOps_.end()) return;

    // Check for per-partition errors.
    bool anyError = false;
    for (std::size_t i = 0; i < opt->results.size(); ++i) {
        const auto& r = opt->results[i];
        if (r.errorCode != 0) {
            anyError = true;
            const auto ec = static_cast<rpc::ErrorCode>(r.errorCode);
            if (ec == rpc::ErrorCode::ProducerFenced ||
                ec == rpc::ErrorCode::InvalidProducerEpoch) {
                handleFencing(r.errorCode);
            }
            break;
        }
    }

    if (!anyError) {
        try { it->second.promise.set_value(); } catch (...) {}
    } else {
        try {
            it->second.promise.set_exception(
                std::make_exception_ptr(
                    std::runtime_error("TxnOffsetCommitResponse: broker error")));
        } catch (...) {}
    }

    pendingOps_.erase(it);
}

void TransactionalProducerImpl::onEndTxnResponse(const uint8_t* buf, std::size_t len)
{
    auto opt = rpc::EndTxnResponse::decode(buf, len);
    if (!opt) return;

    const int64_t corrId = opt->correlationId;
    auto it = pendingOps_.find(corrId);
    if (it == pendingOps_.end()) return;

    const auto ec = static_cast<rpc::ErrorCode>(opt->errorCode);

    if (ec == rpc::ErrorCode::OK) {
        try { it->second.promise.set_value(); } catch (...) {}
    } else if (ec == rpc::ErrorCode::ProducerFenced ||
               ec == rpc::ErrorCode::InvalidProducerEpoch) {
        handleFencing(opt->errorCode);
        try {
            it->second.promise.set_exception(
                std::make_exception_ptr(
                    std::runtime_error(
                        "EndTxn: ProducerFenced — re-initialize (errorCode=" +
                        std::to_string(opt->errorCode) + ")")));
        } catch (...) {}
    } else {
        try {
            it->second.promise.set_exception(
                std::make_exception_ptr(
                    std::runtime_error(
                        "EndTxn failed (errorCode=" +
                        std::to_string(opt->errorCode) + ")")));
        } catch (...) {}
    }

    pendingOps_.erase(it);
}

void TransactionalProducerImpl::failAll(const std::string& reason)
{
    for (auto& [corrId, op] : pendingOps_) {
        try {
            op.promise.set_exception(
                std::make_exception_ptr(std::runtime_error(reason)));
        } catch (...) {}
    }
    pendingOps_.clear();

    // Clear in-flight produce batches (no futures to fail — produce acks
    // are acknowledged via the in-flight map, not via caller futures).
    pendingProduceBatches_.clear();

    if (pendingOffsetOp_) {
        try {
            pendingOffsetOp_->promise.set_exception(
                std::make_exception_ptr(std::runtime_error(reason)));
        } catch (...) {}
        pendingOffsetOp_.reset();
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

TxnPartitionState& TransactionalProducerImpl::getOrCreatePartition(
    int32_t topicId, int32_t partition)
{
    const rune::client::TopicPartitionKey k{topicId, partition};
    auto it = partitions_.find(k);
    if (it == partitions_.end()) {
        auto [ins, ok] = partitions_.emplace(k, TxnPartitionState(topicId, partition));
        return ins->second;
    }
    return it->second;
}

bool TransactionalProducerImpl::sendInitProducerId(
    int64_t sessionId,
    int64_t corrId,
    const std::function<bool(const uint8_t*, std::size_t)>& sendFn)
{
    rpc::InitProducerId msg;
    msg.sessionId            = sessionId;
    msg.correlationId        = corrId;
    msg.transactionTimeoutMs = transactionTimeoutMs_;
    msg.transactionalId      = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(transactionalId_.data()),
        transactionalId_.size());

    const std::size_t needed = msg.encodedSize();
    if (encodeScratch_.size() < needed) encodeScratch_.resize(needed);
    const std::size_t written = msg.encode(encodeScratch_.data());
    if (written == 0) return false;

    return sendFn(encodeScratch_.data(), written);
}

bool TransactionalProducerImpl::sendAddPartitionsToTxn(
    int64_t sessionId,
    int64_t corrId,
    const std::vector<rpc::TxnTopicPartition>& parts,
    const std::function<bool(const uint8_t*, std::size_t)>& sendFn)
{
    rpc::AddPartitionsToTxn msg;
    msg.sessionId     = sessionId;
    msg.correlationId = corrId;
    msg.producerId    = producerId_.load(std::memory_order_acquire);
    msg.producerEpoch = producerEpoch_.load(std::memory_order_acquire);
    msg.transactionalId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(transactionalId_.data()),
        transactionalId_.size());

    const std::size_t needed =
        rpc::AddPartitionsToTxn::encodedSizeFor(msg.transactionalId, parts.size());
    if (encodeScratch_.size() < needed) encodeScratch_.resize(needed);
    const std::size_t written =
        msg.encode(encodeScratch_.data(), std::span<const rpc::TxnTopicPartition>(parts));
    if (written == 0) return false;

    return sendFn(encodeScratch_.data(), written);
}

bool TransactionalProducerImpl::sendEndTxn(
    int64_t sessionId,
    int64_t corrId,
    bool commit,
    const std::function<bool(const uint8_t*, std::size_t)>& sendFn)
{
    rpc::EndTxn msg;
    msg.sessionId     = sessionId;
    msg.correlationId = corrId;
    msg.producerId    = producerId_.load(std::memory_order_acquire);
    msg.producerEpoch = producerEpoch_.load(std::memory_order_acquire);
    msg.commit        = commit ? uint8_t{1} : uint8_t{0};
    msg.transactionalId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(transactionalId_.data()),
        transactionalId_.size());

    const std::size_t needed = msg.encodedSize();
    if (encodeScratch_.size() < needed) encodeScratch_.resize(needed);
    const std::size_t written = msg.encode(encodeScratch_.data());
    if (written == 0) return false;

    return sendFn(encodeScratch_.data(), written);
}

bool TransactionalProducerImpl::sendAddOffsetsToTxn(
    int64_t sessionId,
    int64_t corrId,
    std::span<const uint8_t> grpSpan,
    const std::function<bool(const uint8_t*, std::size_t)>& sendFn)
{
    rpc::AddOffsetsToTxn msg;
    msg.sessionId     = sessionId;
    msg.correlationId = corrId;
    msg.producerId    = producerId_.load(std::memory_order_acquire);
    msg.producerEpoch = producerEpoch_.load(std::memory_order_acquire);
    msg.transactionalId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(transactionalId_.data()),
        transactionalId_.size());
    msg.groupId       = grpSpan;

    const std::size_t needed = msg.encodedSize();
    if (encodeScratch_.size() < needed) encodeScratch_.resize(needed);
    const std::size_t written = msg.encode(encodeScratch_.data());
    if (written == 0) return false;

    return sendFn(encodeScratch_.data(), written);
}

bool TransactionalProducerImpl::sendTxnOffsetCommit(
    int64_t sessionId,
    int64_t corrId,
    std::span<const uint8_t> grpSpan,
    const std::vector<rpc::TxnOffsetCommitEntry>& entries,
    const std::function<bool(const uint8_t*, std::size_t)>& sendFn)
{
    rpc::TxnOffsetCommit msg;
    msg.sessionId     = sessionId;
    msg.correlationId = corrId;
    msg.producerId    = producerId_.load(std::memory_order_acquire);
    msg.producerEpoch = producerEpoch_.load(std::memory_order_acquire);
    msg.transactionalId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(transactionalId_.data()),
        transactionalId_.size());
    msg.groupId       = grpSpan;

    const std::span<const rpc::TxnOffsetCommitEntry> entrsSpan(entries);
    const std::size_t needed =
        rpc::TxnOffsetCommit::encodedSizeFor(msg.transactionalId, grpSpan, entrsSpan);
    if (encodeScratch_.size() < needed) encodeScratch_.resize(needed);
    const std::size_t written = msg.encode(encodeScratch_.data(), entrsSpan);
    if (written == 0) return false;

    return sendFn(encodeScratch_.data(), written);
}

void TransactionalProducerImpl::handleFencing(int16_t /*errorCode*/)
{
    producerId_.store(kNoPid, std::memory_order_release);
    producerEpoch_.store(0, std::memory_order_release);
    txnActive_.store(false, std::memory_order_release);
    addedPartitions_.clear();
    partitions_.clear();

    // Clear in-flight produce batches (cannot be retried after epoch/pid reset).
    pendingProduceBatches_.clear();

    // Fail all pending ops (including sendOffsets).
    const std::string reason = "TransactionalProducer: ProducerFenced — re-initialize";
    for (auto& [corrId, op] : pendingOps_) {
        try {
            op.promise.set_exception(
                std::make_exception_ptr(std::runtime_error(reason)));
        } catch (...) {}
    }
    pendingOps_.clear();

    if (pendingOffsetOp_) {
        try {
            pendingOffsetOp_->promise.set_exception(
                std::make_exception_ptr(std::runtime_error(reason)));
        } catch (...) {}
        pendingOffsetOp_.reset();
    }
}

} // namespace rune::client::producer
