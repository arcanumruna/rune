// src/producer/TransactionalProducerImpl.cpp
//
// TX-5 — TransactionalProducerImpl: Tier-3 transactional produce + idempotent
// sequencing.
//
// Threading discipline (DR-BUG-1 / ProducerHub):
//   App thread: submitXxx() methods acquire cmdMtx_ and push to cmdQueue_;
//               returns a std::future immediately (zero blocking).
//   Agent thread: doWork() drains cmdQueue_ (under brief lock), then emits
//               the appropriate wire frames via sendFn.
//   No lock on the hot-path frame encode/send path (single-writer: agent).
//
// Idempotent sequence:
//   TxnPartitionState::nextBaseSequence advances monotonically per partition.
//   On retry (same send), the caller is responsible for re-submitting; the
//   sequence is NOT reset — the broker sees the same seq and dedups.
//
// Fencing:
//   On ProducerFenced or InvalidProducerEpoch in any response, all pending ops
//   are failed and producerId_ is reset to kNoPid.

#include <rune/client/producer/TransactionalProducerImpl.h>
#include <rune/client/producer/TransactionalProducer.h>
#include <rune/client/codec/DataCodec.h>

#include <rune/protocol/client/TxnMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/Crc32c.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <stdexcept>

namespace rune::client::producer {

namespace rpc = rune::protocol::client;
namespace rb  = rune::protocol::batch;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

// Write LE64 into buf at offset.
inline void writeLE64(uint8_t* buf, int off, int64_t v) noexcept {
    const uint64_t u = static_cast<uint64_t>(v);
    for (int i = 0; i < 8; ++i)
        buf[off + i] = static_cast<uint8_t>(u >> (i * 8));
}

// Write LE32 into buf at offset.
inline void writeLE32(uint8_t* buf, int off, int32_t v) noexcept {
    const uint32_t u = static_cast<uint32_t>(v);
    buf[off+0] = static_cast<uint8_t>(u);
    buf[off+1] = static_cast<uint8_t>(u >> 8);
    buf[off+2] = static_cast<uint8_t>(u >> 16);
    buf[off+3] = static_cast<uint8_t>(u >> 24);
}

// Write LE16s into buf at offset.
inline void writeLE16s(uint8_t* buf, int off, int16_t v) noexcept {
    const uint16_t u = static_cast<uint16_t>(v);
    buf[off+0] = static_cast<uint8_t>(u);
    buf[off+1] = static_cast<uint8_t>(u >> 8);
}

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
        builder.setBaseSequence(ps.nextBaseSequence);

        builder.buildInto(batchScratch_);

        // Advance sequence.
        ps.nextBaseSequence += builder.recordCount();

        // Encode Produce frame.
        rpc::Produce msg;
        msg.sessionId      = sessionId;
        msg.correlationId  = 0; // acks=None for individual txn records (ack via EndTxn)
        msg.topicId        = sr.topicId;
        msg.partition      = sr.partition;
        msg.partitionKnown = 1;
        msg.acks           = 0;
        msg.key            = {};
        msg.recordBatch    = std::span<const uint8_t>(batchScratch_);

        const std::size_t needed = msg.encodedSize();
        if (encodeScratch_.size() < needed) encodeScratch_.resize(needed);
        const std::size_t written = msg.encode(encodeScratch_.data());
        if (written > 0) {
            sendFn(encodeScratch_.data(), written);
            ++work;
        }
    }

    return work;
}

// ---------------------------------------------------------------------------
// Agent-thread: response handlers
// ---------------------------------------------------------------------------

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

void TransactionalProducerImpl::resolvePendingOp(int64_t corrId,
                                                   bool success,
                                                   const std::string& errorMsg)
{
    auto it = pendingOps_.find(corrId);
    if (it == pendingOps_.end()) return;

    if (success) {
        try { it->second.promise.set_value(); } catch (...) {}
    } else {
        try {
            it->second.promise.set_exception(
                std::make_exception_ptr(std::runtime_error(errorMsg)));
        } catch (...) {}
    }

    pendingOps_.erase(it);
}

} // namespace rune::client::producer
