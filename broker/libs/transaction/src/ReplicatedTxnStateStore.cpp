// ---------------------------------------------------------------------------
// ReplicatedTxnStateStore.cpp — Replicated transaction state store (TX-3).
//
// Record body encoding reuses the TransactionLog on-disk layout exactly
// (magic 0x544B4E4C, kFixedHeaderSize=52, CRC32C tail) so one decoder serves
// both the disk-based TransactionLog and this replicated-partition store.
//
// See header for design notes.
// ---------------------------------------------------------------------------

#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/transaction/detail/Crc32c.h>

#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>  // RecordBatchReader (for rebuildFromLog)

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <set>

namespace rune::transaction {

// ---------------------------------------------------------------------------
// Constants (match TransactionLog)
// ---------------------------------------------------------------------------
namespace {

static constexpr uint32_t kMagic          = 0x544B4E4Cu;
static constexpr uint32_t kFixedHeaderSize = 52u;
static constexpr uint32_t kChecksumSize   = 4u;
static constexpr uint8_t  kFlagFencing    = 0x01u;   // header byte 27, bit0 (F15)

template<typename T>
inline void write_le(uint8_t* p, T v) noexcept { std::memcpy(p, &v, sizeof(T)); }

template<typename T>
inline T read_le(const uint8_t* p) noexcept { T v; std::memcpy(&v, p, sizeof(T)); return v; }

} // anonymous namespace

// ---------------------------------------------------------------------------
// CRC32C — software Castagnoli (same as TransactionLog's crc32c)
// ---------------------------------------------------------------------------

/*static*/ uint32_t ReplicatedTxnStateStore::crc32c(
    const uint8_t* data, std::size_t len, uint32_t crc) noexcept
{
    // F15 S1: constexpr table (no std::call_once — shared-nothing law).
    return detail::crc32c(data, len, crc);
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------
ReplicatedTxnStateStore::ReplicatedTxnStateStore(
    rune::replication::PartitionReplica* replica)
    : replica_(replica)
{
    assert(replica_ != nullptr);
    recordScratch_.reserve(512);
}

// ---------------------------------------------------------------------------
// encodeRecord — encode TransactionMetadata into the TransactionLog layout.
// ---------------------------------------------------------------------------
void ReplicatedTxnStateStore::encodeRecord(
    const TransactionMetadata& meta, int32_t coordinatorEpoch)
{
    const auto txnIdLen  = static_cast<uint16_t>(meta.txnId.size());
    const auto partCount = static_cast<uint16_t>(meta.partitions.size());

    std::size_t partBytes = 0;
    for (const auto& tp : meta.partitions)
        partBytes += 2 + tp.topic.size() + 4;

    // Pending-offset section:
    //   offsetCount(uint16) + per-offset: groupLen(uint16) topicLen(uint16)
    //   partition(int32) offset(int64) group(bytes) topic(bytes)
    const auto offsetCount = static_cast<uint16_t>(meta.pendingOffsets.size());
    std::size_t offsetSectionBytes = 2; // offsetCount prefix
    for (const auto& po : meta.pendingOffsets) {
        offsetSectionBytes += 2 + 2 + 4 + 8
                            + po.group.size()
                            + po.tp.topic.size();
    }

    const uint32_t totalLength =
        kFixedHeaderSize
        + static_cast<uint32_t>(txnIdLen)
        + static_cast<uint32_t>(partBytes)
        + static_cast<uint32_t>(offsetSectionBytes)
        + kChecksumSize;

    recordScratch_.resize(totalLength);
    uint8_t* p = recordScratch_.data();

    write_le<uint32_t>(p +  0, kMagic);
    write_le<uint32_t>(p +  4, totalLength);
    write_le<int64_t> (p +  8, meta.txnStartNs);
    write_le<int64_t> (p + 16, meta.producerId);
    write_le<int16_t> (p + 24, meta.producerEpoch);
    p[26] = static_cast<uint8_t>(meta.state);
    // F15 concern 3: flags byte (was always-0 padding, so old records decode as
    // no flags).  bit0 = fencingEpoch (the txn was ended by an InitProducerId fence).
    p[27] = meta.fencingEpoch ? kFlagFencing : 0u;
    write_le<int32_t> (p + 28, coordinatorEpoch);
    write_le<int32_t> (p + 32, meta.txnTimeoutMs);
    write_le<uint16_t>(p + 36, txnIdLen);
    write_le<uint16_t>(p + 38, partCount);
    write_le<int64_t> (p + 40, meta.pendingNewProducerId);
    write_le<int16_t> (p + 48, meta.pendingNewProducerEpoch);
    write_le<uint16_t>(p + 50, 0u); // padding to kFixedHeaderSize = 52

    std::size_t pos = kFixedHeaderSize;

    // txnId bytes.
    std::memcpy(p + pos, meta.txnId.data(), txnIdLen);
    pos += txnIdLen;

    // Partition tuples.
    for (const auto& tp : meta.partitions) {
        auto topicLen = static_cast<uint16_t>(tp.topic.size());
        write_le<uint16_t>(p + pos, topicLen); pos += 2;
        std::memcpy(p + pos, tp.topic.data(), topicLen); pos += topicLen;
        write_le<int32_t>(p + pos, tp.partition); pos += 4;
    }

    // Pending-offset section.
    write_le<uint16_t>(p + pos, offsetCount); pos += 2;
    for (const auto& po : meta.pendingOffsets) {
        auto groupLen = static_cast<uint16_t>(po.group.size());
        auto topicLen = static_cast<uint16_t>(po.tp.topic.size());
        write_le<uint16_t>(p + pos, groupLen);        pos += 2;
        write_le<uint16_t>(p + pos, topicLen);        pos += 2;
        write_le<int32_t> (p + pos, po.tp.partition); pos += 4;
        write_le<int64_t> (p + pos, po.offset);       pos += 8;
        std::memcpy(p + pos, po.group.data(), groupLen);         pos += groupLen;
        std::memcpy(p + pos, po.tp.topic.data(), topicLen);      pos += topicLen;
    }

    assert(pos + kChecksumSize == totalLength);

    const uint32_t cksum = crc32c(p, pos);
    write_le<uint32_t>(p + pos, cksum);
}

// ---------------------------------------------------------------------------
// put — encode + append + update in-memory map on success.
// ---------------------------------------------------------------------------
rune::replication::Offset ReplicatedTxnStateStore::putAt(const TransactionMetadata& meta,
                                                         int32_t coordinatorEpoch)
{
    encodeRecord(meta, coordinatorEpoch);

    // CP-1: use appendKeyed() so the on-disk record carries the transactionalId
    // as the record key, enabling LogCleaner to compact __transaction_state by key.
    // The VALUE remains the full TransactionLog-encoded blob so applyRecord() /
    // rebuildFromLog() continue to work unchanged.
    const std::span<const uint8_t> keySpan{
        reinterpret_cast<const uint8_t*>(meta.txnId.data()),
        meta.txnId.size()};
    const rune::replication::Offset base =
        replica_->appendKeyed(
            keySpan,
            std::span<const uint8_t>(recordScratch_.data(), recordScratch_.size()),
            /*acksAll=*/true);

    if (base < 0) {
        // minIsr-gated: reject without updating in-memory map.
        return -1;
    }

    // Update in-memory map only on success.
    index_[meta.txnId] = meta;
    return base;
}

// ---------------------------------------------------------------------------
// putPidBlockCeiling — F15 I7: magic(4) totalLength(4) ceiling(8) crc32c(4).
// ---------------------------------------------------------------------------
rune::replication::Offset ReplicatedTxnStateStore::putPidBlockCeiling(ProducerId ceiling)
{
    constexpr uint32_t kLen = 4u + 4u + 8u + kChecksumSize;
    std::array<uint8_t, kLen> rec{};
    write_le<uint32_t>(rec.data() + 0, kPidBlockMagic);
    write_le<uint32_t>(rec.data() + 4, kLen);
    write_le<int64_t> (rec.data() + 8, ceiling);
    write_le<uint32_t>(rec.data() + 16, crc32c(rec.data(), 16));
    const std::span<const uint8_t> key{reinterpret_cast<const uint8_t*>(kPidBlockKey.data()), kPidBlockKey.size()};
    const rune::replication::Offset base = replica_->appendKeyed(
        key, std::span<const uint8_t>(rec.data(), rec.size()), /*acksAll=*/true);
    return base < 0 ? -1 : base;   // persistedPidCeiling_ folds only committed records (rebuild)
}

// ---------------------------------------------------------------------------
// get
// ---------------------------------------------------------------------------
const TransactionMetadata*
ReplicatedTxnStateStore::get(const TransactionalId& txnId) const noexcept
{
    const auto it = index_.find(txnId);
    if (it == index_.end()) return nullptr;
    return &it->second;
}

// ---------------------------------------------------------------------------
// applyRecord — decode the TransactionLog record layout and fold into index_.
//
// payload = the bytes we appended to append() — the encoded record.
// The RecordBatch wrapper stores our payload as the record's value (the
// wrapper record's key is always empty).
// ---------------------------------------------------------------------------
void ReplicatedTxnStateStore::applyRecord(std::string_view payload)
{
    const auto* p = reinterpret_cast<const uint8_t*>(payload.data());
    const std::size_t sz = payload.size();

    // F15 I7: a pid block ceiling record (its own record-type tag).
    if (sz >= 4u + 4u + 8u + kChecksumSize && read_le<uint32_t>(p) == kPidBlockMagic) {
        if (read_le<uint32_t>(p + 4) != 4u + 4u + 8u + kChecksumSize) return;
        if (read_le<uint32_t>(p + 16) != crc32c(p, 16)) return;
        const int64_t ceiling = read_le<int64_t>(p + 8);
        if (ceiling > persistedPidCeiling_) persistedPidCeiling_ = ceiling;
        return;
    }

    // Need at least the fixed header + CRC.
    if (sz < kFixedHeaderSize + kChecksumSize) return;

    // Verify magic.
    if (read_le<uint32_t>(p) != kMagic) return;

    // Verify totalLength is consistent with payload size.
    const uint32_t totalLength = read_le<uint32_t>(p + 4);
    if (sz < totalLength) return;

    // Verify CRC (covers all bytes before the trailing CRC field).
    const uint32_t storedCrc = read_le<uint32_t>(p + totalLength - kChecksumSize);
    const uint32_t computed   = crc32c(p, totalLength - kChecksumSize);
    if (storedCrc != computed) return;

    // Decode fixed fields.
    const int64_t   txnStartNs       = read_le<int64_t> (p +  8);
    const int64_t   producerId       = read_le<int64_t> (p + 16);
    const int16_t   producerEpoch    = read_le<int16_t> (p + 24);
    const TxnState  state            = static_cast<TxnState>(p[26]);
    const bool      fencing          = (p[27] & kFlagFencing) != 0;
    const int32_t   coordEpoch       = read_le<int32_t> (p + 28);
    const int32_t   txnTimeoutMs     = read_le<int32_t> (p + 32);
    const uint16_t  txnIdLen         = read_le<uint16_t>(p + 36);
    const uint16_t  partCount        = read_le<uint16_t>(p + 38);
    const int64_t   pendingNewPid    = read_le<int64_t> (p + 40);
    const int16_t   pendingNewEpoch  = read_le<int16_t> (p + 48);

    std::size_t pos = kFixedHeaderSize;
    const std::size_t limit = totalLength - kChecksumSize;

    // txnId.
    if (pos + txnIdLen > limit) return;
    TransactionalId txnId(reinterpret_cast<const char*>(p + pos), txnIdLen);
    pos += txnIdLen;

    // Partition tuples.
    std::set<TopicPartition> partitions;
    for (uint16_t i = 0; i < partCount; ++i) {
        if (pos + 2 > limit) return;
        const uint16_t topicLen = read_le<uint16_t>(p + pos); pos += 2;
        if (pos + topicLen + 4 > limit) return;
        std::string topic(reinterpret_cast<const char*>(p + pos), topicLen);
        pos += topicLen;
        const int32_t partition = read_le<int32_t>(p + pos); pos += 4;
        partitions.insert(TopicPartition{std::move(topic), partition});
    }

    // Pending-offset section.
    std::vector<PendingTxnOffset> pendingOffsets;
    if (pos + 2 <= limit) {
        const uint16_t offsetCount = read_le<uint16_t>(p + pos); pos += 2;
        for (uint16_t i = 0; i < offsetCount; ++i) {
            if (pos + 2 + 2 + 4 + 8 > limit) break;
            const uint16_t groupLen = read_le<uint16_t>(p + pos); pos += 2;
            const uint16_t topicLen = read_le<uint16_t>(p + pos); pos += 2;
            const int32_t  part     = read_le<int32_t> (p + pos); pos += 4;
            const int64_t  offset   = read_le<int64_t> (p + pos); pos += 8;
            if (pos + groupLen + topicLen > limit) break;
            std::string group(reinterpret_cast<const char*>(p + pos), groupLen);
            pos += groupLen;
            std::string topic(reinterpret_cast<const char*>(p + pos), topicLen);
            pos += topicLen;

            PendingTxnOffset po;
            po.group        = std::move(group);
            po.tp.topic     = std::move(topic);
            po.tp.partition = part;
            po.offset       = offset;
            pendingOffsets.push_back(std::move(po));
        }
    }

    // Build metadata and fold into index_ (last-write-wins).
    TransactionMetadata meta;
    meta.txnId                   = std::move(txnId);
    meta.producerId              = producerId;
    meta.producerEpoch           = producerEpoch;
    meta.state                   = state;
    meta.txnStartNs              = txnStartNs;
    meta.coordinatorEpoch        = coordEpoch;
    meta.txnTimeoutMs            = txnTimeoutMs;
    meta.partitions              = std::move(partitions);
    meta.pendingNewProducerId    = pendingNewPid;
    meta.pendingNewProducerEpoch = pendingNewEpoch;
    meta.pendingOffsets          = std::move(pendingOffsets);
    meta.fencingEpoch            = fencing;   // F15 concern 3: persisted in byte 27
    // Recovered Prepare* states need markers written (B2 resolution).
    meta.markersPending = (state == TxnState::PrepareCommit
                           || state == TxnState::PrepareAbort);

    index_[meta.txnId] = std::move(meta);
}

// ---------------------------------------------------------------------------
// rebuildFromLog — replay [0, HW) across ALL segments.
//
// Same multi-segment loop as ReplicatedOffsetStore::rebuildFromLog() (the CG-2
// durability fix): Log::read() resolves only ONE segment per call, so we loop
// advancing fetchOffset to the next-batch start until we reach the HW.
// ---------------------------------------------------------------------------
void ReplicatedTxnStateStore::rebuildFromLog()
{
    index_.clear();
    persistedPidCeiling_ = -1;

    const rune::replication::Offset hw = replica_->highWatermark();
    if (hw <= 0) return;

    static constexpr int32_t kChunkBytes = 64 * 1024 * 1024; // 64 MiB per segment read

    // F15 fix round 1: __transaction_state is COMPACTED.  Start at the first
    // live segment (a compacted/retained prefix), and when a read inside a
    // segment returns nothing (its superseded tail was compacted away) skip to
    // the next segment base instead of stopping — stopping silently dropped all
    // later segments' txn state.  Mirrors ReplicatedOffsetStore CP-3/CP-4.
    rune::storage::Log* diskLog = replica_->log();   // null for memory-backed replicas
    rune::replication::Offset fetchOffset = 0;
    if (diskLog != nullptr) {
        const auto bases = diskLog->segmentBaseOffsets();
        if (!bases.empty() && bases.front() > 0) fetchOffset = bases.front();
    }

    while (fetchOffset < hw) {
        std::vector<uint8_t> data = replica_->read(fetchOffset, kChunkBytes);
        if (data.empty()) {
            if (diskLog == nullptr) break;
            const auto bases = diskLog->segmentBaseOffsets();
            const auto it = std::upper_bound(bases.begin(), bases.end(), static_cast<int64_t>(fetchOffset));
            if (it == bases.end()) break;
            fetchOffset = static_cast<rune::replication::Offset>(*it);
            continue;
        }

        std::size_t pos = 0;
        rune::replication::Offset lastBatchEnd = fetchOffset; // sentinel: no progress

        while (pos < data.size()) {
            const uint8_t*    batchStart = data.data() + pos;
            const std::size_t remaining  = data.size() - pos;

            if (remaining < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead))
                break;

            rune::protocol::RecordBatchReader reader(batchStart, remaining);
            if (!reader.ensureValid()) break;

            const int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

            // Each outer batch has exactly 1 record: key="", value=<our payload>.
            reader.forEachRecord([this](std::string_view /*key*/,
                                  std::string_view val,
                                  int64_t /*offset*/,
                                  int64_t /*ts*/) {
                applyRecord(val);
            });

            const rune::replication::Offset batchNextOffset =
                reader.baseOffset() + reader.lastOffsetDelta() + 1;
            if (batchNextOffset > lastBatchEnd)
                lastBatchEnd = batchNextOffset;

            pos += static_cast<std::size_t>(tl);
        }

        if (lastBatchEnd <= fetchOffset) break; // no progress → stop
        fetchOffset = lastBatchEnd;
    }
}

} // namespace rune::transaction
