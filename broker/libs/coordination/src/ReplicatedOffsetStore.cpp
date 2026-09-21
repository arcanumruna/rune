// ---------------------------------------------------------------------------
// ReplicatedOffsetStore.cpp — Replicated consumer committed-offset store.
//
// See header for design notes and allocation behavior documentation.
//
// ## On-wire layout (what gets stored in the Log)
//
// PartitionReplica::append(payload) wraps the caller's bytes as the VALUE of
// a single Kafka v2 record inside a new RecordBatch (the wrapper record has
// an empty key).  Therefore the payload we pass to append() IS the on-disk
// value for each commit record.
//
// We use a compact little-endian payload layout:
//
//   payload = keyLen(uint16)  + key_bytes
//           + valLen(uint16)  + val_bytes
//
// where:
//   key_bytes = groupLen(uint16) + group + topicLen(uint16) + topic + partition(int32)
//   val_bytes = offset(int64)   + tsNs(int64) + metaLen(uint16) + meta
//
// On rebuildFromLog() we iterate the outer RecordBatches from the replica Log
// and in the forEach callback receive:
//   record.key   = keyScratch_  (CP-1: the compaction identity bytes)
//                  or ""        (pre-CP-1 records: empty — backward-compat)
//   record.value = <payload>    (our compact key+val encoding)
//
// We always decode the payload from record.value (works for both old and new
// records, since the value layout is unchanged).  The record.key is used by
// the LogCleaner for compaction routing and is ignored during replay.
// ---------------------------------------------------------------------------

#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/protocol/RecordBatch.h>

#include <cassert>
#include <cstring>

namespace rune::coordination {

// ---------------------------------------------------------------------------
// Little-endian encode/decode helpers (TU-local)
// ---------------------------------------------------------------------------
namespace {

inline void pushLE16(std::vector<uint8_t>& v, uint16_t x) {
    v.push_back(static_cast<uint8_t>(x));
    v.push_back(static_cast<uint8_t>(x >> 8));
}
inline void pushLE32(std::vector<uint8_t>& v, int32_t x) {
    uint32_t u = static_cast<uint32_t>(x);
    v.push_back(static_cast<uint8_t>(u));
    v.push_back(static_cast<uint8_t>(u >> 8));
    v.push_back(static_cast<uint8_t>(u >> 16));
    v.push_back(static_cast<uint8_t>(u >> 24));
}
inline void pushLE64(std::vector<uint8_t>& v, int64_t x) {
    uint64_t u = static_cast<uint64_t>(x);
    for (int i = 0; i < 8; ++i) v.push_back(static_cast<uint8_t>(u >> (i * 8)));
}

inline uint16_t getLE16(const uint8_t* p) {
    return static_cast<uint16_t>(uint16_t(p[0]) | (uint16_t(p[1]) << 8));
}
inline int32_t getLE32(const uint8_t* p) {
    return static_cast<int32_t>(
        uint32_t(p[0]) | (uint32_t(p[1]) << 8) |
        (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24));
}
inline int64_t getLE64(const uint8_t* p) {
    uint64_t u = 0;
    for (int i = 0; i < 8; ++i) u |= static_cast<uint64_t>(p[i]) << (i * 8);
    return static_cast<int64_t>(u);
}

} // namespace

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

ReplicatedOffsetStore::ReplicatedOffsetStore(rune::replication::PartitionReplica* replica)
    : replica_(replica)
{
    assert(replica_ != nullptr);
    // Pre-reserve scratch buffers to reduce per-commit allocations.
    keyScratch_.reserve(256);
    valScratch_.reserve(64);
    batchScratch_.reserve(512);
}

// ---------------------------------------------------------------------------
// Internal: encode key into keyScratch_
// ---------------------------------------------------------------------------

void ReplicatedOffsetStore::encodeKey(const GroupId& group, const TopicPartition& tp)
{
    // key_bytes = groupLen(uint16) + group + topicLen(uint16) + topic + partition(int32)
    keyScratch_.clear();
    pushLE16(keyScratch_, static_cast<uint16_t>(group.size()));
    keyScratch_.insert(keyScratch_.end(), group.begin(), group.end());
    pushLE16(keyScratch_, static_cast<uint16_t>(tp.topic.size()));
    keyScratch_.insert(keyScratch_.end(), tp.topic.begin(), tp.topic.end());
    pushLE32(keyScratch_, tp.partition);
}

// ---------------------------------------------------------------------------
// Internal: encode value into valScratch_
// ---------------------------------------------------------------------------

void ReplicatedOffsetStore::encodeVal(Offset offset, int64_t tsNs, std::string_view metadata)
{
    // val_bytes = offset(int64) + tsNs(int64) + metaLen(uint16) + meta
    valScratch_.clear();
    pushLE64(valScratch_, offset);
    pushLE64(valScratch_, tsNs);
    pushLE16(valScratch_, static_cast<uint16_t>(metadata.size()));
    valScratch_.insert(valScratch_.end(), metadata.begin(), metadata.end());
}

// ---------------------------------------------------------------------------
// commit
// ---------------------------------------------------------------------------

bool ReplicatedOffsetStore::commit(const GroupId&        group,
                                    const TopicPartition& tp,
                                    Offset                offset,
                                    std::string_view      metadata,
                                    int64_t               tsNs)
{
    // Guard: group name, topic name, and metadata must each fit in uint16_t.
    // A size() >= 64 KiB would silently truncate the encoded length to a wrong
    // value and mis-frame every record after this one in rebuildFromLog().
    if (group.size()      > UINT16_MAX) return false;
    if (tp.topic.size()   > UINT16_MAX) return false;
    if (metadata.size()   > UINT16_MAX) return false;

    // 1. Encode key + value into reusable scratch buffers.
    encodeKey(group, tp);
    encodeVal(offset, tsNs, metadata);

    // 2. Build the compact payload (key-section + val-section) into batchScratch_.
    //
    // PartitionReplica::append() wraps whatever bytes we supply as the VALUE of
    // a single Kafka v2 record inside a fresh RecordBatch.  The wrapper record's
    // key is always empty.  On rebuildFromLog() we receive the wrapper record and
    // read our payload from its value field.
    //
    // payload = keyLen(uint16) + key_bytes + valLen(uint16) + val_bytes
    //
    // Allocation behavior: batchScratch_ is a member vector pre-reserved in the
    // constructor; it only reallocates if the payload grows beyond the initial
    // reserve (rare).  keyScratch_/valScratch_ are also member vectors.  No
    // per-commit heap allocation beyond amortised vector growth.
    batchScratch_.clear();
    pushLE16(batchScratch_, static_cast<uint16_t>(keyScratch_.size()));
    batchScratch_.insert(batchScratch_.end(), keyScratch_.begin(), keyScratch_.end());
    pushLE16(batchScratch_, static_cast<uint16_t>(valScratch_.size()));
    batchScratch_.insert(batchScratch_.end(), valScratch_.begin(), valScratch_.end());

    // 3. Append to the replica Log (acksAll=true → majority-durable).
    //
    // CP-1: use appendKeyed() so the on-disk RecordBatch carries a real key
    // equal to keyScratch_ (the compact identity: groupLen|group|topicLen|topic|
    // partition).  This enables the LogCleaner to compact __consumer_offsets by
    // key equality.  The VALUE remains batchScratch_ (the full keyLen+keyBytes+
    // valLen+valBytes payload) so rebuildFromLog() continues to decode correctly
    // from the value field for both old (empty-key) and new (keyed) records.
    const rune::replication::Offset base =
        replica_->appendKeyed(
            std::span<const uint8_t>(keyScratch_.data(), keyScratch_.size()),
            std::span<const uint8_t>(batchScratch_.data(), batchScratch_.size()),
            /*acksAll=*/true);

    if (base < 0) {
        // minIsr-gated: the append was rejected.  Do NOT update in-memory map.
        return false;
    }

    // 4. Update the in-memory map only on success.
    CommittedOffset co;
    co.offset            = offset;
    co.metadata          = std::string(metadata);
    co.commitTimestampNs = tsNs;
    index_[group][tp]    = std::move(co);
    return true;
}

// ---------------------------------------------------------------------------
// fetch
// ---------------------------------------------------------------------------

std::optional<CommittedOffset>
ReplicatedOffsetStore::fetch(const GroupId& group, const TopicPartition& tp) const
{
    auto git = index_.find(group);
    if (git == index_.end()) return std::nullopt;
    auto tit = git->second.find(tp);
    if (tit == git->second.end()) return std::nullopt;
    return tit->second;
}

// ---------------------------------------------------------------------------
// fetchAll
// ---------------------------------------------------------------------------

std::vector<std::pair<TopicPartition, CommittedOffset>>
ReplicatedOffsetStore::fetchAll(const GroupId& group) const
{
    std::vector<std::pair<TopicPartition, CommittedOffset>> result;
    auto git = index_.find(group);
    if (git == index_.end()) return result;
    result.reserve(git->second.size());
    for (const auto& [tp, co] : git->second) {
        result.emplace_back(tp, co);
    }
    return result;
}

// ---------------------------------------------------------------------------
// applyPayload — decode the compact payload and fold into the in-memory map.
//
// payload = keyLen(uint16) + key_bytes + valLen(uint16) + val_bytes
// key_bytes = groupLen(uint16) + group + topicLen(uint16) + topic + partition(int32)
// val_bytes = offset(int64) + tsNs(int64) + metaLen(uint16) + meta
// ---------------------------------------------------------------------------

void ReplicatedOffsetStore::applyRecord(std::string_view /*unused_key*/,
                                         std::string_view payload)
{
    // The wrapper record always has an empty key; all our data is in the value
    // (the payload we passed to append()).
    const auto* p = reinterpret_cast<const uint8_t*>(payload.data());
    std::size_t rem = payload.size();

    // ---- Decode key section ----
    if (rem < 2u) return;
    uint16_t keyLen = getLE16(p); p += 2; rem -= 2;
    if (rem < keyLen) return;
    const uint8_t* keyStart = p; p += keyLen; rem -= keyLen;

    // Decode key_bytes.
    const uint8_t* kp = keyStart;
    std::size_t ksz = keyLen;

    if (ksz < 2u) return;
    uint16_t groupLen = getLE16(kp); kp += 2; ksz -= 2;
    if (ksz < groupLen) return;
    std::string group(reinterpret_cast<const char*>(kp), groupLen); kp += groupLen; ksz -= groupLen;

    if (ksz < 2u) return;
    uint16_t topicLen = getLE16(kp); kp += 2; ksz -= 2;
    if (ksz < topicLen) return;
    std::string topic(reinterpret_cast<const char*>(kp), topicLen); kp += topicLen; ksz -= topicLen;

    if (ksz < 4u) return;
    int32_t partition = getLE32(kp);

    // ---- Decode val section ----
    if (rem < 2u) return;
    uint16_t valLen = getLE16(p); p += 2; rem -= 2;
    if (rem < valLen) return;
    const uint8_t* vp = p;
    std::size_t vsz = valLen;

    if (vsz < 18u) return; // 8+8+2
    int64_t  offset  = getLE64(vp);   vp += 8; vsz -= 8;
    int64_t  tsNs    = getLE64(vp);   vp += 8; vsz -= 8;
    uint16_t metaLen = getLE16(vp);   vp += 2; vsz -= 2;
    if (vsz < metaLen) return;
    std::string meta(reinterpret_cast<const char*>(vp), metaLen);

    // ---- Fold into index (last-write-wins including lower-offset overwrite) ----
    TopicPartition tp{std::move(topic), partition};
    CommittedOffset co;
    co.offset            = offset;
    co.metadata          = std::move(meta);
    co.commitTimestampNs = tsNs;
    index_[group][std::move(tp)] = std::move(co);
}

// ---------------------------------------------------------------------------
// rebuildFromLog
// ---------------------------------------------------------------------------
//
// Log::read(fetchOffset, maxBytes) resolves only the ONE segment that contains
// fetchOffset — it does NOT span segment boundaries (segments roll at the
// configured segmentRollBytes, default 64 MiB).  A single call with
// fetchOffset=0 and maxBytes=INT32_MAX therefore silently stops at the end of
// the first segment and misses every commit written to subsequent segments.
//
// The fix: loop, advancing fetchOffset to (lastBatchBase + lastOffsetDelta + 1)
// after each chunk, until we reach or exceed the committed high-watermark.
// Each iteration issues one read() that covers one segment (or part of one),
// and we walk the returned batches to both apply records AND compute the next
// fetchOffset.  The loop terminates when the read returns nothing new (empty,
// or no progress made) or when fetchOffset >= hw.
// ---------------------------------------------------------------------------

void ReplicatedOffsetStore::rebuildFromLog()
{
    index_.clear();

    const rune::replication::Offset hw = replica_->highWatermark();
    if (hw <= 0) return; // nothing committed yet

    // Chunk size: large enough to read an entire typical segment in one call.
    // INT32_MAX would work but is unnecessarily large; 64 MiB matches the
    // default segment roll size and avoids requesting more than one segment's
    // worth of bytes per iteration.
    static constexpr int32_t kChunkBytes = 64 * 1024 * 1024; // 64 MiB

    rune::replication::Offset fetchOffset = 0;

    while (fetchOffset < hw) {
        std::vector<uint8_t> data =
            replica_->read(fetchOffset, kChunkBytes);
        if (data.empty()) break; // no more committed data at this offset

        // Walk all RecordBatches in this chunk, applying each record and
        // tracking the next fetchOffset (last batch's committed end + 1).
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

            int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

            // Each outer batch has exactly 1 record: key="", value=<our payload>.
            reader.forEach([this](std::string_view key, std::string_view val,
                                  int64_t /*offset*/, int64_t /*ts*/) {
                applyRecord(key, val);
            });

            // Compute the committed end of this batch: baseOffset + lastOffsetDelta + 1.
            // This is the fetchOffset at which the NEXT batch starts in the log.
            const rune::replication::Offset batchNextOffset =
                reader.baseOffset() + reader.lastOffsetDelta() + 1;
            if (batchNextOffset > lastBatchEnd)
                lastBatchEnd = batchNextOffset;

            pos += static_cast<std::size_t>(tl);
        }

        // If no progress was made (no valid batch decoded), stop to avoid an
        // infinite loop (e.g. a corrupt leading batch at this offset).
        if (lastBatchEnd <= fetchOffset) break;

        fetchOffset = lastBatchEnd;
    }
}

} // namespace rune::coordination
