// ---------------------------------------------------------------------------
// ReadCommittedPushTest.cpp — TX-4 Task 9 + Step 6:
//   read_committed LSO bound + aborted-txn filter + control-marker skip
//   on both client subscriptions and group subscriptions (push path).
//
// Tests:
//   1. OpenTxnHiddenBelowLso
//      - Append non-txn committed batches 0..4, then open-txn batch at offset 5.
//      - LSO = 5; HW = 6.
//      - read_committed subscriber receives offsets 0..4 only.
//      - read_uncommitted sees through to HW (all 6 batches).
//
//   2. AbortedRecordsFilteredControlMarkersNeverDelivered
//      - txn pid=9 writes batches at 5..7; ABORT control marker at 8.
//      - Non-txn batch at 9.
//      - read_committed delivers 0..4 and 9; never 5..7 (aborted) or 8 (marker).
//      - read_uncommitted delivers 0..7, 9; never 8 (control marker, always filtered).
//
//   3. GroupReadCommitted (Step 6)
//      - Group subscription with isolation=1 honours LSO cap.
//      - Receives only committed non-txn batches; open-txn batch hidden.
//
// Harness:
//   - Embedded Aeron media driver (aeron_driver_t, manual main loop thread).
//   - Uses onEgressFrame_ callback to capture Records frames without real clients.
//   - addGroupSubscription is the no-Aeron subscription path.
//   - Log is obtained from TopicRegistry (registry owns partition logs).
//   - No PartitionReplica needed: single-node, LSO tests don't need acks=All.
// ---------------------------------------------------------------------------

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/transaction/Types.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <concurrent/AtomicBuffer.h>

#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;
namespace tx = rune::transaction;
using rune::node::TopicSpec;

namespace rune::gateway {
namespace {

// ---------------------------------------------------------------------------
// Unique temp dir generator.
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "")
{
    static std::atomic<uint32_t> g_seq{0};
    const auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_rcpush_") + std::string(tag) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "") : path(makeTempDir(tag)) {}
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

static int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// TopicSpec helper.
// ---------------------------------------------------------------------------
static TopicSpec makeSpec(std::string name, int32_t partCount, int32_t topicId)
{
    TopicSpec ts;
    ts.name           = std::move(name);
    ts.partitionCount = partCount;
    ts.topicId        = topicId;
    return ts;
}

// ---------------------------------------------------------------------------
// Build a transactional data batch (TRANSACTIONAL bit set, no CONTROL bit).
// Layout: 61-byte header + 7-byte inline record.
// Attributes = 0x0010 (TRANSACTIONAL). One record. baseOffset stamped.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeTxnDataBatch(int64_t pid, int16_t epoch,
                                              int64_t baseOffset)
{
    constexpr std::size_t HEADER = 61;
    const uint8_t rec[] = {0x00, 0x00, 0x00, 0x00, 0x02, 0x42, 0x00};
    const std::size_t total = HEADER + sizeof(rec);

    std::vector<uint8_t> out(total, 0);

    // baseOffset (LE64@0).
    {
        uint64_t u = static_cast<uint64_t>(baseOffset);
        for (int i = 0; i < 8; ++i) out[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    // batchLengthBytes (LE32@8) = total - 12.
    {
        const uint32_t bl = static_cast<uint32_t>(total - 12);
        out[8]  = static_cast<uint8_t>(bl);
        out[9]  = static_cast<uint8_t>(bl >> 8);
        out[10] = static_cast<uint8_t>(bl >> 16);
        out[11] = static_cast<uint8_t>(bl >> 24);
    }
    // partitionLeaderEpoch (LE32@12) = -1.
    out[12] = 0xFF; out[13] = 0xFF; out[14] = 0xFF; out[15] = 0xFF;
    // magic @16 = 2.
    out[16] = 2;
    // attributes (LE16@21) = 0x0010 (TRANSACTIONAL).
    out[21] = 0x10; out[22] = 0x00;
    // lastOffsetDelta (LE32@23) = 0 (one record).
    // producerId (LE64@43).
    {
        uint64_t u = static_cast<uint64_t>(pid);
        for (int i = 0; i < 8; ++i) out[43 + i] = static_cast<uint8_t>(u >> (i * 8));
    }
    // producerEpoch (LE16@51).
    out[51] = static_cast<uint8_t>(epoch);
    out[52] = static_cast<uint8_t>(static_cast<uint16_t>(epoch) >> 8);
    // numRecords (LE32@57) = 1.
    out[57] = 1;
    // Record bytes.
    std::memcpy(out.data() + HEADER, rec, sizeof(rec));

    // CRC32C over [21..end].
    auto crc32c = [](const uint8_t* data, std::size_t len) -> uint32_t {
        uint32_t tbl[256];
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
            tbl[i] = c;
        }
        uint32_t crc = 0xFFFFFFFFu;
        for (std::size_t i = 0; i < len; ++i) crc = tbl[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFFu;
    };
    const uint32_t crc = crc32c(out.data() + 21, total - 21);
    out[17] = static_cast<uint8_t>(crc);
    out[18] = static_cast<uint8_t>(crc >> 8);
    out[19] = static_cast<uint8_t>(crc >> 16);
    out[20] = static_cast<uint8_t>(crc >> 24);

    return out;
}

// ---------------------------------------------------------------------------
// Build a non-transactional data batch with one record.
// Attributes = 0x0000. producerId = -1.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeNonTxnBatch(int64_t baseOffset)
{
    constexpr std::size_t HEADER = 61;
    const uint8_t rec[] = {0x00, 0x00, 0x00, 0x00, 0x02, 0x43, 0x00};
    const std::size_t total = HEADER + sizeof(rec);
    std::vector<uint8_t> out(total, 0);

    // baseOffset (LE64@0).
    {
        uint64_t u = static_cast<uint64_t>(baseOffset);
        for (int i = 0; i < 8; ++i) out[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    // batchLengthBytes (LE32@8).
    {
        const uint32_t bl = static_cast<uint32_t>(total - 12);
        out[8]  = static_cast<uint8_t>(bl);
        out[9]  = static_cast<uint8_t>(bl >> 8);
        out[10] = static_cast<uint8_t>(bl >> 16);
        out[11] = static_cast<uint8_t>(bl >> 24);
    }
    // partitionLeaderEpoch (LE32@12) = -1.
    out[12] = 0xFF; out[13] = 0xFF; out[14] = 0xFF; out[15] = 0xFF;
    // magic @16 = 2.
    out[16] = 2;
    // attributes @21 = 0x00 (non-txn, non-control).
    // producerId (non-txn) = -1 (LE64@43).
    for (int i = 0; i < 8; ++i) out[43 + i] = 0xFF;
    // numRecords (LE32@57) = 1.
    out[57] = 1;
    std::memcpy(out.data() + HEADER, rec, sizeof(rec));

    // CRC.
    auto crc32c = [](const uint8_t* data, std::size_t len) -> uint32_t {
        uint32_t tbl[256];
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
            tbl[i] = c;
        }
        uint32_t crc = 0xFFFFFFFFu;
        for (std::size_t i = 0; i < len; ++i) crc = tbl[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFFu;
    };
    const uint32_t crc = crc32c(out.data() + 21, total - 21);
    out[17] = static_cast<uint8_t>(crc);
    out[18] = static_cast<uint8_t>(crc >> 8);
    out[19] = static_cast<uint8_t>(crc >> 16);
    out[20] = static_cast<uint8_t>(crc >> 24);

    return out;
}

// ---------------------------------------------------------------------------
// Extract baseOffsets from captured Records frames filtered by sessionId.
// ---------------------------------------------------------------------------
static std::vector<int64_t> extractBaseOffsets(
    int64_t filterSid,
    const std::vector<std::pair<int64_t,std::vector<uint8_t>>>& frames)
{
    std::vector<int64_t> offsets;
    for (const auto& [sid, frame] : frames) {
        if (sid != filterSid) continue;
        auto recOpt = pc::Records::decode(frame.data(), frame.size());
        if (!recOpt) continue;
        offsets.push_back(recOpt->baseOffset);
    }
    return offsets;
}

// ---------------------------------------------------------------------------
// Build an encoded Subscribe frame (the isolation carrier a group member sends
// after entering Stable — TX-5 sendSubscribeForPartitions).
// ---------------------------------------------------------------------------
static std::vector<uint8_t> buildSubscribeFrame(int64_t sid, int32_t topicId,
                                                int32_t partition, uint8_t isolation)
{
    pc::Subscribe s;
    s.sessionId      = sid;
    s.correlationId  = 4242;
    s.topicId        = topicId;
    s.partition      = partition;
    s.partitionKnown = 1;
    s.fromOffset     = 0;
    s.isolation      = isolation;
    std::vector<uint8_t> buf(s.encodedSize());
    const std::size_t n = s.encode(buf.data());
    buf.resize(n);
    return buf;
}

// Count how many times each baseOffset was delivered to a session (dup detector).
static std::map<int64_t,int> deliveryCounts(
    int64_t filterSid,
    const std::vector<std::pair<int64_t,std::vector<uint8_t>>>& frames)
{
    std::map<int64_t,int> counts;
    for (const auto& [sid, frame] : frames) {
        if (sid != filterSid) continue;
        auto recOpt = pc::Records::decode(frame.data(), frame.size());
        if (!recOpt) continue;  // non-Records frame (e.g. SubscribeResponse) — skip
        counts[recOpt->baseOffset]++;
    }
    return counts;
}

} // namespace (anonymous — helper functions only)

// ===========================================================================
// ReadCommittedPushFixture
// (MUST be outside anonymous namespace so GTest factory can instantiate it.)
// ===========================================================================
class ReadCommittedPushFixture : public ::testing::Test {
public:
    static constexpr int32_t kIngressStream = 7001;
    static constexpr int32_t kTopicId       = 0;   // "events" → vector index 0
    static constexpr int32_t kPartition     = 0;
    static constexpr int     kPumpIter      = 50'000;

    // All captured egress frames: (sessionId, encoded bytes).
    // Single shared callback; tests filter by sessionId.
    std::vector<std::pair<int64_t,std::vector<uint8_t>>> allFrames_;

    void SetUp() override
    {
        tmpDir_  = std::make_unique<TempDir>("fix");
        dataDir_ = std::make_unique<TempDir>("data");
        fs::create_directories(dataDir_->path);

        startDriver();
        connectAeron();

        // TopicRegistry owns the partition log.
        std::vector<TopicSpec> specs = { makeSpec("events", /*partitions=*/1, /*topicId=*/0) };
        registry_ = std::make_unique<TopicRegistry>(specs, dataDir_->path.string());

        gw_ = std::make_unique<ClientGateway>(
            aeron_, "aeron:ipc", kIngressStream, /*thisNodeId=*/1);
        gw_->setTopicRegistry(registry_.get());

        // Single shared egress capture callback: routes all frames to allFrames_.
        gw_->setOnEgressFrame([this](int64_t sid, const uint8_t* buf, std::size_t len) {
            allFrames_.push_back({sid, std::vector<uint8_t>(buf, buf + len)});
        });
    }

    void TearDown() override
    {
        gw_.reset();
        registry_.reset();
        aeron_.reset();
        stopDriver();
        dataDir_.reset();
        tmpDir_.reset();
    }

    // -----------------------------------------------------------------------
    // Append a batch directly to the log (bypassing gateway produce path).
    // Returns the base offset of the appended batch.
    // -----------------------------------------------------------------------
    int64_t appendToLog(const std::vector<uint8_t>& batch)
    {
        rune::storage::Log* log = registry_->log(kTopicId, kPartition);
        if (!log || batch.size() < 27u) return -1;

        // Read lastOffsetDelta (LE32@23).
        const uint8_t* p = batch.data() + 23;
        const int32_t lod = static_cast<int32_t>(
            static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
            (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24));

        const int64_t base = log->logEndOffset();
        const int64_t last = base + static_cast<int64_t>(lod >= 0 ? lod : 0);
        log->append(batch.data(), batch.size(), last);
        return base;
    }

    // -----------------------------------------------------------------------
    // Pump doWork until condition is met or kPumpIter is exceeded.
    // -----------------------------------------------------------------------
    void pump(std::function<bool()> done, int iterations = kPumpIter)
    {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{10};
        for (int i = 0; i < iterations && clock::now() < deadline && !done(); ++i) {
            gw_->doWork(nowNs());
            // driverThread_ (startDriver()) is the driver's ONE pumper; do not pump
            // aeron_driver_main_do_work here too (second-pumper race, spec Sec 3.6).
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
    }

    // -----------------------------------------------------------------------
    // Inject a test session. Returns its sessionId.
    // -----------------------------------------------------------------------
    int64_t injectSession()
    {
        return gw_->injectTestSession(nowNs());
    }

protected:
    std::unique_ptr<TempDir>         tmpDir_, dataDir_;
    std::unique_ptr<TopicRegistry>   registry_;
    std::unique_ptr<ClientGateway>   gw_;
    std::shared_ptr<aeron::Aeron>    aeron_;

    aeron_driver_context_t*          driverCtx_     = nullptr;
    aeron_driver_t*                  driver_        = nullptr;
    std::atomic<bool>                driverRunning_{false};
    std::thread                      driverThread_;

    // -----------------------------------------------------------------------
    // Aeron driver lifecycle (protected so derived fixtures can call them).
    // -----------------------------------------------------------------------
    void startDriver()
    {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        fs::create_directories(aeronDir);

        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
        aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
        aeron_driver_context_set_term_buffer_length(driverCtx_, 64 * 1024);

        ASSERT_EQ(aeron_driver_init(&driver_, driverCtx_), 0)
            << "aeron_driver_init failed: " << aeron_errmsg();
        ASSERT_EQ(aeron_driver_start(driver_, /*manual_main_loop=*/true), 0)
            << "aeron_driver_start failed: " << aeron_errmsg();

        driverRunning_.store(true, std::memory_order_release);
        driverThread_ = std::thread([this]() {
            while (driverRunning_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(
                    driver_, aeron_driver_main_do_work(driver_));
            }
        });

        // Wait for CnC file.
        // No caller-side pumping: aeron_driver_init/start already wrote
        // cnc.dat and stamped the heartbeat synchronously before driverThread_
        // even starts; pumping here would be a second concurrent pumper on
        // the single-pumper driver (spec Sec 3.6).
        const std::string cncPath = aeronDir + "/cnc.dat";
        ASSERT_TRUE(fs::exists(cncPath)) << "Aeron CnC file not created";
    }

    void connectAeron()
    {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr) << "Aeron::connect failed";
    }

    void stopDriver()
    {
        if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
            if (driverThread_.joinable()) driverThread_.join();
        }
        if (driver_) {
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::milliseconds{50};
            while (std::chrono::steady_clock::now() < deadline) {
                if (aeron_driver_main_do_work(driver_) == 0) break;
                std::this_thread::sleep_for(std::chrono::microseconds{200});
            }
            aeron_driver_close(driver_);
            driver_ = nullptr;
        }
        if (driverCtx_) {
            aeron_driver_context_close(driverCtx_);
            driverCtx_ = nullptr;
        }
    }

};

// ===========================================================================
// Test 1: OpenTxnHiddenBelowLso
//
// Scenario:
//   - Offsets 0..4: non-txn committed batches.
//   - Offset 5: transactional batch (pid=7, no marker → open txn).
//   - HW (= LEO in no-replica mode) = 6.
//
// Expected:
//   - read_uncommitted (isolation=0): sees all 6 batches (0..5).
//   - read_committed  (isolation=1): sees only 0..4 (LSO=5 hides offset 5).
// ===========================================================================
TEST_F(ReadCommittedPushFixture, OpenTxnHiddenBelowLso)
{
    // Append 5 non-txn batches (offsets 0..4).
    for (int i = 0; i < 5; ++i) {
        appendToLog(makeNonTxnBatch(i));
    }

    rune::storage::Log* log = registry_->log(kTopicId, kPartition);
    ASSERT_EQ(log->logEndOffset(), 5);

    // Append transactional batch at offset 5 (pid=7, no marker → open txn).
    appendToLog(makeTxnDataBatch(/*pid=*/7, /*epoch=*/0, /*baseOffset=*/5));
    ASSERT_EQ(log->logEndOffset(), 6);

    // Feed LSO tracker: open txn for pid=7 at offset 5.
    gw_->notifyTransactionalBatch(kTopicId, kPartition, /*pid=*/7, /*baseOffset=*/5);

    // Inject two sessions.
    const int64_t sidU = injectSession();
    const int64_t sidC = injectSession();

    // Create group subscriptions (no-Aeron path).
    const int64_t subU = gw_->addGroupSubscription(sidU, kTopicId, kPartition, 0, /*isolation=*/0);
    const int64_t subC = gw_->addGroupSubscription(sidC, kTopicId, kPartition, 0, /*isolation=*/1);
    ASSERT_GT(subU, 0);
    ASSERT_GT(subC, 0);

    // Drive pushSubscriptions until both converge.
    pump([&]() {
        return extractBaseOffsets(sidU, allFrames_).size() >= 6 &&
               extractBaseOffsets(sidC, allFrames_).size() >= 5;
    });

    const auto offsU = extractBaseOffsets(sidU, allFrames_);
    const auto offsC = extractBaseOffsets(sidC, allFrames_);

    // read_uncommitted: 6 batches (0..5).
    EXPECT_EQ(offsU.size(), 6u)
        << "[TX-4] read_uncommitted should deliver all 6 batches (0..5)";
    for (int64_t i = 0; i < 6; ++i) {
        EXPECT_TRUE(std::find(offsU.begin(), offsU.end(), i) != offsU.end())
            << "[TX-4] read_uncommitted missing batch at offset " << i;
    }

    // read_committed: 5 batches (0..4). LSO=5 hides offset 5.
    EXPECT_EQ(offsC.size(), 5u)
        << "[TX-4] read_committed should deliver only 5 batches (0..4)";
    for (int64_t i = 0; i < 5; ++i) {
        EXPECT_TRUE(std::find(offsC.begin(), offsC.end(), i) != offsC.end())
            << "[TX-4] read_committed missing committed batch at offset " << i;
    }
    EXPECT_TRUE(std::find(offsC.begin(), offsC.end(), int64_t{5}) == offsC.end())
        << "[TX-4] read_committed must NOT deliver open-txn batch at offset 5";
}

// ===========================================================================
// Test 2: AbortedRecordsFilteredControlMarkersNeverDelivered
//
// Scenario:
//   - Offsets 0..4: non-txn committed batches.
//   - Offsets 5..7: txn batches (pid=9), aborted.
//   - Offset 8: ABORT control batch for pid=9.
//   - Offset 9: non-txn committed batch.
//
// Expected:
//   - read_committed:  delivers 0..4 and 9; never 5..7 (aborted), never 8 (control).
//   - read_uncommitted: delivers 0..7 and 9; never 8 (control, always filtered).
// ===========================================================================
TEST_F(ReadCommittedPushFixture, AbortedRecordsFilteredControlMarkersNeverDelivered)
{
    // Append offsets 0..4 (non-txn committed).
    for (int i = 0; i < 5; ++i) {
        appendToLog(makeNonTxnBatch(i));
    }

    rune::storage::Log* log = registry_->log(kTopicId, kPartition);
    ASSERT_EQ(log->logEndOffset(), 5);

    // Append offsets 5..7 (txn pid=9, will be aborted).
    for (int i = 5; i <= 7; ++i) {
        appendToLog(makeTxnDataBatch(/*pid=*/9, /*epoch=*/0, i));
        gw_->notifyTransactionalBatch(kTopicId, kPartition, /*pid=*/9, i);
    }
    ASSERT_EQ(log->logEndOffset(), 8);

    // Append ABORT control batch at offset 8 for pid=9.
    auto abortMarker = tx::ControlMarker::build(/*pid=*/9, /*epoch=*/0,
                                                tx::ControlType::Abort, /*coordEpoch=*/0);
    // Stamp baseOffset=8 into the marker (LE64@0).
    {
        uint64_t u = 8ULL;
        for (int i = 0; i < 8; ++i)
            abortMarker[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    // Compute lastOffset from lastOffsetDelta (LE32@23).
    int32_t lod = 0;
    if (abortMarker.size() >= 27u) {
        const uint8_t* p = abortMarker.data() + 23;
        lod = static_cast<int32_t>(
            static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
            (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24));
    }
    log->append(abortMarker.data(), abortMarker.size(), /*lastOffset=*/8 + lod);
    ASSERT_EQ(log->logEndOffset(), 9);

    // Notify gateway: ABORT marker for pid=9 at offset 8.
    gw_->notifyControlMarker(kTopicId, kPartition, /*pid=*/9, /*markerOffset=*/8, /*commit=*/false);

    // Append offset 9 (non-txn committed).
    appendToLog(makeNonTxnBatch(9));
    ASSERT_EQ(log->logEndOffset(), 10);

    // Inject sessions.
    const int64_t sidU = injectSession();
    const int64_t sidC = injectSession();

    const int64_t subU = gw_->addGroupSubscription(sidU, kTopicId, kPartition, 0, /*isolation=*/0);
    const int64_t subC = gw_->addGroupSubscription(sidC, kTopicId, kPartition, 0, /*isolation=*/1);
    ASSERT_GT(subU, 0);
    ASSERT_GT(subC, 0);

    // Pump until both see expected counts.
    // read_uncommitted: 9 batches (0..7, 9). Never offset 8 (control).
    // read_committed:   6 batches (0..4, 9). Never 5..7 (aborted), never 8 (control).
    pump([&]() {
        return extractBaseOffsets(sidU, allFrames_).size() >= 9 &&
               extractBaseOffsets(sidC, allFrames_).size() >= 6;
    });

    const auto ou = extractBaseOffsets(sidU, allFrames_);
    const auto oc = extractBaseOffsets(sidC, allFrames_);

    // read_uncommitted: 0..7, 9 (=9 batches). Never offset 8 (control).
    EXPECT_EQ(ou.size(), 9u)
        << "[TX-4] read_uncommitted should deliver 9 batches (0..7,9)";
    EXPECT_TRUE(std::find(ou.begin(), ou.end(), int64_t{8}) == ou.end())
        << "[TX-4] read_uncommitted must NEVER deliver control batch at offset 8";
    for (int64_t i = 0; i < 8; ++i) {
        EXPECT_TRUE(std::find(ou.begin(), ou.end(), i) != ou.end())
            << "[TX-4] read_uncommitted missing batch at offset " << i;
    }
    EXPECT_TRUE(std::find(ou.begin(), ou.end(), int64_t{9}) != ou.end())
        << "[TX-4] read_uncommitted should deliver committed batch at offset 9";

    // read_committed: 0..4, 9 (=6 batches). Never 5..7 (aborted), never 8 (control).
    EXPECT_EQ(oc.size(), 6u)
        << "[TX-4] read_committed should deliver 6 batches (0..4, 9)";
    for (int64_t i = 5; i <= 8; ++i) {
        EXPECT_TRUE(std::find(oc.begin(), oc.end(), i) == oc.end())
            << "[TX-4] read_committed must NOT deliver batch at offset " << i
            << " (aborted: 5..7; control: 8)";
    }
    EXPECT_TRUE(std::find(oc.begin(), oc.end(), int64_t{9}) != oc.end())
        << "[TX-4] read_committed should deliver committed batch at offset 9";
}

// ===========================================================================
// Test 3 (Step 6): GroupReadCommitted
//
// Scenario: group subscription with isolation=1 honours LSO cap.
//   - Offsets 0..4: non-txn committed.
//   - Offset 5: open transactional batch (pid=42, no marker yet).
//
// Expected:
//   - Group subscriber (isolation=1) receives only 0..4.
// ===========================================================================
TEST_F(ReadCommittedPushFixture, GroupReadCommitted)
{
    // Append 5 non-txn batches (offsets 0..4).
    for (int i = 0; i < 5; ++i) {
        appendToLog(makeNonTxnBatch(i));
    }

    // Append open-txn batch at offset 5 (pid=42).
    appendToLog(makeTxnDataBatch(/*pid=*/42, /*epoch=*/0, /*baseOffset=*/5));
    gw_->notifyTransactionalBatch(kTopicId, kPartition, /*pid=*/42, /*baseOffset=*/5);

    rune::storage::Log* log = registry_->log(kTopicId, kPartition);
    ASSERT_EQ(log->logEndOffset(), 6);

    // Group subscriber with isolation=1.
    const int64_t sid   = injectSession();
    const int64_t subId = gw_->addGroupSubscription(
        sid, kTopicId, kPartition, /*fromOffset=*/0, /*isolation=*/1);
    ASSERT_GT(subId, 0);

    pump([&]() {
        return extractBaseOffsets(sid, allFrames_).size() >= 5;
    });

    const auto offs = extractBaseOffsets(sid, allFrames_);
    EXPECT_EQ(offs.size(), 5u)
        << "[TX-4 Step6] Group read_committed should deliver 5 batches (0..4)";
    EXPECT_TRUE(std::find(offs.begin(), offs.end(), int64_t{5}) == offs.end())
        << "[TX-4 Step6] Group read_committed must NOT deliver open-txn batch at offset 5";
}

// ===========================================================================
// Test 4 (TX-6 regression): GroupSubscribeCarrierNoDuplicate
//
// Replicates the DR-E2E double-delivery bug at integration speed.
//
// A group member's partition is first registered by the coordinator via
// addGroupSubscription() (isGroup=true).  The member THEN sends a Subscribe
// carrier frame to convey its isolation level (TX-5 sendSubscribeForPartitions).
//
// Before the fix, handleSubscribe() always created a SECOND, independent
// subscription for the same (session, topic, partition) — so pushSubscriptions()
// delivered every record TWICE (the E2E "eos-no-dup: N records delivered 2×").
//
// After the fix, the carrier merges into the existing group subscription:
//   - subscriptionCount() stays 1 (no duplicate),
//   - the group sub's isolation is updated in place,
//   - each record is delivered exactly once.
// ===========================================================================
TEST_F(ReadCommittedPushFixture, GroupSubscribeCarrierNoDuplicate)
{
    // Append 5 non-txn committed batches (offsets 0..4).
    for (int i = 0; i < 5; ++i) {
        appendToLog(makeNonTxnBatch(i));
    }
    rune::storage::Log* log = registry_->log(kTopicId, kPartition);
    ASSERT_EQ(log->logEndOffset(), 5);

    // Coordinator registers the group subscription with the conservative default
    // isolation=read_committed (matches GroupCoordinationService::handleSyncGroup).
    const int64_t sid   = injectSession();
    const int64_t subId = gw_->addGroupSubscription(
        sid, kTopicId, kPartition, /*fromOffset=*/0, /*isolation=*/1);
    ASSERT_GT(subId, 0);
    ASSERT_EQ(gw_->subscriptionCount(), 1u);
    ASSERT_EQ(gw_->groupMemberSubCount(sid), 1u);

    // Member sends its isolation carrier (read_uncommitted).  This MUST update
    // the existing group sub in place, NOT create a second subscription.
    const auto frame = buildSubscribeFrame(sid, kTopicId, kPartition, /*isolation=*/0);
    gw_->applySubscribeForTest(frame.data(), frame.size());

    EXPECT_EQ(gw_->subscriptionCount(), 1u)
        << "[TX-6] Subscribe carrier must NOT create a duplicate subscription "
           "(root cause of E2E 2× delivery)";
    EXPECT_EQ(gw_->groupMemberSubCount(sid), 1u)
        << "[TX-6] group sub tracking must remain single-entry after carrier";
    EXPECT_EQ(gw_->subscriptionIsolationForTest(sid, kTopicId, kPartition), 0)
        << "[TX-6] carrier must downgrade the group sub to read_uncommitted";

    // Pump and assert exactly-once delivery of every batch.
    pump([&]() {
        return extractBaseOffsets(sid, allFrames_).size() >= 5;
    });

    const auto counts = deliveryCounts(sid, allFrames_);
    EXPECT_EQ(counts.size(), 5u) << "[TX-6] all 5 batches (0..4) must be delivered";
    for (const auto& [off, n] : counts) {
        EXPECT_EQ(n, 1) << "[TX-6] batch at offset " << off
                        << " delivered " << n << "× (duplicate delivery bug)";
    }
}

// ===========================================================================
// Test 5 (TX-6 regression): GroupSubscribeCarrierReadCommittedNoLeak
//
// Replicates the DR-E2E read_committed isolation leak.
//
// Before the fix, the group subscription created by the coordinator was
// hardcoded isolation=read_uncommitted, so it delivered ABORTED transactional
// records even to a member that asked for read_committed (the E2E
// "read-committed-isolation: N aborted/open records leaked").
//
// After the fix, the group sub defaults to read_committed and the carrier keeps
// it there; aborted/open/control batches are filtered, with no duplicates.
// ===========================================================================
TEST_F(ReadCommittedPushFixture, GroupSubscribeCarrierReadCommittedNoLeak)
{
    // Offsets 0..4: non-txn committed.
    for (int i = 0; i < 5; ++i) appendToLog(makeNonTxnBatch(i));
    rune::storage::Log* log = registry_->log(kTopicId, kPartition);
    ASSERT_EQ(log->logEndOffset(), 5);

    // Offsets 5..7: txn pid=9 (will be aborted).
    for (int i = 5; i <= 7; ++i) {
        appendToLog(makeTxnDataBatch(/*pid=*/9, /*epoch=*/0, i));
        gw_->notifyTransactionalBatch(kTopicId, kPartition, /*pid=*/9, i);
    }
    // ABORT control marker at offset 8.
    auto abortMarker = tx::ControlMarker::build(/*pid=*/9, /*epoch=*/0,
                                                tx::ControlType::Abort, /*coordEpoch=*/0);
    {
        uint64_t u = 8ULL;
        for (int i = 0; i < 8; ++i) abortMarker[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    int32_t lod = 0;
    if (abortMarker.size() >= 27u) {
        const uint8_t* p = abortMarker.data() + 23;
        lod = static_cast<int32_t>(
            static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
            (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24));
    }
    log->append(abortMarker.data(), abortMarker.size(), /*lastOffset=*/8 + lod);
    gw_->notifyControlMarker(kTopicId, kPartition, /*pid=*/9, /*markerOffset=*/8, /*commit=*/false);

    // Offset 9: non-txn committed.
    appendToLog(makeNonTxnBatch(9));
    ASSERT_EQ(log->logEndOffset(), 10);

    // Coordinator registers group sub (conservative default isolation=1).
    const int64_t sid   = injectSession();
    const int64_t subId = gw_->addGroupSubscription(
        sid, kTopicId, kPartition, /*fromOffset=*/0, /*isolation=*/1);
    ASSERT_GT(subId, 0);

    // Member's carrier confirms read_committed.  Still a single subscription.
    const auto frame = buildSubscribeFrame(sid, kTopicId, kPartition, /*isolation=*/1);
    gw_->applySubscribeForTest(frame.data(), frame.size());
    ASSERT_EQ(gw_->subscriptionCount(), 1u)
        << "[TX-6] read_committed carrier must not duplicate the group sub";
    EXPECT_EQ(gw_->subscriptionIsolationForTest(sid, kTopicId, kPartition), 1);

    // read_committed: deliver 0..4 and 9 (6 batches) exactly once; never 5..8.
    pump([&]() {
        return extractBaseOffsets(sid, allFrames_).size() >= 6;
    });

    const auto counts = deliveryCounts(sid, allFrames_);
    EXPECT_EQ(counts.size(), 6u)
        << "[TX-6] read_committed group consumer should see 6 batches (0..4, 9)";
    for (int64_t i = 5; i <= 8; ++i) {
        EXPECT_EQ(counts.count(i), 0u)
            << "[TX-6] aborted/control batch at offset " << i
            << " leaked into read_committed group consumer";
    }
    for (const auto& [off, n] : counts) {
        EXPECT_EQ(n, 1) << "[TX-6] batch at offset " << off
                        << " delivered " << n << "× (duplicate delivery bug)";
    }
}

// ===========================================================================
// Test 6 (TX-6b regression): GroupReadUncommittedSeesAbortedAfterCarrier
//
// Repro for the Docker E2E failure in test_tx_read_committed_smoke:
//   - A read_uncommitted group consumer missed aborted-txn records because the
//     group subscription was created with the conservative default isolation=1,
//     the broker pushed records (skipping aborted ones + advancing nextOffset),
//     and by the time the Subscribe carrier arrived with isolation=0, nextOffset
//     was already past the aborted records (permanently lost).
//
// Fix: the push loop stalls at the first aborted transactional batch for a
// pre-carrier group sub (!groupCarrierReceived).  The carrier then arrives,
// sets groupCarrierReceived=true and isolation=0, and the next push cycle
// delivers the aborted records as required by read_uncommitted semantics.
//
// Scenario:
//   - Offsets 0..4: non-txn committed.
//   - Offsets 5..7: txn pid=9, aborted.
//   - Offset 8: ABORT control marker.
//   - Offset 9: non-txn committed.
//
// Expected (after fix — this test was RED before the fix):
//   - Group sub created with isolation=1 (default, pre-carrier).
//   - Several push cycles run WITHOUT the carrier: no record is delivered from
//     offset 5 onward yet (stall at offset 5 = first aborted batch).
//   - Carrier arrives with isolation=0.
//   - After pumping: batches 0..7 and 9 are delivered (9 batches); offset 8
//     (control) is never delivered in either isolation mode.
// ===========================================================================
TEST_F(ReadCommittedPushFixture, GroupReadUncommittedSeesAbortedAfterCarrier)
{
    // Offsets 0..4: non-txn committed.
    for (int i = 0; i < 5; ++i) appendToLog(makeNonTxnBatch(i));

    rune::storage::Log* log = registry_->log(kTopicId, kPartition);
    ASSERT_EQ(log->logEndOffset(), 5);

    // Offsets 5..7: txn pid=9, aborted.
    for (int i = 5; i <= 7; ++i) {
        appendToLog(makeTxnDataBatch(/*pid=*/9, /*epoch=*/0, i));
        gw_->notifyTransactionalBatch(kTopicId, kPartition, /*pid=*/9, i);
    }
    // ABORT control marker at offset 8 (resolves the txn).
    auto abortMarker = tx::ControlMarker::build(/*pid=*/9, /*epoch=*/0,
                                                tx::ControlType::Abort, /*coordEpoch=*/0);
    {
        uint64_t u = 8ULL;
        for (int i = 0; i < 8; ++i)
            abortMarker[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    int32_t lod = 0;
    if (abortMarker.size() >= 27u) {
        const uint8_t* p = abortMarker.data() + 23;
        lod = static_cast<int32_t>(
            static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
            (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24));
    }
    log->append(abortMarker.data(), abortMarker.size(), /*lastOffset=*/8 + lod);
    gw_->notifyControlMarker(kTopicId, kPartition, /*pid=*/9, /*markerOffset=*/8,
                             /*commit=*/false);
    // Offset 9: non-txn committed.
    appendToLog(makeNonTxnBatch(9));
    ASSERT_EQ(log->logEndOffset(), 10);

    // Coordinator creates group subscription with default isolation=1 (pre-carrier).
    const int64_t sid   = injectSession();
    const int64_t subId = gw_->addGroupSubscription(
        sid, kTopicId, kPartition, /*fromOffset=*/0, /*isolation=*/1, /*pendingCarrier=*/true);
    ASSERT_GT(subId, 0);

    // Pump several cycles WITHOUT sending the carrier.
    // Before the fix: nextOffset would advance past the aborted records.
    // After the fix: nextOffset stalls at offset 5 (first aborted batch).
    pump([&]() { return false; }, 200);  // run 200 duty cycles unconditionally

    // Verify: pre-carrier, batches 0..4 may have been delivered but NOT 5..7 or 9.
    // (nextOffset must not have advanced past 5 into the aborted records.)
    const auto preCarrierOffsets = extractBaseOffsets(sid, allFrames_);
    for (int64_t i = 5; i <= 9; ++i) {
        EXPECT_TRUE(std::find(preCarrierOffsets.begin(), preCarrierOffsets.end(), i)
                    == preCarrierOffsets.end())
            << "[TX-6b] pre-carrier: batch at offset " << i
            << " must NOT be delivered before the isolation carrier arrives";
    }

    // Now send the isolation carrier: read_uncommitted (isolation=0).
    const auto frame = buildSubscribeFrame(sid, kTopicId, kPartition, /*isolation=*/0);
    gw_->applySubscribeForTest(frame.data(), frame.size());
    ASSERT_EQ(gw_->subscriptionIsolationForTest(sid, kTopicId, kPartition), 0)
        << "[TX-6b] carrier must set isolation=read_uncommitted";

    // Pump until all 9 expected batches (0..7, 9) are delivered.
    pump([&]() {
        return extractBaseOffsets(sid, allFrames_).size() >= 9;
    });

    const auto allOffsets = extractBaseOffsets(sid, allFrames_);

    // read_uncommitted: must see 0..7 and 9 (=9 batches). Never offset 8 (control).
    EXPECT_GE(allOffsets.size(), 9u)
        << "[TX-6b] read_uncommitted group consumer should deliver >= 9 batches "
           "(0..7, 9) after carrier arrival";

    // Aborted records 5..7 MUST be present.
    for (int64_t i = 5; i <= 7; ++i) {
        EXPECT_TRUE(std::find(allOffsets.begin(), allOffsets.end(), i) != allOffsets.end())
            << "[TX-6b] read_uncommitted must deliver aborted batch at offset " << i
            << " after isolation carrier sets isolation=0";
    }
    // Control batch (offset 8) MUST NOT be present.
    EXPECT_TRUE(std::find(allOffsets.begin(), allOffsets.end(), int64_t{8}) == allOffsets.end())
        << "[TX-6b] control batch at offset 8 must never be delivered";
    // Non-txn post-abort batch (offset 9) MUST be present.
    EXPECT_TRUE(std::find(allOffsets.begin(), allOffsets.end(), int64_t{9}) != allOffsets.end())
        << "[TX-6b] non-txn batch at offset 9 must be delivered";
}

// ===========================================================================
// TX-7: ReadCommittedRebuildOnFailoverFixture
//
// Tests that rebuildProducerStateFromLogs() ALSO rebuilds per-partition
// read_committed state (LsoTracker + AbortedTxnIndex) on Follower→Leader
// transition.
//
// Before TX-7 fix: partitionTxnStates_ is EMPTY after failover →
//   lso(hw) returns hw (no open txns tracked) → open-txn records leak through
//   read_committed; abortedIndex is empty → aborted records also leak.
//
// After TX-7 fix: ReadCommittedView::rebuild seeds LsoTracker + AbortedTxnIndex
//   from the committed log → LSO pins correctly; aborted records are filtered.
//
// TEST 1 — AbortedRecordFilteredAfterFailover:
//   Verifies the AbortedTxnIndex is rebuilt correctly after failover.
//   Scenario:
//     offset 0..2 — non-txn committed (always visible).
//     offset 3..4 — txn pid=13 data batches (ABORTED).
//     offset 5    — ABORT control marker for pid=13.
//     offset 6    — non-txn committed (visible: LSO=HW since txn resolved).
//   After failover rebuild:
//     - offsets 0,1,2,6 delivered (committed, no open txns blocking LSO).
//     - offsets 3,4 NOT delivered (aborted — AbortedTxnIndex rebuilt from log).
//     - offset 5 NOT delivered (control marker, never delivered in any mode).
//
// TEST 2 — OpenTxnPinsLsoAfterFailover:
//   Verifies the LsoTracker is rebuilt correctly after failover.
//   Scenario:
//     offset 0..2 — non-txn committed.
//     offset 3    — txn pid=11 data batch (OPEN — no marker).
//   After failover rebuild:
//     - LSO = 3 (open txn at offset 3 pins it).
//     - offsets 0,1,2 delivered (< LSO=3).
//     - offset 3 NOT delivered (open txn, above LSO=3).
// ===========================================================================

class ReadCommittedRebuildOnFailoverFixture : public ReadCommittedPushFixture {
public:
    // Current leadership state exposed to the gateway.
    bool isLeader_ = false;

    void SetUp() override {
        // Build dirs first (parent SetUp also does this; we replicate it so we
        // can create the gateway with leaderView_ wired in).
        tmpDir_  = std::make_unique<TempDir>("tx7fix");
        dataDir_ = std::make_unique<TempDir>("tx7data");
        fs::create_directories(dataDir_->path);

        startDriver();
        connectAeron();

        std::vector<TopicSpec> specs = { makeSpec("events", /*partitions=*/1, /*topicId=*/0) };
        registry_ = std::make_unique<TopicRegistry>(specs, dataDir_->path.string());

        gw_ = std::make_unique<ClientGateway>(
            aeron_, "aeron:ipc", kIngressStream, /*thisNodeId=*/1,
            ClientGateway::kDefaultSessionTimeoutNs,
            /*leaderView=*/[this]() -> LeaderView {
                return LeaderView{isLeader_, ""};
            });
        gw_->setTopicRegistry(registry_.get());

        gw_->setOnEgressFrame([this](int64_t sid, const uint8_t* buf, std::size_t len) {
            allFrames_.push_back({sid, std::vector<uint8_t>(buf, buf + len)});
        });
    }

};

// ---------------------------------------------------------------------------
// TX7 Test 1: AbortedRecordFilteredAfterFailover
//
// Pre-fix: empty AbortedTxnIndex → aborted records 3,4 leak into read_committed.
// Post-fix: rebuild from log seeds aborted range → records filtered (BUG FIXED).
// ---------------------------------------------------------------------------
TEST_F(ReadCommittedRebuildOnFailoverFixture, AbortedRecordFilteredAfterFailover)
{
    rune::storage::Log* log = registry_->log(kTopicId, kPartition);
    ASSERT_NE(log, nullptr);

    // Step 1. Run one doWork() as follower so wasLeader_=false is recorded.
    isLeader_ = false;
    gw_->doWork(nowNs());

    // Step 2. Populate the log (simulates data replicated from L1).
    //
    //   offsets 0,1,2  — non-txn committed.
    //   offsets 3,4    — txn pid=13 data batches (ABORTED).
    //   offset  5      — ABORT control marker for pid=13.
    //   offset  6      — non-txn committed (LSO=HW after marker, so visible).
    for (int i = 0; i < 3; ++i) appendToLog(makeNonTxnBatch(i));
    ASSERT_EQ(log->logEndOffset(), 3);

    appendToLog(makeTxnDataBatch(/*pid=*/13, /*epoch=*/0, /*baseOffset=*/3));
    appendToLog(makeTxnDataBatch(/*pid=*/13, /*epoch=*/0, /*baseOffset=*/4));
    ASSERT_EQ(log->logEndOffset(), 5);

    // ABORT control marker for pid=13 at offset 5.
    auto abortMarker = tx::ControlMarker::build(/*pid=*/13, /*epoch=*/0,
                                                tx::ControlType::Abort, /*coordEpoch=*/0);
    {
        uint64_t u = 5ULL;
        for (int i = 0; i < 8; ++i)
            abortMarker[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    {
        int32_t lod = 0;
        if (abortMarker.size() >= 27u) {
            const uint8_t* p = abortMarker.data() + 23;
            lod = static_cast<int32_t>(
                static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
                (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24));
        }
        log->append(abortMarker.data(), abortMarker.size(), /*lastOffset=*/5 + lod);
    }
    ASSERT_EQ(log->logEndOffset(), 6);

    // Non-txn committed batch at offset 6.
    appendToLog(makeNonTxnBatch(6));
    ASSERT_EQ(log->logEndOffset(), 7);

    // partitionTxnStates_ is intentionally EMPTY (never populated as follower).
    // This is the pre-fix bug state: after failover, partitionTxnStates_ is empty
    // → AbortedTxnIndex has no entries → aborted records 3,4 would LEAK.
    // The TX-7 fix rebuilds it from the log in rebuildProducerStateFromLogs().

    // Step 3. Flip to leader → doWork() fires Follower→Leader rebuild.
    isLeader_ = true;
    gw_->doWork(nowNs());

    // Step 4. Inject a read_committed subscriber.
    const int64_t sid   = injectSession();
    const int64_t subId = gw_->addGroupSubscription(
        sid, kTopicId, kPartition, /*fromOffset=*/0, /*isolation=*/1);
    ASSERT_GT(subId, 0);

    // Pump until committed batches 0,1,2,6 are delivered.
    // (After the fix, LSO=HW=7 since the abort marker resolved the txn;
    //  aborted records 3,4 should be filtered by the rebuilt AbortedTxnIndex.)
    pump([&]() {
        const auto offs = extractBaseOffsets(sid, allFrames_);
        bool has0=false, has1=false, has2=false, has6=false;
        for (int64_t o : offs) {
            if (o == 0) has0 = true;
            if (o == 1) has1 = true;
            if (o == 2) has2 = true;
            if (o == 6) has6 = true;
        }
        return has0 && has1 && has2 && has6;
    });

    const auto delivered = extractBaseOffsets(sid, allFrames_);

    // Committed non-txn records (0,1,2,6) MUST be delivered.
    for (int64_t expected : {int64_t{0}, int64_t{1}, int64_t{2}, int64_t{6}}) {
        EXPECT_TRUE(std::find(delivered.begin(), delivered.end(), expected) != delivered.end())
            << "[TX-7] committed batch at offset " << expected
            << " must be visible under read_committed after failover rebuild";
    }

    // ABORTED records (3, 4) must NOT be delivered.
    // Pre-fix: empty AbortedTxnIndex → isAborted() always false → LEAKED (BUG).
    // Post-fix: rebuild seeds aborted range [3..5] → records filtered.
    EXPECT_TRUE(std::find(delivered.begin(), delivered.end(), int64_t{3}) == delivered.end())
        << "[TX-7] CRITICAL: aborted batch at offset 3 MUST NOT be delivered "
           "under read_committed after failover rebuild. "
           "Pre-fix: empty AbortedTxnIndex after failover → record leaks (BUG).";
    EXPECT_TRUE(std::find(delivered.begin(), delivered.end(), int64_t{4}) == delivered.end())
        << "[TX-7] CRITICAL: aborted batch at offset 4 MUST NOT be delivered "
           "under read_committed after failover rebuild. "
           "Pre-fix: empty AbortedTxnIndex after failover → record leaks (BUG).";

    // Control marker (offset 5) must never be delivered.
    EXPECT_TRUE(std::find(delivered.begin(), delivered.end(), int64_t{5}) == delivered.end())
        << "[TX-7] control batch at offset 5 must never be delivered in any mode";
}

// ---------------------------------------------------------------------------
// TX7 Test 2: OpenTxnPinsLsoAfterFailover
//
// Pre-fix: empty LsoTracker → lso(hw)==hw → open-txn record (offset 3) LEAKED.
// Post-fix: rebuild seeds open-txn at offset 3 → lso=3 → batch hidden.
// ---------------------------------------------------------------------------
TEST_F(ReadCommittedRebuildOnFailoverFixture, OpenTxnPinsLsoAfterFailover)
{
    rune::storage::Log* log = registry_->log(kTopicId, kPartition);
    ASSERT_NE(log, nullptr);

    // Step 1. Run one doWork() as follower so wasLeader_=false is recorded.
    isLeader_ = false;
    gw_->doWork(nowNs());

    // Step 2. Populate the log.
    //   offsets 0,1,2 — non-txn committed.
    //   offset  3     — txn pid=11 data batch (OPEN — no marker).
    for (int i = 0; i < 3; ++i) appendToLog(makeNonTxnBatch(i));
    ASSERT_EQ(log->logEndOffset(), 3);

    appendToLog(makeTxnDataBatch(/*pid=*/11, /*epoch=*/0, /*baseOffset=*/3));
    ASSERT_EQ(log->logEndOffset(), 4);

    // partitionTxnStates_ is intentionally EMPTY (pre-fix bug state).

    // Step 3. Flip to leader → doWork() fires Follower→Leader rebuild.
    isLeader_ = true;
    gw_->doWork(nowNs());

    // Step 4. Inject a read_committed subscriber.
    const int64_t sid   = injectSession();
    const int64_t subId = gw_->addGroupSubscription(
        sid, kTopicId, kPartition, /*fromOffset=*/0, /*isolation=*/1);
    ASSERT_GT(subId, 0);

    // Pump until committed batches 0,1,2 are delivered.
    // After the fix: LSO=3 (open txn at offset 3 pins LSO); offsets 0,1,2 visible.
    // Pre-fix: LSO=HW=4 → offset 3 also delivered (BUG).
    pump([&]() {
        const auto offs = extractBaseOffsets(sid, allFrames_);
        bool has0=false, has1=false, has2=false;
        for (int64_t o : offs) {
            if (o == 0) has0 = true;
            if (o == 1) has1 = true;
            if (o == 2) has2 = true;
        }
        return has0 && has1 && has2;
    });

    // Run a few more cycles to confirm offset 3 does NOT leak.
    pump([&]() { return false; }, 500);

    const auto delivered = extractBaseOffsets(sid, allFrames_);

    // Committed non-txn records (0,1,2) MUST be delivered (< LSO=3).
    for (int64_t expected : {int64_t{0}, int64_t{1}, int64_t{2}}) {
        EXPECT_TRUE(std::find(delivered.begin(), delivered.end(), expected) != delivered.end())
            << "[TX-7] committed batch at offset " << expected
            << " must be visible under read_committed (< LSO=3) after failover rebuild";
    }

    // OPEN-TXN record (offset 3) must NOT be delivered.
    // Pre-fix: empty LsoTracker → lso(hw)=4=hw → offset 3 < bound=4 → LEAKED (BUG).
    // Post-fix: rebuild seeds open-txn at offset 3 → lso=3 → offset 3 >= bound=3 → hidden.
    EXPECT_TRUE(std::find(delivered.begin(), delivered.end(), int64_t{3}) == delivered.end())
        << "[TX-7] CRITICAL: open-txn batch at offset 3 MUST NOT be delivered "
           "under read_committed (LSO must pin at 3 after failover rebuild). "
           "Pre-fix: empty LsoTracker → lso(hw)=hw → record leaks (BUG).";
}

} // namespace rune::gateway
