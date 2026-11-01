// CheckedWritesProducePathHookTest.cpp - handleProduceBatch<LogT, HookT>: where an active write hook is
// called (check after dedupe and before the min-ISR gate, stage before the append, onAppended after the
// PSM update) and that the hook-less forwarder is unchanged.

#include <gtest/gtest.h>

#include <rune/storage/Log.h>
#include <rune/storage/ProducePath.h>
#include <rune/storage/ProducePathImpl.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

namespace rune::storage {
namespace {

namespace pc = rune::protocol::client;
namespace fs = std::filesystem;

struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<uint32_t> seq{0};
        path = fs::temp_directory_path() / ("rune_cwph_" + std::to_string(::getpid()) + "_" + std::to_string(seq++) + "_" +
                                            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(path);
    }
    ~TmpDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

std::vector<uint8_t> makeBatch(int64_t pid, int32_t seq, const char* value = "v") {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    if (pid >= 0) {
        b.setProducerId(pid);
        b.setProducerEpoch(0);
        b.setBaseSequence(seq);
    }
    b.append("k", value, 1000);
    return b.build();
}

struct RecordingHook {
    static constexpr bool kActive = true;
    std::array<char, 8>   trace{};
    int                   n = 0;
    pc::ErrorCode         checkResult = pc::ErrorCode::OK;
    const std::vector<uint8_t>* substitute = nullptr;
    int64_t               appendedBase = -1, appendedLast = -1;
    int32_t               seenRecords = -1;

    pc::ErrorCode check(const uint8_t*, std::size_t, int32_t recordCount) {
        trace[n++]  = 'C';
        seenRecords = recordCount;
        return checkResult;
    }
    template <class LogT>
    HookStaged stage(LogT&, const uint8_t* b, std::size_t len) {
        trace[n++] = 'S';
        if (substitute) return {pc::ErrorCode::OK, substitute->data(), substitute->size()};
        return {pc::ErrorCode::OK, b, len};
    }
    void onAppended(int64_t base, int64_t last) {
        trace[n++]   = 'A';
        appendedBase = base;
        appendedLast = last;
    }
    uint64_t assignedBase() const noexcept { return 77; }
    std::string traceStr() const { return std::string(trace.data(), static_cast<std::size_t>(n)); }
};

class CheckedWritesProducePathHook : public ::testing::Test {
protected:
    CheckedWritesProducePathHook() : log_(dir_.path.string()) {}

    ProduceOutcome run(std::vector<uint8_t>& batch, RecordingHook& hook, int64_t pid = -1, int32_t seq = -1,
                       bool acksAll = false, int isr = 0, int minIsr = 1) {
        return handleProduceBatch<Log, RecordingHook>(batch.data(), batch.size(), 1, 0, pid, 0, seq, log_, psm_, acksAll, isr,
                                                      minIsr, 1, false, hook);
    }

    TmpDir              dir_;
    Log                 log_;
    ProducerStateManager psm_;
};

}  // namespace

TEST_F(CheckedWritesProducePathHook, CallsCheckStageOnAppendedInOrder) {
    RecordingHook h;
    auto b = makeBatch(11, 0);
    const auto out = run(b, h, 11, 0);
    ASSERT_TRUE(out.isOK());
    EXPECT_EQ(h.traceStr(), "CSA");
    EXPECT_EQ(h.appendedBase, 0);
    EXPECT_EQ(h.appendedLast, 0);
    EXPECT_EQ(h.seenRecords, 1);
    EXPECT_EQ(out.assignedBase, 77u);
    EXPECT_EQ(log_.logEndOffset(), 1);
}

TEST_F(CheckedWritesProducePathHook, NonIdempotentBatchStillRunsCheck) {
    RecordingHook h;
    auto b = makeBatch(-1, -1);
    ASSERT_TRUE(run(b, h).isOK());
    EXPECT_EQ(h.traceStr(), "CSA");
}

TEST_F(CheckedWritesProducePathHook, CheckRefusalAppendsNothingAndLeavesThePsm) {
    RecordingHook h;
    h.checkResult = pc::ErrorCode::InvalidRequest;
    auto b = makeBatch(11, 0);
    auto out = run(b, h, 11, 0);
    EXPECT_EQ(out.error, pc::ErrorCode::InvalidRequest);
    EXPECT_EQ(log_.logEndOffset(), 0);
    EXPECT_EQ(h.traceStr(), "C");

    RecordingHook ok;
    out = run(b, ok, 11, 0);
    ASSERT_TRUE(out.isOK());
    EXPECT_FALSE(out.isDuplicate);
    EXPECT_EQ(out.baseOffset, 0);
}

TEST_F(CheckedWritesProducePathHook, CheckRunsBeforeTheMinIsrGate) {
    auto b = makeBatch(-1, -1);
    RecordingHook refusing;
    refusing.checkResult = pc::ErrorCode::InvalidRequest;
    EXPECT_EQ(run(b, refusing, -1, -1, true, 1, 2).error, pc::ErrorCode::InvalidRequest);

    RecordingHook passing;
    EXPECT_EQ(run(b, passing, -1, -1, true, 1, 2).error, pc::ErrorCode::MinIsrViolation);
    EXPECT_EQ(passing.traceStr(), "C");
}

TEST_F(CheckedWritesProducePathHook, StagedBytesAreWhatIsAppended) {
    auto original = makeBatch(-1, -1, "v");
    const auto substitute = makeBatch(-1, -1, "w");
    ASSERT_EQ(original.size(), substitute.size());
    RecordingHook h;
    h.substitute = &substitute;
    ASSERT_TRUE(run(original, h).isOK());
    const auto stored = log_.read(0, 1 << 16);
    ASSERT_GE(stored.size(), substitute.size());
    // Everything after the base offset field is the substitute's bytes (the base offset is restamped).
    EXPECT_TRUE(std::equal(substitute.begin() + 8, substitute.end(), stored.begin() + 8));
    rune::protocol::RecordBatchReader reader(stored.data(), stored.size());
    EXPECT_TRUE(reader.ensureValid());
}

TEST_F(CheckedWritesProducePathHook, ForwarderRunsTheSameCoreAsNoWriteHook) {
    auto b = makeBatch(11, 0);
    const auto viaForwarder = handleProduceBatch(b.data(), b.size(), 1, 0, 11, 0, 0, log_, psm_, false, 0, 1, 1);
    ASSERT_TRUE(viaForwarder.isOK());
    EXPECT_EQ(viaForwarder.assignedBase, 0u);
    NoWriteHook none;
    auto b2 = makeBatch(11, 1);
    const auto viaPolicy = handleProduceBatch<Log, NoWriteHook>(b2.data(), b2.size(), 1, 0, 11, 0, 1, log_, psm_, false, 0, 1, 1, false, none);
    ASSERT_TRUE(viaPolicy.isOK());
    EXPECT_EQ(viaPolicy.baseOffset, 1);
}

TEST(CheckedWritesProducePathHookRegression, DuplicateNeverCallsTheHookRegression) {
    TmpDir dir;
    Log log(dir.path.string());
    ProducerStateManager psm;
    auto b = makeBatch(11, 0);
    RecordingHook first, second;
    auto call = [&](RecordingHook& h) {
        return handleProduceBatch<Log, RecordingHook>(b.data(), b.size(), 1, 0, 11, 0, 0, log, psm, false, 0, 1, 1, false, h);
    };
    ASSERT_TRUE(call(first).isOK());
    const auto dup = call(second);
    EXPECT_TRUE(dup.isDuplicate);
    EXPECT_EQ(second.n, 0);
    EXPECT_EQ(log.logEndOffset(), 1);
}

TEST(CheckedWritesProducePathHookRegression, OutOfOrderSequenceIsAnsweredBeforeTheHookRegression) {
    TmpDir dir;
    Log log(dir.path.string());
    ProducerStateManager psm;
    auto first = makeBatch(12, 0);
    RecordingHook h0;
    const auto firstOut = handleProduceBatch<Log, RecordingHook>(first.data(), first.size(), 1, 0, 12, 0, 0, log, psm, false, 0, 1, 1, false, h0);
    ASSERT_TRUE(firstOut.isOK());
    auto gap = makeBatch(12, 5);
    RecordingHook h;
    const auto out = handleProduceBatch<Log, RecordingHook>(gap.data(), gap.size(), 1, 0, 12, 0, 5, log, psm, false, 0, 1, 1, false, h);
    EXPECT_EQ(out.error, pc::ErrorCode::OutOfOrderSequence);
    EXPECT_EQ(h.n, 0);

    // A new producer that does not start at 0 is UnknownProducerId (D6), also before the hook.
    auto unknown = makeBatch(13, 5);
    RecordingHook hu;
    const auto unknownOut = handleProduceBatch<Log, RecordingHook>(unknown.data(), unknown.size(), 1, 0, 13, 0, 5, log, psm, false, 0, 1, 1, false, hu);
    EXPECT_EQ(unknownOut.error, pc::ErrorCode::UnknownProducerId);
    EXPECT_EQ(hu.n, 0);
}

}  // namespace rune::storage
