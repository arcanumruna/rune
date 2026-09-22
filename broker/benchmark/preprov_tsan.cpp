// TSan harness for the background segment pre-provisioning handoff.
//
// Exercises the exact concurrency of the runtime:
//   - APPEND thread: appendNoSync (which rolls → installNextSegment adopt +
//     enqueueStagingRename) + drainStagingRenamesDone (applies logPath_ updates)
//     + occasional read() / segmentBaseOffsets().  This is the ONLY mutator of
//     segments_ (single-writer invariant).
//   - BACKGROUND thread: preprovisionPending() (builds staging segments) +
//     finalizeStagingRenames() (deferred disk renames).  NEVER touches segments_.
//
// A clean run (0 races) proves the single-writer invariant holds and the
// release/acquire handoff (preparedSegment_, preprovInFlight_, rename rings) is
// correctly synchronized.
//
// Build (x86_64 TSan):
//   c++ -std=c++23 -O1 -g -fsanitize=thread -Ilibs/storage/include \
//     -I../rune-protocol/include benchmark/preprov_tsan.cpp \
//     build-tsan/libs/storage/librune_storage.a -o /tmp/preprov_tsan
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

using namespace rune;
namespace fs = std::filesystem;

static std::vector<uint8_t> makeBatch(int64_t off) {
    protocol::RecordBatchBuilder b(off, 1'700'000'000'000LL + off);
    b.append("key01234", "value-payload-to-add-some-bytes", 1'700'000'000'000LL + off);
    return b.build();
}

int main() {
    auto dir = fs::temp_directory_path() / ("preprov_tsan_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    // Small rollBytes → frequent rolls → heavy exercise of the adopt/handoff path.
    storage::Log log(dir.string(), 2 * 1024);

    std::atomic<bool> stop{false};

    // Background thread: pre-provision + finalize renames (never touches segments_).
    std::thread bg([&] {
        while (!stop.load(std::memory_order_acquire)) {
            int w = log.preprovisionPending();
            w += log.finalizeStagingRenames();
            if (w == 0) std::this_thread::yield();
        }
    });

    // Append thread (single writer of segments_): appends → rolls → drains.
    std::thread appender([&] {
        int64_t off = 0;
        for (int i = 0; i < 40000; ++i) {
            auto b = makeBatch(off);
            log.appendNoSync(b.data(), b.size(), off);
            ++off;
            log.drainStagingRenamesDone();
            if ((i & 63) == 0) log.sync();
            if ((i & 127) == 0) { (void)log.segmentBaseOffsets(); (void)log.logEndOffset(); }
            if ((i & 255) == 0 && off > 10) { (void)log.read(off - 5, 4096); }
        }
    });

    appender.join();
    stop.store(true, std::memory_order_release);
    bg.join();

    printf("preprov_tsan: completed LEO=%lld segments=%zu\n",
           (long long)log.logEndOffset(), log.segmentBaseOffsets().size());
    fs::remove_all(dir);
    return 0;
}
