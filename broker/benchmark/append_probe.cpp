// Instrumentation harness: bucket appendNoSync latency by "was this a roll append".
// Drives the background segment pre-provisioner + deferred-rename finalizer on a
// separate thread (as the runtime does: LogCleanerAgent calls
// Log::preprovisionPending() + Log::finalizeStagingRenames() each cleaner cycle),
// and drains completed renames on the append thread (Log::drainStagingRenamesDone).
// So the roll-append cost reflects the ZERO-SYSCALL adopt path (in-memory rebase),
// not a synchronous create.
//
// Build (arm64, from source, RELEASE):
//   c++ -std=c++23 -O2 -DNDEBUG -Ilibs/storage/include -I../protocol/include \
//     benchmark/append_probe.cpp libs/storage/src/Log.cpp libs/storage/src/LogSegment.cpp \
//     libs/storage/src/OffsetIndex.cpp libs/storage/src/MappedFile.cpp -o /tmp/append_probe
// Args: rollBytes nAppends bg syncEvery
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

using namespace rune;
namespace fs = std::filesystem;

static std::vector<uint8_t> makeBatch(int64_t off) {
    protocol::RecordBatchBuilder b(off, 1'700'000'000'000LL + off);
    b.append("key01234", "v", 1'700'000'000'000LL + off);
    return b.build();
}

static double pct(std::vector<int64_t>& v, double p) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    size_t idx = (size_t)(p * (v.size() - 1));
    return v[idx] / 1000.0; // ns -> us
}

int main(int argc, char** argv) {
    int64_t rollBytes = 4 * 1024;
    int nAppends = 5000;
    bool bg = true;
    int syncEvery = 0; // 0 = never sync() (all dirty at seal); >0 = group-commit cadence
    if (argc > 1) rollBytes = atoll(argv[1]);
    if (argc > 2) nAppends = atoi(argv[2]);
    if (argc > 3) bg = atoi(argv[3]) != 0;
    if (argc > 4) syncEvery = atoi(argv[4]);

    auto dir = fs::temp_directory_path() / ("append_probe_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    storage::Log log(dir.string(), rollBytes);

    std::atomic<bool> stop{false};
    std::thread bgThread;
    if (bg) {
        bgThread = std::thread([&] {
            while (!stop.load(std::memory_order_acquire)) {
                int w = log.preprovisionPending();
                w += log.finalizeStagingRenames();
                if (w == 0) std::this_thread::yield();
            }
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(5)); // let first staging seg build
    }

    // warmup
    int64_t off = 0;
    for (int w = 0; w < 200; ++w) { auto b = makeBatch(off); log.append(b, off); ++off; }

    std::vector<std::vector<uint8_t>> batches(nAppends);
    std::vector<int64_t> offs(nAppends);
    off = log.logEndOffset();
    for (int i = 0; i < nAppends; ++i) { offs[i] = off; batches[i] = makeBatch(off); ++off; }

    std::vector<int64_t> rollLat, normalLat, allLat;
    auto segCount = [&]{ return log.segmentBaseOffsets().size(); };
    size_t prevSegs = segCount();

    for (int i = 0; i < nAppends; ++i) {
        auto t0 = std::chrono::steady_clock::now();
        log.appendNoSync(batches[i].data(), batches[i].size(), offs[i]);
        auto t1 = std::chrono::steady_clock::now();
        int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        allLat.push_back(ns);
        size_t nowSegs = segCount();
        if (nowSegs != prevSegs) { rollLat.push_back(ns); prevSegs = nowSegs; }
        else normalLat.push_back(ns);
        // Append thread applies completed staging renames (off the timed region).
        log.drainStagingRenamesDone();
        if (syncEvery > 0 && (i % syncEvery) == (syncEvery - 1)) log.sync();
    }

    stop.store(true, std::memory_order_release);
    if (bgThread.joinable()) bgThread.join();

    printf("rollBytes=%lld appends=%d bg=%d syncEvery=%d rolls=%zu (1 per %.1f appends)\n",
           (long long)rollBytes, nAppends, (int)bg, syncEvery, rollLat.size(),
           rollLat.empty() ? 0.0 : (double)nAppends / rollLat.size());
    printf("ALL     p50=%.1f p90=%.1f p99=%.1f p999=%.1f max=%.1f (us)\n",
           pct(allLat,.5), pct(allLat,.9), pct(allLat,.99), pct(allLat,.999),
           allLat.empty()?0:*std::max_element(allLat.begin(),allLat.end())/1000.0);
    printf("NORMAL  p50=%.1f p99=%.1f p999=%.1f max=%.1f (us)  n=%zu\n",
           pct(normalLat,.5), pct(normalLat,.99), pct(normalLat,.999),
           normalLat.empty()?0:*std::max_element(normalLat.begin(),normalLat.end())/1000.0, normalLat.size());
    printf("ROLL    p50=%.1f p99=%.1f p999=%.1f max=%.1f (us)  n=%zu\n",
           pct(rollLat,.5), pct(rollLat,.99), pct(rollLat,.999),
           rollLat.empty()?0:*std::max_element(rollLat.begin(),rollLat.end())/1000.0, rollLat.size());

    fs::remove_all(dir);
    return 0;
}
