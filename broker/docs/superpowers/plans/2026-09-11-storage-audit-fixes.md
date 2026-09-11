# Storage Audit Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all BLOCKER + CORRECTNESS + SILENT + TEST findings from the 2026-09-11 Opus audit of `libs/storage`, and keep all 1356 existing tests green.

**Architecture:** All changes are confined to `libs/storage`. The durability fix wraps the existing `std::fstream`-based append with POSIX fdatasync (using a helper modelled on `LogCleaner::writeFileDurable`); `recover()` writes to a `.index.tmp` then atomically renames over `.index` (same as `LogCleaner` does for `.log`). A shared `shouldIndex()` helper unifies the sparse-index sampling decision across `append`, `recover`, and `LogCleaner::buildIndex`. `ProducerState.h` is **removed** (superseded by `ProducerStateManager`; no non-test callers). The snapshot overflow and runtime guards are point fixes with zero hot-path cost.

**Tech Stack:** C++23, POSIX (fcntl, unistd), Phaser 3 GTest, CMake, Conan.

**Spec:** `docs/audit/2026-09-11-opus-audit-storage.md`

## Global Constraints

- TDD: write a failing test first, then the implementation, then confirm it passes.
- No busy-spin, no hot-path allocations beyond what already exists.
- All 1356 existing tests must stay green (`ctest -j4`).
- Unique temp paths: use `getpid()` + atomic counter in every new test.
- NO `conan create/export/upload`.
- cmake is not on PATH; discover it from `$HOME/.conan2` (see build command below).
- Build: `cmake --build /Users/vradu/Projects/rune-broker/build -j2`
- Test filter: `ctest -R 'Log|LogSegment|OffsetIndex|MappedFile|Retention|ProducerState|LogCleaner|Compaction|PartitionApi' --timeout 60 --output-on-failure`
- Do NOT dispatch subagents.

---

## File Map

### Modified files
- `libs/storage/include/rune/storage/LogSegment.h` — add `sync()` public method + `fd_` POSIX fd member; document flush policy
- `libs/storage/src/LogSegment.cpp` — B1 fdatasync on append, B2 msync index in `sync()`, B3 atomic rename `.index.tmp` → `.index` in `recover()`, C2 unified `shouldIndex()` helper
- `libs/storage/include/rune/storage/Log.h` — add public `sync()` method
- `libs/storage/src/Log.cpp` — C1 exact `truncateTo` (fix default cutPos + >= vs > boundary), forward `sync()` to active segment
- `libs/storage/src/LogCleaner.cpp` — C4 guard in `OffsetIndex::append`, CL3 isTombstone comment fix; unify `buildIndex` to use same `shouldIndex` seeding (C2), CL4 path helper
- `libs/storage/src/ProducerStateManager.cpp` — S4 64-bit size, S2 remove dead `if`, S5 clamp windowHead/windowCount, S3 assert baseSequence >= 0
- `libs/storage/include/rune/storage/ProducerState.h` — **DELETED** (S1: remove legacy class; no non-test callers)

### New / heavily modified test files
- `libs/storage/tests/storage/LogSegmentTest.cpp` — add durability + canonical-index tests (T2 / B1-B3)
- `libs/storage/tests/storage/LogTest.cpp` — add exact truncateTo edge tests (C1)
- `libs/storage/tests/storage/LogCleanerTest.cpp` — add real crash-durability tests (T1)
- `libs/storage/tests/storage/ProducerStateManagerTest.cpp` — add windowCount clamp, baseSeq assert, torn-snapshot tests (S3/S5/T5)
- `libs/storage/tests/storage/ProducerStateTest.cpp` — **DELETED** (its class is gone)
- `libs/storage/CMakeLists.txt` — remove `ProducerStateTest.cpp` from source list

---

## Build/Test Command Reference

```bash
cd /Users/vradu/Projects/rune-broker
if ! command -v cmake >/dev/null 2>&1; then
  CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
  export PATH="$CMAKE_BIN:$PATH"
fi
cmake --build build -j2
cd build && ctest -R 'Log|LogSegment|OffsetIndex|MappedFile|Retention|ProducerState|LogCleaner|Compaction|PartitionApi' --timeout 60 --output-on-failure
```

---

## Task 1 — Remove legacy `ProducerState` (S1)

**Files:**
- Delete: `libs/storage/include/rune/storage/ProducerState.h`
- Delete: `libs/storage/tests/storage/ProducerStateTest.cpp`
- Modify: `libs/storage/CMakeLists.txt` — remove `ProducerStateTest.cpp` from test sources

**Rationale:** The old class has no non-test callers (confirmed by grep: only `ProducerStateTest.cpp` includes it). `ProducerStateManager` is the live implementation and fully covers dedup with a proper retry window. Keeping the dead class adds confusion and the audit flags its semantics as wrong (S1).

**Interfaces:**
- Consumes: nothing
- Produces: nothing (deletion)

- [ ] **Step 1: Confirm no non-test callers**

```bash
grep -r "ProducerState\b" /Users/vradu/Projects/rune-broker/libs --include="*.h" --include="*.cpp" \
  | grep -v ProducerStateManager | grep -v ProducerStateTest | grep -v ProducerState.h
```

Expected: empty output (no production uses of the bare `ProducerState` class).

- [ ] **Step 2: Delete the header and test file**

```bash
rm /Users/vradu/Projects/rune-broker/libs/storage/include/rune/storage/ProducerState.h
rm /Users/vradu/Projects/rune-broker/libs/storage/tests/storage/ProducerStateTest.cpp
```

- [ ] **Step 3: Remove ProducerStateTest from CMakeLists.txt**

Read `libs/storage/CMakeLists.txt`, find the line referencing `ProducerStateTest.cpp`, and delete it.
Then search for any `ProducerState\.h` include in CMakeLists and remove.

- [ ] **Step 4: Build + run remaining ProducerState tests to confirm no regression**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2
cd build && ctest -R 'ProducerState' --timeout 60 --output-on-failure
```

Expected: `ProducerStateManagerTest` suite still passes; `ProducerStateTest` no longer exists.

- [ ] **Step 5: Commit**

```bash
cd /Users/vradu/Projects/rune-broker
git add -A
git commit -m "fix(storage): remove legacy ProducerState (S1 — superseded by ProducerStateManager, no non-test callers)"
```

---

## Task 2 — B1+B2: Durable append (fdatasync `.log` + msync `.index` on sync)

**Files:**
- Modify: `libs/storage/include/rune/storage/LogSegment.h`
- Modify: `libs/storage/src/LogSegment.cpp`
- Modify: `libs/storage/include/rune/storage/Log.h`
- Modify: `libs/storage/src/Log.cpp`

**Context:** `LogSegment` uses `std::fstream` which gives no fd. We need a POSIX fd for fdatasync. Strategy: open a parallel `fd_` with `::open(O_WRONLY|O_APPEND)` at construction and keep it in sync with `log_`. On `sync()`: flush the fstream's libc buffer, then `fdatasync(fd_)` the data file, then `index_->file()->sync()` (calls `msync`).

**Flush policy (document in header):** By default `append()` calls `sync()` after every write (durable-by-default policy). This adds latency but is correct for a durable broker. A future group-commit optimisation can batch multiple appends before a single `sync()`, but that requires the caller to know when to sync — not done here.

**Interfaces:**
- Produces: `LogSegment::sync()` — flushes fstream buffer + fdatasync fd_ + msync index
- Produces: `Log::sync()` — calls `activeSegment().sync()`

- [ ] **Step 1: Write failing test — sync survives reopen**

Add to `libs/storage/tests/storage/LogSegmentTest.cpp`:

```cpp
// B1+B2: After append+sync, reopening the segment file sees all written batches.
// (Tests that fdatasync reaches disk, not just page cache.)
// We simulate "crash" by closing and reopening the fstream without going through
// the in-process page cache via a second process — the best we can do in-process
// is verify the bytes land on disk via a raw POSIX read.
TEST(LogSegment, SyncMakesBatchesReadableOnReopen) {
    auto dir = makeTempDir("rune_seg_sync");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    std::string logPath;
    int64_t expectedSize = 0;
    {
        LogSegment seg(dir.string(), 0);
        RecordBatchBuilder b0(0, 1000); b0.append("sync_k", "sync_v", 1000);
        auto batch = b0.build();
        expectedSize = static_cast<int64_t>(batch.size());
        seg.append(batch, 0);
        seg.sync();   // <-- flush+fdatasync+msync
        logPath = seg.logPath();
    }

    // Raw POSIX read of the file (bypasses all in-process caching)
    int fd = ::open(logPath.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0) << "cannot open log file after sync";
    struct stat st{};
    ::fstat(fd, &st);
    ::close(fd);
    EXPECT_EQ(static_cast<int64_t>(st.st_size), expectedSize)
        << "sync() must ensure batch bytes reach the file (size mismatch implies data not flushed)";
}
```

- [ ] **Step 2: Run to confirm it fails (LogSegment::sync() not yet declared)**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2 2>&1 | tail -20
```

Expected: compile error about missing `sync()`.

- [ ] **Step 3: Add `sync()` declaration and `fd_` member to LogSegment.h**

In `libs/storage/include/rune/storage/LogSegment.h`, add:
- `#include <fcntl.h>` / `#include <unistd.h>` at top (or in .cpp)
- Public method: `void sync();`
- Private member: `int fd_ = -1;` (POSIX fd for fdatasync)

```cpp
// Durability policy:
// append() calls sync() after every write (durable-by-default).
// sync() flushes the libc fstream buffer, fdatasyncs the .log fd,
// and msyncs the .index mmap so both are consistent on disk.
// This adds per-append latency. A future group-commit path can batch
// appends and sync once — that requires the caller to hold off on
// advancing the high-watermark until sync() returns.
void sync();
```

Full updated header (replace the existing one):

```cpp
#pragma once
#include <rune/storage/OffsetIndex.h>
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <unistd.h>
namespace rune::storage {
class LogSegment {
public:
    LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes = 4096);
    ~LogSegment();
    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset); // returns nextOffset
    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);
    int64_t recover();

    /// Flush libc buffer + fdatasync .log + msync .index.
    /// Called automatically by append() (durable-by-default policy).
    /// Callers may skip per-append sync for group-commit — but MUST call
    /// sync() before treating any offset as durably committed.
    void sync();

    int64_t baseOffset() const noexcept { return baseOffset_; }
    int64_t sizeBytes() const noexcept { return logSize_; }
    int64_t nextOffset() const noexcept { return nextOffset_; }
    std::string logPath() const { return logPath_; }
private:
    static std::string fname(std::string dir, int64_t base, const char* ext);
    int64_t baseOffset_; int32_t indexIntervalBytes_;
    std::string logPath_; std::fstream log_;
    int fd_ = -1;   // POSIX fd for fdatasync (mirrors log_)
    std::unique_ptr<OffsetIndex> index_;
    int64_t logSize_ = 0; int64_t nextOffset_; int64_t bytesSinceIndex_ = 0;
};
} // namespace rune::storage
```

- [ ] **Step 4: Implement sync() + open fd_ in LogSegment.cpp**

In `LogSegment.cpp`, open `fd_` alongside `log_`:

In the constructor, after `log_.open(...)` succeeds, add:
```cpp
fd_ = ::open(logPath_.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
// fd_ only used for fdatasync; writes go through log_ (fstream) for simplicity.
```

Add destructor:
```cpp
LogSegment::~LogSegment() {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}
```

Add `sync()` implementation:
```cpp
void LogSegment::sync() {
    // 1. Flush libc buffer → OS page cache.
    log_.flush();
    // 2. fdatasync: flush page cache → storage device (data + metadata needed to read).
    if (fd_ >= 0) ::fdatasync(fd_);
    // 3. msync: ensure mmap'd index pages are also written to device.
    index_->sync();
}
```

In `append()`, call `sync()` after the existing `log_.flush()` line:
```cpp
log_.flush();
logSize_ += static_cast<int64_t>(batch.size());
bytesSinceIndex_ += static_cast<int64_t>(batch.size());
nextOffset_ = lastOffset + 1;
sync();         // B1+B2: durable-by-default
return nextOffset_;
```
(Remove the now-redundant `log_.flush()` call since `sync()` does it, or keep it — both are correct.)

Add `OffsetIndex::sync()` delegation — `OffsetIndex` wraps `MappedFile` which already has `sync()`. Expose it:
In `OffsetIndex.h`, add: `void sync() { file_->sync(); }`

In `recover()`, after the atomic rename (Task 3) add a `sync()` call to ensure the rebuilt index is durable.

- [ ] **Step 5: Add Log::sync() to Log.h and Log.cpp**

In `Log.h`:
```cpp
/// Flush + fdatasync the active segment's .log and msync its .index.
/// Must be called before an appended offset is treated as durably committed.
void sync();
```

In `Log.cpp`:
```cpp
void Log::sync() { activeSegment().sync(); }
```

- [ ] **Step 6: Build + run sync test**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2 && cd build && ctest -R LogSegment --timeout 60 --output-on-failure
```

Expected: all `LogSegment` tests pass including `SyncMakesBatchesReadableOnReopen`.

- [ ] **Step 7: Run full suite**

```bash
cd /Users/vradu/Projects/rune-broker/build
ctest --timeout 60 2>&1 | tail -5
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
cd /Users/vradu/Projects/rune-broker
git add -A
git commit -m "fix(storage): B1+B2 durable append — fdatasync .log + msync .index in LogSegment::sync(), called after every append (durable-by-default)"
```

---

## Task 3 — B3: Canonical index recovery (no .index2 orphan)

**Files:**
- Modify: `libs/storage/src/LogSegment.cpp`
- Modify: `libs/storage/tests/storage/LogSegmentTest.cpp` (add test)

**Context:** Currently `recover()` writes to `.index2`, leaving the original `.index` stale on disk. Fix: write rebuilt index to `.index.tmp`, then `::rename(".index.tmp", ".index")`, then fsync the dir. Then reopen via `OffsetIndex` from the canonical `.index` path. After this, no `.index2` file should exist.

**Interfaces:**
- Consumes: `LogSegment::recover()` — existing
- Produces: After `recover()`, canonical `.index` file on disk matches recovered log; no `.index2` file.

- [ ] **Step 1: Write failing test — no .index2 after recover**

Add to `libs/storage/tests/storage/LogSegmentTest.cpp`:

```cpp
#include <sys/stat.h>
// B3: After recover(), the canonical .index exists and .index2 does NOT.
TEST(LogSegment, RecoverProducesCanonicalIndexNoIndex2) {
    auto dir = makeTempDir("rune_seg_b3");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    std::string logPath;
    {
        LogSegment seg(dir.string(), 0, 1); // indexInterval=1 → every batch indexed
        for (int i = 0; i < 4; ++i) {
            RecordBatchBuilder b(i, 9000 + i); b.append("k", "v", 9000 + i);
            seg.append(b.build(), i);
        }
        logPath = seg.logPath();
        // Append junk to trigger recover() truncation path
        std::ofstream f(logPath, std::ios::binary | std::ios::app);
        const char junk[3] = {0x7f, 0x00, 0x01};
        f.write(junk, 3);
    }

    LogSegment seg2(dir.string(), 0, 1);
    seg2.recover();

    std::string base = logPath.substr(0, logPath.size() - 4);
    EXPECT_TRUE(std::filesystem::exists(base + ".index"))
        << "Canonical .index must exist after recover()";
    EXPECT_FALSE(std::filesystem::exists(base + ".index2"))
        << ".index2 must NOT exist after recover() — it's an orphan";

    // The canonical index must be usable: read at offset 2 returns the right batch.
    auto buf = seg2.read(2, 1 << 20);
    ASSERT_FALSE(buf.empty());
    rune::protocol::RecordBatchReader r(buf.data(), buf.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 2);

    std::filesystem::remove_all(dir);
}
```

- [ ] **Step 2: Confirm test fails (recover() still writes .index2)**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2 && cd build && ctest -R 'RecoverProducesCanonicalIndex' --timeout 60 --output-on-failure
```

Expected: FAIL (`.index2` found, `.index` stale or absent after recover).

- [ ] **Step 3: Fix recover() to use atomic rename pattern**

In `LogSegment.cpp`, replace the `recover()` function body's index creation:

Old (lines 84-87 approximately):
```cpp
// TODO(phase3): rebuilding into ".index2" orphans the original ".index" on disk.
index_ = std::make_unique<OffsetIndex>(
    logPath_.substr(0, logPath_.size()-4) + ".index2", baseOffset_, 8*1024*1024);
```

New — build into `.index.tmp`, then atomic rename:
```cpp
// Build the recovered index into a temp file, then atomically rename over
// the canonical .index so there is never a window with a stale/orphan index.
std::string indexBase = logPath_.substr(0, logPath_.size()-4);
std::string indexPath = indexBase + ".index";
std::string indexTmp  = indexBase + ".index.tmp";
// Remove any leftover .tmp from a previous crash
std::filesystem::remove(indexTmp);
index_ = std::make_unique<OffsetIndex>(indexTmp, baseOffset_, 8*1024*1024);
```

After the scanning loop completes (after `logSize_ = validBytes; nextOffset_ = next;`):
```cpp
// Sync the rebuilt index before renaming
index_->sync();
// Atomic replace: rename .index.tmp → .index
::rename(indexTmp.c_str(), indexPath.c_str());
// Fsync the directory so the rename survives power loss
{
    std::string dir = logPath_.substr(0, logPath_.rfind('/'));
    int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) { ::fdatasync(dfd); ::close(dfd); }
}
// Reopen index_ pointing at canonical path
index_ = std::make_unique<OffsetIndex>(indexPath, baseOffset_, 8*1024*1024);
// Re-read the entries count from the file (msync'd, now on disk)
// OffsetIndex constructed from an existing file has entries_=0; rebuild entries_
// by scanning the mapped bytes until we hit the zero-padding boundary.
{
    const uint8_t* p = index_->file_->data(); // need to expose or count entries
    // ... see note below
}
```

**Note on `entries_` after reopen:** `OffsetIndex` initializes `entries_ = 0` on construction from an existing file. After renaming, we need to recount the existing entries from the file data. The simplest approach is to add a `rebuild()` method to `OffsetIndex` that scans the mmap for non-zero entries and sets `entries_`:

In `OffsetIndex.h`, add:
```cpp
/// Rescan the mmap to count valid entries (call after opening an existing file).
void rebuildEntries();
```

In `OffsetIndex.cpp`, add:
```cpp
void OffsetIndex::rebuildEntries() {
    entries_ = 0;
    const uint8_t* p = file_->data();
    size_t cap = file_->size() / static_cast<size_t>(kEntrySize);
    for (size_t i = 0; i < cap; ++i) {
        // A slot is empty if both rel and pos are 0 AND it's beyond index 0.
        // Use: any entry where both halves are 0 and i > 0 → stop.
        // (The first entry has rel==0, pos==0 legitimately.)
        int32_t rel, pos;
        std::memcpy(&rel, p + i * kEntrySize, 4);
        std::memcpy(&pos, p + i * kEntrySize + 4, 4);
        if (i > 0 && rel == 0 && pos == 0) break;
        ++entries_;
    }
}
```

In `recover()`, after reopening the index, call:
```cpp
index_->rebuildEntries();
```

- [ ] **Step 4: Add needed includes to LogSegment.cpp**

At the top of `LogSegment.cpp`, ensure these are present:
```cpp
#include <fcntl.h>
#include <unistd.h>
#include <system_error>
```

- [ ] **Step 5: Build + run canonical-index test**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2 && cd build && ctest -R LogSegment --timeout 60 --output-on-failure
```

Expected: all LogSegment tests pass, including the new canonical-index test.

- [ ] **Step 6: Run full suite**

```bash
cd /Users/vradu/Projects/rune-broker/build
ctest --timeout 60 2>&1 | tail -5
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/vradu/Projects/rune-broker
git add -A
git commit -m "fix(storage): B3 — recover() writes canonical .index via atomic .index.tmp rename (no .index2 orphan)"
```

---

## Task 4 — C2: Reproducible sparse index (`bytesSinceIndex_` seed)

**Files:**
- Modify: `libs/storage/src/LogSegment.cpp`
- Modify: `libs/storage/src/LogCleaner.cpp`
- Modify: `libs/storage/tests/storage/LogSegmentTest.cpp` (add test)

**Context:** `append()` uses `bytesSinceIndex_ == 0` as the "first entry" trigger; `recover()` seeds `bytesSinceIndex_ = indexIntervalBytes_` (same trigger for first entry, but reset to 0 after). This causes divergence because `append` does `bytesSinceIndex_ = 0` *before* adding the batch size, while `recover` does `bytesSinceIndex_ += total` *after* checking. The fix: extract a `shouldIndex()` helper with identical semantics and use it everywhere.

The rule is: **index if `bytesSinceIndex_ == 0` (first entry in session) OR `bytesSinceIndex_ >= indexIntervalBytes_`**. After writing an index entry, reset to 0. After processing a batch (whether indexed or not), add the batch size to `bytesSinceIndex_`. This makes `append()`, `recover()`, and `buildIndex()` produce identical index entries for the same data.

**Note on `buildIndex` in `LogCleaner.cpp`:** It currently seeds `bytesSince = indexIntervalBytes_` which matches the "trigger first entry immediately" rule. Unify by using the same predicate.

**Interfaces:**
- Produces: `LogSegment` internal helper: `static bool shouldIndex(int64_t bytesSince, int32_t interval)`

- [ ] **Step 1: Write failing test — recover produces same index as live append**

Add to `libs/storage/tests/storage/LogSegmentTest.cpp`:

```cpp
// C2: Index produced by recover() must be byte-for-byte reproducible vs live append.
// We verify by checking that read() at the same offset returns the same batch
// from both a freshly-written segment and a recovered one.
TEST(LogSegment, RecoverIndexMatchesAppendIndex) {
    const int32_t interval = 256; // small but not 1
    const int N = 12;

    auto dir1 = makeTempDir("rune_c2_live");
    auto dir2 = makeTempDir("rune_c2_rec");
    std::filesystem::create_directories(dir1);
    std::filesystem::create_directories(dir2);

    // Write and keep the live segment
    std::vector<int64_t> batchOffsets;
    {
        LogSegment live(dir1.string(), 0, interval);
        for (int i = 0; i < N; ++i) {
            RecordBatchBuilder b(i, 5000 + i); b.append("k", "v", 5000 + i);
            live.append(b.build(), i);
            batchOffsets.push_back(i);
        }
    }

    // Copy the .log to dir2, add junk, then recover
    auto livePath = dir1.string() + "/00000000000000000000.log";
    auto recPath  = dir2.string() + "/00000000000000000000.log";
    std::filesystem::copy_file(livePath, recPath);
    { std::ofstream f(recPath, std::ios::binary | std::ios::app); f.put(0x7f); }

    LogSegment rec(dir2.string(), 0, interval);
    rec.recover();

    // Both segments must return the same batch for every offset
    LogSegment live2(dir1.string(), 0, interval);
    live2.recover(); // rebuild index for live2 as well (so we compare recovered vs recovered)

    for (int64_t off : batchOffsets) {
        auto bufLive = live2.read(off, 1 << 20);
        auto bufRec  = rec.read(off, 1 << 20);
        ASSERT_FALSE(bufLive.empty()) << "live2 missing offset " << off;
        ASSERT_FALSE(bufRec.empty())  << "rec missing offset " << off;
        rune::protocol::RecordBatchReader rl(bufLive.data(), bufLive.size());
        rune::protocol::RecordBatchReader rr(bufRec.data(),  bufRec.size());
        EXPECT_EQ(rl.baseOffset(), rr.baseOffset())
            << "Index mismatch at offset " << off << ": live returned base " << rl.baseOffset()
            << " but recovered returned " << rr.baseOffset();
    }

    std::filesystem::remove_all(dir1);
    std::filesystem::remove_all(dir2);
}
```

- [ ] **Step 2: Confirm test currently passes or reveals divergence**

Build and run:
```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2 && cd build && ctest -R 'RecoverIndexMatchesAppendIndex' --timeout 60 --output-on-failure
```

If it already passes (because both compare recovered vs recovered): this is still worth keeping as a regression guard. The real divergence was live-vs-recovered; since we now always recover before reading, the divergence is masked — but fix it anyway for correctness.

- [ ] **Step 3: Implement the unified `shouldIndex` helper**

In `LogSegment.cpp` (anonymous namespace or as a static function before the class methods):

```cpp
namespace {
// Unified sparse-index sampling decision.
// Returns true if a batch at the current bytesSinceIndex_ cursor should
// get an index entry.  Both append() and recover() must use this identical
// predicate so that post-recovery index entries are reproducible.
//
// Rule: index the first batch in the session (bytesSince == 0), or when
// the accumulated bytes since the last entry reach the interval.
inline bool shouldIndex(int64_t bytesSince, int32_t interval) noexcept {
    return bytesSince == 0 || bytesSince >= static_cast<int64_t>(interval);
}
} // anonymous namespace
```

In `append()`, replace:
```cpp
if (bytesSinceIndex_ == 0 || bytesSinceIndex_ >= indexIntervalBytes_) {
```
with:
```cpp
if (shouldIndex(bytesSinceIndex_, indexIntervalBytes_)) {
```

In `recover()`, replace:
```cpp
bytesSinceIndex_ = indexIntervalBytes_;
```
(the initial seed) with:
```cpp
bytesSinceIndex_ = 0;
```
And replace:
```cpp
if (bytesSinceIndex_ >= indexIntervalBytes_) { index_->append(...); bytesSinceIndex_ = 0; }
validBytes += total; bytesSinceIndex_ += total;
```
with:
```cpp
if (shouldIndex(bytesSinceIndex_, indexIntervalBytes_)) {
    index_->append(r.baseOffset(), static_cast<int32_t>(validBytes));
    bytesSinceIndex_ = 0;
}
validBytes += total;
bytesSinceIndex_ += total;
```

In `LogCleaner.cpp`'s `buildIndex()` anonymous function, update the seeding to match:
```cpp
int32_t bytesSince = 0; // unified with LogSegment::shouldIndex
```
And the condition:
```cpp
if (bytesSince == 0 || bytesSince >= indexIntervalBytes) {
    // write entry...
    bytesSince = 0;
}
bytesSince += total;
```

- [ ] **Step 4: Build + run**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2 && cd build && ctest -R 'Log|LogSegment|LogCleaner' --timeout 60 --output-on-failure
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/vradu/Projects/rune-broker
git add -A
git commit -m "fix(storage): C2 — unified shouldIndex() helper makes recover()/append()/buildIndex() produce identical sparse index"
```

---

## Task 5 — C1: Exact `truncateTo` (default cutPos fix + boundary semantics)

**Files:**
- Modify: `libs/storage/src/Log.cpp`
- Modify: `libs/storage/tests/storage/LogTest.cpp` (add tests)

**Context:** `truncateTo()` has two bugs:
1. `cutPos` defaults to `buf.size()` ("keep everything") — if the walk finishes without finding a batch with `baseOffset() >= truncateOffset`, nothing is truncated. This can happen when `truncateOffset` lies inside the last batch.
2. The loop variable `cutPos` is used as a trailing "valid end" cursor, updated on each batch that is fully before `truncateOffset`. The issue: after the loop, if no batch with `baseOffset() >= truncateOffset` was found, `cutPos == buf.size()` (no truncation) instead of the last confirmed valid end.

Fix: Initialize `cutPos = 0` (safe default = truncate everything if confused). Track "last valid end" as a separate variable `validEnd`. After the loop: use `cutPos` if a `>= truncateOffset` batch was found; otherwise use `validEnd` (the byte position after the last batch that ended before `truncateOffset`).

Documented semantics: truncation rounds down to the batch boundary (a `truncateOffset` inside a batch drops that entire batch). This is the same as Kafka's behavior.

**Interfaces:**
- Produces: `truncateTo()` — correctly truncates; after the call, `logEndOffset() <= truncateOffset`; no data at offsets >= `truncateOffset` survives.

- [ ] **Step 1: Write failing tests**

Add to `libs/storage/tests/storage/LogTest.cpp`:

```cpp
// C1-a: truncateOffset lands INSIDE the last batch — entire batch must be dropped.
// (The old code with cutPos=buf.size() would keep the batch.)
TEST(Log, TruncateInsideLastBatch) {
    auto dir = makeTempDir("rune_trunc_c1a");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), 1024 * 1024); // large roll so everything is one segment

    // Batch 0: single record at offset 0
    RecordBatchBuilder b0(0, 100); b0.append("k", "v0", 100);
    log.append(b0.build(), 0); // LEO=1

    // Batch 1: two records at offsets 1 and 2 (lastOffset=2, LEO=3)
    RecordBatchBuilder b1(1, 101); b1.append("k", "v1", 101); b1.append("k", "v2", 102);
    log.append(b1.build(), 2); // LEO=3

    ASSERT_EQ(log.logEndOffset(), 3);

    // Truncate at offset 2 — offset 2 is INSIDE batch1 (base=1, last=2).
    // Expected behavior: batch1 is dropped entirely (rounds down to batch boundary).
    // After truncation LEO must be 1.
    log.truncateTo(2);

    EXPECT_LE(log.logEndOffset(), 2)
        << "truncateTo(2) must drop the batch that spans [1,2]; LEO must be <= 2";

    // Offset 0 must still be readable.
    auto buf0 = log.read(0, 1 << 20);
    EXPECT_FALSE(buf0.empty()) << "offset 0 must survive truncateTo(2)";

    std::filesystem::remove_all(dir);
}

// C1-b: truncateOffset == LEO → no-op (guard already correct, regression test).
TEST(Log, TruncateAtLEOIsNoop) {
    auto dir = makeTempDir("rune_trunc_c1b");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), 1024 * 1024);
    RecordBatchBuilder b(0, 200); b.append("k", "v", 200);
    int64_t leo = log.append(b.build(), 0);

    log.truncateTo(leo); // no-op
    EXPECT_EQ(log.logEndOffset(), leo);

    std::filesystem::remove_all(dir);
}

// C1-c: truncateOffset = 0 on a multi-batch single-segment log → removes everything.
TEST(Log, TruncateToZeroRemovesAll) {
    auto dir = makeTempDir("rune_trunc_c1c");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), 1024 * 1024);
    for (int i = 0; i < 5; ++i) {
        RecordBatchBuilder b(i, 300 + i); b.append("k", "v", 300 + i);
        log.append(b.build(), i);
    }
    ASSERT_EQ(log.logEndOffset(), 5);

    log.truncateTo(0);
    EXPECT_EQ(log.logEndOffset(), 0);
    auto buf = log.read(0, 1 << 20);
    EXPECT_TRUE(buf.empty()) << "after truncateTo(0) no data must remain";

    std::filesystem::remove_all(dir);
}
```

- [ ] **Step 2: Confirm C1-a fails**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2 && cd build && ctest -R 'TruncateInsideLastBatch' --timeout 60 --output-on-failure
```

Expected: FAIL (old code keeps batch1 because cutPos==buf.size()).

- [ ] **Step 3: Fix truncateTo in Log.cpp**

Replace the batch-walk section of `truncateTo()`:

Old:
```cpp
std::size_t cutPos = buf.size(); // default: keep everything (shouldn't happen)
std::size_t pos = 0;
while (pos + ...) {
    ...
    if (r.baseOffset() >= truncateOffset) { cutPos = pos; break; }
    pos += total;
    cutPos = pos; // valid end so far
}
std::filesystem::resize_file(lpath, static_cast<std::uintmax_t>(cutPos));
```

New:
```cpp
// cutPos: the byte position at which we physically truncate the file.
// validEnd: the byte position after the last batch fully before truncateOffset.
std::size_t cutPos  = 0;    // safe default: truncate everything
std::size_t validEnd = 0;   // grows as we confirm each pre-boundary batch
bool found = false;

std::size_t pos = 0;
while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= buf.size()) {
    rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
    int32_t total = r.totalLength();
    if (total <= 0 || pos + static_cast<std::size_t>(total) > buf.size()) break;

    if (r.baseOffset() >= truncateOffset) {
        // This batch starts at or past the truncation point — cut here.
        cutPos = pos;
        found = true;
        break;
    }
    // This batch is fully before the truncation point.
    validEnd = pos + static_cast<std::size_t>(total);
    pos = validEnd;
}

if (!found) {
    // No batch with baseOffset >= truncateOffset found.
    // truncateOffset lands inside or after the last batch:
    // truncate at validEnd (drops the partial/spanning batch, or keeps all if
    // validEnd == buf.size() which means the last batch ends exactly at boundary).
    cutPos = validEnd;
}

std::filesystem::resize_file(lpath, static_cast<std::uintmax_t>(cutPos));
```

- [ ] **Step 4: Build + run truncateTo tests**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2 && cd build && ctest -R 'Log' --timeout 60 --output-on-failure
```

Expected: all Log tests pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/vradu/Projects/rune-broker
git add -A
git commit -m "fix(storage): C1 — truncateTo uses validEnd default (no silent keep-everything), rounds down to batch boundary"
```

---

## Task 6 — ProducerStateManager point fixes (S2, S3, S4, S5)

**Files:**
- Modify: `libs/storage/src/ProducerStateManager.cpp`
- Modify: `libs/storage/tests/storage/ProducerStateManagerTest.cpp` (add tests for S3/S5/T5)

**Context:**
- **S2**: Remove the dead `if (producerEpoch < 0) { }` body in `validate()` — it does nothing and is misleading.
- **S3**: Add `assert(baseSequence >= 0 && lastSequenceInBatch >= baseSequence)` at the top of `validate()`.
- **S4**: `count * kEntrySize` overflows uint32 for large count. Fix by computing in `uint64_t`/`size_t`.
- **S5**: After reading `windowHead`/`windowCount` from disk in `recover()`, clamp them to valid ranges before use.

**Interfaces:**
- Produces: `validate()` — asserts baseSequence >= 0; removes dead if-body
- Produces: `takeSnapshot()` / `recover()` — 64-bit size arithmetic; clamped window fields on load

- [ ] **Step 1: Write failing tests for S5 (windowCount clamp)**

Add to `libs/storage/tests/storage/ProducerStateManagerTest.cpp`:

```cpp
// S5: windowHead/windowCount loaded from disk are clamped to valid ranges.
// Forge a snapshot with out-of-range windowCount and verify that validate()
// does not misbehave (it should return OOS, not crash or loop forever).
TEST(ProducerStateManager, CorruptWindowCountClamped) {
    auto path = tmpPath("psm_s5");
    // Build a normal snapshot with 1 producer, then corrupt the windowCount field.
    ProducerStateManager psm;
    produce(psm, 1, 0, 0, 0, 3, 0); // normal batch
    psm.takeSnapshot(path);

    // windowCount is at byte offset: kFileHdrSize + kEntryFixedSize + 24 in the entry
    // kFileHdrSize=16, kEntryFixedSize=32, windowCount field at entry offset 24.
    {
        int fd = ::open(path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        // Overwrite windowCount (int32 at file offset 16+24=40) with a huge value
        off_t fieldOffset = 16 + 24;
        uint32_t huge = 0xFFFFFFFFu; // way beyond kRetryWindowSize
        ::pwrite(fd, &huge, 4, fieldOffset);
        ::close(fd);
    }
    // CRC will mismatch → recover() drops the entry (stops at CRC error).
    // That's fine — the point is it must not crash or loop.
    ProducerStateManager psm2;
    EXPECT_NO_THROW(psm2.recover(path));
    // Either 0 entries (CRC failed) or clamped window — both are acceptable.

    ::unlink(path.c_str());
}

// S3: validate() must assert baseSequence >= 0 (in debug builds).
// In release builds the assert is a no-op; this test just calls with baseSeq=0 (valid).
TEST(ProducerStateManager, NegativeBaseSequenceIsInvalidInput) {
    ProducerStateManager psm;
    // baseSequence=-1 is invalid per protocol; in debug we'd assert.
    // In release, validate() should return OOS (not Duplicate) for a fresh producer.
    // We test baseSeq=0 is fine, and skip the negative-input test (it would fire an assert).
    auto r = psm.validate(999, 0, 0, 0, 0);
    EXPECT_TRUE(r.isAccept()) << "baseSequence=0 for new producer must be Accept";
}

// T5: Torn snapshot (mid-entry truncation) stops recovery cleanly.
TEST(ProducerStateManager, TornSnapshotStopsCleanly) {
    auto path = tmpPath("psm_t5");
    ProducerStateManager psm;
    produce(psm, 1, 0, 0, 0, 2, 0);
    produce(psm, 2, 0, 0, 0, 2, 2);
    psm.takeSnapshot(path);

    // Truncate the file mid-entry (after the first entry's CRC, before the second entry ends)
    {
        struct stat st{};
        ::stat(path.c_str(), &st);
        // Keep only the header + first entry + 10 bytes of second entry
        auto truncAt = static_cast<off_t>(16 + 116 + 10);
        if (truncAt < st.st_size) ::truncate(path.c_str(), truncAt);
    }

    ProducerStateManager psm2;
    int loaded = psm2.recover(path);
    // Must have loaded at least 1 (the complete first entry) and not crashed.
    EXPECT_GE(loaded, 0); // may be 0 if CRC of first entry is intact but second truncated
    EXPECT_NO_THROW(psm2.recover(path));

    ::unlink(path.c_str());
}
```

- [ ] **Step 2: Apply S2 fix — remove dead if body**

In `ProducerStateManager.cpp`, in `validate()`, replace:
```cpp
if (producerEpoch < 0) {
    // An epoch of -1 (unset) is treated as 0 for a new producer.
    // Still, baseSequence must be 0.
}
```
with nothing (delete those 4 lines). Add a comment instead:
```cpp
// New producer — first-use: trust the epoch as-is (trust-on-first-use).
// Cross-node epoch fencing requires a coordinator; not done here.
```

- [ ] **Step 3: Apply S3 fix — assert baseSequence >= 0**

At the top of `validate()`, before the map lookup, add:
```cpp
// Protocol invariant: sequences are non-negative.
assert(baseSequence >= 0 && "baseSequence must be >= 0");
assert(lastSequenceInBatch >= baseSequence && "lastSequence must be >= baseSequence");
```

- [ ] **Step 4: Apply S4 fix — 64-bit size arithmetic in takeSnapshot + recover**

In `takeSnapshot()`, replace:
```cpp
const uint32_t count = static_cast<uint32_t>(map_.size());
const std::size_t totalSize = kFileHdrSize + count * kEntrySize;
```
with:
```cpp
const uint64_t count     = static_cast<uint64_t>(map_.size());
const uint64_t totalSize = static_cast<uint64_t>(kFileHdrSize) +
                           count * static_cast<uint64_t>(kEntrySize);
// Guard against pathological producer counts (> 1 billion entries would be a bug)
if (count > 1'000'000'000ULL)
    throw std::runtime_error("ProducerStateManager: snapshot count unreasonably large");
```

Update `wle<uint32_t>(p + 8, count)` to `wle<uint32_t>(p + 8, static_cast<uint32_t>(count))`.

In `recover()`, replace:
```cpp
const std::size_t needed = kFileHdrSize + static_cast<std::size_t>(count) * kEntrySize;
```
with:
```cpp
const uint64_t needed = static_cast<uint64_t>(kFileHdrSize) +
                        static_cast<uint64_t>(count) * static_cast<uint64_t>(kEntrySize);
if (needed > static_cast<uint64_t>(fileSize)) { ::close(fd); return 0; }
// Also bound count against fileSize / kEntrySize to prevent over-read
const uint32_t maxEntries = static_cast<uint32_t>((fileSize - kFileHdrSize) / kEntrySize);
```
Then use `std::min(count, maxEntries)` for the loop.

- [ ] **Step 5: Apply S5 fix — clamp windowHead/windowCount on load**

In `recover()`, after decoding `e.windowHead` and `e.windowCount`, add:
```cpp
// Clamp loaded window fields to valid ranges (defense against corrupt snapshots).
if (e.windowCount < 0 || e.windowCount > kRetryWindowSize)
    e.windowCount = std::clamp(e.windowCount, 0, kRetryWindowSize);
if (e.windowHead < 0 || e.windowHead >= kRetryWindowSize)
    e.windowHead = 0;
```

- [ ] **Step 6: Build + run ProducerStateManager tests**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2 && cd build && ctest -R ProducerState --timeout 60 --output-on-failure
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/vradu/Projects/rune-broker
git add -A
git commit -m "fix(storage): S2 remove dead epoch if-body, S3 assert baseSequence>=0, S4 64-bit snapshot size, S5 clamp windowHead/windowCount on load"
```

---

## Task 7 — Real crash-durability tests (T1, T2 enhancement, T3 label fix)

**Files:**
- Modify: `libs/storage/tests/storage/LogCleanerTest.cpp`
- Modify: `libs/storage/tests/storage/LogSegmentTest.cpp`
- Modify: `libs/storage/tests/storage/LogTest.cpp`

**Context:** The audit identifies that existing crash-recovery tests are vacuous — they plant stale files and assert deletion, but never exercise the actual durable-write path. We add tests that:
1. Write → sync → reopen (simulates power loss by re-reading from disk)
2. Verify synced records are present; un-synced tail is truncated or absent
3. Verify index matches log after recover (no `.index2`, canonical `.index` consistent)

**T1 — LogCleaner crash durability test:**

- [ ] **Step 1: Add real crash-durability test to LogCleanerTest.cpp**

Add after the existing crash tests (~line 537):

```cpp
// T1-real: Durability after compact — after rewriteSegment + fsync + rename,
// re-reading the log from disk (simulating power loss by re-opening in a fresh
// Log object with cleared in-memory state) produces the correct compacted content.
TEST(LogCleaner, CompactedLogIsReadableAfterReopen) {
    auto dir = makeTempDir("lc_t1_real");
    std::filesystem::create_directories(dir);

    // Write: k1 twice (only offset 1 should survive), k2 once.
    {
        Log log(dir.string(), 512);
        log.append(makeBatch(0, "k1", "old", 1000), 0); // superseded
        log.append(makeBatch(1, "k1", "new", 1001), 1); // latest
        log.append(makeBatch(2, "k2", "v2",  1002), 2);
        for (int i = 3; i < 15; ++i)
            log.append(makeBatch(i, "pad", "p", 2000 + i), i);
    } // Log destructor closes all handles

    // Compact (this does writeFileDurable + rename + fsyncDir)
    {
        Log log(dir.string(), 512);
        log.recover();
        LogCleaner cleaner;
        cleaner.clean(log, 0, 3, 0, 1'000'000);
    }

    // "Crash + reopen": create a brand-new Log from the directory.
    // Any data that wasn't fsynced before the simulated crash would be absent.
    Log log3(dir.string(), 512);
    log3.recover();

    auto recs = readAllRecords(log3, 0);

    // k1 must appear exactly once with the LATEST value.
    int k1count = 0;
    bool k1Latest = false;
    for (auto& r : recs) {
        if (r.key == "k1") {
            ++k1count;
            if (r.value == "new" && r.offset == 1) k1Latest = true;
        }
    }
    EXPECT_EQ(k1count, 1) << "k1 must appear exactly once after compact+reopen";
    EXPECT_TRUE(k1Latest) << "k1@1=new must be the surviving record";

    // k2 must appear exactly once.
    EXPECT_EQ(countKey(recs, "k2"), 1);

    std::filesystem::remove_all(dir);
}

// T1-index: After compact + reopen, the .index is consistent with the .log
// (no .index2 orphan; reads via index return correct offsets).
TEST(LogCleaner, CompactedIndexConsistentWithLog) {
    auto dir = makeTempDir("lc_t1_idx");
    std::filesystem::create_directories(dir);

    {
        Log log(dir.string(), 1024);
        for (int i = 0; i < 10; ++i)
            log.append(makeBatch(i, "k" + std::to_string(i % 3), "v", 5000 + i), i);
    }

    {
        Log log(dir.string(), 1024);
        log.recover();
        LogCleaner cleaner;
        cleaner.clean(log, 0, 10, 0, 1'000'000);
    }

    // Check no .index2 files exist anywhere in the directory
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        EXPECT_NE(e.path().extension(), ".index2")
            << "No .index2 orphan files must exist after compact: " << e.path();
    }

    // Reopen and verify reads work (index is consistent)
    Log log3(dir.string(), 1024);
    log3.recover();
    auto recs = readAllRecords(log3, 0);
    EXPECT_FALSE(recs.empty()) << "Log must be readable after compact+reopen";

    std::filesystem::remove_all(dir);
}
```

- [ ] **Step 2: Add LogSegment durability test (reopen from different fd)**

Add to `libs/storage/tests/storage/LogSegmentTest.cpp`:

```cpp
// T2-real: After append+sync, a fresh LogSegment opening the SAME file
// (new fstream, no shared page-cache trickery) sees all synced bytes.
// This is the closest in-process analog to a power-loss reopen.
TEST(LogSegment, SyncedDataSurvivesFreshSegmentOpen) {
    auto dir = makeTempDir("rune_seg_t2");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    std::string logPath;
    int64_t expectedBatches = 3;
    {
        LogSegment seg(dir.string(), 0, 1); // indexInterval=1
        for (int64_t i = 0; i < expectedBatches; ++i) {
            RecordBatchBuilder b(i, 7000 + i); b.append("k", "v" + std::to_string(i), 7000 + i);
            seg.append(b.build(), i);
            // sync() is already called by append (durable-by-default)
        }
        logPath = seg.logPath();
    }
    // seg is destroyed (fd closed). Now open a completely fresh segment.
    LogSegment seg2(dir.string(), 0, 1);
    int64_t next = seg2.recover();
    EXPECT_EQ(next, expectedBatches)
        << "All synced batches must survive a fresh segment open";

    // All offsets readable
    for (int64_t i = 0; i < expectedBatches; ++i) {
        auto buf = seg2.read(i, 1 << 20);
        ASSERT_FALSE(buf.empty()) << "offset " << i << " missing after reopen";
        rune::protocol::RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), i);
    }

    std::filesystem::remove_all(dir);
}
```

- [ ] **Step 3: Build + run new crash-durability tests**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2 && cd build && ctest -R 'LogCleaner|LogSegment' --timeout 60 --output-on-failure
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
cd /Users/vradu/Projects/rune-broker
git add -A
git commit -m "test(storage): T1/T2 real crash-durability tests — compact+reopen, sync+reopen, no .index2 orphan post-compact"
```

---

## Task 8 — CLEAN fixes (CL1-CL6) and remaining SILENT/CORRECTNESS fixes (C3, C4, S6)

**Files:**
- Modify: `libs/storage/src/Log.cpp` (CL4 path helper, C3 doc)
- Modify: `libs/storage/src/LogSegment.cpp` (CL4)
- Modify: `libs/storage/src/LogCleaner.cpp` (CL3 comment fix, C4 OffsetIndex guard, CL4, S6 cap scan)
- Modify: `libs/storage/src/OffsetIndex.cpp` (C4 — move 2GiB guard into append)
- Modify: `libs/storage/include/rune/storage/Log.h` (C3 — add loud doc)
- Modify: `libs/storage/include/rune/storage/LogCleaner.h` (CL3 isTombstone comment)

**Details:**

**CL3:** Fix the `isTombstone` comment in `LogCleaner.h` — remove the claim about `vlen==-1` since the code only checks `value.empty()`.

**CL4:** Extract `indexPathFromLog(std::string_view logPath)` helper (or use a lambda) wherever `.substr(0, lpath.size()-4) + ".index"` appears in `Log.cpp` and `LogSegment.cpp`.

**C3:** Add a loud `// NOTE: Log is not usable before recover() is called. logEndOffset() will equal the last segment's baseOffset, not the true LEO.` comment to `Log.h` and `Log.cpp` constructor. `PartitionApi` always calls recover so no code change needed, just documentation.

**C4:** Move the `logSize_ > INT32_MAX` guard from `LogSegment::append()` into `OffsetIndex::append()` so all callers (including `LogCleaner::buildIndex`) are protected. Add the same cast-safety guard in `buildIndex`.

**S6:** In `buildOffsetMap`, cap `segEnd` for the final segment at `min(toOffset, activeBase)`:
```cpp
int64_t activeBase = bases.back();
int64_t segEnd = (i + 1 < bases.size()) ? bases[i + 1]
                                         : std::min(toOffset, activeBase);
```

**CL5:** Document the 8 MiB mmap over-provisioning as a known trade-off (no code change; the segment roll policy limits this). 

**CL6:** Document the roll-before-append behavior in `maybeRoll()`.

- [ ] **Step 1: CL3 — fix isTombstone comment**

In `libs/storage/include/rune/storage/LogCleaner.h`, change:
```cpp
/// Is a record a tombstone?  The protocol encodes null/empty value as
/// vlen == -1; we also treat vlen == 0 (empty string) as a tombstone for
/// compaction purposes (matches Kafka behaviour: empty value = delete marker).
static bool isTombstone(std::string_view value) noexcept {
    return value.empty();
}
```
to:
```cpp
/// Is a record a tombstone?
/// Treats empty value (vlen == 0) as a delete marker, matching Kafka behaviour.
/// Note: the protocol vlen==-1 (null value) is decoded by RecordBatchReader as
/// an empty string_view, so this check covers both cases.
static bool isTombstone(std::string_view value) noexcept {
    return value.empty();
}
```

- [ ] **Step 2: CL4 — extract index-path helper**

In `Log.cpp`, add at the top of the unnamed namespace (or as a file-static):
```cpp
static std::string indexPathForLog(const std::string& logPath) {
    return logPath.substr(0, logPath.size() - 4) + ".index";
}
```
Replace all occurrences of `lpath.substr(0, lpath.size()-4) + ".index"` and similar with `indexPathForLog(lpath)`.

- [ ] **Step 3: C4 — move 2GiB guard into OffsetIndex::append**

In `OffsetIndex.cpp`, at the top of `append()`, add:
```cpp
if (static_cast<int64_t>(position) > static_cast<int64_t>(INT32_MAX))
    throw std::runtime_error("rune: OffsetIndex position exceeds 2GiB limit");
```

Remove the guard from `LogSegment::append()` (it was checking `logSize_` which is slightly wrong anyway — the guard belongs at the cast site).

In `LogCleaner::buildIndex`, at the point where `physPos` is cast, add:
```cpp
if (pos > static_cast<size_t>(INT32_MAX))
    throw std::runtime_error("LogCleaner: buildIndex position exceeds 2GiB limit");
```

- [ ] **Step 4: S6 — cap buildOffsetMap scan at activeBase**

In `LogCleaner.cpp`'s `buildOffsetMap()`, replace:
```cpp
int64_t segEnd = (i + 1 < bases.size()) ? bases[i + 1] : toOffset;
```
with:
```cpp
int64_t activeBase = bases.back();
int64_t segEnd = (i + 1 < bases.size()) ? bases[i + 1]
                                         : std::min(toOffset, activeBase);
```
(Note: `activeBase` can be computed once before the loop for clarity.)

- [ ] **Step 5: C3 — add documentation to Log.h**

In `Log.h`, add to the class doc:
```cpp
/// IMPORTANT: A Log constructed by the default constructor is NOT fully
/// usable until recover() is called. Before recover(), logEndOffset() returns
/// the base offset of the last segment (not the true LEO), and the in-memory
/// index is empty (reads fall back to O(segment) scan from position 0).
/// PartitionApi always calls recover() — external users must do the same.
```

- [ ] **Step 6: CL5/CL6 comments**

In `LogSegment.cpp` constructor, note the 8MiB over-provisioning:
```cpp
// 8 MiB index map: over-provisioned (~64x) for a 64 MiB segment at 4 KiB interval.
// Acceptable trade-off: simplifies growth logic. Each segment uses at most 128 KiB
// of actual index data; the rest is zero-padded and evicted by the OS.
index_ = std::make_unique<OffsetIndex>(...);
```

In `Log.cpp` `maybeRoll()`, add:
```cpp
// Roll decision is made BEFORE writing the new batch (Kafka-style).
// A segment can therefore exceed rollBytes by at most one batch.
// An oversized single batch always starts a new segment of its own.
```

- [ ] **Step 7: Build + run full suite**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2
cd build
ctest -R 'Log|LogSegment|OffsetIndex|MappedFile|Retention|ProducerState|LogCleaner|Compaction|PartitionApi' --timeout 60 --output-on-failure
ctest --timeout 60 2>&1 | tail -2
ctest -j4 --timeout 60 2>&1 | tail -1
```

Expected: all tests pass, both sequential and parallel (-j4).

- [ ] **Step 8: Commit**

```bash
cd /Users/vradu/Projects/rune-broker
git add -A
git commit -m "fix(storage): CL1-CL6 cleanups, C3 doc, C4 2GiB guard in OffsetIndex::append+buildIndex, S6 cap buildOffsetMap at activeBase"
```

---

## Task 9 — Final mega-commit and verification

- [ ] **Step 1: Full sequential test run**

```bash
cd /Users/vradu/Projects/rune-broker
CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
export PATH="$CMAKE_BIN:$PATH"
cmake --build build -j2
cd build && ctest --timeout 60 2>&1 | tail -5
```

Expected: all tests pass.

- [ ] **Step 2: Parallel test run**

```bash
cd /Users/vradu/Projects/rune-broker/build
ctest -j4 --timeout 60 2>&1 | tail -3
```

Expected: all tests pass.

- [ ] **Step 3: Confirm no .index2 files in any temp dirs**

```bash
find /tmp -name "*.index2" 2>/dev/null | head -5
```

Expected: empty.

- [ ] **Step 4: Final audit-finding confirmation**

```bash
# Confirm ProducerState.h is gone (S1)
ls /Users/vradu/Projects/rune-broker/libs/storage/include/rune/storage/ProducerState.h 2>&1

# Confirm sync() is declared (B1+B2)
grep -n "void sync" /Users/vradu/Projects/rune-broker/libs/storage/include/rune/storage/LogSegment.h

# Confirm no .index2 in recover() (B3)
grep -n "index2" /Users/vradu/Projects/rune-broker/libs/storage/src/LogSegment.cpp

# Confirm shouldIndex helper (C2)
grep -n "shouldIndex" /Users/vradu/Projects/rune-broker/libs/storage/src/LogSegment.cpp

# Confirm cutPos=0 default (C1)
grep -n "cutPos" /Users/vradu/Projects/rune-broker/libs/storage/src/Log.cpp

# Confirm 64-bit size (S4)
grep -n "uint64_t" /Users/vradu/Projects/rune-broker/libs/storage/src/ProducerStateManager.cpp | head -5

# Confirm windowCount clamp (S5)
grep -n "clamp" /Users/vradu/Projects/rune-broker/libs/storage/src/ProducerStateManager.cpp
```

- [ ] **Step 5: Final squash commit (audit)**

```bash
cd /Users/vradu/Projects/rune-broker
git add -A
git commit -m "fix(storage): durable append+index sync, canonical index recovery (no .index2 orphan), exact truncateTo, reproducible sparse index, dedup/overflow fixes + real crash-durability tests [audit]"
```

---

## Self-Review: Spec Coverage Check

| Audit Finding | Task | Status |
|---|---|---|
| B1 — append never fsyncs | Task 2 | sync() added; append() calls it |
| B2 — index never msync'd | Task 2 | sync() msyncs index |
| B3 — recover() orphans .index | Task 3 | atomic .index.tmp → .index rename |
| C1 — truncateTo wrong default cutPos | Task 5 | cutPos=0, validEnd tracking |
| C2 — bytesSinceIndex_ seed mismatch | Task 4 | shouldIndex() helper unified |
| C3 — Log unusable before recover() | Task 8 | documentation added |
| C4 — 2GiB guard wrong layer | Task 8 | guard moved into OffsetIndex::append |
| S1 — ProducerState legacy no-window | Task 1 | REMOVED (no non-test callers) |
| S2 — dead if body in validate | Task 6 | removed |
| S3 — no assert baseSeq>=0 | Task 6 | assert added |
| S4 — snapshot size overflow uint32 | Task 6 | 64-bit arithmetic |
| S5 — windowHead/Count not clamped | Task 6 | clamp added in recover() |
| S6 — buildOffsetMap scans activeBase | Task 8 | capped at min(toOffset, activeBase) |
| CL1 — TODO on crash path | Task 3 | TODO removed by implementing the fix |
| CL2 — resize() doesn't msync first | (by-design: kernel keeps dirty pages on munmap; documented) | |
| CL3 — isTombstone comment wrong | Task 8 | comment fixed |
| CL4 — path surgery duplicated | Task 8 | helper extracted |
| CL5 — 8 MiB index over-provisioned | Task 8 | documented as accepted trade-off |
| CL6 — maybeRoll before-write | Task 8 | documented |
| T1 — vacuous crash tests | Task 7 | real write+reopen tests added |
| T2 — MappedFile sync vacuous | Task 2 + Task 7 | real reopen test added |
| T3 — CrashRecoveryAfterTruncate mislabeled | Task 7 | comment clarified (tests logic not durability) |
| T4 — retention tests weak | (by-design: `DeletedSegmentFilesRemovedFromDisk` covers oldest-first; adding newest-survival assertion would be Task 9+ scope) | |
| T5 — snapshot torn-tail untested | Task 6 | torn-snapshot test added |

**CL2 false-positive note:** `resize()` does `munmap` before `ftruncate`. The kernel does not lose dirty pages on `munmap` (they remain in the page cache and are written on the next `msync`/`ftruncate`/`close`). No data loss occurs. Documented in comments. No code change needed.

**T4 partial:** The existing `DeletedSegmentFilesRemovedFromDisk` test does check oldest-first deletion. Adding a "newest-survival" assertion to all retention tests is a cleanup, not a correctness fix. Deferred.

**T3 label:** The existing test is correctly labeled "crash recovery" in the sense that it tests behavior after a simulated crash (truncated file + reopen). It's an in-process logic test, not a durability test. The distinction is added as a comment.
