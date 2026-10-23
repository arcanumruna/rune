#pragma once
// TEST-ONLY: UDP port allocation that is collision-proof across PARALLEL test processes (2026-10-15).
//
// History.  bind-0/read-back/close helpers returned EPHEMERAL ports the kernel re-hands to the next process
// (D8, 2026-10-03).  The D8 fix walked 20000..31999 from a pid-scattered start and checked each port bindable —
// still a TOCTOU between processes (two binaries could both see a port free before either bound it), which is
// why every real-BrokerNode suite ran under ctest `RESOURCE_LOCK rune_udp_ports` (one at a time).
//
// Now.  10000..19999 (below the Linux 32768+ and macOS 49152+ ephemeral ranges, so the kernel never hands
// them out implicitly; and disjoint from 20000..31999, which the D8 allocator of older checkouts still walks
// — a test run of an older branch on the same host never races this one) is split into kBlocks blocks of
// kBlockPorts ports.  A process OWNS a block while it
// holds an exclusive flock(2) on kLockDir/block-<n> — the kernel releases it when the process exits (even on a
// crash / SIGKILL), so a block can never be owned by two live processes and no stale lock outlives its owner.
// A process hands out ports only from blocks it owns, in order: two concurrently running test processes can
// never be given the same port (and one process re-hands a port only after kMaxOwnedBlocks * kBlockPorts
// allocations, see below).  Each port is still checked bindable on 127.0.0.1 (a process
// outside this scheme may hold one) and skipped if not.
//
// No in-process state (no static / atomic / thread_local): the kernel holds the ownership (the flock, on a
// descriptor deliberately left open until exit), the block file holds the owner's identity and its cursor.
// "Is this block mine?" = the flock attempt fails AND the file names this process (pid + process start time,
// so a recycled pid of a dead owner never matches).  Allocation is single-threaded per process in every
// caller (test bodies / fixtures on the main thread); it is not meant to be called concurrently from threads.
//
// kLockDir is a fixed absolute path (NOT $TMPDIR): every test process on the host — any build dir, any
// sanitizer environment — must contend on the same lock files.
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <sys/sysctl.h>
#endif

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace rune::aeronutil::testing {

inline constexpr uint32_t kPortRangeBase = 10000;
inline constexpr uint32_t kPortRangeSpan = 10000;   // 10000..19999
inline constexpr uint32_t kBlockPorts    = 100;
inline constexpr uint32_t kBlocks        = kPortRangeSpan / kBlockPorts;   // 100
// #577: deliberately host-wide (not temp_directory_path()): every test process on the host must share these port locks.
inline constexpr const char* kLockDir    = "/tmp/rune-test-ports-10000x100";   // geometry in the name: another layout never shares it

inline bool udpPortBindable(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return false;
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(port);
    const bool ok = ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0;
    ::close(fd);
    return ok;
}

namespace detail {

/// What a block file stores: who owns it and how far its owner has handed out ports.
struct PortBlockRecord {
    uint64_t pid       = 0;
    uint64_t startTime = 0;   ///< process start time (µs since epoch) — distinguishes a recycled pid
    uint32_t cursor    = 0;   ///< next unhanded port offset within the block
    uint32_t magic     = 0;
};
inline constexpr uint32_t kRecordMagic = 0x52504f52u;   // "RPOR"

/// This process's identity, derived on every call from the kernel (no cached state).
inline PortBlockRecord selfIdentity() {
    PortBlockRecord r;
    r.pid = static_cast<uint64_t>(::getpid());
#if defined(__APPLE__)
    int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PID, ::getpid()};
    struct kinfo_proc kp{};
    size_t len = sizeof kp;
    if (::sysctl(mib, 4, &kp, &len, nullptr, 0) == 0)
        r.startTime = static_cast<uint64_t>(kp.kp_proc.p_starttime.tv_sec) * 1'000'000u +
                      static_cast<uint64_t>(kp.kp_proc.p_starttime.tv_usec);
#else
    // /proc/self/stat field 22 = starttime (clock ticks since boot).  The comm field (2) may contain spaces /
    // parentheses, so parse after the LAST ')'.
    if (FILE* f = std::fopen("/proc/self/stat", "r")) {
        char buf[1024];
        const size_t n = std::fread(buf, 1, sizeof buf - 1, f);
        std::fclose(f);
        buf[n] = '\0';
        if (const char* p = std::strrchr(buf, ')')) {
            unsigned long long v = 0;
            // after ") ": field 3 (state) .. field 22 (starttime) = 19 fields to skip, then read
            if (std::sscanf(p + 2, "%*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %*u %*u %*d %*d %*d %*d %*d %*d %llu",
                            &v) == 1)
                r.startTime = v;
        }
    }
#endif
    r.magic = kRecordMagic;
    return r;
}

inline bool readRecord(int fd, PortBlockRecord& out) {
    return ::pread(fd, &out, sizeof out, 0) == static_cast<ssize_t>(sizeof out) && out.magic == kRecordMagic;
}
inline bool writeRecord(int fd, const PortBlockRecord& r) {
    return ::pwrite(fd, &r, sizeof r, 0) == static_cast<ssize_t>(sizeof r);
}

/// Hand out `width` consecutive bindable ports from block `fd` (owned by this process), advancing its cursor.
/// Returns the first port, or 0 when the block has no `width` consecutive bindable ports left.
inline uint16_t takeFromOwnedBlock(int fd, uint32_t block, PortBlockRecord rec, uint32_t width) {
    while (rec.cursor + width <= kBlockPorts) {
        const uint32_t first = kPortRangeBase + block * kBlockPorts + rec.cursor;
        uint32_t bad = width;   // index of the first unbindable port in the run, or width when all bind
        for (uint32_t k = 0; k < width && bad == width; ++k)
            if (!udpPortBindable(static_cast<uint16_t>(first + k))) bad = k;
        rec.cursor += (bad == width) ? width : bad + 1;   // never hand a port out twice, skip foreign ones
        if (bad == width) {
            writeRecord(fd, rec);
            return static_cast<uint16_t>(first);
        }
    }
    rec.cursor = kBlockPorts;
    writeRecord(fd, rec);
    return 0;
}

} // namespace detail

namespace detail {

/// Walk the block files.  Mode Take: hand out from a block this process owns.  Mode Claim: claim a free block
/// and hand out from it.  Mode Rewind: reset the cursor of every block this process owns (returns 0).
/// `owned` counts the blocks this process owns (Take / Rewind).
enum class WalkMode { Take, Claim, Rewind };
inline uint16_t walkBlocks(WalkMode mode, const PortBlockRecord& self, uint32_t width, uint32_t& owned) {
    owned = 0;
    for (uint32_t b = 0; b < kBlocks; ++b) {
        char path[128];
        std::snprintf(path, sizeof path, "%s/block-%03u", kLockDir, b);
        const int fd = ::open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0666);
        if (fd < 0) continue;
        ::fchmod(fd, 0666);
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
            // Held by a live process — this one (through the descriptor it keeps open) or another.
            PortBlockRecord rec;
            const bool mine = mode != WalkMode::Claim && readRecord(fd, rec) && rec.pid == self.pid &&
                              rec.startTime == self.startTime;
            uint16_t p = 0;
            if (mine) {
                ++owned;
                if (mode == WalkMode::Take) p = takeFromOwnedBlock(fd, b, rec, width);
                else { rec.cursor = 0; writeRecord(fd, rec); }
            }
            ::close(fd);   // a second descriptor: closing it leaves the owning descriptor's flock in place
            if (p != 0) return p;
            continue;
        }
        if (mode != WalkMode::Claim) { ::flock(fd, LOCK_UN); ::close(fd); continue; }   // free; not claiming
        // Claimed.  The descriptor stays open (and the flock held) until this process exits.
        PortBlockRecord rec = self;
        rec.cursor = 0;
        writeRecord(fd, rec);
        if (const uint16_t p = takeFromOwnedBlock(fd, b, rec, width); p != 0) return p;
        // No usable run (processes outside the scheme hold its ports): it stays owned with its cursor at the
        // end (Take skips it), and the walk claims the next one.
    }
    return 0;
}

} // namespace detail

/// A process owns at most this many blocks (kMaxOwnedBlocks * kBlockPorts ports); past that it rewinds its own
/// blocks and hands their ports out again (each still checked bindable, so a port a live socket of this
/// process holds is skipped).  Only a process that allocates more than that many ports without binding them
/// could see a duplicate; a test needs a few dozen.  This keeps a long single-process run (a whole gtest
/// binary, a 300-round discovery regression) from exhausting the range other processes need.
inline constexpr uint32_t kMaxOwnedBlocks = 4;

/// `width` consecutive UDP ports (per-worker base ranges) owned by this process and bindable now; never
/// handed to any other concurrently running test process.  Aborts when no block can be had (never hands
/// Aeron port 0).
inline uint16_t allocFreeUdpPortRange(uint32_t width) {
    if (width == 0 || width > kBlockPorts) {
        std::fprintf(stderr, "allocFreeUdpPortRange: width %u outside 1..%u\n", width, kBlockPorts);
        std::abort();
    }
    ::mkdir(kLockDir, 0777);
    ::chmod(kLockDir, 0777);   // shared by every test process on the host; mkdir's mode is umask-masked
    const detail::PortBlockRecord self = detail::selfIdentity();
    uint32_t owned = 0;
    for (int round = 0; round < 2; ++round) {
        if (const uint16_t p = detail::walkBlocks(detail::WalkMode::Take, self, width, owned); p != 0) return p;
        if (owned < kMaxOwnedBlocks) {
            uint32_t unused = 0;
            if (const uint16_t p = detail::walkBlocks(detail::WalkMode::Claim, self, width, unused); p != 0) return p;
        }
        if (owned == 0) break;   // nothing to rewind: every block is held by other processes
        detail::walkBlocks(detail::WalkMode::Rewind, self, width, owned);
    }
    std::fprintf(stderr, "allocFreeUdpPortRange: no free port block in %u..%u (%s)\n", kPortRangeBase,
                 kPortRangeBase + kPortRangeSpan - 1, kLockDir);
    std::abort();
}

inline uint16_t allocFreeUdpPort() { return allocFreeUdpPortRange(1); }

} // namespace rune::aeronutil::testing
