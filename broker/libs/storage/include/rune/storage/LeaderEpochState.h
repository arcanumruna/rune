#pragma once
// LeaderEpochState.h — hot-path B F7: the leader-epoch durability file that the
// owning worker writes through its io backend (never a synchronous fsync on the
// worker).
//
// One pre-allocated file per disk log, `<log dir>/leader-epoch-state`, holding
// TWO slots (the recovery-point checkpoint's scheme, Log::kRecoveryPointFile):
// every update writes the slot NOT holding the newest durable image, with a
// higher sequence, then fdatasyncs — one io-backend request (write + datasync),
// no rename, no directory fsync (the file is created once, tmp + fsync + rename
// + directory fsync, off the worker).  Restore picks the valid slot with the
// highest sequence; a torn write fails its CRC and the other slot stands.
//
// A slot carries BOTH durable leader-epoch facts:
//   - the restart fence (the highest epoch this replica ever led at), and
//   - the leader-epoch cache (epoch → start offset).
//
// Slot layout (little-endian):
//   [0]  magic u32 "LEST" | [4] version u16 | [6] reserved u16 | [8] seq u64
//   [16] fence i64 | [24] count u32 | [28] crc32c u32 over [0,28) ++ entries
//   [32] count × { epoch i32, startOffset i64 }
// The slot stride (file size / 2, a multiple of 4096) bounds `count`: a cache
// that outgrows it re-creates the file larger (cold, counted).
#include <rune/protocol/Crc32c.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <algorithm>
#include <cstring>
#include <iterator>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace rune::storage {

struct LeaderEpochState {
    static constexpr const char*   kFileName    = "leader-epoch-state";
    static constexpr const char*   kTmpFileName = "leader-epoch-state.tmp";
    static constexpr uint32_t      kMagic       = 0x5453454Cu;   // "LEST"
    static constexpr uint16_t      kVersion     = 1;
    static constexpr std::size_t   kHeaderBytes = 32;
    static constexpr std::size_t   kEntryBytes  = 12;
    static constexpr std::size_t   kCrcOffset   = 28;
    /// Per slot.  D21 review L1: pre-sized so a history practically never
    /// outgrows it (outgrowing re-creates the file — fsync + rename + dir fsync
    /// — on whichever thread writes next, possibly a worker): 52 KiB stride,
    /// 4434 entries.  The file is SPARSE past each slot's reserved head
    /// (reserveFor: one block for a short history), so the pre-size costs
    /// address space, not blocks (review Md4), while an in-place write only
    /// ever lands on allocated blocks (D23).
    static constexpr std::size_t   kMinEntries  = 4096;

    using Entries = std::vector<std::pair<int32_t, int64_t>>;

    enum class FileState : uint8_t { Missing, Corrupt, Valid };
    struct Read {
        FileState state = FileState::Missing;
        uint64_t  seq   = 0;
        int       slot  = -1;
        int64_t   fence = -1;
        Entries   entries;
        std::size_t stride = 0;
    };

    [[nodiscard]] static std::string path(const std::string& dir) { return dir + "/" + kFileName; }

    /// Slot stride that holds `entries` entries (4096-aligned).
    [[nodiscard]] static constexpr std::size_t strideFor(std::size_t entries) noexcept {
        const std::size_t want = kHeaderBytes + entries * kEntryBytes;
        return (want + 4095u) & ~static_cast<std::size_t>(4095u);
    }
    [[nodiscard]] static constexpr std::size_t capacityOf(std::size_t stride) noexcept {
        return stride < kHeaderBytes ? 0 : (stride - kHeaderBytes) / kEntryBytes;
    }
    /// Bytes of an encoded slot holding `count` entries.
    [[nodiscard]] static constexpr std::size_t imageBytes(std::size_t count) noexcept {
        return kHeaderBytes + count * kEntryBytes;
    }
    /// D23 (D21 review L4): the head of each slot whose blocks are ALLOCATED
    /// (written with zeros at create / enable) — room for twice `entries` (at
    /// least one block), block-aligned, never past the slot.  An in-place slot
    /// write must fit it (else the cold re-create), so the worker's write +
    /// fdatasync never allocates a block (no ENOSPC on a full disk).
    [[nodiscard]] static constexpr std::size_t reserveFor(std::size_t entries, std::size_t stride) noexcept {
        const std::size_t want = (imageBytes(2 * entries + 2) + 4095u) & ~static_cast<std::size_t>(4095u);
        const std::size_t r = want < 4096u ? 4096u : want;
        return r < stride ? r : stride;
    }

    /// Encode a slot into `out` (at least imageBytes(n) bytes).  No allocation.
    template <class It>
    static std::size_t encode(uint8_t* out, uint64_t seq, int64_t fence, It first, It last) noexcept {
        return encodeWith(out, seq, fence, [&](auto&& emit) {
            for (It it = first; it != last; ++it) emit(it->first, it->second);
        });
    }
    /// Encode a slot whose entries `forEach(emit)` produces (emit(epoch, start)
    /// in ascending order).  No allocation.
    template <class F>
    static std::size_t encodeWith(uint8_t* out, uint64_t seq, int64_t fence, F&& forEach) noexcept {
        std::size_t n = 0;
        uint8_t* e = out + kHeaderBytes;
        forEach([&](int32_t ep, int64_t off) {
            putLe32_(e, static_cast<uint32_t>(ep));
            putLe64_(e + 4, static_cast<uint64_t>(off));
            e += kEntryBytes;
            ++n;
        });
        std::memset(out, 0, kHeaderBytes);
        putLe32_(out + 0, kMagic);
        out[4] = static_cast<uint8_t>(kVersion);
        out[5] = static_cast<uint8_t>(kVersion >> 8);
        putLe64_(out + 8, seq);
        putLe64_(out + 16, static_cast<uint64_t>(fence));
        putLe32_(out + 24, static_cast<uint32_t>(n));
        const uint32_t crc = crcOf_(out, n);
        putLe32_(out + kCrcOffset, crc);
        return imageBytes(n);
    }

    /// Decode one slot of `len` readable bytes.  False = invalid (magic,
    /// version, bounds or CRC).
    static bool decode(const uint8_t* in, std::size_t len, uint64_t& seq, int64_t& fence, Entries* entries) {
        if (len < kHeaderBytes) return false;
        if (getLe32_(in) != kMagic) return false;
        if (static_cast<uint16_t>(in[4] | (in[5] << 8)) != kVersion) return false;
        const uint32_t n = getLe32_(in + 24);
        if (imageBytes(n) > len) return false;
        if (getLe32_(in + kCrcOffset) != crcOf_(in, n)) return false;
        seq   = getLe64_(in + 8);
        fence = static_cast<int64_t>(getLe64_(in + 16));
        if (seq == 0) return false;
        if (entries != nullptr) {
            entries->clear();
            entries->reserve(n);
            const uint8_t* e = in + kHeaderBytes;
            for (uint32_t i = 0; i < n; ++i, e += kEntryBytes)
                entries->emplace_back(static_cast<int32_t>(getLe32_(e)), static_cast<int64_t>(getLe64_(e + 4)));
        }
        return true;
    }

    /// Read the newest valid slot of `<dir>/leader-epoch-state`.  Cold.
    static Read read(const std::string& dir) {
        Read out;
        const std::string p = path(dir);
        const int fd = ::open(p.c_str(), O_RDONLY);
        if (fd < 0) {
            if (errno != ENOENT) out.state = FileState::Corrupt;
            return out;
        }
        out.state = FileState::Corrupt;
        struct stat st{};
        if (::fstat(fd, &st) == 0 && st.st_size >= static_cast<off_t>(2 * 4096) && (st.st_size % 8192) == 0) {
            const std::size_t stride = static_cast<std::size_t>(st.st_size) / 2;
            out.stride = stride;
            std::vector<uint8_t> buf(stride);
            Entries tmp;
            for (int slot = 0; slot < 2; ++slot) {
                const ssize_t n = ::pread(fd, buf.data(), stride, static_cast<off_t>(slot) * static_cast<off_t>(stride));
                if (n != static_cast<ssize_t>(stride)) continue;
                uint64_t seq = 0; int64_t fence = -1;
                if (!decode(buf.data(), stride, seq, fence, &tmp)) continue;
                if (out.state != FileState::Valid || seq > out.seq) {
                    out.state = FileState::Valid; out.seq = seq; out.slot = slot; out.fence = fence;
                    out.entries.swap(tmp);
                }
            }
        }
        ::close(fd);
        return out;
    }

    /// Create (or re-create, larger) the file with slot 0 = the given image and
    /// slot 1 empty: tmp + write + fsync + rename + FULL directory fsync.  Returns
    /// the open fd of the renamed file (kept for every later slot write) or -errno.
    /// Cold — never on a worker duty pass in steady state.
    template <class It>
    static int create(const std::string& dir, std::size_t stride, uint64_t seq, int64_t fence, It first, It last) {
        const auto count = static_cast<std::size_t>(std::distance(first, last));
        // D23: slot 0's image + zeros, then slot 1's head: the reserved head
        // of both slots is written (allocated) here, off the worker.
        const std::size_t reserved = reserveFor(count, stride);
        std::vector<uint8_t> img(std::max(imageBytes(count), reserved), 0);
        (void)encode(img.data(), seq, fence, first, last);
        const std::size_t len = img.size();
        const std::string tmp = dir + "/" + kTmpFileName;
        const std::string dst = path(dir);
        int err = 0;
        const int fd = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) return -(errno != 0 ? errno : EIO);
        // Sparse past the reserved heads (D21 L1): the size first (slot 1 reads
        // as zeros = empty), then slot 0's head (image + zeros) and slot 1's
        // head (zeros) — D23: the blocks an in-place write can reach.
        if (::ftruncate(fd, static_cast<off_t>(2 * stride)) != 0) err = errno != 0 ? errno : EIO;
        if (err == 0 && ::pwrite(fd, img.data(), len, 0) != static_cast<ssize_t>(len)) err = errno != 0 ? errno : ENOSPC;
        if (err == 0) {
            std::fill(img.begin(), img.end(), uint8_t{0});
            if (::pwrite(fd, img.data(), reserved, static_cast<off_t>(stride)) != static_cast<ssize_t>(reserved))
                err = errno != 0 ? errno : ENOSPC;
        }
        if (err == 0 && ::fsync(fd) != 0) err = errno != 0 ? errno : EIO;
        if (err == 0 && ::rename(tmp.c_str(), dst.c_str()) != 0) err = errno != 0 ? errno : EIO;
        if (err == 0) {
            const int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
            if (dfd < 0 || ::fsync(dfd) != 0) err = errno != 0 ? errno : EIO;
            if (dfd >= 0) ::close(dfd);
        }
        if (err != 0) {
            ::close(fd);
            ::unlink(tmp.c_str());
            return -err;
        }
        return fd;
    }

    /// Review M1 ruling (no downgrade support): once leader-epoch-state holds a
    /// DURABLE image, the legacy `leader-epoch-checkpoint` / `leader-epoch-fence`
    /// (and their .tmp) are removed — unlink + one FULL directory fsync — so no
    /// binary can ever trust stale legacy data.  Returns the number removed, or
    /// -errno when the directory fsync failed.  Cold (never on a worker).
    static int removeLegacy(const std::string& dir) {
        static constexpr const char* kNames[] = {"leader-epoch-checkpoint", "leader-epoch-checkpoint.tmp",
                                                 "leader-epoch-fence", "leader-epoch-fence.tmp"};
        int removed = 0;
        for (const char* n : kNames)
            if (::unlink((dir + "/" + n).c_str()) == 0) ++removed;
        if (removed == 0) return 0;
        const int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
        int err = 0;
        if (dfd < 0 || ::fsync(dfd) != 0) err = errno != 0 ? errno : EIO;
        if (dfd >= 0) ::close(dfd);
        return err != 0 ? -err : removed;
    }

    /// D23: allocate the reserved head (`bytes`) of both slots of an EXISTING
    /// file (one created sparse by an earlier build): zeros over the slot NOT
    /// holding the newest image (`keepSlot`, -1: none) and over the newest
    /// slot past its image (`keepLen` bytes kept), then fdatasync.  The newest
    /// image is never touched, so a crash here loses nothing.  0 or errno.
    /// Cold (construction / enable — never a worker duty pass).
    static int reserve(int fd, std::size_t stride, std::size_t bytes, int keepSlot, std::size_t keepLen) {
        if (bytes > stride) bytes = stride;
        std::vector<uint8_t> zeros;
        bool wrote = false;
        for (int slot = 0; slot < 2; ++slot) {
            const off_t head = static_cast<off_t>(slot) * static_cast<off_t>(stride);
            // Already allocated (this build's create, or a written slot): no
            // write, no sync — a restart of N partitions costs N lseeks.
            const off_t hole = ::lseek(fd, head, SEEK_HOLE);
            if (hole >= head + static_cast<off_t>(bytes)) continue;
            const std::size_t from = slot == keepSlot ? std::min(keepLen, bytes) : 0;
            if (from >= bytes) continue;
            const std::size_t n = bytes - from;
            if (zeros.empty()) zeros.assign(bytes, 0);
            if (::pwrite(fd, zeros.data(), n, head + static_cast<off_t>(from)) != static_cast<ssize_t>(n))
                return errno != 0 ? errno : ENOSPC;
            wrote = true;
        }
        if (!wrote) return 0;
#if defined(__APPLE__)
        if (::fsync(fd) != 0) return errno != 0 ? errno : EIO;
#else
        if (::fdatasync(fd) != 0) return errno != 0 ? errno : EIO;
#endif
        return 0;
    }

    /// Open the existing file for slot writes.  -errno on failure.  Cold.
    static int openExisting(const std::string& dir) {
        const int fd = ::open(path(dir).c_str(), O_RDWR);
        return fd >= 0 ? fd : -(errno != 0 ? errno : EIO);
    }

private:
    static void putLe32_(uint8_t* p, uint32_t v) noexcept { for (int i = 0; i < 4; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i)); }
    static void putLe64_(uint8_t* p, uint64_t v) noexcept { for (int i = 0; i < 8; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i)); }
    static uint32_t getLe32_(const uint8_t* p) noexcept { uint32_t v = 0; for (int i = 3; i >= 0; --i) v = (v << 8) | p[i]; return v; }
    static uint64_t getLe64_(const uint8_t* p) noexcept { uint64_t v = 0; for (int i = 7; i >= 0; --i) v = (v << 8) | p[i]; return v; }
    /// CRC over the header up to the CRC field, then the entries.
    static uint32_t crcOf_(const uint8_t* slot, std::size_t n) noexcept {
        const uint32_t h = rune::protocol::crc32c(slot, kCrcOffset);
        if (n == 0) return h;
        return rune::protocol::crc32c(h, slot + kHeaderBytes, n * kEntryBytes);
    }
};

} // namespace rune::storage
