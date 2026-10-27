#include <rune/storage/MappedFile.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdexcept>
#include <cerrno>
#include <cstring>
#include <string>
namespace rune::storage {

namespace {

// Back every byte of a writable map with real blocks before it is mapped.  A
// sparse ftruncate'd file maps fine, but the first store into a page with no
// block behind it raises SIGBUS when the filesystem is full; allocating up front
// turns that into an exception at open/resize, where callers already handle it.
// Off the hot path: maps are made at segment/index creation (the provisioner).
void allocateBacking(int fd, size_t bytes, const std::string& path) {
#if defined(__linux__)
    int rc;
    do { rc = ::posix_fallocate(fd, 0, static_cast<off_t>(bytes)); } while (rc == EINTR);
    if (rc != 0)
        throw std::runtime_error("fallocate failed: " + path + ": " + std::strerror(rc));
#else
    (void)fd; (void)bytes; (void)path;   // macOS: no posix_fallocate; dev-only platform
#endif
}
// F3 fix-round-1: RAII guard that closes (and resets to -1) the referenced fd
// unless dismissed.  Used by the ctor and by resize()'s read-only reopen so
// that a throw AFTER a successful open() (ftruncate/mmap failure) never
// leaks the fd.  Without this, a persistently-failing lazy-map retry path
// (e.g. ensureReadMap() hammering a backing file whose mmap always fails)
// leaks one fd per attempt and eventually exhausts the process's fd table.
struct FdCloseGuard {
    int& fd;
    bool dismissed = false;
    ~FdCloseGuard() { if (!dismissed && fd >= 0) { ::close(fd); fd = -1; } }
};
} // namespace

MappedFile::MappedFile(std::string path, size_t initialBytes, bool writable, bool prefault, unsigned fileMode)
    : path_(std::move(path)), size_(initialBytes), writable_(writable) {
    fd_ = ::open(path_.c_str(), writable ? (O_RDWR|O_CREAT) : O_RDONLY, static_cast<mode_t>(fileMode));
    if (fd_ < 0) throw std::runtime_error("open failed: " + path_);
    FdCloseGuard fdGuard{fd_};
    // Never SHRINK an existing file: the index is now right-sized and may have
    // grown-on-demand in a prior run.  Opening it with a smaller initialBytes must
    // not ftruncate away written entries.  Map at least the existing file size.
    {
        off_t existing = ::lseek(fd_, 0, SEEK_END);
        if (existing > 0 && static_cast<size_t>(existing) > size_)
            size_ = static_cast<size_t>(existing);
        ::lseek(fd_, 0, SEEK_SET);
    }
    if (writable && ::ftruncate(fd_, static_cast<off_t>(size_)) != 0)
        throw std::runtime_error("ftruncate failed: " + path_);  // fdGuard closes fd_
    if (writable) allocateBacking(fd_, size_, path_);
    mapNow();                                                    // fdGuard closes fd_ on throw
    fdGuard.dismissed = true;   // success: everything past this point is non-throwing
    if (prefault && writable_ && data_) prefaultWrite();
    if (!writable_) {
        // F3: a read-only mapping never needs its fd again — mmap(MAP_SHARED)
        // keeps the page-cache binding alive via the mapping itself, and a
        // read-only mapping is never ftruncate'd.  Close the fd right away so N
        // retained (e.g. sealed) segments don't each hold an extra fd forever.
        ::close(fd_);
        fd_ = -1;
    }
}
MappedFile::MappedFile(BorrowedFd borrowedFd, std::string path, size_t initialBytes)
    : path_(std::move(path)), size_(initialBytes), writable_(false), borrowedFd_(borrowedFd.fd), borrowed_(true) {
    if (borrowedFd_ < 0) throw std::runtime_error("MappedFile: invalid borrowed fd for " + path_);
    struct stat st{};
    if (::fstat(borrowedFd_, &st) == 0 && st.st_size > 0 && static_cast<size_t>(st.st_size) > size_)
        size_ = static_cast<size_t>(st.st_size);
    fd_ = borrowedFd_;     // mapNow() maps fd_
    try { mapNow(); } catch (...) { fd_ = -1; throw; }
    fd_ = -1;              // never owned: the destructor must not close it
}
void MappedFile::prefaultWrite() noexcept {
    if (!writable_ || data_ == nullptr) return;
    // Touch one byte per page (write: the PTE is mapped writable, so the first
    // real write never takes a fault — not even a write-protect one) to move
    // the cold fault cost off the append/roll path.  D28: through VOLATILE
    // accesses — the old `data_[off] = data_[off]` is a no-op self-assignment
    // the optimizer deleted, so every Release build shipped with no prefault
    // (the worker faulted its OffsetIndex::append writes on mmap_lock).
    const size_t page = static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    volatile uint8_t* const v = data_;
    for (size_t off = 0; off < size_; off += page) v[off] = v[off];
}
void MappedFile::mapNow() {
    int prot = writable_ ? (PROT_READ|PROT_WRITE) : PROT_READ;
    void* p = ::mmap(nullptr, size_, prot, MAP_SHARED, fd_, 0);
    if (p == MAP_FAILED) throw std::runtime_error("mmap failed: " + path_);
    data_ = static_cast<uint8_t*>(p);
}
void MappedFile::resize(size_t newBytes) {
    if (data_) ::munmap(data_, size_);
    data_ = nullptr;   // exception-safety: never leave a dangling pointer around
                        // if ftruncate/reopen/mmap below throws before remapping.
    size_ = 0;          // consistent "nothing mapped" state until the new mapping succeeds
    try {
        if (writable_) {
            if (::ftruncate(fd_, static_cast<off_t>(newBytes)) != 0)
                throw std::runtime_error("ftruncate failed: " + path_);
            allocateBacking(fd_, newBytes, path_);
            size_ = newBytes;
            mapNow();
        } else if (borrowed_) {
            // D3b r2: remap through the borrowed fd (the file's inode) — never by path.
            // D20 review L5: an unbound one (retired) fails rather than guess.
            if (borrowedFd_ < 0) throw std::runtime_error("MappedFile: resize of an unbound borrowed mapping " + path_);
            fd_ = borrowedFd_;
            size_ = newBytes;
            try { mapNow(); } catch (...) { fd_ = -1; throw; }
            fd_ = -1;
        } else {
            // F3: the fd was closed right after the initial mmap (read-only mode
            // holds no fd between calls).  Reopen briefly to remap at the new size,
            // then close it again — a read-only mapping never needs it once mapped.
            // Defensive: never silently overwrite a still-live fd_ (shouldn't
            // happen given the invariant above, but costs nothing to guard).
            if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
            fd_ = ::open(path_.c_str(), O_RDONLY, 0644);
            if (fd_ < 0) throw std::runtime_error("open failed: " + path_);
            FdCloseGuard fdGuard{fd_};   // F3 fix-round-1: no fd leak if mapNow() throws
            size_ = newBytes;
            mapNow();
            fdGuard.dismissed = true;
            ::close(fd_);
            fd_ = -1;
        }
    } catch (...) {
        // F3 fix-round-1: leave the mapping in a fully consistent "nothing
        // mapped" state on ANY failure (ftruncate/open/mmap) — data()==nullptr,
        // size()==0.  The fd itself is handled above (writable_ fd is
        // untouched/long-lived; read-only fd is closed by FdCloseGuard before
        // this catch runs).
        data_ = nullptr;
        size_ = 0;
        throw;
    }
}
int MappedFile::sync() { if (data_ && ::msync(data_, size_, MS_SYNC) != 0) return errno; return 0; }
int MappedFile::sync(size_t bytes) {
    if (!data_) return 0;
    // Round the sync length up to a page boundary and clamp to the mapping size
    // so we never msync past the mapping.  Syncing only the written prefix avoids
    // walking the ~2048 zero pages of the over-provisioned 8 MiB index.
    static const size_t kPage = static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    size_t aligned = bytes == 0 ? kPage : ((bytes + kPage - 1) / kPage) * kPage;
    if (aligned > size_) aligned = size_;
    if (::msync(data_, aligned, MS_SYNC) != 0) return errno;
    return 0;
}
MappedFile::~MappedFile() { if (data_) ::munmap(data_, size_); if (fd_>=0) ::close(fd_); }
} // namespace rune::storage
