#include <rune/storage/MappedFile.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdexcept>
namespace rune::storage {
MappedFile::MappedFile(std::string path, size_t initialBytes, bool writable)
    : path_(std::move(path)), size_(initialBytes), writable_(writable) {
    fd_ = ::open(path_.c_str(), writable ? (O_RDWR|O_CREAT) : O_RDONLY, 0644);
    if (fd_ < 0) throw std::runtime_error("open failed: " + path_);
    if (writable && ::ftruncate(fd_, static_cast<off_t>(size_)) != 0)
        throw std::runtime_error("ftruncate failed: " + path_);
    mapNow();
}
void MappedFile::mapNow() {
    int prot = writable_ ? (PROT_READ|PROT_WRITE) : PROT_READ;
    void* p = ::mmap(nullptr, size_, prot, MAP_SHARED, fd_, 0);
    if (p == MAP_FAILED) throw std::runtime_error("mmap failed: " + path_);
    data_ = static_cast<uint8_t*>(p);
}
void MappedFile::resize(size_t newBytes) {
    if (data_) ::munmap(data_, size_);
    size_ = newBytes;
    if (writable_ && ::ftruncate(fd_, static_cast<off_t>(size_)) != 0)
        throw std::runtime_error("ftruncate failed: " + path_);
    mapNow();
}
void MappedFile::sync() { if (data_) ::msync(data_, size_, MS_SYNC); }
MappedFile::~MappedFile() { if (data_) ::munmap(data_, size_); if (fd_>=0) ::close(fd_); }
} // namespace rune::storage
