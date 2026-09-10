#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
namespace rune::storage {
class MappedFile {
public:
    MappedFile(std::string path, size_t initialBytes, bool writable);
    ~MappedFile();
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    uint8_t* data() noexcept { return data_; }
    const uint8_t* data() const noexcept { return data_; }
    size_t size() const noexcept { return size_; }
    void resize(size_t newBytes);
    void sync();
private:
    std::string path_; uint8_t* data_ = nullptr; size_t size_ = 0; int fd_ = -1; bool writable_;
    void mapNow();
};
} // namespace rune::storage
