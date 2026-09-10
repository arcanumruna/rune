#pragma once
#include <cstdint>
#include <unordered_map>
namespace rune::storage {
class ProducerState {
public:
    enum class Result { Ok, Duplicate, OutOfSequence };
    Result validateAndUpdate(int64_t producerId, int16_t epoch, int32_t baseSequence, int32_t recordCount) {
        auto it = last_.find(producerId);
        int32_t expected = (it == last_.end()) ? 0 : it->second.nextSeq;
        if (it != last_.end() && baseSequence < expected) return Result::Duplicate;
        if (baseSequence != expected) return Result::OutOfSequence;
        last_[producerId] = { epoch, baseSequence + recordCount };
        return Result::Ok;
    }
private:
    struct Entry { int16_t epoch; int32_t nextSeq; };
    std::unordered_map<int64_t, Entry> last_;
};
} // namespace rune::storage
