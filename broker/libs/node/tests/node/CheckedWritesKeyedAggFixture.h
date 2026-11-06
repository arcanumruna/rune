// Part 07 Task 11: shared fixtures for the keyed-aggregate WorkerViewSet tests (a device source, device_history, a growing log).
#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>

#include "CheckedWritesKeyedViewFixture.h"

namespace cwagg {

using cwkeyed::kSrcTopic;
using cwkeyed::kViewTopic;
using rune::codec::PrimType;

inline rune::codec::Descriptor deviceSource() {
    return rune::codec::DescriptorBuilder("device", 1)
        .add("eventId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64).addFixedStr("deviceFp", 32)
        .add("platform", PrimType::Enum).add("lastSeen", PrimType::Int64).add("bytes", PrimType::UInt64).build();
}

/// key (userId, deviceFp), prefix userId; count(*) as uses, sum(bytes) as total, min(lastSeen) as firstSeen, max(lastSeen) as lastSeen,
/// last(platform) as platform.
inline rune::view::CompileResult deviceHistory(const rune::codec::Descriptor& d) {
    rune::view::ViewPlan v;
    v.viewName = "device_history";
    v.sources  = {{kSrcTopic, "userId"}};
    v.keyComponents = v.groupBy = {{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}};
    v.output = {{"uses", PrimType::Int64, 0, "", rune::view::MergeRule::CountAll},
                {"total", PrimType::UInt64, 0, "bytes", rune::view::MergeRule::Sum},
                {"firstSeen", PrimType::Int64, 0, "lastSeen", rune::view::MergeRule::Min},
                {"lastSeen", PrimType::Int64, 0, "lastSeen", rune::view::MergeRule::Max},
                {"platform", PrimType::Enum, 0, "platform", rune::view::MergeRule::Last}};
    const rune::codec::Descriptor* srcs[] = {&d};
    auto r = rune::view::compileView(v, srcs, kViewTopic);
    if (!r) throw std::runtime_error(r.error().message);
    return std::move(*r);
}

struct Ev {
    uint64_t id = 0;
    uint64_t user = 0;
    std::string fp;
    uint32_t platform = 0;
    int64_t lastSeen = 0;
    uint64_t bytes = 0;
    bool tombstone = false;
};

inline std::string valueOf(const rune::codec::Descriptor& d, const Ev& e) {
    std::string rec(4 + d.blockLength, '\0');
    rec[0] = static_cast<char>(d.blockLength);
    rec[1] = static_cast<char>(d.blockLength >> 8);
    rec[2] = static_cast<char>(d.version);
    rec[3] = static_cast<char>(d.version >> 8);
    const auto put = [&](const char* field, uint64_t v) {
        const auto* f = d.find(field);
        for (uint32_t i = 0; i < rune::codec::fieldWidth(*f); ++i) rec[4 + f->offset + i] = static_cast<char>(v >> (8 * i));
    };
    put("eventId", e.id);
    put("userId", e.user);
    put("platform", e.platform);
    put("lastSeen", static_cast<uint64_t>(e.lastSeen));
    put("bytes", e.bytes);
    const auto* fp = d.find("deviceFp");
    for (size_t i = 0; i < e.fp.size(); ++i) rec[4 + fp->offset + i] = e.fp[i];
    return rec;
}

/// userId big-endian, then the 32-byte deviceFp, zero padded: the view key of (userId, deviceFp).
inline std::vector<uint8_t> groupKey(uint64_t user, const std::string& fp) {
    std::vector<uint8_t> k = cwkeyed::beKey({user});
    for (size_t i = 0; i < 32; ++i) k.push_back(i < fp.size() ? static_cast<uint8_t>(fp[i]) : uint8_t{0});
    return k;
}

/// A source log that grows one batch per append; readRange hands out all of it (the fold skips what it already folded).
class Log {
public:
    explicit Log(const rune::codec::Descriptor& d) : d_(d) {}
    void append(const std::vector<Ev>& evs) {
        rune::protocol::RecordBatchBuilder b(next_, 0);
        for (const Ev& e : evs) b.append(cwkeyed::wireKey(e.id), e.tombstone ? std::string() : valueOf(d_, e), 0);
        const auto bytes = b.build();
        bytes_.insert(bytes_.end(), bytes.begin(), bytes.end());
        next_ += static_cast<int64_t>(evs.size());
    }
    [[nodiscard]] int64_t end() const noexcept { return next_; }
    [[nodiscard]] rune::storage::LogReadView read(int64_t, int64_t) const {
        return rune::storage::LogReadView{bytes_.data(), bytes_.size(), 0, next_};
    }
private:
    const rune::codec::Descriptor& d_;
    std::vector<uint8_t> bytes_;
    int64_t next_ = 0;
};

}  // namespace cwagg
