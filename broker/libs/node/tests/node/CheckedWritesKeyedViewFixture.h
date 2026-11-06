// Part 07 Task 10: shared fixtures for the keyed WorkerViewSet tests (a character source, keyed plans, a growing log).
#pragma once
#include <cstdint>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>

namespace cwkeyed {

using rune::codec::PrimType;

inline constexpr int32_t kSrcTopic  = 5;
inline constexpr int32_t kViewTopic = 0x40000007;

inline rune::codec::Descriptor characterSource() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64)
        .addFixedStr("name", 16).addFixedStr("tag", 80).add("gender", PrimType::UInt16).build();
}

inline rune::view::CompileResult compileKeyed(const rune::codec::Descriptor& d, std::vector<rune::view::KeyComponent> key,
                                              std::vector<std::string> select, int32_t viewTopic = kViewTopic) {
    rune::view::ViewPlan v;
    v.viewName = "keyed_view";
    v.sources  = {{kSrcTopic, "characterId"}};
    for (const auto& k : key) {
        if (k.keyFunction) v.unicodeVersion = rune::codec::kUnicodeVersion;
    }
    v.keyComponents = std::move(key);
    for (const auto& s : select) v.output.push_back({s, d.find(s)->type, 0, s, rune::view::MergeRule::Replace});
    const rune::codec::Descriptor* srcs[] = {&d};
    auto r = rune::view::compileView(v, srcs, viewTopic);
    if (!r) throw std::runtime_error(r.error().message);
    return std::move(*r);
}

/// key (userId, characterId), prefix userId; output slots: userId, characterId, gender.
inline rune::view::CompileResult accountCharacters(const rune::codec::Descriptor& d) {
    return compileKeyed(d, {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, {"gender"});
}

/// key (lower(name), characterId), prefix lower(name); output slots: nameKey, characterId, gender.
inline rune::view::CompileResult namedCharacters(const rune::codec::Descriptor& d) {
    return compileKeyed(d, {{"name", 1, "nameKey"}, {"characterId", 0, "characterId"}}, {"gender"});
}

struct Rec {
    uint64_t characterId = 0;
    uint64_t userId      = 0;
    std::string name;
    uint16_t gender  = 0;
    bool tombstone   = false;
};

inline std::string wireKey(uint64_t characterId) {
    std::string k(8, '\0');
    for (int i = 0; i < 8; ++i) k[static_cast<size_t>(i)] = static_cast<char>(characterId >> (8 * i));
    return k;
}

inline std::vector<uint8_t> beKey(std::initializer_list<uint64_t> parts) {
    std::vector<uint8_t> k;
    for (uint64_t p : parts)
        for (int i = 7; i >= 0; --i) k.push_back(static_cast<uint8_t>(p >> (8 * i)));
    return k;
}

inline std::vector<uint8_t> nameKey(const std::string& lowered, uint64_t characterId) {
    std::vector<uint8_t> k(24, 0);
    for (size_t i = 0; i < lowered.size(); ++i) k[i] = static_cast<uint8_t>(lowered[i]);
    const auto id = beKey({characterId});
    k.insert(k.end(), id.begin(), id.end());
    return k;
}

inline std::string valueOf(const rune::codec::Descriptor& d, const Rec& r) {
    std::string rec(4 + d.blockLength, '\0');
    rec[0] = static_cast<char>(d.blockLength);
    rec[1] = static_cast<char>(d.blockLength >> 8);
    rec[2] = static_cast<char>(d.version);
    rec[3] = static_cast<char>(d.version >> 8);
    const auto put = [&](const char* field, uint64_t v) {
        const auto* f = d.find(field);
        for (uint32_t i = 0; i < rune::codec::fieldWidth(*f); ++i) rec[4 + f->offset + i] = static_cast<char>(v >> (8 * i));
    };
    put("characterId", r.characterId);
    put("userId", r.userId);
    put("gender", r.gender);
    const auto* n = d.find("name");
    for (size_t i = 0; i < r.name.size(); ++i) rec[4 + n->offset + i] = r.name[i];
    return rec;
}

/// A source log that grows one batch per append; readRange hands out all of it (the fold skips what it already folded).
class Log {
public:
    explicit Log(const rune::codec::Descriptor& d) : d_(d) {}
    void append(const std::vector<Rec>& recs) {
        rune::protocol::RecordBatchBuilder b(next_, 0);
        for (const Rec& r : recs) b.append(wireKey(r.characterId), r.tombstone ? std::string() : valueOf(d_, r), 0);
        const auto bytes = b.build();
        bytes_.insert(bytes_.end(), bytes.begin(), bytes.end());
        next_ += static_cast<int64_t>(recs.size());
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

}  // namespace cwkeyed
