// libs/view/src/UniqueClaims.cpp
#include <rune/view/UniqueClaims.h>

#include <array>
#include <cstring>
#include <rune/codec/ViewKeyEncoding.h>

namespace rune::view {

namespace {

constexpr uint8_t kHasHead = 1, kHasPrev = 1, kHasNext = 2;

RowLayout payloadLayout(uint32_t rowLen) {
    RowLayout l;
    l.rowLen = rowLen;
    return l;
}

} // namespace

int compareSourceKeys(rune::codec::PrimType keyType, std::string_view a, std::string_view b) noexcept {
    std::array<uint8_t, 256> ea, eb;
    const std::span<uint8_t> oa(ea.data(), a.size()), ob(eb.data(), b.size());
    (void)rune::codec::encodeKeyComponent(keyType, 0, decodeSlotValue(reinterpret_cast<const uint8_t*>(a.data()), keyType, static_cast<uint16_t>(a.size())), oa);
    (void)rune::codec::encodeKeyComponent(keyType, 0, decodeSlotValue(reinterpret_cast<const uint8_t*>(b.data()), keyType, static_cast<uint16_t>(b.size())), ob);
    return std::memcmp(ea.data(), eb.data(), a.size());
}

UniqueClaims::UniqueClaims(uint16_t keyWidth, uint8_t wireWidth, uint32_t rowLen, rune::codec::PrimType sourceKeyType, uint64_t hashSeed,
                           std::size_t indexSlots, std::size_t chunkBytes)
    : keyWidth_(keyWidth), wireWidth_(wireWidth), rowLen_(rowLen), keyType_(sourceKeyType),
      index_(payloadLayout(2u * wireWidth + 4u + 1u), indexSlots, chunkBytes, ViewTableKeys{static_cast<uint8_t>(keyWidth), 0, hashSeed}),
      extrasTable_(payloadLayout(keyWidth + 2u * wireWidth + 1u + rowLen), indexSlots, chunkBytes, ViewTableKeys{wireWidth, 0, hashSeed}) {}

const uint8_t* UniqueClaims::ownerOf(const ViewKey& n, TxnAccess* x) const noexcept { return index(x).get(n); }

const uint8_t* UniqueClaims::extraKeyOf(std::string_view wire, TxnAccess* x) const noexcept {
    return extrasOf(x).get(ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(wire.data()), wire.size()}));
}

bool UniqueClaims::hasExtras(const ViewKey& n, TxnAccess* x) const noexcept {
    const uint8_t* p = index(x).get(n);
    if (p == nullptr) return false;
    uint32_t count;
    std::memcpy(&count, p + countAt(), 4);
    return count != 0;
}

void UniqueClaims::setOwner(const ViewKey& n, std::string_view wire, TxnAccess* x) noexcept {
    uint8_t* p = index(x).put(n);   // a new entry is zeroed: no extras
    std::memcpy(p, wire.data(), wireWidth_);
}

void UniqueClaims::eraseOwner(const ViewKey& n, TxnAccess* x) noexcept { index(x).erase(n); }

void UniqueClaims::addExtra(const ViewKey& n, std::string_view wire, const uint8_t* image, TxnAccess* x) noexcept {
    TxnTable ext = extrasOf(x);
    uint8_t* ip = index(x).put(n);
    uint32_t count;
    std::memcpy(&count, ip + countAt(), 4);
    ++count;
    std::memcpy(ip + countAt(), &count, 4);
    const bool hadHead = (ip[flagsAt()] & kHasHead) != 0;
    uint8_t oldHead[256];
    if (hadHead) std::memcpy(oldHead, ip + headAt(), wireWidth_);
    std::memcpy(ip + headAt(), wire.data(), wireWidth_);
    ip[flagsAt()] |= kHasHead;

    uint8_t* e = ext.put(wireKey(reinterpret_cast<const uint8_t*>(wire.data())));
    std::memcpy(e, n.bytes.data(), keyWidth_);
    if (hadHead) {
        std::memcpy(e + nextAt(), oldHead, wireWidth_);
        e[linksAt()] = kHasNext;
        uint8_t* h = ext.put(wireKey(oldHead));
        std::memcpy(h + prevAt(), wire.data(), wireWidth_);
        h[linksAt()] |= kHasPrev;
    }
    std::memcpy(e + imageAt(), image, rowLen_);
    if (x == nullptr || x->chain == nullptr) ++extras_;
}

void UniqueClaims::updateExtraImage(std::string_view wire, const uint8_t* image, TxnAccess* x) noexcept {
    uint8_t* e = extrasOf(x).put(wireKey(reinterpret_cast<const uint8_t*>(wire.data())));
    std::memcpy(e + imageAt(), image, rowLen_);
}

void UniqueClaims::removeExtra(std::string_view wire, TxnAccess* x) noexcept {
    TxnTable ext = extrasOf(x);
    const ViewKey self = wireKey(reinterpret_cast<const uint8_t*>(wire.data()));
    const uint8_t* e = ext.get(self);
    if (e == nullptr) return;
    const ViewKey n = ViewKey::fromBytes({e, keyWidth_});
    const uint8_t links = e[linksAt()];
    uint8_t prev[256], next[256];
    std::memcpy(prev, e + prevAt(), wireWidth_);
    std::memcpy(next, e + nextAt(), wireWidth_);

    uint8_t* ip = index(x).put(n);
    uint32_t count;
    std::memcpy(&count, ip + countAt(), 4);
    --count;
    std::memcpy(ip + countAt(), &count, 4);
    if ((links & kHasPrev) != 0) {
        uint8_t* p = ext.put(wireKey(prev));
        if ((links & kHasNext) != 0) std::memcpy(p + nextAt(), next, wireWidth_);
        else p[linksAt()] &= static_cast<uint8_t>(~kHasNext);
    } else if ((links & kHasNext) != 0) {
        std::memcpy(ip + headAt(), next, wireWidth_);
    } else {
        ip[flagsAt()] &= static_cast<uint8_t>(~kHasHead);
    }
    if ((links & kHasNext) != 0) {
        uint8_t* q = ext.put(wireKey(next));
        if ((links & kHasPrev) != 0) std::memcpy(q + prevAt(), prev, wireWidth_);
        else q[linksAt()] &= static_cast<uint8_t>(~kHasPrev);
    }
    ext.erase(self);
    if (x == nullptr || x->chain == nullptr) --extras_;
}

bool UniqueClaims::smallestExtra(const ViewKey& n, std::string_view& wire, const uint8_t*& image, TxnAccess* x) const noexcept {
    const TxnTable ext = extrasOf(x);
    const uint8_t* ip = index(x).get(n);
    if (ip == nullptr || (ip[flagsAt()] & kHasHead) == 0) return false;
    const uint8_t* cur = ip + headAt();
    const uint8_t* best = nullptr;
    const uint8_t* bestRow = nullptr;
    for (;;) {
        const uint8_t* e = ext.get(wireKey(cur));
        if (best == nullptr || compareSourceKeys(keyType_, {reinterpret_cast<const char*>(cur), wireWidth_}, {reinterpret_cast<const char*>(best), wireWidth_}) < 0) {
            best = cur;
            bestRow = e;
        }
        if ((e[linksAt()] & kHasNext) == 0) break;
        cur = e + nextAt();
    }
    wire = {reinterpret_cast<const char*>(best), wireWidth_};
    image = bestRow + imageAt();
    return true;
}

} // namespace rune::view
