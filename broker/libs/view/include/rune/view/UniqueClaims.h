// libs/view/include/rune/view/UniqueClaims.h
//
// The claimants of a unique, owner-mapped view (§8.3): per view key N, the owner (the claimant with the smallest source key,
// by value) and the extras, each with the row image it would give N if promoted.  Two ViewTables, so a checkpoint carries them
// as v3 tables 6 and 7.
#pragma once
#include <cstddef>
#include <cstdint>
#include <string_view>

#include <rune/codec/PrimType.h>
#include <rune/view/ViewKey.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

namespace rune::view {

// Orders two source wire keys of `keyType` by value: <0, 0, >0.
[[nodiscard]] int compareSourceKeys(rune::codec::PrimType keyType, std::string_view a, std::string_view b) noexcept;

class UniqueClaims {
public:
    UniqueClaims(uint16_t keyWidth, uint8_t wireWidth, uint32_t rowLen, rune::codec::PrimType sourceKeyType, uint64_t hashSeed,
                 std::size_t indexSlots = 4096, std::size_t chunkBytes = 32u << 20);   // cold

    // Pointers into the tables are valid until the next mutation.
    [[nodiscard]] const uint8_t* ownerOf(const ViewKey& n) const noexcept;               // owner wire bytes, nullptr = no row
    [[nodiscard]] const uint8_t* extraKeyOf(std::string_view wire) const noexcept;       // N this wire is an extra of
    [[nodiscard]] bool hasExtras(const ViewKey& n) const noexcept;
    [[nodiscard]] int compare(std::string_view a, std::string_view b) const noexcept { return compareSourceKeys(keyType_, a, b); }
    [[nodiscard]] std::size_t extras() const noexcept { return extras_; }                // the uniqueConflicts gauge

    void setOwner(const ViewKey& n, std::string_view wire) noexcept;
    void eraseOwner(const ViewKey& n) noexcept;
    void addExtra(const ViewKey& n, std::string_view wire, const uint8_t* image) noexcept;
    void updateExtraImage(std::string_view wire, const uint8_t* image) noexcept;
    void removeExtra(std::string_view wire) noexcept;   // no-op for a wire that is not an extra
    [[nodiscard]] bool smallestExtra(const ViewKey& n, std::string_view& wire, const uint8_t*& image) const noexcept;

    ViewTable& indexTable() noexcept { return index_; }     // checkpoint v3 table 6
    ViewTable& extrasTable() noexcept { return extrasTable_; }   // checkpoint v3 table 7
    [[nodiscard]] const ViewTable& indexTable() const noexcept { return index_; }
    [[nodiscard]] const ViewTable& extrasTable() const noexcept { return extrasTable_; }
    void recountAfterReplay() noexcept { extras_ = extrasTable_.size(); }   // cold

private:
    // index payload:  [ownerWire W][extraCount u32][headExtra W][flags u8: bit0 = has head]
    // extras payload: [N K][prev W][next W][links u8: bit0 prev, bit1 next][image rowLen]
    [[nodiscard]] std::size_t countAt() const noexcept { return wireWidth_; }
    [[nodiscard]] std::size_t headAt() const noexcept { return wireWidth_ + 4u; }
    [[nodiscard]] std::size_t flagsAt() const noexcept { return 2u * wireWidth_ + 4u; }
    [[nodiscard]] std::size_t prevAt() const noexcept { return keyWidth_; }
    [[nodiscard]] std::size_t nextAt() const noexcept { return keyWidth_ + wireWidth_; }
    [[nodiscard]] std::size_t linksAt() const noexcept { return keyWidth_ + 2u * wireWidth_; }
    [[nodiscard]] std::size_t imageAt() const noexcept { return linksAt() + 1u; }
    [[nodiscard]] ViewKey wireKey(const uint8_t* p) const noexcept { return ViewKey::fromBytes({p, wireWidth_}); }

    uint16_t                keyWidth_;
    uint8_t                 wireWidth_;
    uint32_t                rowLen_;
    rune::codec::PrimType   keyType_;
    std::size_t             extras_ = 0;
    ViewTable               index_;
    ViewTable               extrasTable_;
};

} // namespace rune::view
