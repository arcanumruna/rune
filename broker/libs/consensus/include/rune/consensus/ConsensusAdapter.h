#pragma once
// ConsensusAdapter.h — Decodes incoming SBE consensus frames and dispatches
// to a ConsensusMessageHandler.
//
// onFragment() is the per-frame entry point:
//   - peekTemplateId(buf) → switch on templateId
//   - decode with the matching P2P-4 decoder (no alloc; views into buf)
//   - call handler_.onXxx(nowNs, ...decoded fields...)
//
// FRAGMENT_LIMIT = 10 mirrors the Java ConsensusAdapter constant. The real
// Aeron poll loop (P2P-8) wraps this in a FragmentAssembler and calls
// subscription.poll(fragmentAssembler, FRAGMENT_LIMIT) per duty cycle.
//
// Unknown templateIds are silently ignored (forward-compat).

#include <rune/consensus/ConsensusMessageHandler.h>
#include <rune/consensus/ConsensusMessages.h>
#include <cstdint>
#include <cstddef>

namespace rune::consensus {

class ConsensusAdapter {
public:
    // Mirrors Java ConsensusAdapter.FRAGMENT_LIMIT.
    // P2P-8 wraps this in an aeron FragmentAssembler.
    static constexpr int FRAGMENT_LIMIT = 10;

    explicit ConsensusAdapter(ConsensusMessageHandler& handler) noexcept
        : handler_{handler}
    {}

    /// Decode one SBE frame and dispatch to the handler.
    /// buf[0..len) is a complete SBE frame starting with an 8-byte header.
    void onFragment(int64_t nowNs, const uint8_t* buf, std::size_t len);

private:
    ConsensusMessageHandler& handler_;
};

} // namespace rune::consensus
