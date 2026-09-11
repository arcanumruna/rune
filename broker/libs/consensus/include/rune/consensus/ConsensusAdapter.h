#pragma once
// ConsensusAdapter.h — Decodes incoming SBE consensus frames and dispatches
// to a ConsensusMessageHandler.
//
// onFragment() is the per-frame entry point:
//   - bounds-check: len >= ConsensusMessageHeader::ENCODED_SIZE (header peek)
//   - peekTemplateId(buf) → switch on templateId
//   - bounds-check: len >= Msg::TOTAL_SIZE before each decode
//   - decode with the matching P2P-4 decoder (no alloc; views into buf)
//   - call handler_.onXxx(nowNs, ...decoded fields...)
//
// A3 fix (audit 2026-09-11): 'len' was previously ignored (/*len*/ comment),
// allowing an OOB read on truncated/corrupt frames.  Every decode path now
// validates len >= required size and increments ignoredFrameCount_ on rejection.
//
// FRAGMENT_LIMIT = 10 mirrors the Java ConsensusAdapter constant. The real
// Aeron poll loop (P2P-8) wraps this in a FragmentAssembler and calls
// subscription.poll(fragmentAssembler, FRAGMENT_LIMIT) per duty cycle.
//
// Unknown templateIds are silently ignored (forward-compat); they also increment
// ignoredFrameCount_ so callers can surface dropped/unknown frames.

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
    /// Frames shorter than the required message size are rejected (no OOB read);
    /// ignoredFrameCount() increments on each rejected or unknown frame.
    void onFragment(int64_t nowNs, const uint8_t* buf, std::size_t len);

    /// Count of frames rejected due to truncation, unknown templateId, or OOB guard.
    /// Useful for monitoring/alerting on corrupt ingress.
    [[nodiscard]] int64_t ignoredFrameCount() const noexcept { return ignoredFrameCount_; }

private:
    ConsensusMessageHandler& handler_;
    int64_t                  ignoredFrameCount_{0};
};

} // namespace rune::consensus
