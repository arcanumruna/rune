#pragma once
// AdminEgressDrain.h - issue #209: the control thread's drain of the admin -> control egress ring.
//
// The ring carries two kinds of answers: catalog DDL answers (kMsgEgressFrame, through the DdlAnswerGate: an OK waits
// until its topic serves, FIFO behind it) and the D24 gated security OKs (kMsgEgressFrameGated, held by the
// SecurityAnswerGate until this broker enforces the event).  The ring used to be drained only while the DDL gate held
// nothing, so a catalog OK held up to kDdlAnswerMaxHoldNs stalled every security answer behind it.
//
// The drain now always runs and routes per frame; it is bounded by the DDL gate's free ROOM instead of its emptiness:
// before each single-message read it stops when the gate holds something and has less than 2 * (maxMsgLength + 32)
// bytes free.  2x: a SpillFifo record may not fit contiguously at the tail and wraps to the front, so free bytes
// split in two pieces - with 2x the larger piece always takes one record (<= maxMsgLength + 16 bytes).  So the
// gate's Full path is unreachable; frames that do not fit stay in the ring until a release makes room.
// Catalog answers stay FIFO among themselves (all pass through the gate); a security answer may overtake a held
// catalog answer - clients match answers by correlation id.
//
// Control thread, cold path (DDL answers only).  No allocation.
#include <rune/node/AdminChannels.h>
#include <rune/node/DdlAnswerGate.h>
#include <rune/protocol/client/MessageHeader.h>

#include <spdlog/spdlog.h>

#include <cstdint>
#include <cstring>
#include <span>

namespace rune::node {

/// Drains up to `msgLimit` frames of `ring` (an admin egress ring: [session:i64][frame], gated security frames
/// [session:i64][event offset:i64][frame]).  `serves(topicId)`: the DDL gate's readiness predicate;
/// `send(session, frame, n)`: the session egress; `holdSecurity(session, eventOffset, span)`: the security gate.
/// Returns the frames consumed.
template <class Ring, class Serves, class Send, class HoldSecurity>
int drainAdminEgress(Ring& ring, DdlAnswerGate& gate, int msgLimit, int64_t nowNs, Serves&& serves, Send&& send,
                     HoldSecurity&& holdSecurity) {
    const std::size_t room = 2u * (static_cast<std::size_t>(ring.maxMsgLength()) + 32u);
    auto onFrame = [&](int32_t tid, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                       aeron::util::index_t len) {
        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        const auto n = static_cast<std::size_t>(len);
        if (n < 8) return;
        int64_t session;
        std::memcpy(&session, p, 8);
        if (tid == kMsgEgressFrameGated) [[unlikely]] {
            // Authentication Task 10 (D24): [session][event offset][DdlResponse] - held until this broker's view applied
            // the event; a full security gate answers Internal at once, never drops.
            if (n < 16 + rune::protocol::client::MessageHeader::ENCODED_SIZE) return;
            holdSecurity(session, admin_wire::getI64(p + 8), std::span<const uint8_t>(p + 16, n - 16));
            return;
        }
        switch (gate.offer(session, p + 8, n - 8, nowNs, serves)) {
            case DdlAnswerGate::Offer::Pass: send(session, p + 8, n - 8); break;
            case DdlAnswerGate::Offer::Held: break;
            case DdlAnswerGate::Offer::Full:   // unreachable by the room check: never dropped silently
                spdlog::critical("AdminEgress: DDL answer gate full - sending the answer ungated");
                send(session, p + 8, n - 8);
                break;
        }
    };
    int consumed = 0;
    while (consumed < msgLimit) {
        if (!gate.empty() && gate.freeBytes() < room) break;   // no room for one more frame: it waits in the ring
        const int got = ring.drain(onFrame, 1);
        if (got == 0) break;
        consumed += got;
    }
    return consumed;
}

}  // namespace rune::node
