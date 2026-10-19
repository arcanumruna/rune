#pragma once
// SecurityAnswerGate.h — the security DDL answer gate (authentication plan D24, review focus 4).
//
// A security DDL's OK means "THIS broker already enforces it": a GRANT then an immediate request on the same broker
// must pass; a REVOKE then an immediate request must fail.  The leader's admin thread answers once the event is
// committed and applied (SecurityAdmin) - but the CONTROL thread enforces: it owns the SecurityView, the sessions'
// permission tables and the workers' write bitmaps.  The admin thread therefore posts the OK as kMsgEgressFrameGated
// ([session][event offset][DdlResponse]) and the control thread HOLDS it here until
//   view.appliedOffset() >= offset  AND  the permission refresh of that event reached every worker
// (Task 13/14 call markRefreshed; until then the caller passes the applied offset).  `release` is handed that
// min(applied, refreshed) offset on every control pass.
//
// No-hang: a full gate answers Internal at once (never drops, never blocks); a halted view answers every held OK
// Internal (failAll) - the OK would claim an enforcement this broker can no longer give.
//
// Control-thread-owned, single-threaded.  Preallocated (kSlots x kFrameMax); hold / release never allocate.

#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>

namespace rune::node {

class SecurityAnswerGate {
public:
    static constexpr std::size_t kSlots    = 256;
    static constexpr std::size_t kFrameMax = 512;   // a DdlResponse OK carries no detail: ~50 bytes

    SecurityAnswerGate() : slots_(std::make_unique<Slot[]>(kSlots)) {}

    [[nodiscard]] std::size_t size() const noexcept { return count_; }
    [[nodiscard]] bool empty() const noexcept { return count_ == 0; }
    [[nodiscard]] uint64_t internalAnswers() const noexcept { return internalAnswers_; }

    /// Hold the OK `frame` of the event at `offset` for `sessionId`.  A frame that does not fit a slot, or a full gate,
    /// is answered Internal through `send` at once - never dropped.
    template <class Send>
    void hold(int64_t sessionId, int64_t offset, std::span<const uint8_t> frame, Send&& send) {
        if (count_ == kSlots || frame.size() > kFrameMax) [[unlikely]] {
            answerInternal_(sessionId, frame, count_ == kSlots ? "the security answer gate is full - the change may have been "
                                                                 "applied; retry with the same requestId"
                                                               : "the security answer is too large to hold",
                            send);
            return;
        }
        Slot& s = slots_[count_++];
        s.session = sessionId;
        s.offset  = offset;
        s.len     = static_cast<uint16_t>(frame.size());
        std::memcpy(s.frame.data(), frame.data(), frame.size());
    }

    /// Send every held OK whose event offset <= `enforcedOffset` (this broker's view applied it AND its refresh
    /// reached every worker), in the order held.  Returns the number sent.
    template <class Send>
    int release(int64_t enforcedOffset, Send&& send) {
        int sent = 0;
        std::size_t keep = 0;
        for (std::size_t i = 0; i < count_; ++i) {
            Slot& s = slots_[i];
            if (s.offset <= enforcedOffset) {
                send(s.session, s.frame.data(), static_cast<std::size_t>(s.len));
                ++sent;
            } else {
                if (keep != i) slots_[keep] = s;
                ++keep;
            }
        }
        count_ = keep;
        return sent;
    }

    /// The mirror halted: every held OK is answered Internal.
    template <class Send>
    int failAll(std::string_view detail, Send&& send) {
        int n = 0;
        for (std::size_t i = 0; i < count_; ++i) {
            const Slot& s = slots_[i];
            answerInternal_(s.session, std::span<const uint8_t>(s.frame.data(), s.len), detail, send);
            ++n;
        }
        count_ = 0;
        return n;
    }

private:
    struct Slot {
        int64_t session = 0;
        int64_t offset  = -1;
        uint16_t len    = 0;
        std::array<uint8_t, kFrameMax> frame{};
    };

    /// A DdlResponse{Internal, detail} for the request the held OK answered (same session + correlation id).
    template <class Send>
    void answerInternal_(int64_t sessionId, std::span<const uint8_t> okFrame, std::string_view detail, Send& send) {
        namespace pc = rune::protocol::client;
        ++internalAnswers_;
        const auto h = pc::MessageHeader::decode(okFrame.data(), okFrame.size());
        pc::DdlResponse r;
        r.sessionId     = h ? h->sessionId : sessionId;
        r.correlationId = h ? h->correlationId : 0;
        r.errorCode     = static_cast<uint16_t>(pc::ErrorCode::Internal);
        r.detail        = detail.substr(0, 256);
        uint8_t buf[pc::MessageHeader::ENCODED_SIZE + pc::DdlResponse::BODY_FIXED + 2 + 256];
        const std::size_t n = r.encode(buf);
        if (n > 0) send(sessionId, buf, n);
    }

    std::unique_ptr<Slot[]> slots_;
    std::size_t count_ = 0;
    uint64_t internalAnswers_ = 0;
};

} // namespace rune::node
