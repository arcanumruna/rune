#pragma once
// OpTarget.h — F8c C4/C5: a completion's destination as the DUTY side holds
// it, and the plain-function-pointer seams that let the group / txn libraries
// (which do not link the Client) issue token ops and deliver their single
// completion through the Client's completion-queue registry.
//
//   * OpTarget        — a CompletionHandler, or a registered Completions'
//                        (CqRef, reservation).  `counted`: the Client pinned
//                        the registry slot when the op was applied (I3).
//   * CompletionSink  — duty-side delivery (fn + ctx; the Client's deliver_).
//                        No sink (hub/impl unit tests): a handler target is
//                        called directly, a CQ target is dropped.
//   * TargetedOpSink  — issue side: validates the target, reserves credit,
//                        encodes the target into the HubCommand and submits it
//                        (applied directly on the duty thread, else the agent
//                        command ring).  Only Accepted is followed by exactly
//                        one completion.
//   * AndWaitSink     — the cold-path *AndWait sugar: a short-lived
//                        Completions, issue, pump-until-done / idle-poll.
//
// No std::function, no virtual call, no allocation on any of these.

#include <rune/client/Completion.h>
#include <rune/client/detail/HubCommand.h>

#include <chrono>
#include <cstdint>
#include <span>
#include <string>

namespace rune::client {
struct CompletionTarget;  // rune/client/Completions.h
}

namespace rune::client::detail {

struct OpTarget {
    CompletionHandler handler{};          ///< fn != nullptr: the callback form
    uint32_t          reservedBytes = 0;  ///< CQ form: the issuer's reservation
    CqRef             cqRef = kNoCqRef;   ///< CQ form: registry reference
    bool              counted = false;    ///< CQ form: pins its slot (I3 quarantine)
};

/// Target <-> HubCommand words b / c (a carries the Token).
///   handler form: b = fn, c = ctx
///   CQ form:      b = 0,  c = reservedBytes << 32 | counted << 16 | cqRef
inline void encodeTarget(const OpTarget& t, HubCommand& cmd) noexcept {
    if (t.handler.fn != nullptr) {
        cmd.b = static_cast<int64_t>(reinterpret_cast<uintptr_t>(t.handler.fn));
        cmd.c = static_cast<int64_t>(reinterpret_cast<uintptr_t>(t.handler.ctx));
        return;
    }
    cmd.b = 0;
    cmd.c = static_cast<int64_t>((static_cast<uint64_t>(t.reservedBytes) << 32) |
                                 (static_cast<uint64_t>(t.counted ? 1u : 0u) << 16) |
                                 static_cast<uint64_t>(t.cqRef));
}

/// The words b / c as written by encodeTarget (a producer-ring message carries them without a HubCommand).
[[nodiscard]] inline OpTarget decodeTargetWords(int64_t wordB, int64_t wordC) noexcept {
    OpTarget t;
    if (wordB != 0) {
        using Fn = decltype(CompletionHandler::fn);
        t.handler.fn  = reinterpret_cast<Fn>(static_cast<uintptr_t>(wordB));
        t.handler.ctx = reinterpret_cast<void*>(static_cast<uintptr_t>(wordC));
        return t;
    }
    const auto w    = static_cast<uint64_t>(wordC);
    t.reservedBytes = static_cast<uint32_t>(w >> 32);
    t.counted       = ((w >> 16) & 1u) != 0;
    t.cqRef         = static_cast<CqRef>(w & 0xFFFFu);
    return t;
}

[[nodiscard]] inline OpTarget decodeTarget(const HubCommand& cmd) noexcept { return decodeTargetWords(cmd.b, cmd.c); }

/// Duty-side delivery of one completion (the Client's registry).
struct CompletionSink {
    void (*fn)(void* ctx, const OpTarget& t, const Completion& c,
               std::span<const uint8_t> payload) noexcept = nullptr;
    void* ctx = nullptr;

    void deliver(const OpTarget& t, const Completion& c, std::span<const uint8_t> payload = {}) const noexcept {
        if (fn != nullptr) {
            fn(ctx, t, c, payload);
            return;
        }
        if (t.handler.fn != nullptr) {  // no registry (unit rigs): the callback form only
            Completion done = c;
            done.payloadLen = static_cast<uint32_t>(payload.size());
            t.handler(done, payload);
        }
    }
};

/// Issue one token op carried by `cmd` (its op / handle / payload words set by
/// the caller; this sink writes a = token and the target words).
using TargetedOpSink = OpResult (*)(void* ctx, HubCommand& cmd, const CompletionTarget& target,
                                    Token token, OpKind op);

/// *AndWait: issue + wait.  Returns the op's completion (status NOT checked —
/// the caller maps it to its own exception types); `text` receives the inline
/// payload (error text).  Throws std::runtime_error when the op could not be
/// issued or the wait timed out, std::logic_error from a duty-cycle callback /
/// a non-owner Invoker thread.
using AndWaitIssue = OpResult (*)(void* issueCtx, const CompletionTarget& target, Token token);
using AndWaitSink  = Completion (*)(void* ctx, const char* what, std::chrono::nanoseconds timeout,
                                    AndWaitIssue issue, void* issueCtx, std::string* text);

} // namespace rune::client::detail
