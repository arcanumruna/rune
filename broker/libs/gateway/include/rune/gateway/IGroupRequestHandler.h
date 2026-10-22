#pragma once
// IGroupRequestHandler.h — CG-4 fix: abstract interface for consumer-group request
// handling, owned by the gateway lib so ClientGateway depends only on this interface
// rather than on rune_node symbols.
//
// GroupCoordinationService (in rune_node) implements this interface.
// ClientGateway holds an IGroupRequestHandler* (set via setGroupService) and
// dispatches group messages through it.  rune_gateway therefore has NO direct
// dependency on rune_node symbols at link time.
//
// Direction of dependencies (after this fix):
//   rune_gateway  → (no rune_node symbols)
//   rune_node     → rune_gateway    (GroupCoordinationService uses ClientGateway API)
//
// Namespace: rune::gateway
// C++23.

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace rune::gateway {

struct IGroupRequestHandler {
    virtual ~IGroupRequestHandler() = default;

    // Each method decodes the request from buf[0..len-1], acts, encodes the
    // response into out[0..outCap-1], and returns the number of bytes written.
    // 0 means "no answer" (the gateway answers Internal) - except for
    // handleOffsetCommit, where 0 means DEFERRED (#433): the answer waits for its
    // __consumer_offsets records to commit and the handler sends it itself.
    //
    // buf and out MUST NOT overlap.
    // outCap is the capacity of out[] in bytes.
    // Never throws.  Never allocates from heap in steady state.
    virtual size_t handleJoinGroup   (const uint8_t* buf, size_t len,
                                      int32_t clientVersion,   // the session's negotiated version: gates the filter trailer
                                      int64_t nowNs,
                                      uint8_t* out, size_t outCap) = 0;

    virtual size_t handleSyncGroup   (const uint8_t* buf, size_t len,
                                      int64_t nowNs,
                                      uint8_t* out, size_t outCap) = 0;

    virtual size_t handleHeartbeat   (const uint8_t* buf, size_t len,
                                      int64_t nowNs,
                                      uint8_t* out, size_t outCap) = 0;

    virtual size_t handleLeaveGroup  (const uint8_t* buf, size_t len,
                                      int64_t nowNs,
                                      uint8_t* out, size_t outCap) = 0;

    virtual size_t handleOffsetCommit(const uint8_t* buf, size_t len,
                                      int64_t nowNs,
                                      uint8_t* out, size_t outCap) = 0;

    virtual size_t handleOffsetFetch (const uint8_t* buf, size_t len,
                                      int64_t nowNs,
                                      uint8_t* out, size_t outCap) = 0;

    // No-hang fix rounds 4-5 (item 8 / N23, pre-v5 clients): the gateway
    // answers an old client UnknownMember in place of an Internal /
    // InvalidRequest it would ignore; these make that answer TRUE (the old
    // client re-joins on it).  Cold paths.  Default: nothing to evict.
    //
    // evictMember: the request's own (group, member) — only if that member is
    // mapped to `sessionId` (another session's member is never touched);
    // `memberId` empty = every member of `sessionId` in `groupId` (OffsetFetch
    // names the group only).  Other groups on the same session are untouched.
    virtual void evictMember(std::string_view groupId, std::string_view memberId, int64_t sessionId,
                             int64_t nowNs) { (void)groupId; (void)memberId; (void)sessionId; (void)nowNs; }
    // evictSessionMembers: every member on `sessionId` — ONLY for a request
    // whose body is undecodable (its group is unknown).
    virtual void evictSessionMembers(int64_t sessionId, int64_t nowNs) { (void)sessionId; (void)nowNs; }

    // Authentication Task 14: a live permission change took a session's READ on some groups away.  Every member of
    // `sessionId` whose group `mayRead(ctx, group)` refuses leaves its group (its subscriptions are revoked, the
    // session mapping dropped) - its next Heartbeat / Commit / Fetch is then answered NotAuthorized by the gateway's
    // own check.  Issue #212: so does every member of `sessionId` subscribed to a topic `mayReadTopic(ctx, topic)`
    // refuses (when set), even while its group stays readable - it must not keep an assignment of a topic it may no
    // longer read.  Returns the number of members removed.  Cold; both checks are plain functions (no allocation)
    // sharing `ctx`.
    struct GroupReadCheck {
        bool (*mayRead)(void* ctx, std::string_view groupId) noexcept = nullptr;
        void* ctx = nullptr;
        bool (*mayReadTopic)(void* ctx, std::string_view topicName) noexcept = nullptr;
    };
    virtual int removeUnauthorizedMembers(int64_t sessionId, int64_t nowNs, GroupReadCheck check) {
        (void)sessionId; (void)nowNs; (void)check;
        return 0;
    }
    // Issue #212: the session ended for good (revoked: user dropped / disabled, TERMINATE SESSIONS): its members leave
    // their groups now, so the group rebalances at once instead of at their heartbeat timeout.  Cold.  Default: nothing.
    virtual void removeSessionMembers(int64_t sessionId, int64_t nowNs) { (void)sessionId; (void)nowNs; }
};

} // namespace rune::gateway
