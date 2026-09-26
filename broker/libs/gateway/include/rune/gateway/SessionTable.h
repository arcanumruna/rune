#pragma once
// SessionTable.h — Per-session egress tracking for the ClientGateway (BR-4).
//
// Design:
//   - Monotonically-increasing sessionId generator (starts at 1).
//   - Flat unordered_map: sessionId → Session.
//   - All operations are called from the single doWork() thread — no locks.

#include <Aeron.h>
#include <ExclusivePublication.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

namespace rune::gateway {

// ---------------------------------------------------------------------------
// Session — one connected client.
// ---------------------------------------------------------------------------
struct Session {
    int64_t                                        sessionId   = 0;
    std::shared_ptr<aeron::ExclusivePublication>   egress;        // acks/responses (gateway-written)
    int64_t                                        lastSeenNs  = 0;

    // Hybrid egress: the client's egress endpoint (captured at Connect) and a
    // SECOND publication to it, dedicated to Records pushed by the owning worker
    // (per-consumer UNICAST → independent per-consumer offsets).  Lazily created
    // on the first subscribe and written ONLY by the worker thread (the gateway
    // never touches it — shared-nothing single-writer).  Dropped with the session.
    std::string                                    egressChannel;
    int32_t                                        egressStreamId = 0;
    std::shared_ptr<aeron::ExclusivePublication>   recordEgress;  // Records (worker-written)
};

// ---------------------------------------------------------------------------
// SessionTable
// ---------------------------------------------------------------------------
class SessionTable {
public:
    SessionTable() = default;

    // No copies — owns ExclusivePublications via shared_ptr (but the table
    // itself is not meant to be copied).
    SessionTable(const SessionTable&)            = delete;
    SessionTable& operator=(const SessionTable&) = delete;

    // -----------------------------------------------------------------------
    // nextSessionId() — monotonically-increasing, starts at 1.
    // -----------------------------------------------------------------------
    [[nodiscard]] int64_t nextSessionId() noexcept {
        return ++nextId_;
    }

    // -----------------------------------------------------------------------
    // add() — insert a new session.  Returns a reference valid until remove().
    // -----------------------------------------------------------------------
    Session& add(int64_t                                      sessionId,
                 std::shared_ptr<aeron::ExclusivePublication> egress,
                 int64_t                                      nowNs,
                 std::string                                  egressChannel = {},
                 int32_t                                      egressStreamId = 0) {
        Session s;
        s.sessionId      = sessionId;
        s.egress         = std::move(egress);
        s.lastSeenNs     = nowNs;
        s.egressChannel  = std::move(egressChannel);
        s.egressStreamId = egressStreamId;
        auto [it, _] = sessions_.emplace(sessionId, std::move(s));
        return it->second;
    }

    // -----------------------------------------------------------------------
    // find() — returns nullptr if the session is absent.
    // -----------------------------------------------------------------------
    [[nodiscard]] Session* find(int64_t sessionId) noexcept {
        auto it = sessions_.find(sessionId);
        if (it == sessions_.end()) return nullptr;
        return &it->second;
    }

    // -----------------------------------------------------------------------
    // remove() — returns true if the session existed and was removed.
    // -----------------------------------------------------------------------
    bool remove(int64_t sessionId) noexcept {
        return sessions_.erase(sessionId) > 0;
    }

    // -----------------------------------------------------------------------
    // touch() — update lastSeenNs (no-op if absent; silently ignored).
    // -----------------------------------------------------------------------
    void touch(int64_t sessionId, int64_t nowNs) noexcept {
        auto it = sessions_.find(sessionId);
        if (it != sessions_.end()) {
            it->second.lastSeenNs = nowNs;
        }
    }

    // -----------------------------------------------------------------------
    // size() — number of active sessions.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t size() const noexcept {
        return sessions_.size();
    }

    // -----------------------------------------------------------------------
    // forEach() — iterate for BR-5 timeout reaping.  Callback: (Session&) → void.
    // -----------------------------------------------------------------------
    template <typename Fn>
    void forEach(Fn&& fn) {
        for (auto& [id, session] : sessions_) {
            fn(session);
        }
    }

private:
    int64_t                              nextId_   = 0;
    std::unordered_map<int64_t, Session> sessions_;
};

} // namespace rune::gateway
