#pragma once
// PubAddBroker.h — re-adds of a LEGACY per-worker plane's peer publications run
// on the CONTROL thread (DR-15 fix round 1, review MEDIUM-2).
//
// A legacy per-worker plane rides the NODE Aeron client, whose admin calls
// (addExclusivePublication / findExclusivePublication) the control thread
// already makes.  A worker thread making them too would contend on the client
// conductor's admin lock — a new cross-thread lock path in a shared-nothing
// design.  So a legacy plane never makes an admin call after startup: its
// worker posts "add peer P" on req_, the control thread (AeronPartitionTransport
// ::poll — the node client's admin owner) adds + resolves it and hands the
// publication back on resp_ (ownership of a heap-held shared_ptr crosses as the
// ring message; the receiver frees the holder).
//
// Rings: req_ worker → control, resp_ control → worker (one consumer each).
// Cold path only (a peer that errored at startup, re-added on its host's
// resolution event or address change); allocation is fine here.
//
// D24(b): an add carries the address the owner's resolution inbox holds for
// the peer's host — the control thread adds on that NUMERIC endpoint (never the
// driver's possibly stale name cache); a publication superseded by a re-add is
// handed back (kRelease) and dropped on the control thread, never the worker.

#include <rune/consensus/ControlRing.h>
#include <rune/consensus/PeerResolution.h>
#include <rune/replication/Types.h>

#include <arpa/inet.h>
#include <netinet/in.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <spdlog/spdlog.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace rune::replication {

class PubAddBroker {
public:
    using PubPtr = std::shared_ptr<aeron::ExclusivePublication>;

    PubAddBroker(std::vector<std::pair<NodeId, std::string>> peerChannels, int32_t streamId)
        : channels_(std::move(peerChannels)), streamId_(streamId) {}

    /// Precondition: both the owner and the control thread are joined.
    ~PubAddBroker() {
        drainResults_([](NodeId, PubPtr) {});
        for (auto& o : outbox_) delete o.holder;
        (void)req_.poll([](std::int32_t type, std::int64_t, std::int64_t b) {
            if (type == kRelease) delete reinterpret_cast<PubPtr*>(static_cast<std::intptr_t>(b));
        });
    }
    PubAddBroker(const PubAddBroker&)            = delete;
    PubAddBroker& operator=(const PubAddBroker&) = delete;

    // ---- OWNER (worker) side -------------------------------------------------
    /// false = request ring full (the owner keeps the peer waiting).
    /// `ipv4` (network order) is the address the owner holds for the peer's
    /// host; ipKnown=false adds on the configured channel unchanged.
    [[nodiscard]] bool requestAdd(NodeId peer, std::uint32_t ipv4 = 0, bool ipKnown = false) noexcept {
        return req_.offer(kAdd, peer, ipKnown ? static_cast<std::int64_t>(ipv4) + 1 : 0);
    }
    /// Hands a superseded publication to the control thread to drop (the last
    /// reference releases it through the node client's admin lock).  false =
    /// ring full / allocation failed: `pub` is left untouched with the caller.
    [[nodiscard]] bool release(PubPtr& pub) noexcept {
        // D24 review MEDIUM-2: MOVE the reference into the holder before it is
        // offered — a copy would let the control thread drop its reference
        // first and leave the LAST one (the admin-lock release) on the worker.
        PubPtr* holder = nullptr;
        try { holder = new PubPtr(std::move(pub)); } catch (...) { return false; }   // pub untouched on throw
        if (!req_.offer(kRelease, 0, static_cast<std::int64_t>(reinterpret_cast<std::intptr_t>(holder)))) {
            pub = std::move(*holder);   // never offered: hand it back
            delete holder;
            return false;
        }
        return true;
    }

    /// onResult(peer, pub) — pub == nullptr: the registration failed.
    template <typename F>
    int pollResults(F&& onResult) noexcept { return drainResults_(onResult); }

    // ---- CONTROL side (the node client's admin thread) ------------------------
    int doWork(aeron::Aeron& client) noexcept {
        int work = req_.poll([&](std::int32_t type, std::int64_t a, std::int64_t b) {
            if (type == kRelease) {
                delete reinterpret_cast<PubPtr*>(static_cast<std::intptr_t>(b));   // drop on THIS thread
                ++releases_;
                return;
            }
            if (type != kAdd) return;
            const NodeId peer = static_cast<NodeId>(a);
            const std::string* ch = channelOf_(peer);
            if (ch == nullptr) { outbox_.push_back({peer, nullptr}); return; }
            try {
                lastAddChannel_ = b > 0 ? numericChannel_(*ch, static_cast<std::uint32_t>(b - 1)) : *ch;
                pending_.push_back({peer, client.addExclusivePublication(lastAddChannel_, streamId_)});
            } catch (const std::exception& e) {
                spdlog::warn("PubAddBroker: peer {} addExclusivePublication {} failed: {}", peer, lastAddChannel_,
                             e.what());
                outbox_.push_back({peer, nullptr});
            }
        });
        for (std::size_t i = 0; i < pending_.size();) {
            PubPtr pub;
            bool failed = false;
            try {
                pub = client.findExclusivePublication(pending_[i].regId);
            } catch (const std::exception& e) {
                spdlog::warn("PubAddBroker: peer {} publication unresolved: {}", pending_[i].peer, e.what());
                failed = true;
            }
            if (!pub && !failed) { ++i; continue; }
            outbox_.push_back({pending_[i].peer, pub ? new PubPtr(std::move(pub)) : nullptr});
            pending_[i] = pending_.back();
            pending_.pop_back();
            ++work;
        }
        std::size_t sent = 0;
        for (; sent < outbox_.size(); ++sent) {
            const Out& o = outbox_[sent];
            if (!resp_.offer(kResult, o.peer, static_cast<std::int64_t>(reinterpret_cast<std::intptr_t>(o.holder))))
                break;   // full: the rest go next pass (order kept)
        }
        if (sent > 0) outbox_.erase(outbox_.begin(), outbox_.begin() + static_cast<std::ptrdiff_t>(sent));
        return work;
    }

    [[nodiscard]] const std::string& lastAddChannelForTest() const noexcept { return lastAddChannel_; }
    [[nodiscard]] uint64_t releasesForTest() const noexcept { return releases_; }

private:
    static constexpr std::int32_t kAdd = 1, kResult = 2, kRelease = 3;
    struct Pending { NodeId peer; int64_t regId; };
    struct Out     { NodeId peer; PubPtr* holder; };

    template <typename F>
    int drainResults_(F&& onResult) noexcept {
        return resp_.poll([&](std::int32_t type, std::int64_t a, std::int64_t b) {
            if (type != kResult) return;
            auto* holder = reinterpret_cast<PubPtr*>(static_cast<std::intptr_t>(b));
            PubPtr pub;
            if (holder != nullptr) { pub = std::move(*holder); delete holder; }
            onResult(static_cast<NodeId>(a), std::move(pub));
        });
    }
    [[nodiscard]] static std::string numericChannel_(const std::string& channel, std::uint32_t ipv4) {
        in_addr a{};
        a.s_addr = ipv4;
        char buf[INET_ADDRSTRLEN] = {};
        if (::inet_ntop(AF_INET, &a, buf, sizeof buf) == nullptr) return channel;
        return rune::consensus::PeerHostTable::withEndpointHost(channel, buf);
    }
    [[nodiscard]] const std::string* channelOf_(NodeId peer) const noexcept {
        for (const auto& [id, ch] : channels_) if (id == peer) return &ch;
        return nullptr;
    }

    std::vector<std::pair<NodeId, std::string>> channels_;
    int32_t                                     streamId_;
    rune::consensus::ControlRing<1024>          req_;    // owner → control
    rune::consensus::ControlRing<1024>          resp_;   // control → owner
    std::vector<Pending>                        pending_;   // control-owned
    std::vector<Out>                            outbox_;    // control-owned
    std::string                                 lastAddChannel_;   // control-owned
    uint64_t                                    releases_ = 0;     // control-owned
};

} // namespace rune::replication
