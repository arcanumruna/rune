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
// resolution event); allocation is fine here.

#include <rune/consensus/ControlRing.h>
#include <rune/replication/Types.h>

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
    }
    PubAddBroker(const PubAddBroker&)            = delete;
    PubAddBroker& operator=(const PubAddBroker&) = delete;

    // ---- OWNER (worker) side -------------------------------------------------
    /// false = request ring full (the owner keeps the peer waiting).
    [[nodiscard]] bool requestAdd(NodeId peer) noexcept { return req_.offer(kAdd, peer); }

    /// onResult(peer, pub) — pub == nullptr: the registration failed.
    template <typename F>
    int pollResults(F&& onResult) noexcept { return drainResults_(onResult); }

    // ---- CONTROL side (the node client's admin thread) ------------------------
    int doWork(aeron::Aeron& client) noexcept {
        int work = req_.poll([&](std::int32_t type, std::int64_t a, std::int64_t) {
            if (type != kAdd) return;
            const NodeId peer = static_cast<NodeId>(a);
            const std::string* ch = channelOf_(peer);
            if (ch == nullptr) { outbox_.push_back({peer, nullptr}); return; }
            try {
                pending_.push_back({peer, client.addExclusivePublication(*ch, streamId_)});
            } catch (const std::exception& e) {
                spdlog::warn("PubAddBroker: peer {} addExclusivePublication {} failed: {}", peer, *ch, e.what());
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

private:
    static constexpr std::int32_t kAdd = 1, kResult = 2;
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
};

} // namespace rune::replication
