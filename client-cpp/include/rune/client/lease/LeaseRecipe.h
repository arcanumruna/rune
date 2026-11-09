// include/rune/client/lease/LeaseRecipe.h - LeaseRecipeHost<Capacity> and the shared recipe core of LeaseOwnership / LeaseElection (Task 20; spec 12.3).
//
// The recipes are etcd's concurrency.Mutex / Election over ONE lease topic (one partition, a string(N) key, N >= name + 26) and ONE shared
// LeaseWatch.  A LeaseRecipeHost ties together (client, session, watch, topic) for a process: it owns the single completion queue of the
// recipes' own operations (grants, key reads, best-effort revokes) and routes the session's lost / at-risk callbacks to the recipe whose lease
// they concern.  Many recipes (one per name) share a host, so a gateway with 2 000 elections runs one watch subscription, not 2 000.
//
// Wiring (all on the ONE thread that owns the session, the watch and the recipes; the client may run in either mode):
//   LeaseSession<..> session(client, OnLost, OnAtRisk);          // OnLost / OnAtRisk call host.onLost / host.onAtRisk FIRST (below)
//   LeaseWatch<T>    watch(client, topic, {.names = N, .waitersPerName = W, ...});
//   session.observe(watch);                                      // the session learns the ends of its leases from the watch
//   LeaseRecipeHost<N> host(client, session, watch, topic);
//   LeaseOwnership m(host, "jobs/42");  LeaseElection e(host, "leader/eu");   // BEFORE watch.startFromNow(prefixes): they register their names
//   watch.startFromNow(...);
//   loop: client.doWork(); session.doWork(now); watch.doWork(now); host.doWork(now);
// Forwarding: the application's session callbacks call `host.onLost(ref, reason)` and `host.onAtRisk(ref, edge)` first; each returns true when
// `ref` belongs to a recipe (the recipe has handled it: the application does nothing more) and false otherwise.  Destroy the recipes before the
// host, and the host before the session and the watch (a watch destroyed first makes its recipes invalid() and every call InvalidArgument).
//
// The key.  Every grant a recipe sends has a NEW key:  name + '/' + <holder id, 16 lowercase hex> + '-' + <campaign number, 8 lowercase hex>,
// zero-padded to the topic's key width N (a string(N) column puts exactly N bytes, zero padded, on the wire).  The holder id is the session's
// holderId() (a 64-bit random id drawn once); the campaign number counts EVERY grant the recipe sends (claim, tryClaim, campaign and the re-grant
// after an unknown outcome), starting at 1.  A re-campaign after a release or a lost lease therefore never meets its old key (Herald F1).
//
// Completions.  claim / tryClaim / campaign take a CompletionTarget and a token and complete it exactly once with OpKind::LeaseCampaign: Ok
// (value = the lease_version, the fencing token) when the recipe owns the name; DeliveryFailed with ClientErrorCode::LeaseClaimed (tryClaim, the
// name is owned), LeaseLost (the lease was lost first) or LeaseReleased (release() first); or the status of a definitive failure of the grant
// (BrokerError, RecordsRejected, ...).  The completion is delivered from the thread that
// drives the recipes (claim() / release() / host.doWork() / the session's callbacks): a CompletionHandler target is invoked inline, a
// Completions target is written directly (its credit is reserved by the call).  The second needs the client in Invoker mode - in Thread mode
// the duty thread is the queue's one producer, so claim() answers InvalidArgument for a Completions target there; a handler works in both.
//
// Unknown grant outcomes.  A grant answered VerdictUnknown, StoredIdUnknown, DeliveryFailed, Timeout or SessionLost may have been applied.  The
// recipe reads its key from the lease view: live with lease_holder == its holder id -> adopted at that version (no second grant); anything else
// -> a new grant with the next campaign number.  At most kMaxRegrants unknown outcomes in a row, then the campaign completes with the last status.
#pragma once

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/lease/LeaseWatch.h>

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace rune::client {

/// What a recipe tells its owner (function pointers + a context, like LeaseEventFn).  Both optional.
struct LeaseRecipeCallbacks {
    void* ctx = nullptr;
    /// The held lease (an owner, or a suspended one) was lost.  Runs once per loss, after a pending campaign completed LeaseLost.
    void (*lost)(void* ctx, LostReason reason) = nullptr;
    /// The held lease entered / left the at-risk state.  Runs on the transition only: Entered suspends the recipe (isOwner() == false), Cleared
    /// re-evaluates it (it owns again only if its key is still the lowest).
    void (*atRisk)(void* ctx, AtRiskEdge edge) = nullptr;
};

namespace detail {

class LeaseRecipeHostCore;

/// The non-template body shared by LeaseOwnership and LeaseElection (src/lease/LeaseRecipe.cpp).
class LeaseRecipeCore {
public:
    enum class Mode : uint8_t { Claim, TryClaim };
    static constexpr uint32_t kKeySuffix   = 26;   ///< '/' + 16 hex + '-' + 8 hex
    static constexpr uint32_t kMaxRegrants = 8;    ///< unknown grant outcomes in a row before the campaign gives up

    /// `rowCapacity`: the bytes of the caller's row a LeaseElection keeps for re-grants (0 = the recipe grants the topic's template row).
    LeaseRecipeCore(LeaseRecipeHostCore& host, std::string_view name, const LeaseRecipeCallbacks& cb, uint32_t rowCapacity);
    ~LeaseRecipeCore();
    LeaseRecipeCore(const LeaseRecipeCore&)            = delete;
    LeaseRecipeCore& operator=(const LeaseRecipeCore&) = delete;

    // ---- the front ends ----
    [[nodiscard]] bool valid() const noexcept { return valid_; }
    /// Grants a new key.  InvalidArgument: invalid(), not idle (a claim / release / loss is still settling), a row outside the election's limits.
    /// QueueFull / BackPressured / Closed: the grant could not be sent (nothing happened).  `row` empty = the topic's template row (ownership).
    [[nodiscard]] OpResult claim(Mode mode, std::span<const uint8_t> row, CompletionTarget target, Token token);
    /// The same, with the row already encoded into rowBuffer()[0, rowLen).
    [[nodiscard]] OpResult claimStored(Mode mode, std::size_t rowLen, CompletionTarget target, Token token);
    [[nodiscard]] OpResult release();
    /// A fenced update of the held lease's row (owner only).  The row is copied into rowBuffer() with this recipe's key patched in.
    [[nodiscard]] OpResult proclaim(std::span<const uint8_t> row, CompletionTarget target, Token token);
    [[nodiscard]] OpResult proclaimStored(std::size_t rowLen, CompletionTarget target, Token token);
    [[nodiscard]] std::span<uint8_t> rowBuffer() noexcept { return {row_.data(), row_.size()}; }
    [[nodiscard]] bool    isOwner() const noexcept { return std::holds_alternative<Owner>(st_); }
    [[nodiscard]] int64_t token() const noexcept { const auto* o = std::get_if<Owner>(&st_); return o != nullptr ? o->version : 0; }
    [[nodiscard]] std::optional<std::pair<int64_t, std::span<const uint8_t>>> leader() const noexcept;
    using ObserveFn = void (*)(void* ctx, int64_t version, std::span<const uint8_t> key, std::span<const uint8_t> bytes);
    /// One observer: called once per change of the name's lowest (key, version) (see LeaseElection::observe).  false: invalid() or already set.
    [[nodiscard]] bool setObserver(ObserveFn fn, void* ctx);
    [[nodiscard]] uint64_t evaluations() const noexcept { return evaluations_; }   ///< test accessor: how often the owner check ran
    [[nodiscard]] uint32_t campaignNo() const noexcept { return campaignNo_; }
    [[nodiscard]] std::span<const uint8_t> key() const noexcept { return {key_, keyLen_}; }   ///< the current campaign's key

    // ---- the host's side ----
    [[nodiscard]] bool holdsRef(LeaseRef ref) const noexcept;
    void onLost(LostReason reason);
    void onAtRisk(AtRiskEdge edge);
    void onCompletion(uint32_t tag, uint32_t campaignNo, const Completion& c, std::span<const uint8_t> payload);
    [[nodiscard]] bool releasing() const noexcept { return std::holds_alternative<Releasing>(st_); }
    void pollReleasing();
    static void onWatchEvent(void* ctx, const LeaseEvent& ev);
    static void onWatchGone(void* ctx, LeaseWatchBase* watch);
    void hostGone() noexcept;

private:
    // ---- the rich state: each arm holds exactly the fields it needs ----
    struct Idle {};
    struct Granting { uint32_t campaignNo = 0; int64_t sentNs = 0; };             ///< the grant is in flight
    struct Reading { uint32_t campaignNo = 0; int64_t sentNs = 0; OpStatus unknown = OpStatus::Timeout; uint16_t unknownCode = 0; };   ///< unknown outcome: reading the key
    struct Joining { int64_t version = 0; LeaseRef ref{}; };                      ///< granted, in the session; waiting for the watch to hold the key
    struct Waiting {                                                              ///< not the lowest: only the predecessor's end re-evaluates
        int64_t  version = 0;
        LeaseRef ref{};
        int64_t  predVersion = 0;
        uint8_t  predKeyLen = 0;
        uint8_t  predKey[rune::protocol::kMaxLeaseKeyBytes]{};
    };
    struct Owner { int64_t version = 0; LeaseRef ref{}; };
    struct Suspended { int64_t version = 0; LeaseRef ref{}; bool wasOwner = false; };   ///< at risk: not an owner until Cleared
    struct Releasing { LeaseRef ref{}; };                                         ///< the session's fenced revoke is on its way
    struct Abandoned { uint32_t campaignNo = 0; };                                ///< released while the grant / read was in flight: clean up its answer
    using State = std::variant<Idle, Granting, Reading, Joining, Waiting, Owner, Suspended, Releasing, Abandoned>;

    struct Pending {   ///< the campaign the caller waits for
        bool           active = false;
        Completions*   cq = nullptr;
        CompletionHandler handler{};
        Token          token = 0;
        uint32_t       reserved = 0;
    };

    [[nodiscard]] OpResult sendGrant_(int64_t& sentNs);
    void complete_(OpStatus st, uint16_t err, int64_t value);
    void startRead_(OpStatus unknown, uint16_t code, int64_t sentNs);
    void regrant_(OpStatus unknown, uint16_t code);
    void join_(int64_t version, uint64_t ttlMs, int64_t sentNs);
    void settle_();
    void evaluate_(int64_t version, LeaseRef ref);
    void becomeOwner_(int64_t version, LeaseRef ref);
    void bestEffortRevoke_(int64_t version);
    void observeCheck_(const LeaseEvent* ev);
    void onKeyEvent_(const LeaseEvent& ev);
    [[nodiscard]] bool refOf_(LeaseRef& ref, int64_t& version) const noexcept;
    void buildKey_(uint32_t campaignNo);

    LeaseRecipeHostCore* host_ = nullptr;
    LeaseWatchBase*      watch_ = nullptr;
    int32_t              nameId_ = -1;
    bool                 valid_ = false;
    int                  hostIdx_ = -1;
    LeaseRecipeCallbacks cb_{};
    State                st_{};
    Mode                 mode_ = Mode::Claim;
    Pending              pend_{};
    uint32_t             campaignNo_ = 0;
    uint32_t             regrants_ = 0;
    uint64_t             evaluations_ = 0;
    uint8_t              nameLen_ = 0;
    uint8_t              name_[rune::protocol::kMaxLeaseKeyBytes]{};
    uint8_t              keyLen_ = 0;
    uint8_t              key_[rune::protocol::kMaxLeaseKeyBytes]{};
    std::size_t          rowLen_ = 0;   ///< the stored row's length (0 = grant the template)
    std::vector<uint8_t> row_;
    ObserveFn            observer_ = nullptr;
    void*                observerCtx_ = nullptr;
    int64_t              lastReported_ = -1;
};

/// The non-template body of a LeaseRecipeHost.
class LeaseRecipeHostCore {
public:
    static constexpr uint32_t kTagGrant = 1, kTagRead = 2, kTagRevoke = 3;

    LeaseRecipeHostCore(Client& client, LeaseSessionCore& session, LeaseWatchBase& watch, LeaseTopicCore& topic, LeaseRecipeCore** table,
                        uint32_t capacity, int16_t* slotMap, uint32_t slotMapSize);
    ~LeaseRecipeHostCore();
    LeaseRecipeHostCore(const LeaseRecipeHostCore&)            = delete;
    LeaseRecipeHostCore& operator=(const LeaseRecipeHostCore&) = delete;

    /// Forwarded from the session's callbacks: true when `ref` belongs to a recipe (handled).
    bool onLost(LeaseRef ref, LostReason reason);
    bool onAtRisk(LeaseRef ref, AtRiskEdge edge);
    /// Drains the host's completion queue (grants, reads) and finishes releases.  Returns the work done.
    int doWork(int64_t nowNs);

    /// The host can serve recipes: a fixed-width key column, a readable lease view layout, a session no larger than the slot map.
    [[nodiscard]] bool usable() const noexcept { return usable_; }
    [[nodiscard]] uint64_t holderId() const noexcept { return session_.holderId(); }
    [[nodiscard]] Client& client() noexcept { return client_; }
    [[nodiscard]] LeaseSessionCore& session() noexcept { return session_; }
    [[nodiscard]] LeaseWatchBase& watch() noexcept { return watch_; }
    [[nodiscard]] LeaseTopicCore& topic() noexcept { return topic_; }
    [[nodiscard]] Completions& queue() noexcept { return cq_; }
    [[nodiscard]] uint32_t recipeCount() const noexcept { return count_; }

    // ---- the recipes' side ----
    [[nodiscard]] int attach(LeaseRecipeCore* rc) noexcept;
    void detach(int idx) noexcept;
    void mapRef(LeaseRef ref, int idx) noexcept;
    void unmapRef(LeaseRef ref) noexcept;
    [[nodiscard]] static Token tokenOf(int idx, uint32_t tag, uint32_t campaignNo) noexcept {
        return (uint64_t{static_cast<uint32_t>(idx)} << 36) | (uint64_t{tag} << 32) | campaignNo;
    }
    void noteReleasing(int delta) noexcept { releasing_ = static_cast<uint32_t>(static_cast<int64_t>(releasing_) + delta); }

private:
    static CompletionsConfig queueConfig_(uint32_t capacity, uint32_t viewRowLen);

    Client&           client_;
    LeaseSessionCore& session_;
    LeaseWatchBase&   watch_;
    LeaseTopicCore&   topic_;
    LeaseRecipeCore** table_;
    uint32_t          capacity_;
    int16_t*          slotMap_;
    uint32_t          slotMapSize_;
    uint32_t          count_ = 0;
    uint32_t          releasing_ = 0;
    bool              usable_ = false;
    Completions       cq_;
};

template <class X>
[[nodiscard]] decltype(auto) recipeCoreOf(X& x) noexcept {
    if constexpr (requires { x.core(); }) return x.core();
    else return (x);
}

}  // namespace detail

/// One host per (client, session, watch, topic).  Capacity: the most recipes (names) it serves, 1..4095.  SessionCapacity: the largest
/// LeaseSession capacity it accepts (the lease-slot -> recipe map, 2 bytes a slot); a larger session makes the host unusable.
template <uint32_t Capacity, uint32_t SessionCapacity = 4095>
class LeaseRecipeHost {
    static_assert(Capacity >= 1 && Capacity <= 4095, "LeaseRecipeHost: Capacity must be 1..4095 (the recipe index rides in a 12-bit token field)");
    static_assert(SessionCapacity >= 1 && SessionCapacity <= 4095, "LeaseRecipeHost: SessionCapacity must be 1..4095");

public:
    /// `session`: a LeaseSession<..> (or its core); `topic`: a LeaseTopic<T> (or its core).  Everything is allocated here; nothing afterwards.
    template <class SessionT, class TopicT>
    LeaseRecipeHost(Client& client, SessionT& session, LeaseWatchBase& watch, TopicT& topic)
        : core_(client, detail::recipeCoreOf(session), watch, detail::recipeCoreOf(topic), table_.data(), Capacity, slotMap_.data(),
                SessionCapacity) {}
    LeaseRecipeHost(const LeaseRecipeHost&)            = delete;
    LeaseRecipeHost& operator=(const LeaseRecipeHost&) = delete;

    bool onLost(LeaseRef ref, LostReason reason) { return core_.onLost(ref, reason); }
    bool onAtRisk(LeaseRef ref, AtRiskEdge edge) { return core_.onAtRisk(ref, edge); }
    int  doWork(int64_t nowNs) { return core_.doWork(nowNs); }
    [[nodiscard]] bool usable() const noexcept { return core_.usable(); }
    [[nodiscard]] uint64_t holderId() const noexcept { return core_.holderId(); }
    [[nodiscard]] uint32_t recipeCount() const noexcept { return core_.recipeCount(); }
    [[nodiscard]] detail::LeaseRecipeHostCore& core() noexcept { return core_; }

private:
    std::array<detail::LeaseRecipeCore*, Capacity> table_{};
    std::array<int16_t, SessionCapacity>           slotMap_{};
    detail::LeaseRecipeHostCore                    core_;
};

}  // namespace rune::client
