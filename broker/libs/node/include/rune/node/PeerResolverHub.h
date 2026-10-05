#pragma once
// PeerResolverHub.h — the ONE cold thread that resolves peer hostnames
// (DR-15 fix round 1, 2026-10-05).  See rune/consensus/PeerResolution.h.
//
// - Owns the immutable PeerHostTable and one PeerResolutionRing per consumer
//   (embedded driver conductors + owners of peer publications).
// - Its agent ("rune-resolver", an AgentRunner with a parking backoff idle)
//   calls the (blocking) resolve function — getaddrinfo by default — for each
//   managed host on its own cadence: an unresolved host is re-probed with an
//   exponential backoff (10 ms .. 1 s), a resolved one re-checked every 1 s
//   (an IP change after a pod restart).  A first resolution or a changed
//   address is posted to EVERY consumer — driver caches first, owners second,
//   so a re-add triggered by the owner's event always finds the address in its
//   driver's cache (ring program order + the CnC command ring's ordering).
// - A ring that is full is re-synchronised (every known address re-posted) on
//   the next pass — never a lost event.
//
// The cadence is the cold thread's probe rate only; no correctness decision on
// any duty thread depends on time.
//
// Lifecycle (controller thread): construct → resolveAllNow() (startup thread,
// before any driver exists) → subscribe*() (each prefilled with the known
// addresses) → start() → stop().  subscribe*() only before start().

#include <rune/consensus/ConsensusAgent.h>   // Agent (complete before AgentRunner)
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/NanoClock.h>
#include <rune/consensus/PeerResolution.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace rune::node {

class PeerResolverHub {
public:
    /// Blocking resolve of `host` to an IPv4 address (network order).  Called
    /// on the hub thread (and on the startup thread by resolveAllNow) only.
    using ResolveFn = std::function<bool(const std::string& host, std::uint32_t& ipv4)>;

    /// getaddrinfo(AF_INET, SOCK_DGRAM) — Aeron's default resolver semantics.
    static bool resolveWithGetaddrinfo(const std::string& host, std::uint32_t& ipv4);

    /// Managed hosts = the distinct NON-numeric hosts of `hosts`.
    PeerResolverHub(const std::vector<std::string>& hosts, ResolveFn fn);
    ~PeerResolverHub();
    PeerResolverHub(const PeerResolverHub&)            = delete;
    PeerResolverHub& operator=(const PeerResolverHub&) = delete;

    [[nodiscard]] const rune::consensus::PeerHostTable& table() const noexcept { return table_; }

    /// Controller, before start(): one probe of every host (blocking).
    void resolveAllNow();
    /// Controller, before start(): a consumer ring, prefilled with every known address.
    [[nodiscard]] rune::consensus::PeerResolutionSource subscribeDriver() { return subscribe_(true); }
    [[nodiscard]] rune::consensus::PeerResolutionSource subscribeOwner() { return subscribe_(false); }
    /// Controller, before start(): an ad-hoc host-resolution port (the client
    /// gateway's reply-channel hostnames — fix round 2, LOW-4).
    [[nodiscard]] rune::consensus::HostResolvePort subscribeHostResolver();

    /// Start / stop (join) the hub thread (only when there is something to
    /// resolve: a managed host or a host-resolution port).  Idempotent.
    void start();
    void stop() noexcept;

    /// TEST SEAM: one hub pass at `nowNs` on the CALLING thread (the hub thread
    /// must not be running).  Returns hosts probed.
    int runOnceForTest(int64_t nowNs) { return doWork_(nowNs); }
    /// TEST SEAM: make every host due for a probe on the next pass (hub thread
    /// not running).
    void makeAllDueForTest() noexcept { for (auto& h : hosts_) h.nextProbeNs = 0; }

    static constexpr int64_t kPendingMinNs  = 10'000'000;      // 10 ms
    static constexpr int64_t kPendingMaxNs  = 1'000'000'000;   // 1 s
    static constexpr int64_t kRecheckNs     = 1'000'000'000;   // resolved: IP change check

private:
    struct HostState {
        bool          has         = false;
        std::uint32_t ipv4        = 0;
        int64_t       nextProbeNs = 0;
        int64_t       backoffNs   = kPendingMinNs;
    };
    struct Sub {
        std::unique_ptr<rune::consensus::PeerResolutionRing> ring;
        bool driver = false;
        bool dirty  = false;   // an offer failed: re-post every known address
    };
    class Agent final : public rune::consensus::Agent {
    public:
        explicit Agent(PeerResolverHub& hub) noexcept : hub_(hub) {}
        int doWork(int64_t nowNs) override { return hub_.doWork_(nowNs); }
        const char* name() const noexcept override { return "rune-resolver"; }
    private:
        PeerResolverHub& hub_;
    };

    rune::consensus::PeerResolutionSource subscribe_(bool driver);
    int  doWork_(int64_t nowNs);
    /// 0 = unresolved, 1 = same address confirmed, 2 = first known / changed.
    int  probe_(std::size_t i, int64_t nowNs);
    int  serveHostRequests_();
    void publish_(std::size_t i);
    bool post_(Sub& s, std::size_t i) noexcept;
    void resync_(Sub& s) noexcept;

    rune::consensus::PeerHostTable table_;
    ResolveFn                      fn_;
    std::vector<HostState>         hosts_;   // hub-thread-owned once started
    std::vector<Sub>               subs_;    // fixed before start()
    struct Port {
        std::unique_ptr<rune::consensus::HostResolveRing> req, resp;
        std::vector<std::pair<std::int64_t, std::int64_t>> outbox;   // {reqId, ipv4 | -1} not yet posted
    };
    std::vector<Port>              ports_;   // fixed before start()
    Agent                          agent_{*this};
    rune::consensus::MonotonicNanoClock clock_;
    std::unique_ptr<rune::consensus::BackoffIdleStrategy> idle_;
    std::unique_ptr<rune::consensus::AgentRunner>         runner_;
};

} // namespace rune::node
