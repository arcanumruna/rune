#include <rune/node/PeerResolverHub.h>

#include <spdlog/spdlog.h>

#include <netdb.h>
#include <sys/socket.h>

#include <algorithm>
#include <chrono>

namespace rune::node {

bool PeerResolverHub::resolveWithGetaddrinfo(const std::string& host, std::uint32_t& ipv4) {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;
    addrinfo* res = nullptr;
    if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || res == nullptr) return false;
    bool ok = false;
    for (addrinfo* r = res; r != nullptr && !ok; r = r->ai_next) {
        if (r->ai_family == AF_INET && r->ai_addr != nullptr) {
            ipv4 = reinterpret_cast<const sockaddr_in*>(r->ai_addr)->sin_addr.s_addr;
            ok   = true;
        }
    }
    ::freeaddrinfo(res);
    return ok;
}

namespace {
std::vector<std::string> managedHosts(const std::vector<std::string>& hosts) {
    std::vector<std::string> out;
    for (const auto& h : hosts) {
        if (h.empty() || rune::consensus::PeerHostTable::isNumericHost(h)) continue;
        if (std::find(out.begin(), out.end(), h) == out.end()) out.push_back(h);
    }
    return out;
}
} // namespace

PeerResolverHub::PeerResolverHub(const std::vector<std::string>& hosts, ResolveFn fn)
    : table_(managedHosts(hosts)), fn_(fn ? std::move(fn) : ResolveFn(&resolveWithGetaddrinfo)),
      hosts_(table_.size()) {}

PeerResolverHub::~PeerResolverHub() { stop(); }

void PeerResolverHub::resolveAllNow() {
    for (std::size_t i = 0; i < hosts_.size(); ++i) (void)probe_(i, 0);
}

rune::consensus::PeerResolutionSource PeerResolverHub::subscribe_(bool driver) {
    Sub s;
    s.ring   = std::make_unique<rune::consensus::PeerResolutionRing>();
    s.driver = driver;
    resync_(s);   // prefill with every known address
    subs_.push_back(std::move(s));
    return {&table_, subs_.back().ring.get()};
}

void PeerResolverHub::start() {
    if (runner_ || table_.size() == 0) return;   // nothing to resolve: no thread
    // A cold thread: it parks between probes (never spins a core).
    idle_   = std::make_unique<rune::consensus::BackoffIdleStrategy>(
        1, 1, std::chrono::milliseconds{1}, std::chrono::milliseconds{10});
    runner_ = std::make_unique<rune::consensus::AgentRunner>(agent_, *idle_, clock_);
    runner_->start();
}

void PeerResolverHub::stop() noexcept {
    if (runner_) {
        runner_->stopAndJoin();
        runner_.reset();
    }
}

int PeerResolverHub::doWork_(int64_t nowNs) {
    int work = 0;
    // Drivers first: a driver ring re-synchronised now re-announces to every
    // owner after it, so no owner re-adds against a stale driver cache.
    bool driverResynced = false, driverDirty = false;
    for (auto& s : subs_)
        if (s.driver && s.dirty) { resync_(s); driverResynced = true; driverDirty |= s.dirty; }
    for (auto& s : subs_) {
        if (s.driver) continue;
        if (driverDirty) { s.dirty = true; continue; }   // never ahead of a driver cache
        if (s.dirty || driverResynced) resync_(s);
    }
    for (std::size_t i = 0; i < hosts_.size(); ++i) {
        if (nowNs < hosts_[i].nextProbeNs) continue;
        ++work;
        if (probe_(i, nowNs)) publish_(i);
    }
    return work;
}

bool PeerResolverHub::probe_(std::size_t i, int64_t nowNs) {
    HostState& h = hosts_[i];
    std::uint32_t ip = 0;
    const bool ok = fn_(table_.host(i), ip);
    if (!ok) {
        h.nextProbeNs = nowNs + h.backoffNs;
        h.backoffNs   = std::min(h.backoffNs * 2, kPendingMaxNs);
        return false;   // keep the last known address (Aeron keeps its destination)
    }
    h.nextProbeNs = nowNs + kRecheckNs;
    h.backoffNs   = kPendingMinNs;
    if (h.has && h.ipv4 == ip) return false;
    char buf[INET_ADDRSTRLEN] = {};
    in_addr a{};
    a.s_addr = ip;
    ::inet_ntop(AF_INET, &a, buf, sizeof buf);
    spdlog::info("PeerResolverHub: peer host {} {} {}", table_.host(i), h.has ? "now at" : "resolved to", buf);
    h.has  = true;
    h.ipv4 = ip;
    return true;
}

void PeerResolverHub::publish_(std::size_t i) {
    // Driver caches first, owners second (see header).  A driver ring that
    // could not take it holds the owners back: they are re-announced after
    // that driver's resync (next pass), never ahead of it.
    bool driverMissed = false;
    for (auto& s : subs_)
        if (s.driver && (s.dirty || !post_(s, i))) { s.dirty = true; driverMissed = true; }
    for (auto& s : subs_)
        if (!s.driver && (driverMissed || s.dirty || !post_(s, i))) s.dirty = true;
}

bool PeerResolverHub::post_(Sub& s, std::size_t i) noexcept {
    return s.ring->offer(rune::consensus::kPeerResolvedV4, static_cast<std::int64_t>(i),
                         static_cast<std::int64_t>(hosts_[i].ipv4));
}

void PeerResolverHub::resync_(Sub& s) noexcept {
    s.dirty = false;
    for (std::size_t i = 0; i < hosts_.size(); ++i)
        if (hosts_[i].has && !post_(s, i)) { s.dirty = true; return; }
}

} // namespace rune::node
