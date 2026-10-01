// src/Completions.cpp — F8c C2: the Client-bound parts of Completions
// (registration handshake + the §3.3 release protocol).  The polled queue
// itself is header-only (rune/client/Completions.h).

#include <rune/client/Client.h>
#include <rune/client/Completions.h>

namespace rune::client {

Completions::Completions(Client& client, CompletionsConfig cfg)
    : Completions(cfg) {
    // Delegated: the object is fully constructed here, so if registration
    // throws the destructor runs — with client_ still null it frees the ring
    // locally (standalone path).
    client.registerCq_(*this);
}

Completions::~Completions() { release_(); }

void Completions::release_() noexcept {
    detail::CompletionRing* ring = std::exchange(ring_, nullptr);
    if (ring == nullptr) return;  // moved-from
    if (client_ == nullptr || ring->producerClosed()) {
        // Standalone, born closed, or handed back by Client close (§3.3 (b)):
        // nobody else will ever touch the ring again.
        delete ring;
        return;
    }
    // Registered and live: the Client decides (CqRelease, §3.3 (a)); it frees
    // the ring itself, or — when its close() is in progress — waits for the
    // hand-back and frees it here.  Never touches `client_` after a close
    // handed the ring back (the producer-closed check above).
    ring_ = ring;
    client_->releaseCq_(*this);
    ring_ = nullptr;
    client_ = nullptr;
}

} // namespace rune::client
