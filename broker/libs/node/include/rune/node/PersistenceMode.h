#pragma once
// PersistenceMode — a topic's log backend (Phase 2b E: "memory topics" are now
// TRANSIENT topics).  Its own header so TopicSchema (SchemaRegistry.h) can carry
// it without pulling in NodeConfig.h.
//
// Values equal the DefineTopic / TopicSchema wire byte and are APPEND-ONLY.
#include <cstdint>

namespace rune::node {

enum class PersistenceMode : uint8_t {
    Disk      = 0,   ///< On-disk Log (default). Survives node restarts.
    Transient = 1,   ///< InMemoryLog: replicated to the ISR before ack, NEVER written to
                     ///  disk.  Metadata survives a restart; data does not (a restarted
                     ///  node rejoins empty; after a full-cluster restart the topic is
                     ///  empty).  A transient topic that must survive a node death
                     ///  must use majority or all-in-sync durability.
};

/// Wire byte → mode.  Only 1 is Transient; callers reject values > 1 at decode.
[[nodiscard]] constexpr PersistenceMode persistenceFromWire(uint8_t b) noexcept {
    return b == 1 ? PersistenceMode::Transient : PersistenceMode::Disk;
}
[[nodiscard]] constexpr uint8_t persistenceToWire(PersistenceMode m) noexcept {
    return static_cast<uint8_t>(m);
}

} // namespace rune::node
