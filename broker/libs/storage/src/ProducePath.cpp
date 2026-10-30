// ProducePath.cpp — Shared produce-batch helper (E5.1.2b, moved to rune_storage E5.1.2d).
//
// Contains the core produce-batch logic extracted from
// ClientGateway::handleProduce (inline path) so that BOTH the gateway and the
// per-worker ingress (E5.1.2d) can call the same implementation and share
// idempotency / CRC / restamp / durability semantics.
//
// Moved from libs/gateway/src/ to libs/storage/src/ (E5.1.2d) to eliminate the
// rune_node ↔ rune_gateway circular-dep workaround.  Both rune_gateway and
// rune_node depend on rune_storage, so this is a true leaf location.
//
// See ProducePath.h for the full contract.

#include <rune/storage/ProducePathImpl.h>

namespace rune::storage {

template ProduceOutcome handleProduceBatch<Log, NoWriteHook>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, Log&,
    ProducerStateManager&, bool, int, int, uint8_t, bool, NoWriteHook&) noexcept;
template ProduceOutcome handleProduceBatch<InMemoryLog, NoWriteHook>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, InMemoryLog&,
    ProducerStateManager&, bool, int, int, uint8_t, bool, NoWriteHook&) noexcept;

} // namespace rune::storage
