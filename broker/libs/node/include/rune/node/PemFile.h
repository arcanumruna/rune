#pragma once
// PemFile.h — Task 9 (RUNE_TLS): cert/key FILE PATH -> PEM TEXT loading.
//
// Deliberately its OWN header/.cpp (not folded into NodeConfig.cpp, which
// includes Worker.h -> RecordsPush.h -> <ExclusivePublication.h>, i.e. Aeron):
// this keeps loadPemFile() usable from an Aeron-free unit test target the
// same way Task 8/8b extracted CryptoState.h / CryptoFrame.h / RecordsSeal.h
// out of their Aeron-linked homes for the same reason (see
// libs/gateway/tests/RequireTlsDowngradeTest.cpp's file header).
//
// Cold path only (startup / SIGHUP reload) — never the hot path.

#include <string>

namespace rune::node {

/// Read a PEM file's full contents into `out` (text mode — PEM is ASCII).
/// Returns false (and clears `out`) on any failure: missing file, unreadable,
/// or empty. Never throws.
[[nodiscard]] bool loadPemFile(const std::string& path, std::string& out) noexcept;

} // namespace rune::node
