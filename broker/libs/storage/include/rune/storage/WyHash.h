#pragma once
// wyhash lives in protocol/WyHash.h (shared with the client); every broker caller keeps using rune::storage::detail.
#include <rune/protocol/WyHash.h>

namespace rune::storage {
namespace detail {
using rune::protocol::detail::kSecret0;
using rune::protocol::detail::kSecret1;
using rune::protocol::detail::kSecret2;
using rune::protocol::detail::kSecret3;
using rune::protocol::detail::wymix;
using rune::protocol::detail::wyread3;
using rune::protocol::detail::wyread4;
using rune::protocol::detail::wyread8;
using rune::protocol::detail::wyhash64;
}  // namespace detail
}  // namespace rune::storage
