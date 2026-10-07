#pragma once
// RingDoorbell moved to rune/consensus/detail/RingDoorbell.h (D19 L3: rune_storage
// embeds one in its concrete provisioning channel); node code keeps its names.
#include <rune/consensus/detail/RingDoorbell.h>

namespace rune::node::detail {
using rune::consensus::detail::RingDoorbell;
using rune::consensus::detail::RingWatch;
} // namespace rune::node::detail
