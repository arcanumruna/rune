#pragma once
// UdpMessageSink.h — moved to rune/consensus/UdpMessageSink.h (Task 11 fix 2:
// the consensus publisher calls the concrete sink directly).  Alias kept.
#include <rune/consensus/UdpMessageSink.h>

namespace rune::node {
using UdpMessageSink = rune::consensus::UdpMessageSink;
} // namespace rune::node
