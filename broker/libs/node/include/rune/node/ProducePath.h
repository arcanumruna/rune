#pragma once
// ProducePath.h — Forwarding header (E5.1.2d).
//
// The ProducePath implementation was moved to rune_storage (leaf lib) so that
// both rune_gateway and rune_node can use it without a circular dependency.
// This header forwards to the canonical location for any existing include sites.
//
// Previously: compiled into rune_gateway (E5.1.2b workaround).
// Now: compiled into rune_storage — rune_node, rune_gateway, and Worker all
//      pick it up through the rune_storage PUBLIC include path.

#include <rune/storage/ProducePath.h>
