# Release note: NAT-safe egress — response-control ports

## New ports and environment variables

Every per-session reply can ride an Aeron response publication whose control endpoint is a *response-control port*:
one for the gateway and one per worker.

- `RUNE_RESPONSE_CONTROL_BASE_PORT`: worker `w` binds base + `w` (shard mode only). 7th `RUNE_PEERS` segment.
- `RUNE_GATEWAY_RESPONSE_CONTROL_PORT`: the gateway's port (shard and legacy mode). 8th `RUNE_PEERS` segment.
- Absent = derived (shard mode); `0` is refused.

## Derived layout

The ports sit in the top of the node's derived egress-control block: gateway = `egressBase + 63`, worker `w` =
`egressBase + 63 - W + w` (W up to 31). Every node derives a node's ports by one rule: when that node's 6th segment is
absent or equals its derived egress base. Otherwise none are derived and the node logs a warning. An env value that
peers do not compute for the node and that is not advertised refuses to start.

## Upgrade order

Add segments 7 and 8 to `RUNE_PEERS` only after every node runs this version: older nodes refuse entries with more
than 6 segments.

## Co-located peers

The top W+1 ports of each derived egress-control block are now bound. A co-located peer with explicit ports inside
them is refused at startup.
