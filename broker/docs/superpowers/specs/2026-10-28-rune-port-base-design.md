# Rune port base: one port setting per cluster: design

**Status:** agreed, 2026-10-28. **Deferred:** start after the NAT-safe egress work
([2026-10-26-rune-nat-egress-design.md](2026-10-26-rune-nat-egress-design.md)) has merged; it adds the
response-control ports this layout places.
**Origin:** the Arcanum CLI review (several Rune clusters on one host; friendly ports).

## 1. Problem
A node binds 3 + 3W UDP ports today (W = workers), and 4 + 4W after NAT egress:

| Port | Count | Set by today |
|---|---|---|
| consensus | 1 | `RUNE_PEERS` (`id@host:consensus[:client[:repl...]]`) |
| client | 1 | `RUNE_PEERS` and `RUNE_CLIENT_ENDPOINT` (must agree) |
| replication | 1 | `RUNE_PEERS` |
| gateway response control (NAT egress) | 1 | derived |
| worker ingress | W | `RUNE_WORKER_INGRESS_BASE_PORT`, or derived |
| worker replication | W | `RUNE_WORKER_REPL_BASE_PORT`, or derived |
| worker egress control | W | `RUNE_WORKER_EGRESS_CONTROL_BASE_PORT`, or derived |
| worker response control (NAT egress) | W | derived |

- The three fixed ports are written by hand in `RUNE_PEERS`, again in `RUNE_CLIENT_ENDPOINT`, and again in compose's
  `ip_local_reserved_ports`.
- Derived worker ports (`deriveShardBasePorts`: 8200 + 192 × nodeId) depend only on the node id, so node 0 of two
  clusters on one host collide. Today the only fixes are distinct node ids per cluster, or three `*_BASE_PORT`
  variables plus a matching `RUNE_PEERS` entry.
- The usual 40111/40200/40222 sit inside Linux's default ephemeral range (32768–60999), so every compose file needs
  the `ip_local_reserved_ports` sysctl.

## 2. Decision
- **`RUNE_PORT_BASE=P`**, the same on every node of a cluster; default 20000. Node `id` owns the 256-port block
  `[P + 256 × id, P + 256 × id + 255]`:

  | Offset | Port |
  |---|---|
  | +0 | consensus |
  | +1 | client |
  | +2 | replication |
  | +3 | gateway response control |
  | +4 + w | worker ingress, w in [0, 63) |
  | +67 + w | worker replication |
  | +130 + w | worker egress control |
  | +193 + w | worker response control (ends at +255) |

  Four 63-wide worker planes plus four fixed ports fill exactly 256. A node with 64 workers
  (`NodeConfig::kMaxShardWorkers`) sets explicit ports.
- **`RUNE_PEERS` shrinks to `0@h1,1@h2,2@h3`.** Every node computes every peer's ports from P and the peer's id.
- **`RUNE_CLIENT_ENDPOINT` defaults** to `<client bind host>:P + 256 × self + 1`.
- **Explicit ports still win:** ports written in `RUNE_PEERS`, `RUNE_CLIENT_ENDPOINT` and the `*_BASE_PORT`
  variables override the derived ones, so existing deployments start unchanged. With no `RUNE_PORT_BASE` and no
  explicit ports, the node uses the default P (a change of default ports: release note).
- **Bounds:** a block that passes 65535 refuses to start, naming the node id and P. A block inside the kernel's
  ephemeral range keeps today's startup WARN (`HostDefaults.cpp`, `ephemeralPortConflicts`).
- **Clients** keep needing only one bootstrap address. `--host h` defaults its port to P + 1 (node 0's client port);
  `host:port` still works.

## 3. Several clusters on one host
- **A cluster is a P.** Give each cluster on a host its own base, at least `256 × (highest node id + 1)` apart:
  three-node clusters at 20000, 20768, 21536 and so on.
- **Capacity below the ephemeral range** (32768) for three-node clusters with ids 0–2:

  | P | Node blocks that fit | 3-node clusters |
  |---|---|---|
  | 20000 (default) | 49 | 16 |
  | 8192 | 96 | 32 |

  Today's derivation fits 127 nodes (42 three-node clusters) because its blocks are 192 wide and carry no fixed
  ports. Lowering the default P trades against other services that commonly sit near 8000–9000.
- **Each node keeps its own `RUNE_DATA_DIR`;** the Aeron dir follows it.
- **Docker:** each container has its own network namespace, so only published host ports can collide.

## 4. Seeing and checking ports
- **`rune-broker ports`** prints every port a config binds (node, plane, worker, port) and exits; it also takes
  `--cluster` to print all nodes. Operators use it for firewall rules and to compare two clusters before starting.
- **Startup** logs the same table.
- **Bind failures** already refuse to start, naming the port (`BrokerNode::anchorFixedEndpoints_`, review A-I5).
  Config validation already refuses collisions inside a node and with co-located peers in its own `RUNE_PEERS`
  (`NodeConfig::validateShardConfig`). A node cannot know about another cluster on its host: that clash shows only
  as the startup refusal, which `rune-broker ports` lets an operator rule out first.
- `nodeBoundPorts` is the one list behind all three (validation, the ephemeral-range WARN, `rune-broker ports`).

## 5. Out of scope
- Ports picked at bind time and announced. The consensus and client ports must be fixed (peers and clients reach them
  before any exchange). Worker ports could be announced, since clients already learn them from Metadata, but peers
  derive each other's worker replication ports from config; announcing them changes Metadata and peer replication.
  Revisit only with a concrete need.

## 6. Interaction matrix
- **Config sources:** P only; P plus explicit `RUNE_PEERS` ports; P plus `*_BASE_PORT`; explicit only (no P); none
  (default P). Self entry disagreeing with the derived value (refused, as today).
- **Topology:** 1 node and 3 nodes on one host; 3 nodes on three hosts; two and three clusters on one host with
  distinct P; two clusters with the same P (refused at bind, named).
- **Workers:** W = 1, 2, 63 (derived) and 64 (explicit required, refused without).
- **Bounds:** P such that the last block ends at 65535 (accepted) and at 65536 (refused); P inside the ephemeral
  range (WARN); P ≤ 1023 (refused).
- **Version skew:** a mixed cluster where one node sets P and another lists explicit ports for the same values
  (works); different P on two nodes (refused: self entry disagrees, or the peer is unreachable and named).
- **Timing:** restart of one node with an unchanged P; restart after changing P on one node only (refused).
- Tests: table-driven unit tests over `deriveNodePorts` and `NodeConfig` parsing, a 1-node and 3-node broker
  integration slice on derived ports, the docker e2e compose files moved to `RUNE_PORT_BASE` with the default config.

## 7. Review focus
- Every node derives the same port for every peer, from P and the id only.
- Explicit ports always win; no existing compose file or env changes behaviour except the documented default.
- Every bound port is in `nodeBoundPorts`, the startup table and `rune-broker ports`; none is missing from the
  bind anchors.
- No change on any hot path: port derivation is startup-only.
