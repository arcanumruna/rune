# Release note: DR bugs — derived shard ports, link MTU, per-worker egress host

These changes affect deployments and upgrades.

## 1. Derived shard ports moved below the ephemeral range — no rolling upgrade across this change

When a broker runs in shard mode (the default) with no explicit per-worker base ports, it derives them from
its node id: node *n* uses the block `[8200 + 192n, 8200 + 192n + 191]`. The old block started at **41000**.

- **Why:** the old block lay inside Linux's ephemeral port range (32768–60999). A broker restarted after a crash
  could find its own replication port taken by one of its own auto-bound sockets, and refused to start
  ("Address already in use").
- **Every node computes the derived ports of every peer locally.** A cluster that mixes old and new binaries
  disagrees on every derived port: replication and per-worker client traffic go to ports nobody binds.
  **A rolling upgrade across this change is not supported.** Stop every node, upgrade, start every node. A
  cluster that sets explicit base ports in `RUNE_PEERS` (5th–6th segments) or `RUNE_WORKER_*_BASE_PORT` is
  unaffected.
- The startup "effective config" line now names the base: `ports{... derivedBase=8200}`. Two nodes logging
  different values are a mixed-version cluster.
- Any port a node binds inside the kernel's ephemeral range (explicit ports included) is a startup WARN naming
  the sysctl that reserves it (`net.ipv4.ip_local_reserved_ports`). The docker compose files reserve theirs.
- Derived blocks of node ids ≥ 114 overlap the Kubernetes NodePort range (30000–32767) on hostNetwork
  deployments; set explicit ports there.

## 2. Per-worker egress control host is derived from the advertised host

`RUNE_WORKER_EGRESS_CONTROL_HOST` used to default to `127.0.0.1`, which silently turned per-worker egress off
(clients fell back to the legacy egress path) in every multi-host or container deployment. It now defaults to
the node's advertised `RUNE_PEERS` host when numeric, else the wildcard. An explicit loopback value on a node
advertised at a non-loopback host refuses to start. In legacy worker mode, a per-worker egress whose control
port cannot be bound now refuses to start (it used to WARN and disable that worker's egress).

## 3. Link MTU below the Aeron MTU is reported

- **GSO:** a UDP GSO batch whose segment exceeds a destination's path MTU used to stall that publication silently.
  Such a group is now sent plainly (IP-fragmented). This needs the rune-protocol `fix/dr-bugs` GSO bindings.
- **Watchdog:** a control-thread watchdog probes the interface MTU every 5 s.
  - When the MTU drops below the Aeron MTU plus 28 (IPv4) or 48 (IPv6) bytes, it logs an ERROR once. Datagrams
    above the link MTU arriving from peers or clients are dropped by the kernel.
  - When the link is restored, it logs a WARN once.
  - The Aeron counter `rune_link_mtu_deficit` carries the shortfall.
- **Operator action:** set `RUNE_AERON_MTU` to the link MTU minus 28 (IPv6: minus 48) on every node, or restore
  the link MTU.
