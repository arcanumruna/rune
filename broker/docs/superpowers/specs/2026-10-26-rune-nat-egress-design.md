# Rune NAT-safe egress: design

**Status:** draft, 2026-10-26. Split out of
[2026-10-24-rune-namespaces-and-truncate-design.md](2026-10-24-rune-namespaces-and-truncate-design.md) §10.4.

## Reachability through NAT
Clients run behind NAT: home routers, cloud NAT gateways, most Kubernetes pods. A broker can only reach them over a flow the client opened, the way memcached or DNS replies go to the request's source address.
- **Records egress is already NAT-safe.** It uses per-worker dynamic-MDC publications (`control-mode=dynamic`, `BrokerNode.cpp`). The client's status messages leave from its receive socket, and the broker sends to that observed source address. This is on in shard+fused mode, or with `perWorkerEgressEnabled`; it is off by default otherwise.
- **Per-session egress is not NAT-safe.** That covers ConnectResponse, acks, DDL replies, errors, and Records when per-worker egress is off.
  1. The client declares its own `egressChannel`, defaulting to `aeron:udp?endpoint=localhost:40200`, in ConnectRequest.
  2. The gateway's `SessionAnnouncer` makes the workers open a publication to that address.

  Behind NAT that address is private or unmapped, so the client never receives the replies.
- **Fix:**
  - move per-session egress onto a client-opened flow: dynamic MDC on the session egress, or Aeron response channels (`control-mode=response`);
  - make per-worker egress the default;
  - stop sending a declared endpoint in ConnectRequest.
- **Operations:**
  - one public UDP endpoint per node, plus each worker's egress control port;
  - security groups or firewalls opened for those ports;
  - client keepalive below typical NAT UDP timeouts (about 30 s).
- **Tests:** an e2e leg with the client behind a NAT, e.g. a docker network with masquerade, on the default config.
