# Release note: NAT-safe client egress

Broker version 9, client version 16. A client behind NAT now connects, produces, consumes and runs groups and
transactions with no inbound rule on its side: every reply (ConnectResponse, acks, Records, DDL replies) returns over
the flow the client opened.

## What changed

- Replies ride Aeron response channels (Aeron 1.51.0) instead of the client's declared egress address.
- The gateway and every worker have a response-control port (see [nat-egress.md](nat-egress.md) for the derived layout).
- The client sets the seed's gateway response port: `ClientConfig::gatewayResponseControlPort`,
  `--gateway-response-port`, `RUNE_GATEWAY_RESPONSE_PORT`. Other brokers' ports come from Metadata.
- Each worker session is bound by a 16-byte token (`WorkerBind`), and the worker answers with a `WorkerBindAck`. A TLS session's token is derived from its TLS keys and never sent; a plaintext session's token rides the `ConnectResponse`.
- The session's source address in principals and logs is the observed one, not the declared one (fixes #227).

## Compatibility

- Older clients are unchanged: they keep the declared-address path against a v9 broker.
- A v16 client against an older broker, or with the port left at `0`, uses the declared path.
- Add `RUNE_PEERS` segments 7 and 8 only after every node runs this version.

## Ports to open

Inbound UDP from clients: the client port, each worker's ingress port, the gateway response-control port and the
per-worker response-control ports. The per-worker egress control ports only with authentication off. Details:
[operations.md](../../../docs/operations.md#nat-safe-client-egress).

## Proof

`bash broker/docker/nat-e2e.sh`: two clients behind one masquerading router, the default 3-node config (auth and TLS),
produce and consume, group, transactions, a leader change, and a negative control (a client on the declared path fails).
