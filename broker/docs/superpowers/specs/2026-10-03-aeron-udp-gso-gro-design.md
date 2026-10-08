# Aeron UDP GSO/GRO transport bindings — design

Date: 2026-10-03.  Status: draft for review.  Owner repos: rune-protocol (component), rune-broker + rune-client-cpp (wiring).

## 1. Why

Measured on k3s-node-01 (3-node acks=all, 1 KiB, saturate, loopback, same build/config, interleaved):
MTU 65504 -> 155-167k rec/s; MTU 1408 -> 43-48k rec/s (all 4 cores ~98% busy: per-packet kernel cost).

A 64 KiB MTU is not usable on real networks (IP fragmentation: one lost fragment drops the whole datagram,
middleboxes drop fragments, reassembly-buffer overflow).  Real deployments run MTU 1408 (Ethernet) or ~8960 (jumbo),
where every datagram pays the full kernel UDP path.  TCP avoids that cost with TSO/GRO.  Linux has the UDP
equivalent: UDP GSO (`UDP_SEGMENT`, 4.18+) and UDP GRO (`UDP_GRO`, 5.0+) — one stack traversal per up-to-64 KiB
super-datagram while the wire carries normal MTU-sized datagrams.  Stock Aeron (C driver) uses neither.

Goal: Rune at MTU 1408 approaches its loopback-64K throughput, with no wire change and no behaviour change where
GSO/GRO is unavailable.

## 2. Binding principles

- Wire format unchanged: peers see ordinary <= MTU datagrams; GSO and non-GSO nodes interoperate in both directions.
- Pure optimization with runtime detection and fallback to the stock Aeron path (macOS/iOS, Windows, old kernels,
  any setsockopt/sendmsg refusal).  Never a correctness dependency.
- No Aeron fork: use Aeron's public `aeron_udp_channel_transport_bindings_t` and
  `aeron_driver_context_set_udp_channel_transport_bindings`.
- Hot path: no allocation, no virtual calls, no locks/atomics; per-transport state only (each driver agent owns its
  transports — shared-nothing).
- On by default (auto-detected); one switch to force it off (`RUNE_AERON_UDP_GSO=0`) for A/B and fault isolation.

## 3. Facts the design rests on (verified in the Aeron source we build against)

- `send_func(data_paths, transport, address, iov[], iov_length, &bytes_sent)`: one call per destination; each
  `iov` is ONE datagram (contiguous term bytes, <= MTU).  Stock impl `aeron_udp_channel_transport_sendv` =
  one `sendmmsg` with one msghdr per iov.  `iov_length <= network_publication_max_messages_per_send`
  (default 2, hard max 16 = `AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND`).
- A fragmented message produces full fragments of exactly MTU bytes (maxPayload = MTU - 32, MTU % 32 == 0), so a
  burst of a large batch is a run of equal-size datagrams — exactly the GSO shape (last segment may be shorter).
- `recvmmsg_func(transport, msgvec[], vlen, &bytes, recv_func, clientd)`: the receiver supplies msgvec with
  64 KiB buffers (`AERON_DRIVER_RECEIVER_MAX_UDP_PACKET_LENGTH`), no control buffers; our function performs the
  syscall, so it can attach cmsg buffers and deliver each datagram through `recv_func(…, buffer, length, addr, …)`.
- Installed package ships `aeronmd/media/aeron_udp_channel_transport_bindings.h`; default functions
  (`aeron_udp_channel_transport_init/recvmmsg/send`) are exported.
- Bench box kernel 5.15 (both features); `lo`: tx-udp-segmentation on.

## 4. Design

### 4.1 Component

`rune-protocol/include/rune/protocol/net/AeronGsoBindings.h` (+ a small `.c`/`.cpp` compiled by each consumer, or
header-only `inline` C-linkage functions — chosen in the plan; both repos already consume rune-protocol as sibling
sources, no publish gate).  Exposes:

```c++
// Returns bindings = stock defaults with init/send/recvmmsg overridden, or the stock defaults unchanged
// when the platform lacks GSO/GRO or RUNE_AERON_UDP_GSO=0.  Static storage; call once per driver context.
aeron_udp_channel_transport_bindings_t* rune_gso_udp_bindings();
```

Pure helpers (no syscalls, unit-testable on any OS):
- `gsoPlan(iov, n, &segSize)` -> how many leading iovs form one GSO send (all == iov[0].len except a shorter last,
  total <= 65,507 B, count <= kernel max segments (64 conservative)); remaining iovs form further groups.
- `groSplit(buf, len, segSize, emit)` -> calls emit(ptr, segLen) for each wire datagram in a GRO buffer.

### 4.2 Send (`send_func`)

1. If the transport's GSO state is Off -> stock `aeron_udp_channel_transport_send`.
2. Walk the iov array in GSO groups (`gsoPlan`).  A group of 1 datagram -> plain send (no cmsg).  A group of k>1 ->
   one `sendmsg` with msg_iov = the k iovs (scatter-gather, no copy) and a `UDP_SEGMENT` cmsg = segSize.
3. All-or-nothing per group: EAGAIN/ENOBUFS -> report bytes sent so far (Aeron already handles short sends: the
   remainder is retried from the term on the next duty cycle — verify exact semantics in the plan, task 1).
4. EIO/EINVAL/EOPNOTSUPP on the first GSO attempt (route/device cannot segment) -> state Off for this transport,
   resend the group via stock sendmmsg, log once at WARN.  Never fails a send that stock Aeron would have made.

### 4.3 Receive (`init_func` + `recvmmsg_func`)

- `init_func`: stock init, then `setsockopt(UDP_GRO, 1)` on receive sockets; failure -> GRO Off for that transport.
- `recvmmsg_func`: attach a per-transport, preallocated cmsg buffer to each msghdr, `recvmmsg`, then per message:
  no `UDP_GRO` cmsg -> deliver as-is; with cmsg segSize -> `groSplit` and call `recv_func` once per datagram
  (same `addr`).  bytes_rcved accounts wire bytes.  Truncation (MSG_TRUNC) handled exactly like stock.
- Aeron's loss/NAK/flow-control logic is unchanged: it still sees individual datagrams.

### 4.4 Sender batching

`max_messages_per_send` defaults to 2 -> at most 2 datagrams per GSO send.  Raise it to 16 when GSO is active
(derived default in HostDefaults/EmbeddedDriver and client TransportConfig; env override kept).  16 x 1408 = 22.5 KiB
per sendmsg.  Evaluate in the plan whether the sender's per-duty scan limit also needs raising.

### 4.5 Wiring

- Broker `EmbeddedDriver.cpp` and client `AeronTransport.cpp` (driver context setup):
  `aeron_driver_context_set_udp_channel_transport_bindings(ctx, rune_gso_udp_bindings())` before init.
- Effective config log line gains `gso=on|off gro=on|off` (proves the measured path, per the bench rules).
- MTU defaults unchanged (broker already derives from the egress interface: loopback 65504, else interface MTU -
  headers); client default aligned to the same derivation.

## 5. Testing

- Unit (any OS): `gsoPlan` (all equal, short last, a longer one breaks the run, mixed destinations are separate
  calls by construction, 64-segment cap, total-bytes cap, single datagram); `groSplit` (exact multiple, short last,
  segSize > len, 1-byte last).
- Real driver, Linux (in-process, loopback): publication -> subscription with GSO+GRO on, byte-exact delivery for
  message sizes 0..1 MiB across MTUs 1408/8192; mixed peers (GSO sender -> non-GRO receiver and reverse);
  loss injection (Aeron loss interceptor) -> NAK retransmit still recovers; fallback path forced (`RUNE_AERON_UDP_GSO=0`
  and simulated EIO) behaves exactly like stock.
- Full real-driver suite in both repos (transport/driver change rule), then DR e2e.
- Perf (k3s-node-01, clean box, logs off, ABAB): MTU 1408 GSO off vs on vs MTU 65504; plus Kafka vs Rune over a veth
  pair at MTU 1500 (network-like, TCP keeps TSO/GRO).  Results into the findings doc.

## 6. Risks / open questions

- Short-send semantics of Aeron's sender with all-or-nothing groups (4.2.3) — confirm before coding.
- Receiver vector capacity (`receiver_io_vector_capacity`) x 64 KiB GRO buffers: memory fine, but one recvmmsg
  may now carry up to vlen x 45 datagrams — check the receiver's per-duty work bound / fairness across images.
- Kernel max GSO segments (64 on older kernels, 128 newer): use 64.
- Patched FR-hook Aeron on the bench VM wraps the same stock functions — restore stock Aeron before final numbers.
- Windows USO/URO and macOS: out of scope (fallback path).
