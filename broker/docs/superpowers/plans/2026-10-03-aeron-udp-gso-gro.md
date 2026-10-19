# Aeron UDP GSO/GRO Transport Bindings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rune brokers and clients send fragmented batches with Linux UDP GSO and receive with UDP GRO, so throughput at a network MTU (1408) approaches the loopback-64K number, with no wire change and a stock-Aeron fallback everywhere else.

**Architecture:** A C component in rune-protocol (`rune_udp_gso.h` pure helpers + `rune_gso_bindings.{h,c}`) implements Aeron's public `aeron_udp_channel_transport_bindings_t` by copying Aeron's stock bindings and overriding `init/close/send/recvmmsg`. Each repo compiles that C file into a `rune_gso` static lib and installs the bindings into its embedded driver context (broker `EmbeddedDriver`, client `AeronTransport`). Per-transport state lives in `transport->bindings_clientd`; totals are rolled up on the conductor (where Aeron runs every transport init/close).

**Tech Stack:** C11 (Aeron driver C API, Linux `UDP_SEGMENT`/`UDP_GRO`), C++23, GoogleTest, CMake/Ninja, Conan toolchain on the VM.

**Spec:** `rune-broker/docs/superpowers/specs/2026-10-03-aeron-udp-gso-gro-design.md`

## Global Constraints

- Wire format unchanged: peers see ordinary <= MTU datagrams; GSO and non-GSO nodes interoperate both ways.
- Pure optimization: runtime detection + fallback to stock Aeron (macOS/iOS/Windows/old kernels/any refusal). Never a correctness dependency.
- No Aeron fork: only `aeron_udp_channel_transport_bindings_t` + `aeron_driver_context_set_udp_channel_transport_bindings`.
- Hot path: no allocation, no virtual calls, no locks, no atomics. Per-transport state only; totals written only by the conductor.
- On by default (auto-detected). Off switches: broker `RUNE_AERON_UDP_GSO=0`, client `RUNE_CLIENT_AERON_UDP_GSO=0`.
- When GSO is installed and `max_messages_per_send` was not set explicitly, use 16 (Aeron's hard max).
- MTU defaults unchanged (broker derives from the egress interface; client default 1408; bench passes `--mtu`).
- Tests never sleep (poll with deadline + `sched_yield`). Every bug fix gets a `*Regression*` test. RED before GREEN.
- Linux-behaviour tests run on k3s-node-01 inside `flock -w 7200 /root/vm.lock`, after `vm-clean.sh`; never `pkill -f`/`pgrep -f`.
- Never push; never `conan create/export/upload`.

## Review Focus

1. Mixed peers (GSO sender -> non-GRO receiver, and reverse): byte-exact delivery — Task 2 `MixedPeersBothDirections`.
2. EAGAIN/back-pressure mid-burst: an all-or-nothing GSO group refused must be re-sent by Aeron with nothing lost or reordered — Task 2 `InjectedEagainIsRetriedWithoutLoss`.
3. Packet loss with GRO on: Aeron NAK recovery still delivers every message — Task 2 `DataLossRecoversWithGroOn`.
4. Device/route cannot segment (EIO/EINVAL/EOPNOTSUPP): transport falls back to sendmmsg without failing a send — Task 2 `UnsupportedGsoFallsBackPerTransport`.
5. IPv6 endpoints (`[::1]`): address length and cmsg level correct for AF_INET6 — Task 2 `Ipv6RoundTrip`.

## File Structure

| File | Repo | Responsibility |
|---|---|---|
| `include/rune/protocol/net/rune_udp_gso.h` | rune-protocol | Pure C helpers: GSO grouping of an iovec run, GRO segment math. No syscalls. |
| `include/rune/protocol/net/rune_gso_bindings.h` | rune-protocol | Public C API: create/install/totals/delete + test seam. Only forward-declares Aeron types. |
| `src/net/rune_gso_bindings.c` | rune-protocol | Linux bindings implementation; non-Linux: no-op stubs (create returns NULL). |
| `tests/protocol/net/UdpGsoPlanTest.cpp` | rune-protocol | Unit tests for the pure helpers (any OS). |
| `CMakeLists.txt` (root) | rune-broker | `enable_language(C)`, `rune_gso` static lib, link into `rune_node`. |
| `libs/node/tests/node/UdpGsoBindingsTest.cpp` | rune-broker | Real-driver Linux tests of the bindings (Review Focus 1-5). |
| `libs/node/{include,src}/…NodeConfig*` | rune-broker | `aeronUdpGso` + `RUNE_AERON_UDP_GSO`. |
| `libs/node/{include,src}/…EmbeddedDriver*` | rune-broker | Create/install/delete bindings; msgs-per-send 16; totals accessor. |
| `libs/node/{include,src}/…HostDefaults*` | rune-broker | `facts.udpGso` probe; `gso=` in the effective-config line. |
| `CMakeLists.txt` | rune-client-cpp | `rune_gso` static lib, linked by the transport lib. |
| `include/rune/client/transport/TransportConfig.h` | rune-client-cpp | `udpGso`, `maxMessagesPerSend` + env. |
| `src/transport/AeronTransport.cpp`, header | rune-client-cpp | Create/install/delete bindings; `udpGsoActive()`. |
| `examples/bench/InvokerBench.cpp` | rune-client-cpp | `gso=` on the RESULT line. |

## Task 0: Branches / worktrees (controller, no review)

- [ ] **Step 1:** rune-protocol (shared sibling checkout `~/Projects/rune-protocol`, consumed by every worktree): `git -C ~/Projects/rune-protocol switch -c feat/udp-gso` (from its current HEAD f3fa865; leave the unrelated modified `CLAUDE.md` alone — never stage it).
- [ ] **Step 2:** broker: `git -C ~/Projects/rune-broker-wt26 worktree add ~/Projects/rune-broker-wt29 -b feat/udp-gso integ/perf-defaults`.
- [ ] **Step 3:** client: `git -C ~/Projects/rune-client-cpp-wt4 worktree add ~/Projects/rune-client-cpp-wt5 -b feat/udp-gso HEAD` (HEAD = ab37438; the uncommitted canOffer WIP in wt4 stays there and is NOT part of this plan).
- [ ] **Step 4:** VM tree: `ssh root@$RUNE_VM_HOST 'mkdir -p /root/gso'`; sync script `/root/gso/sync.sh` is created in Task 2 Step 6.

---

### Task 1: Pure GSO/GRO helpers (rune-protocol)

**Files:**
- Create: `rune-protocol/include/rune/protocol/net/rune_udp_gso.h`
- Create: `rune-protocol/tests/protocol/net/UdpGsoPlanTest.cpp`
- Modify: `rune-protocol/CMakeLists.txt` (add the test file to `rune_protocol_tests`)

**Interfaces:**
- Produces (C, `static inline`, usable from C and C++):
  - `#define RUNE_GSO_MAX_SEGMENTS 64` / `#define RUNE_GSO_MAX_BYTES 65507`
  - `size_t rune_gso_group(const struct iovec *iov, size_t n, size_t max_segs, size_t max_bytes)`
  - `size_t rune_gro_count(size_t len, size_t seg)`
  - `size_t rune_gro_segment_len(size_t len, size_t seg, size_t index)`

- [ ] **Step 1: Write the failing test** — `tests/protocol/net/UdpGsoPlanTest.cpp`:

```cpp
// Pure GSO grouping / GRO split math (no sockets): spec 2026-10-03-aeron-udp-gso-gro-design.md §4.1.
#include "rune/protocol/net/rune_udp_gso.h"

#include <gtest/gtest.h>

#include <vector>

namespace {
std::vector<iovec> lens(std::initializer_list<std::size_t> ls) {
    static char buf[1 << 17];
    std::vector<iovec> v;
    for (auto l : ls) v.push_back(iovec{buf, l});
    return v;
}
std::size_t group(const std::vector<iovec>& v, std::size_t segs = RUNE_GSO_MAX_SEGMENTS,
                  std::size_t bytes = RUNE_GSO_MAX_BYTES) {
    return rune_gso_group(v.data(), v.size(), segs, bytes);
}
} // namespace

TEST(UdpGsoPlan, EmptyRunIsZero) { EXPECT_EQ(rune_gso_group(nullptr, 0, 64, 65507), 0u); }
TEST(UdpGsoPlan, SingleDatagramIsAGroupOfOne) { EXPECT_EQ(group(lens({1408})), 1u); }
TEST(UdpGsoPlan, EqualRunGroupsWhole) { EXPECT_EQ(group(lens({1408, 1408, 1408, 1408})), 4u); }
TEST(UdpGsoPlan, ShorterLastJoinsTheGroup) { EXPECT_EQ(group(lens({1408, 1408, 700})), 3u); }
TEST(UdpGsoPlan, ShorterSegmentEndsTheGroup) { EXPECT_EQ(group(lens({1408, 700, 1408})), 2u); }
TEST(UdpGsoPlan, LongerSegmentBreaksTheRun) { EXPECT_EQ(group(lens({700, 1408, 1408})), 1u); }
TEST(UdpGsoPlan, ZeroLengthNeverGroups) {
    EXPECT_EQ(group(lens({0, 0})), 1u);
    EXPECT_EQ(group(lens({1408, 0})), 1u);
}
TEST(UdpGsoPlan, SegmentCapHolds) {
    std::vector<iovec> v = lens({});
    for (int i = 0; i < 100; ++i) v.push_back(lens({1000})[0]);
    EXPECT_EQ(group(v, 64, 1 << 20), 64u);
}
TEST(UdpGsoPlan, ByteCapHolds) {
    // 46 x 1408 = 64,768 <= 65,507; a 47th would be 66,176 > cap.
    std::vector<iovec> v;
    for (int i = 0; i < 60; ++i) v.push_back(lens({1408})[0]);
    EXPECT_EQ(group(v), 46u);
}
TEST(UdpGsoPlan, OversizeFirstIsAGroupOfOne) { EXPECT_EQ(group(lens({65504, 65504})), 1u); }
TEST(UdpGsoPlan, GroCountAndLengths) {
    EXPECT_EQ(rune_gro_count(0, 0), 1u);         // no GRO cmsg: deliver as-is
    EXPECT_EQ(rune_gro_count(1408, 0), 1u);
    EXPECT_EQ(rune_gro_count(1408, 1408), 1u);   // exactly one segment
    EXPECT_EQ(rune_gro_count(1000, 1408), 1u);   // seg > len
    EXPECT_EQ(rune_gro_count(4224, 1408), 3u);   // exact multiple
    EXPECT_EQ(rune_gro_count(4225, 1408), 4u);   // 1-byte last
    EXPECT_EQ(rune_gro_segment_len(4225, 1408, 0), 1408u);
    EXPECT_EQ(rune_gro_segment_len(4225, 1408, 3), 1u);
    EXPECT_EQ(rune_gro_segment_len(3500, 1408, 2), 684u);
}
```

Add `tests/protocol/net/UdpGsoPlanTest.cpp` to the `add_executable(rune_protocol_tests …)` list in `rune-protocol/CMakeLists.txt`.

- [ ] **Step 2: Run it to verify it fails**

Run: `cd ~/Projects/rune-protocol && cmake --build build --target rune_protocol_tests 2>&1 | tail -5`
Expected: compile error `rune/protocol/net/rune_udp_gso.h: No such file`. (If `build/` is not configured with `-DBUILD_TESTING=ON`, configure it the way `build.sh` in this repo does, adding `-DBUILD_TESTING=ON`.)

- [ ] **Step 3: Implement** — `include/rune/protocol/net/rune_udp_gso.h`:

```c
/* rune_udp_gso.h — pure UDP GSO/GRO math shared by the Aeron transport bindings (rune_gso_bindings.c).
 * No syscalls; C and C++.  Spec: rune-broker docs/superpowers/specs/2026-10-03-aeron-udp-gso-gro-design.md */
#ifndef RUNE_PROTOCOL_NET_RUNE_UDP_GSO_H
#define RUNE_PROTOCOL_NET_RUNE_UDP_GSO_H

#include <stddef.h>
#include <sys/uio.h>

/* Conservative kernel limit (UDP_MAX_SEGMENTS is 64 on older kernels, 128 on newer). */
#define RUNE_GSO_MAX_SEGMENTS 64
/* One GSO super-datagram is one IPv4 UDP datagram before segmentation: <= 65,535 - 20 - 8. */
#define RUNE_GSO_MAX_BYTES 65507

#ifdef __cplusplus
extern "C" {
#endif

/* Number of LEADING iovs (each one wire datagram) that may go out as ONE UDP_SEGMENT send with segment size
 * iov[0].iov_len: every iov equals iov[0].iov_len except that the LAST may be shorter (and then ends the group);
 * count <= max_segs; total bytes <= max_bytes.  Returns 0 for n == 0, else >= 1 (1 = send it plainly). */
static inline size_t rune_gso_group(const struct iovec *iov, size_t n, size_t max_segs, size_t max_bytes)
{
    if (0 == n) return 0;
    const size_t seg = iov[0].iov_len;
    if (0 == seg || seg > max_bytes) return 1;
    size_t total = seg;
    size_t k = 1;
    while (k < n && k < max_segs)
    {
        const size_t len = iov[k].iov_len;
        if (0 == len || len > seg || total + len > max_bytes) break;
        total += len;
        k++;
        if (len < seg) break; /* a shorter segment can only be the last one */
    }
    return k;
}

/* Wire datagrams inside one received buffer of len bytes whose UDP_GRO segment size is seg (0 = no GRO cmsg). */
static inline size_t rune_gro_count(size_t len, size_t seg)
{
    if (0 == seg || seg >= len) return 1;
    return (len + seg - 1) / seg;
}

/* Length of datagram `index` (0-based) in that buffer; only meaningful when rune_gro_count(len, seg) > 1. */
static inline size_t rune_gro_segment_len(size_t len, size_t seg, size_t index)
{
    const size_t off = index * seg;
    return (len - off < seg) ? (len - off) : seg;
}

#ifdef __cplusplus
}
#endif

#endif /* RUNE_PROTOCOL_NET_RUNE_UDP_GSO_H */
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cmake --build build --target rune_protocol_tests && ./build/rune_protocol_tests --gtest_filter='UdpGsoPlan.*'`
Expected: all `UdpGsoPlan.*` PASS; then the whole `./build/rune_protocol_tests` PASS.

- [ ] **Step 5: Commit** (rune-protocol, branch feat/udp-gso; stage only these files)

```bash
git add include/rune/protocol/net/rune_udp_gso.h tests/protocol/net/UdpGsoPlanTest.cpp CMakeLists.txt
git commit -m "feat(net): pure UDP GSO grouping / GRO split helpers"
```

---

### Task 2: GSO/GRO Aeron transport bindings + real-driver tests

**Files:**
- Create: `rune-protocol/include/rune/protocol/net/rune_gso_bindings.h`
- Create: `rune-protocol/src/net/rune_gso_bindings.c`
- Modify: `rune-broker-wt29/CMakeLists.txt` (root: `enable_language(C)`, `rune_gso` target)
- Create: `rune-broker-wt29/libs/node/tests/node/UdpGsoBindingsTest.cpp`
- Modify: `rune-broker-wt29/libs/node/CMakeLists.txt` (test target `rune_udp_gso_tests`, Linux only)

**Interfaces:**
- Consumes: Task 1 helpers.
- Produces (C API, `rune_gso_bindings.h`):
  - `typedef struct rune_gso_totals_stct { uint64_t gso_sends, gso_datagrams, gso_fallbacks, gro_buffers, gro_datagrams, gro_ctrunc_drops; } rune_gso_totals_t;`
  - `int rune_gso_platform_supported(void);` — bitmask: 1 = UDP_SEGMENT, 2 = UDP_GRO (0 on non-Linux).
  - `rune_gso_bindings_t *rune_gso_bindings_create(int want_gso, int want_gro);` — NULL when nothing is wanted+supported.
  - `int rune_gso_bindings_install(aeron_driver_context_t *ctx, rune_gso_bindings_t *b);` — before `aeron_driver_init`.
  - `void rune_gso_bindings_totals(const rune_gso_bindings_t *b, rune_gso_totals_t *out);` — read after `aeron_driver_close`.
  - `void rune_gso_bindings_delete(rune_gso_bindings_t *b);` — after `aeron_driver_context_close`.
  - `void rune_gso_bindings_test_inject(rune_gso_bindings_t *b, int err, int count);` — test seam, call before `aeron_driver_start`: the next `count` GSO sendmsg calls fail with `err`.

- [ ] **Step 1: Pin the Aeron include layout (probe).** On the VM (Linux), compile a 3-line C file to learn which include dirs the internal headers need:

```bash
ssh root@$RUNE_VM_HOST 'I=$(grep -o "/root/.conan2/p/[^;\"]*aeron[^;\"]*/p/include" /root/wt26/rune-broker/build-rel/CMakeCache.txt | head -1); echo $I;
printf "#include \"aeron_driver_context.h\"\n#include \"media/aeron_udp_channel_transport.h\"\n#include \"util/aeron_netutil.h\"\n#include \"util/aeron_error.h\"\nint main(void){return AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX;}\n" > /tmp/gsoprobe.c;
gcc-13 -std=gnu11 -I$I -I$I/aeron -I$I/aeronmd -c /tmp/gsoprobe.c -o /tmp/gsoprobe.o && echo PROBE-OK'
```
Expected: `PROBE-OK`. If a header is missing, add the directory that contains it to the list and record the final list; Step 4's CMake uses exactly that list (`${d}`, `${d}/aeron`, `${d}/aeronmd` per Aeron include root). Also confirm `aeron_udp_channel_transport_bindings_load_media("default")` returns the stock struct: `grep -n "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT" $I/aeronmd/*.h $I/aeronmd/media/*.h` must show `"default"`.

- [ ] **Step 2: Write the failing tests** — `libs/node/tests/node/UdpGsoBindingsTest.cpp`:

```cpp
// Real Aeron driver tests for rune_gso_bindings (Linux): spec 2026-10-03-aeron-udp-gso-gro-design.md §5,
// plan Review Focus 1-5.  Each rig = one embedded driver (SHARED threading, own dir) + an Aeron C client.
#include <gtest/gtest.h>

extern "C" {
#include "aeronmd.h"
#include "aeronc.h"
#include "rune/protocol/net/rune_gso_bindings.h"
}

#include <sched.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
constexpr auto kDeadline = std::chrono::seconds(20);

int g_rigSeq = 0;

struct Rig {
    aeron_driver_context_t* dctx = nullptr;
    aeron_driver_t*         driver = nullptr;
    rune_gso_bindings_t*    gso = nullptr;
    aeron_context_t*        cctx = nullptr;
    aeron_t*                client = nullptr;
    std::string             dir;
    rune_gso_totals_t       totals{};
    bool                    closed = false;

    // gsoMode: 0 = stock Aeron, 1 = GSO+GRO, 2 = GSO+GRO with an injected sendmsg errno (inject before start).
    bool start(int gsoMode, std::size_t mtu, int injectErr = 0, int injectCount = 0) {
        dir = "/dev/shm/rune-gso-test-" + std::to_string(::getpid()) + "-" + std::to_string(++g_rigSeq);
        if (aeron_driver_context_init(&dctx) < 0) return false;
        aeron_driver_context_set_dir(dctx, dir.c_str());
        aeron_driver_context_set_dir_delete_on_start(dctx, true);
        aeron_driver_context_set_dir_delete_on_shutdown(dctx, true);
        aeron_driver_context_set_threading_mode(dctx, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_shared_idle_strategy(dctx, "yield");
        aeron_driver_context_set_term_buffer_length(dctx, 1 << 20);
        aeron_driver_context_set_mtu_length(dctx, mtu);
        aeron_driver_context_set_network_publication_max_messages_per_send(dctx, 16);
        if (gsoMode != 0) {
            gso = rune_gso_bindings_create(1, 1);
            if (gso == nullptr) return false;   // the VM kernel supports both: NULL here is a failure
            if (gsoMode == 2) rune_gso_bindings_test_inject(gso, injectErr, injectCount);
            if (rune_gso_bindings_install(dctx, gso) < 0) return false;
        }
        if (aeron_driver_init(&driver, dctx) < 0) return false;
        if (aeron_driver_start(driver, false) < 0) return false;
        if (aeron_context_init(&cctx) < 0) return false;
        aeron_context_set_dir(cctx, dir.c_str());
        if (aeron_init(&client, cctx) < 0) return false;
        return aeron_start(client) == 0;
    }
    void close() {
        if (closed) return;
        closed = true;
        if (client) aeron_close(client);
        if (cctx) aeron_context_close(cctx);
        if (driver) aeron_driver_close(driver);
        if (gso) rune_gso_bindings_totals(gso, &totals);
        if (dctx) aeron_driver_context_close(dctx);
        if (gso) rune_gso_bindings_delete(gso);
    }
    ~Rig() { close(); }
};

template <class F>
bool pollUntil(F&& done) {
    const auto end = Clock::now() + kDeadline;
    while (!done()) {
        if (Clock::now() > end) return false;
        sched_yield();
    }
    return true;
}

aeron_subscription_t* addSub(aeron_t* c, const char* ch, int32_t stream) {
    aeron_async_add_subscription_t* a = nullptr;
    if (aeron_async_add_subscription(&a, c, ch, stream, nullptr, nullptr, nullptr, nullptr) < 0) return nullptr;
    aeron_subscription_t* s = nullptr;
    pollUntil([&] { return aeron_async_add_subscription_poll(&s, a) != 0; });
    return s;
}
aeron_exclusive_publication_t* addPub(aeron_t* c, const char* ch, int32_t stream) {
    aeron_async_add_exclusive_publication_t* a = nullptr;
    if (aeron_async_add_exclusive_publication(&a, c, ch, stream) < 0) return nullptr;
    aeron_exclusive_publication_t* p = nullptr;
    pollUntil([&] { return aeron_async_add_exclusive_publication_poll(&p, a) != 0; });
    return p;
}

uint8_t patternByte(std::size_t msg, std::size_t i) { return static_cast<uint8_t>((msg * 131u + i * 7u) ^ (i >> 8)); }

struct Received {
    std::size_t next = 0;
    std::vector<std::size_t> sizes;
    bool corrupt = false;
};
void onFragment(void* clientd, const uint8_t* buf, size_t len, aeron_header_t*) {
    auto* r = static_cast<Received*>(clientd);
    const std::size_t m = r->next;
    if (m >= r->sizes.size() || len != r->sizes[m]) { r->corrupt = true; ++r->next; return; }
    for (std::size_t i = 0; i < len; ++i)
        if (buf[i] != patternByte(m, i)) { r->corrupt = true; break; }
    ++r->next;
}

// Publishes `sizes` on `pubRig`, subscribes on `subRig`, checks every message arrives once, in order, byte-exact.
void roundTrip(Rig& pubRig, Rig& subRig, const std::string& channel, const std::vector<std::size_t>& sizes) {
    aeron_subscription_t* sub = addSub(subRig.client, channel.c_str(), 1001);
    ASSERT_NE(sub, nullptr) << aeron_errmsg();
    aeron_exclusive_publication_t* pub = addPub(pubRig.client, channel.c_str(), 1001);
    ASSERT_NE(pub, nullptr) << aeron_errmsg();
    ASSERT_TRUE(pollUntil([&] { return aeron_exclusive_publication_is_connected(pub); }));

    aeron_fragment_assembler_t* fa = nullptr;
    Received rx;
    rx.sizes = sizes;
    ASSERT_EQ(aeron_fragment_assembler_create(&fa, onFragment, &rx), 0);

    std::vector<uint8_t> buf;
    for (std::size_t m = 0; m < sizes.size(); ++m) {
        buf.resize(sizes[m]);
        for (std::size_t i = 0; i < buf.size(); ++i) buf[i] = patternByte(m, i);
        ASSERT_TRUE(pollUntil([&] {
            aeron_subscription_poll(sub, aeron_fragment_assembler_handler, fa, 64);
            return aeron_exclusive_publication_offer(pub, buf.data(), buf.size(), nullptr, nullptr) > 0;
        })) << "offer of message " << m << " never accepted";
    }
    ASSERT_TRUE(pollUntil([&] {
        aeron_subscription_poll(sub, aeron_fragment_assembler_handler, fa, 64);
        return rx.next >= sizes.size();
    })) << "received " << rx.next << " of " << sizes.size();
    EXPECT_FALSE(rx.corrupt);
    EXPECT_EQ(rx.next, sizes.size());
    aeron_fragment_assembler_delete(fa);
}

std::vector<std::size_t> mixedSizes(std::size_t mtu, int count) {
    const std::size_t payload = mtu - 32;
    const std::size_t maxMsg = (1u << 20) / 8;   // term 1 MiB -> max message = term/8
    const std::size_t pattern[] = {1, 100, payload, payload + 1, 3 * payload, 22 * payload + 7, maxMsg};
    std::vector<std::size_t> v;
    for (int i = 0; i < count; ++i) v.push_back(pattern[i % 7]);
    return v;
}
std::string udp(int port, std::size_t mtu, const char* host = "localhost") {
    return "aeron:udp?endpoint=" + std::string(host) + ":" + std::to_string(port) + "|mtu=" + std::to_string(mtu);
}
} // namespace

TEST(UdpGsoBindings, PlatformSupportsBothOnTheBenchKernel) {
    EXPECT_EQ(rune_gso_platform_supported(), 3);
}

TEST(UdpGsoBindings, DisabledCreatesNothing) {
    EXPECT_EQ(rune_gso_bindings_create(0, 0), nullptr);
}

TEST(UdpGsoBindings, RoundTripAllSizesMtu1408UsesGsoAndGro) {
    Rig r;
    ASSERT_TRUE(r.start(1, 1408)) << aeron_errmsg();
    roundTrip(r, r, udp(24601, 1408), mixedSizes(1408, 140));
    r.close();
    EXPECT_GT(r.totals.gso_datagrams, 0u) << "the GSO path never ran";
    EXPECT_GT(r.totals.gro_datagrams, 0u) << "the GRO split never ran";
    EXPECT_EQ(r.totals.gso_fallbacks, 0u);
    EXPECT_EQ(r.totals.gro_ctrunc_drops, 0u);
}

TEST(UdpGsoBindings, RoundTripMtu8192) {
    Rig r;
    ASSERT_TRUE(r.start(1, 8192)) << aeron_errmsg();
    roundTrip(r, r, udp(24602, 8192), mixedSizes(8192, 70));
    r.close();
    EXPECT_GT(r.totals.gso_datagrams, 0u);
}

TEST(UdpGsoBindings, MixedPeersBothDirections) {
    {
        Rig gsoPub, plainSub;
        ASSERT_TRUE(gsoPub.start(1, 1408)) << aeron_errmsg();
        ASSERT_TRUE(plainSub.start(0, 1408)) << aeron_errmsg();
        roundTrip(gsoPub, plainSub, udp(24603, 1408), mixedSizes(1408, 70));
        gsoPub.close();
        EXPECT_GT(gsoPub.totals.gso_datagrams, 0u);
    }
    {
        Rig plainPub, groSub;
        ASSERT_TRUE(plainPub.start(0, 1408)) << aeron_errmsg();
        ASSERT_TRUE(groSub.start(1, 1408)) << aeron_errmsg();
        roundTrip(plainPub, groSub, udp(24604, 1408), mixedSizes(1408, 70));
    }
}

TEST(UdpGsoBindings, InjectedEagainIsRetriedWithoutLoss) {
    Rig r;
    ASSERT_TRUE(r.start(2, 1408, EAGAIN, 50)) << aeron_errmsg();
    roundTrip(r, r, udp(24605, 1408), mixedSizes(1408, 70));
    r.close();
    EXPECT_GT(r.totals.gso_datagrams, 0u) << "GSO must resume after the injected EAGAINs";
    EXPECT_EQ(r.totals.gso_fallbacks, 0u) << "EAGAIN is back-pressure, not 'unsupported'";
}

TEST(UdpGsoBindings, UnsupportedGsoFallsBackPerTransport) {
    Rig r;
    ASSERT_TRUE(r.start(2, 1408, EIO, 1)) << aeron_errmsg();
    roundTrip(r, r, udp(24606, 1408), mixedSizes(1408, 70));
    r.close();
    EXPECT_GE(r.totals.gso_fallbacks, 1u);
}

TEST(UdpGsoBindings, DataLossRecoversWithGroOn) {
    // Aeron's loss interceptor drops ~5% of DATA frames (type 0x01 -> mask bit 0x2) on receive; NAKs recover.
    ::setenv("AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS", "loss", 1);
    ::setenv("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS", "rate=0.05|recv-msg-mask=0x2", 1);
    Rig r;
    const bool ok = r.start(1, 1408);
    ::unsetenv("AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS");
    ::unsetenv("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS");
    ASSERT_TRUE(ok) << aeron_errmsg();
    roundTrip(r, r, udp(24607, 1408), mixedSizes(1408, 70));
    r.close();
    EXPECT_GT(r.totals.gro_datagrams, 0u);
}

TEST(UdpGsoBindings, Ipv6RoundTrip) {
    Rig r;
    ASSERT_TRUE(r.start(1, 1408)) << aeron_errmsg();
    roundTrip(r, r, "aeron:udp?endpoint=[::1]:24608|mtu=1408", mixedSizes(1408, 70));
    r.close();
    EXPECT_GT(r.totals.gso_datagrams, 0u);
}
```

Wire the test target in `libs/node/CMakeLists.txt` inside the existing `if(BUILD_TESTING)` block (next to `rune_session_ack_sink_tests`):

```cmake
    if(UNIX AND NOT APPLE)
        # Real-driver UDP GSO/GRO bindings tests (Linux kernels only: UDP_SEGMENT/UDP_GRO).
        add_executable(rune_udp_gso_tests tests/node/UdpGsoBindingsTest.cpp)
        target_link_libraries(rune_udp_gso_tests PRIVATE rune_gso aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_udp_gso_tests)
    endif()
```

- [ ] **Step 3: Run it to verify it fails** (VM: needs Linux). Sync and build:

```bash
cat > /tmp/gso-sync.sh <<'EOF'
#!/bin/bash
# Mac -> VM source sync for the GSO work (no build dirs, no .git).
set -e
for r in rune-protocol rune-fr rune-codec; do
  rsync -a --delete --exclude build --exclude 'build-*' --exclude .git ~/Projects/$r/ root@$RUNE_VM_HOST:/root/gso/$r/
done
rsync -a --delete --exclude 'build*' --exclude .git ~/Projects/rune-broker-wt29/ root@$RUNE_VM_HOST:/root/gso/rune-broker/
rsync -a --delete --exclude 'build*' --exclude .git ~/Projects/rune-client-cpp-wt5/ root@$RUNE_VM_HOST:/root/gso/rune-client-cpp/
EOF
bash /tmp/gso-sync.sh
ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash -c "cd /root/gso/rune-broker && export CC=gcc-13 CXX=g++-13 && cmake -S . -B build-rel -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=/root/rune-e5/rune-broker/build/Release/build/Release/generators/conan_toolchain.cmake -DBUILD_TESTING=ON -DBUILD_BROKER_NODE_TESTS=ON -DBUILD_GATEWAY_TESTS=ON -DBUILD_NODE_UDP_TESTS=ON -DBUILD_AERON_REPLICATION_TESTS=ON > /root/gso/cfg.log 2>&1 && nice -n 5 ninja -C build-rel -j4 rune_udp_gso_tests 2>&1 | tail -5"'
```
Expected: FAIL to build — `rune_gso` target / `rune_gso_bindings.h` not found.

- [ ] **Step 4: Implement the public header** — `rune-protocol/include/rune/protocol/net/rune_gso_bindings.h`:

```c
/* rune_gso_bindings.h — Aeron UDP transport bindings with Linux UDP GSO (send) and UDP GRO (receive).
 * Install into an aeron_driver_context_t BEFORE aeron_driver_init; delete AFTER aeron_driver_context_close.
 * Non-Linux builds: create() returns NULL (stock Aeron path).  Implementation: src/net/rune_gso_bindings.c.
 * Spec: rune-broker docs/superpowers/specs/2026-10-03-aeron-udp-gso-gro-design.md */
#ifndef RUNE_PROTOCOL_NET_RUNE_GSO_BINDINGS_H
#define RUNE_PROTOCOL_NET_RUNE_GSO_BINDINGS_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct rune_gso_bindings_stct rune_gso_bindings_t;

typedef struct rune_gso_totals_stct
{
    uint64_t gso_sends;        /* successful sendmsg(UDP_SEGMENT) calls */
    uint64_t gso_datagrams;    /* wire datagrams carried by them */
    uint64_t gso_fallbacks;    /* transports that turned GSO off (EIO/EINVAL/EOPNOTSUPP/ENOPROTOOPT) */
    uint64_t gro_buffers;      /* received buffers that held > 1 coalesced datagram */
    uint64_t gro_datagrams;    /* datagrams split out of them */
    uint64_t gro_ctrunc_drops; /* received buffers dropped: control data truncated (cannot split safely) */
}
rune_gso_totals_t;

/* Bitmask of what this host's kernel accepts: 1 = UDP_SEGMENT, 2 = UDP_GRO.  0 on non-Linux. */
int rune_gso_platform_supported(void);

/* NULL when neither (want_gso && supported) nor (want_gro && supported), or on allocation failure. */
rune_gso_bindings_t *rune_gso_bindings_create(int want_gso, int want_gro);

/* aeron_driver_context_set_udp_channel_transport_bindings(ctx, ...); 0 on success, -1 otherwise. */
int rune_gso_bindings_install(aeron_driver_context_t *ctx, rune_gso_bindings_t *b);

/* Sum over every transport closed so far (all are closed by aeron_driver_close). */
void rune_gso_bindings_totals(const rune_gso_bindings_t *b, rune_gso_totals_t *out);

void rune_gso_bindings_delete(rune_gso_bindings_t *b);

/* TEST SEAM — call before aeron_driver_start: the next `count` GSO sendmsg attempts fail with errno `err`. */
void rune_gso_bindings_test_inject(rune_gso_bindings_t *b, int err, int count);

#ifdef __cplusplus
}
#endif

#endif /* RUNE_PROTOCOL_NET_RUNE_GSO_BINDINGS_H */
```

- [ ] **Step 5: Implement the bindings** — `rune-protocol/src/net/rune_gso_bindings.c` (include names as pinned in Step 1):

```c
/* rune_gso_bindings.c — see rune/protocol/net/rune_gso_bindings.h.
 *
 * Threading (verified in Aeron's driver): every transport init/close runs on the CONDUCTOR (endpoint create /
 * *_endpoint_delete); send/recvmmsg run on the sender/receiver agent that owns the endpoint.  Per-transport state
 * (transport->bindings_clientd) is therefore written by exactly one agent at a time, and owner->totals only by the
 * conductor.  No locks, no atomics. */
#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include "rune/protocol/net/rune_gso_bindings.h"

#include <stdlib.h>
#include <string.h>

#if defined(__linux__)

#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/udp.h>

#include "aeron_driver_context.h"
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_channel_transport_bindings.h"
#include "util/aeron_error.h"
#include "util/aeron_netutil.h"

#include "rune/protocol/net/rune_udp_gso.h"

#ifndef SOL_UDP
#define SOL_UDP 17
#endif
#ifndef UDP_SEGMENT
#define UDP_SEGMENT 103
#endif
#ifndef UDP_GRO
#define UDP_GRO 104
#endif

#define RUNE_GSO_UNSUPPORTED (-2)

struct rune_gso_bindings_stct
{
    aeron_udp_channel_transport_bindings_t bindings; /* FIRST: context->udp_channel_transport_bindings points here */
    aeron_udp_channel_transport_bindings_t defaults; /* Aeron's stock bindings: every fallback goes through these */
    int gso_enabled;
    int gro_enabled;
    int inject_errno;
    int inject_count;
    rune_gso_totals_t totals;
};

typedef union rune_gro_ctrl_un
{
    struct cmsghdr align;
    char buf[CMSG_SPACE(sizeof(int))];
}
rune_gro_ctrl_t;

typedef struct rune_gso_transport_state_stct
{
    rune_gso_bindings_t *owner;
    int gso_on;
    int gro_on;
    int gso_logged;
    int gro_logged;
    rune_gso_totals_t t;
    rune_gro_ctrl_t ctrl[AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX];
}
rune_gso_transport_state_t;

static void rune_gso_totals_add(rune_gso_totals_t *dst, const rune_gso_totals_t *src)
{
    dst->gso_sends += src->gso_sends;
    dst->gso_datagrams += src->gso_datagrams;
    dst->gso_fallbacks += src->gso_fallbacks;
    dst->gro_buffers += src->gro_buffers;
    dst->gro_datagrams += src->gro_datagrams;
    dst->gro_ctrunc_drops += src->gro_ctrunc_drops;
}

static int rune_gso_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    struct sockaddr_storage *connect_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf,
    bool is_media_timestamping,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    rune_gso_bindings_t *owner = (rune_gso_bindings_t *)context->udp_channel_transport_bindings;
    if (owner->defaults.init_func(
        transport, bind_addr, multicast_if_addr, connect_addr, multicast_if_index, ttl, socket_rcvbuf, socket_sndbuf,
        is_media_timestamping, context, affinity) < 0)
    {
        return -1;
    }

    rune_gso_transport_state_t *st = (rune_gso_transport_state_t *)calloc(1, sizeof(*st));
    if (NULL == st)
    {
        owner->defaults.close_func(transport);
        AERON_SET_ERR(ENOMEM, "%s", "rune-gso: failed to allocate transport state");
        return -1;
    }
    st->owner = owner;
    st->gso_on = owner->gso_enabled;
    int one = 1;
    st->gro_on = owner->gro_enabled && 0 == transport->timestamp_flags &&
        0 == setsockopt(transport->recv_fd, SOL_UDP, UDP_GRO, &one, sizeof(one));
    transport->bindings_clientd = st;
    return 0;
}

static int rune_gso_close(aeron_udp_channel_transport_t *transport)
{
    rune_gso_transport_state_t *st = (rune_gso_transport_state_t *)transport->bindings_clientd;
    if (NULL == st)
    {
        return aeron_udp_channel_transport_close(transport);
    }
    rune_gso_bindings_t *owner = st->owner;
    rune_gso_totals_add(&owner->totals, &st->t);
    transport->bindings_clientd = NULL;
    free(st);
    return owner->defaults.close_func(transport);
}

/* One sendmsg(UDP_SEGMENT) carrying `count` datagrams of iov[0].iov_len bytes (last may be shorter).
 * Returns count, 0 (transient: Aeron re-sends the whole batch), RUNE_GSO_UNSUPPORTED, or -1 (error set). */
static int rune_gso_sendmsg(
    rune_gso_transport_state_t *st,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t count,
    int64_t *bytes_sent)
{
    union
    {
        struct cmsghdr align;
        char buf[CMSG_SPACE(sizeof(uint16_t))];
    }
    ctrl;
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    memset(&ctrl, 0, sizeof(ctrl));
    if (NULL == transport->connected_address)
    {
        msg.msg_name = address;
        msg.msg_namelen = (socklen_t)AERON_ADDR_LEN(address);
    }
    msg.msg_iov = iov;
    msg.msg_iovlen = count;
    msg.msg_control = ctrl.buf;
    msg.msg_controllen = sizeof(ctrl.buf);
    struct cmsghdr *cm = CMSG_FIRSTHDR(&msg);
    cm->cmsg_level = SOL_UDP;
    cm->cmsg_type = UDP_SEGMENT;
    cm->cmsg_len = CMSG_LEN(sizeof(uint16_t));
    const uint16_t seg = (uint16_t)iov[0].iov_len;
    memcpy(CMSG_DATA(cm), &seg, sizeof(seg));

    ssize_t sent;
    rune_gso_bindings_t *owner = st->owner;
    if (owner->inject_count > 0)
    {
        owner->inject_count--;
        errno = owner->inject_errno;
        sent = -1;
    }
    else
    {
        sent = sendmsg(transport->fd, &msg, 0);
    }

    if (sent < 0)
    {
        const int err = errno;
        if (EAGAIN == err || EWOULDBLOCK == err || EINTR == err || ECONNREFUSED == err || ENOBUFS == err)
        {
            return 0;
        }
        if (EIO == err || EINVAL == err || EOPNOTSUPP == err || ENOPROTOOPT == err)
        {
            return RUNE_GSO_UNSUPPORTED;
        }
        AERON_SET_ERR(err, "rune-gso: sendmsg(UDP_SEGMENT) failed, segments=%zu seg=%u", count, (unsigned)seg);
        return -1;
    }
    *bytes_sent += sent;
    return (int)count;
}

/* Returns the number of iov datagrams sent; a value < iov_length makes Aeron re-send the batch (duplicates of
 * already-sent datagrams are discarded by the receiver by term offset). */
static int rune_gso_send(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent)
{
    rune_gso_transport_state_t *st = (rune_gso_transport_state_t *)transport->bindings_clientd;
    if (NULL == st)
    {
        return aeron_udp_channel_transport_send(data_paths, transport, address, iov, iov_length, bytes_sent);
    }
    if (!st->gso_on || iov_length < 2)
    {
        return st->owner->defaults.send_func(data_paths, transport, address, iov, iov_length, bytes_sent);
    }

    size_t done = 0;
    while (done < iov_length)
    {
        const size_t k = rune_gso_group(iov + done, iov_length - done, RUNE_GSO_MAX_SEGMENTS, RUNE_GSO_MAX_BYTES);
        int r;
        if (1 == k)
        {
            r = st->owner->defaults.send_func(data_paths, transport, address, iov + done, 1, bytes_sent);
            if (r <= 0)
            {
                return r < 0 ? r : (int)done;
            }
            done += 1;
            continue;
        }

        r = rune_gso_sendmsg(st, transport, address, iov + done, k, bytes_sent);
        if (RUNE_GSO_UNSUPPORTED == r)
        {
            st->gso_on = 0;
            st->t.gso_fallbacks++;
            fprintf(stderr, "rune-gso: WARN UDP GSO refused on fd=%d; this transport falls back to sendmmsg\n",
                (int)transport->fd);
            r = st->owner->defaults.send_func(
                data_paths, transport, address, iov + done, iov_length - done, bytes_sent);
            return r < 0 ? r : (int)(done + (size_t)r);
        }
        if (r <= 0)
        {
            return r < 0 ? r : (int)done;
        }
        st->t.gso_sends++;
        st->t.gso_datagrams += k;
        if (!st->gso_logged)
        {
            st->gso_logged = 1;
            fprintf(stderr, "rune-gso: UDP GSO active fd=%d seg=%zu segments=%zu\n",
                (int)transport->fd, iov[done].iov_len, k);
        }
        done += k;
    }
    return (int)done;
}

static int rune_gro_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    rune_gso_transport_state_t *st = (rune_gso_transport_state_t *)transport->bindings_clientd;
    if (NULL == st)
    {
        return aeron_udp_channel_transport_recvmmsg(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
    }
    if (!st->gro_on)
    {
        return st->owner->defaults.recvmmsg_func(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
    }
    if (vlen > AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX)
    {
        vlen = AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX;
    }
    for (size_t i = 0; i < vlen; i++)
    {
        msgvec[i].msg_hdr.msg_control = st->ctrl[i].buf;
        msgvec[i].msg_hdr.msg_controllen = sizeof(st->ctrl[i].buf);
        msgvec[i].msg_hdr.msg_flags = 0;
    }

    struct timespec tv = { .tv_nsec = 0, .tv_sec = 0 };
    const int result = recvmmsg(transport->recv_fd, msgvec, (unsigned int)vlen, 0, &tv);
    if (result < 0)
    {
        const int err = errno;
        if (EINTR == err || EAGAIN == err || ECONNREFUSED == err)
        {
            return 0;
        }
        AERON_SET_ERR(err, "rune-gso: recvmmsg failed, fd=%d, recv_fd=%d", (int)transport->fd, (int)transport->recv_fd);
        return -1;
    }

    int delivered = 0;
    for (int i = 0; i < result; i++)
    {
        struct msghdr *h = &msgvec[i].msg_hdr;
        uint8_t *base = (uint8_t *)h->msg_iov[0].iov_base;
        const size_t len = msgvec[i].msg_len;
        size_t seg = 0;
        for (struct cmsghdr *cm = CMSG_FIRSTHDR(h); NULL != cm; cm = CMSG_NXTHDR(h, cm))
        {
            if (SOL_UDP == cm->cmsg_level && UDP_GRO == cm->cmsg_type)
            {
                int v;
                memcpy(&v, CMSG_DATA(cm), sizeof(v));
                seg = (size_t)v;
            }
        }
        *bytes_rcved += (int64_t)len;
        if (0 != (h->msg_flags & MSG_CTRUNC))
        {
            st->t.gro_ctrunc_drops++; /* cannot know the segment size: drop = loss, Aeron NAKs recover it */
            continue;
        }

        const size_t n = rune_gro_count(len, seg);
        if (1 == n)
        {
            recv_func(transport->data_paths, transport, clientd, transport->dispatch_clientd,
                transport->destination_clientd, base, len, (struct sockaddr_storage *)h->msg_name, NULL);
            delivered++;
            continue;
        }
        st->t.gro_buffers++;
        st->t.gro_datagrams += n;
        if (!st->gro_logged)
        {
            st->gro_logged = 1;
            fprintf(stderr, "rune-gso: UDP GRO active recv_fd=%d seg=%zu datagrams=%zu\n",
                (int)transport->recv_fd, seg, n);
        }
        for (size_t j = 0; j < n; j++)
        {
            recv_func(transport->data_paths, transport, clientd, transport->dispatch_clientd,
                transport->destination_clientd, base + j * seg, rune_gro_segment_len(len, seg, j),
                (struct sockaddr_storage *)h->msg_name, NULL);
        }
        delivered += (int)n;
    }
    return delivered;
}

int rune_gso_platform_supported(void)
{
    const int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
    {
        return 0;
    }
    int seg = 1400;
    int one = 1;
    int mask = 0;
    if (0 == setsockopt(fd, SOL_UDP, UDP_SEGMENT, &seg, sizeof(seg)))
    {
        mask |= 1;
    }
    if (0 == setsockopt(fd, SOL_UDP, UDP_GRO, &one, sizeof(one)))
    {
        mask |= 2;
    }
    close(fd);
    return mask;
}

rune_gso_bindings_t *rune_gso_bindings_create(int want_gso, int want_gro)
{
    const int supported = rune_gso_platform_supported();
    const int gso = want_gso && (supported & 1);
    const int gro = want_gro && (supported & 2);
    if (!gso && !gro)
    {
        return NULL;
    }
    aeron_udp_channel_transport_bindings_t *stock = aeron_udp_channel_transport_bindings_load_media("default");
    if (NULL == stock)
    {
        return NULL;
    }
    rune_gso_bindings_t *b = (rune_gso_bindings_t *)calloc(1, sizeof(*b));
    if (NULL == b)
    {
        return NULL;
    }
    b->defaults = *stock;
    b->bindings = *stock;
    b->bindings.init_func = rune_gso_init;
    b->bindings.close_func = rune_gso_close;
    b->bindings.send_func = rune_gso_send;
    b->bindings.recvmmsg_func = rune_gro_recvmmsg;
    b->bindings.meta_info.name = "rune_gso";
    b->bindings.meta_info.type = "bindings";
    b->bindings.meta_info.source_symbol = b;
    b->gso_enabled = gso;
    b->gro_enabled = gro;
    return b;
}

int rune_gso_bindings_install(aeron_driver_context_t *ctx, rune_gso_bindings_t *b)
{
    if (NULL == ctx || NULL == b)
    {
        return -1;
    }
    return aeron_driver_context_set_udp_channel_transport_bindings(ctx, &b->bindings) < 0 ? -1 : 0;
}

void rune_gso_bindings_totals(const rune_gso_bindings_t *b, rune_gso_totals_t *out)
{
    memset(out, 0, sizeof(*out));
    if (NULL != b)
    {
        *out = b->totals;
    }
}

void rune_gso_bindings_test_inject(rune_gso_bindings_t *b, int err, int count)
{
    if (NULL != b)
    {
        b->inject_errno = err;
        b->inject_count = count;
    }
}

#else /* !__linux__ : stock Aeron everywhere */

struct rune_gso_bindings_stct
{
    int unused;
};

int rune_gso_platform_supported(void) { return 0; }
rune_gso_bindings_t *rune_gso_bindings_create(int want_gso, int want_gro) { (void)want_gso; (void)want_gro; return NULL; }
int rune_gso_bindings_install(aeron_driver_context_t *ctx, rune_gso_bindings_t *b) { (void)ctx; (void)b; return -1; }
void rune_gso_bindings_totals(const rune_gso_bindings_t *b, rune_gso_totals_t *out) { (void)b; memset(out, 0, sizeof(*out)); }
void rune_gso_bindings_test_inject(rune_gso_bindings_t *b, int err, int count) { (void)b; (void)err; (void)count; }

#endif

void rune_gso_bindings_delete(rune_gso_bindings_t *b)
{
    free(b);
}
```

Root `rune-broker-wt29/CMakeLists.txt`, right after the `rune_fr` block (line ~53):

```cmake
# UDP GSO/GRO Aeron transport bindings — rune-protocol sibling C source (spec 2026-10-03-aeron-udp-gso-gro).
# Linux does GSO/GRO; other platforms compile a no-op (create() returns NULL -> stock Aeron).
enable_language(C)
add_library(rune_gso STATIC ${RUNE_PROTOCOL_INCLUDE}/../src/net/rune_gso_bindings.c)
set_target_properties(rune_gso PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON POSITION_INDEPENDENT_CODE ON)
target_include_directories(rune_gso PUBLIC ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_gso PUBLIC aeron::aeron)
if(UNIX AND NOT APPLE)
    foreach(_rune_aeron_inc IN LISTS aeron_INCLUDE_DIRS)
        # Aeron's internal driver headers include siblings relative to these roots (pinned by plan Task 2 Step 1).
        target_include_directories(rune_gso PRIVATE "${_rune_aeron_inc}" "${_rune_aeron_inc}/aeron" "${_rune_aeron_inc}/aeronmd")
    endforeach()
endif()
```
If `aeron_INCLUDE_DIRS` is empty on the VM, use the variable CMakeDeps defines for the package (check `grep -n "aeron_INCLUDE_DIR" build-rel/generators/*aeron*.cmake` in the toolchain's generators dir) — the Step 1 probe's directories are the source of truth.

- [ ] **Step 6: Run the tests to verify they pass** (VM)

```bash
bash /tmp/gso-sync.sh
ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash -c "cd /root/gso/rune-broker && nice -n 5 ninja -C build-rel -j4 rune_udp_gso_tests 2>&1 | tail -3 && ./build-rel/rune_udp_gso_tests 2>&1 | tail -20"'
```
Expected: `[  PASSED  ] 10 tests.` and stderr shows `rune-gso: UDP GSO active ... seg=1408` and `rune-gso: UDP GRO active ...`.

- [ ] **Step 7: Commit** — rune-protocol (`include/rune/protocol/net/rune_gso_bindings.h`, `src/net/rune_gso_bindings.c`): `git commit -m "feat(net): Aeron UDP transport bindings with Linux GSO/GRO + stock fallback"`; rune-broker-wt29 (`CMakeLists.txt`, `libs/node/CMakeLists.txt`, `libs/node/tests/node/UdpGsoBindingsTest.cpp`): `git commit -m "test(node): real-driver UDP GSO/GRO bindings tests (loss, mixed peers, fallback, IPv6)"`.

---

### Task 3: Broker wiring (config, EmbeddedDriver, effective-config line)

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h` (field next to `aeronMaxMessagesPerSend`, doc in the RUNE_AERON_* comment block ~line 505)
- Modify: `libs/node/src/NodeConfig.cpp` (~line 958, env parsing)
- Modify: `libs/node/include/rune/node/EmbeddedDriver.h` (config field + members + accessors)
- Modify: `libs/node/src/EmbeddedDriver.cpp` (mapping line ~19; start ~line 100-145; close ~line 204-240)
- Modify: `libs/node/include/rune/node/HostDefaults.h` (`HostNetFacts::udpGso`), `libs/node/src/HostDefaults.cpp` (probe ~line 449; summary ~line 315)
- Modify: `libs/node/CMakeLists.txt` (`rune_node` links `rune_gso`)
- Test: `libs/node/tests/node/NodeConfigTest.cpp`, `libs/node/tests/node/HostDefaultsTest.cpp`, `libs/node/tests/node/UdpGsoBindingsTest.cpp` (EmbeddedDriver case)

**Interfaces:**
- Consumes: Task 2 C API.
- Produces: `NodeConfig::aeronUdpGso` (bool, default true); `EmbeddedDriverConfig::udpGso` (bool, default true); `bool EmbeddedDriver::udpGsoActive() const noexcept`; `rune_gso_totals_t EmbeddedDriver::gsoTotals() const noexcept` (valid after `close()`); `HostNetFacts::udpGso` (bool); effective-config line gains ` gso=on|off|unsupported`.

- [ ] **Step 1: Write the failing tests.**

`NodeConfigTest.cpp` (follow the file's existing env-injection helper used by the `RUNE_AERON_MAX_MSGS_PER_SEND` tests):

```cpp
TEST(NodeConfig, AeronUdpGsoDefaultsOnAndEnvTurnsItOff) {
    EXPECT_TRUE(NodeConfig{}.aeronUdpGso);
    for (const char* off : {"0", "false"}) {
        auto r = parseWithEnv({{"RUNE_AERON_UDP_GSO", off}});   // the file's helper; same shape as its MAX_MSGS tests
        ASSERT_TRUE(r.error.empty()) << r.error;
        EXPECT_FALSE(r.config.aeronUdpGso) << off;
    }
    auto bad = parseWithEnv({{"RUNE_AERON_UDP_GSO", "maybe"}});
    EXPECT_NE(bad.error.find("RUNE_AERON_UDP_GSO"), std::string::npos);
}
```

`HostDefaultsTest.cpp` (next to `LoopbackLinuxWithTunedSysctlsIsTheFindingsConfig`, reusing its facts builder):

```cpp
TEST(HostDefaults, SummaryReportsUdpGso) {
    for (auto [want, supported, expect] : {std::tuple{true, true, "gso=on"}, std::tuple{true, false, "gso=unsupported"},
                                           std::tuple{false, true, "gso=off"}}) {
        NodeConfig cfg = loopbackShardConfig();          // the file's existing helper
        cfg.aeronUdpGso = want;
        HostNetFacts facts = tunedLoopbackLinuxFacts();  // the file's existing helper
        facts.udpGso = supported;
        const auto hd = applyHostDerivedDefaults(cfg, facts, "uring");
        EXPECT_NE(hd.summary.find(expect), std::string::npos) << hd.summary;
    }
}
```
(If the helpers have different names, use the ones the neighbouring tests use; the assertion is the contract.)

`UdpGsoBindingsTest.cpp` — add:

```cpp
#include "rune/node/EmbeddedDriver.h"

TEST(UdpGsoBindings, EmbeddedDriverInstallsGsoByDefault) {
    rune::node::EmbeddedDriverConfig on;
    on.dir = "/dev/shm/rune-gso-ed-on-" + std::to_string(::getpid());
    rune::node::EmbeddedDriver a(on, rune::node::EmbeddedDriver::DriverMode::Threaded);
    EXPECT_TRUE(a.udpGsoActive());
    a.close();

    rune::node::EmbeddedDriverConfig off = on;
    off.dir += "-off";
    off.udpGso = false;
    rune::node::EmbeddedDriver b(off, rune::node::EmbeddedDriver::DriverMode::Threaded);
    EXPECT_FALSE(b.udpGsoActive());
}
```
(Match the real constructor/mode spelling in `EmbeddedDriver.h`; link `rune_udp_gso_tests` to `rune_node` too.)

- [ ] **Step 2: Run to verify RED** — Mac: `cmake --build build --target rune_node_tests && ./build/rune_node_tests --gtest_filter='NodeConfig.AeronUdpGso*:HostDefaults.SummaryReportsUdpGso'` → compile errors (`aeronUdpGso`, `udpGso` unknown). VM: build `rune_udp_gso_tests` → compile error (`udpGsoActive`).

- [ ] **Step 3: Implement.**

`NodeConfig.h` (next to `aeronMaxMessagesPerSend`; extend the RUNE_AERON_* doc block with `RUNE_AERON_UDP_GSO -> UDP GSO/GRO transport bindings (default on; 0/false = stock Aeron)`):

```cpp
    bool aeronUdpGso = true;   ///< RUNE_AERON_UDP_GSO: Linux UDP GSO/GRO bindings (auto-off when unsupported)
```

`NodeConfig.cpp` (after the MAX_MSGS loop):

```cpp
    if (const char* raw = getenv_fn("RUNE_AERON_UDP_GSO"); raw && *raw) {
        const std::string_view v(raw);
        if (v == "1" || v == "true") cfg.aeronUdpGso = true;
        else if (v == "0" || v == "false") cfg.aeronUdpGso = false;
        else return {{}, std::string("RUNE_AERON_UDP_GSO must be 0/1/false/true, got '") + raw + "'"};
    }
```

`EmbeddedDriver.h`: in the config struct next to `maxMessagesPerSend`: `bool udpGso = true;  ///< install rune_gso bindings (Linux GSO/GRO)`. Members: `rune_gso_bindings_t* gso_ = nullptr; rune_gso_totals_t gsoTotals_{};` (`#include "rune/protocol/net/rune_gso_bindings.h"`). Public:

```cpp
    [[nodiscard]] bool udpGsoActive() const noexcept { return gso_ != nullptr; }
    /// GSO/GRO totals of every transport this driver closed; complete after close().
    [[nodiscard]] rune_gso_totals_t gsoTotals() const noexcept { return gsoTotals_; }
```

`EmbeddedDriver.cpp`:
- mapping (~line 19): `e.udpGso = c.aeronUdpGso;`
- before the `maxMessagesPerSend` block:

```cpp
    if (cfg_.udpGso) {
        gso_ = rune_gso_bindings_create(1, 1);   // NULL when the kernel/platform has neither -> stock Aeron
        if (gso_ != nullptr && rune_gso_bindings_install(ctx_, gso_) < 0) {
            rune_gso_bindings_delete(gso_);
            gso_ = nullptr;
        }
    }
```
- replace the msgs-per-send block:

```cpp
    if (cfg_.maxMessagesPerSend > 0)
        aeron_driver_context_set_network_publication_max_messages_per_send(ctx_, static_cast<uint32_t>(cfg_.maxMessagesPerSend));
    else if (gso_ != nullptr)
        aeron_driver_context_set_network_publication_max_messages_per_send(ctx_, 16U);  // GSO groups up to 16 datagrams
```
- a private helper called right AFTER every `aeron_driver_context_close(ctx_)` (lines ~95, ~138, ~144, ~236):

```cpp
void EmbeddedDriver::releaseGso_() noexcept {
    if (gso_ == nullptr) return;
    rune_gso_bindings_totals(gso_, &gsoTotals_);
    rune_gso_bindings_delete(gso_);
    gso_ = nullptr;
}
```
  At line ~95 (`aeron_driver_context_close` before GSO creation) the helper is a no-op; call it anyway for uniformity.
  `udpGsoActive()` must keep answering true after close for the test: add `bool gsoWasActive_ = false;` set when installed, and make `udpGsoActive()` return `gso_ != nullptr || gsoWasActive_`.

`HostDefaults.h`: `bool udpGso = false;  ///< kernel accepts UDP_SEGMENT and UDP_GRO`. `HostDefaults.cpp` probe: `facts.udpGso = rune_gso_platform_supported() == 3;`. Summary: append `" gso=%s"` after `sparse=%s mtu=%d` … i.e. add to the format right after `mtu=%d` → `mtu=%d gso=%s` with argument `cfg.aeronUdpGso ? (facts.udpGso ? "on" : "unsupported") : "off"` and enlarge `line` to 896.
  Note: `HostDefaultsTest` line 448 asserts `"mtu=65504 socketBuffer=..."` — place `gso=` AFTER `rcvWindow=%d` instead, to keep existing assertions valid: `... rcvWindow=%d gso=%s rings{...`.

`libs/node/CMakeLists.txt`: `target_link_libraries(rune_node PUBLIC rune_gso)` (alongside its existing aeron link).

- [ ] **Step 4: Verify GREEN** — Mac: node tests above PASS + full `ctest -R rune_node` unchanged. VM: sync, build, `./build-rel/rune_udp_gso_tests` → 11 PASS.

- [ ] **Step 5: Commit** (wt29): `git commit -m "feat(node): UDP GSO/GRO bindings in EmbeddedDriver (default on, RUNE_AERON_UDP_GSO) + gso= in effective config"`.

---

### Task 4: Client wiring (TransportConfig, AeronTransport, bench RESULT)

**Files:**
- Modify: `rune-client-cpp-wt5/CMakeLists.txt` (`enable_language(C)`, `rune_gso` target — same block as broker Task 2 Step 5 — linked by `rune_client_transport`)
- Modify: `include/rune/client/transport/TransportConfig.h` (fields ~line 74; env ~line 192)
- Modify: `include/rune/client/transport/AeronTransport.h`, `src/transport/AeronTransport.cpp` (driver ctx setup ~line 526-563; driver close path)
- Modify: `examples/bench/InvokerBench.cpp` (RESULT line ~381-392)
- Test: `tests/transport/TransportConfigTest.cpp` (or the file holding the existing `RUNE_CLIENT_AERON_MTU` env test), `tests/transport/AeronTransportGsoTest.cpp` (new, Linux)

**Interfaces:**
- Consumes: Task 2 C API.
- Produces: `TransportConfig::udpGso` (bool, default true), `TransportConfig::maxMessagesPerSend` (uint32_t, 0 = auto); env `RUNE_CLIENT_AERON_UDP_GSO`, `RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND`; `bool AeronTransport::udpGsoActive() const noexcept`.

- [ ] **Step 1: Write the failing tests.**

Env test (in the file that tests `RUNE_CLIENT_AERON_MTU`; reuse its env helper):

```cpp
TEST(TransportConfigEnv, UdpGsoDefaultsOnEnvTurnsOffAndSetsMsgsPerSend) {
    EXPECT_TRUE(rune::client::transport::TransportConfig{}.udpGso);
    const auto c = configFromEnv({{"RUNE_CLIENT_AERON_UDP_GSO", "0"}, {"RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND", "8"}});
    EXPECT_FALSE(c.udpGso);
    EXPECT_EQ(c.maxMessagesPerSend, 8u);
}
```

`tests/transport/AeronTransportGsoTest.cpp` (Linux; follow the existing AeronTransport test that starts an embedded driver for setup/teardown):

```cpp
TEST(AeronTransportGso, EmbeddedDriverInstallsGsoByDefaultOnLinux) {
    auto cfg = embeddedTestConfig();          // the existing helper in the transport tests
    rune::client::transport::AeronTransport on(cfg);
    ASSERT_TRUE(on.start());
    EXPECT_TRUE(on.udpGsoActive());
    on.close();

    cfg.udpGso = false;
    cfg.dataDir += "-off";
    rune::client::transport::AeronTransport off(cfg);
    ASSERT_TRUE(off.start());
    EXPECT_FALSE(off.udpGsoActive());
}
```
Register it in CMake only `if(UNIX AND NOT APPLE)`.

- [ ] **Step 2: Verify RED** — Mac: env test fails to compile (`udpGso`). VM: `AeronTransportGso` fails to compile.

- [ ] **Step 3: Implement.**

`TransportConfig.h` (next to `mtuLength`):

```cpp
    bool        udpGso             = true;  ///< Linux UDP GSO/GRO transport bindings (auto-off when unsupported)
    uint32_t    maxMessagesPerSend = 0;     ///< Aeron network publication msgs per send; 0 = 16 with GSO, else Aeron's 2
```
Env (after `RUNE_CLIENT_AERON_MTU`):

```cpp
    if (const auto g = readSize("RUNE_CLIENT_AERON_UDP_GSO")) c.udpGso = (*g != 0);
    if (const auto m = readSize("RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND")) {
        if (*m >= 1 && *m <= 16) c.maxMessagesPerSend = static_cast<uint32_t>(*m);
        else warnings.emplace_back("RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND must be in [1, 16] — ignored");
    }
```
(Match the `warnings` container type used by `readSize`.)

`AeronTransport.h`: `#include "rune/protocol/net/rune_gso_bindings.h"`; members `rune_gso_bindings_t* gso_ = nullptr; bool gsoWasActive_ = false;`; public `[[nodiscard]] bool udpGsoActive() const noexcept { return gso_ != nullptr || gsoWasActive_; }`.

`AeronTransport.cpp` after `aeron_driver_context_set_mtu_length(driverCtx_, cfg_.mtuLength);`:

```cpp
    if (cfg_.udpGso) {
        gso_ = rune_gso_bindings_create(1, 1);   // NULL off Linux / unsupported kernel -> stock Aeron
        if (gso_ != nullptr && rune_gso_bindings_install(driverCtx_, gso_) < 0) {
            rune_gso_bindings_delete(gso_);
            gso_ = nullptr;
        }
        gsoWasActive_ = gso_ != nullptr;
    }
    if (cfg_.maxMessagesPerSend > 0)
        aeron_driver_context_set_network_publication_max_messages_per_send(driverCtx_, cfg_.maxMessagesPerSend);
    else if (gso_ != nullptr)
        aeron_driver_context_set_network_publication_max_messages_per_send(driverCtx_, 16U);
```
and right after every `aeron_driver_context_close(driverCtx_)` in the file: `if (gso_ != nullptr) { rune_gso_bindings_delete(gso_); gso_ = nullptr; }`.

`InvokerBench.cpp` RESULT line: append `" gso=%s"` with `(tc.udpGso && rune_gso_platform_supported() == 3) ? "on" : "off"` (include the header).

- [ ] **Step 4: Verify GREEN** — Mac: env test PASS, full transport tests unchanged. VM: sync, configure the client tree once with the toolchain recorded in `/root/e2e/rune-client-cpp/build-rel/CMakeCache.txt` (`grep CMAKE_TOOLCHAIN_FILE`), `-DBUILD_TESTING=ON`, then `AeronTransportGso.*` PASS.

- [ ] **Step 5: Commit** (wt5): `git commit -m "feat(transport): UDP GSO/GRO bindings in the client embedded driver (default on) + gso= in bench RESULT"`.

---

### Task 5: Verification gate (controller; real paths, default config)

- [ ] **Step 1:** VM (inside the lock, after `vm-clean.sh`): broker full suite on the GSO tree — `cd /root/gso/rune-broker && bash scripts/run-broker-tests.sh build-rel` → same pass set as the integ baseline (`/root/wt26/gate-linux.log`); any new failure is owned and fixed (RED repro first).
- [ ] **Step 2:** VM: client full suite (`ctest --test-dir build-rel --output-on-failure`) → green.
- [ ] **Step 3:** VM: the same broker suite with `RUNE_AERON_UDP_GSO=0` exported → green (stock path intact).
- [ ] **Step 4:** DR e2e matrix on k3s-node-01 (images tagged with the git SHA, never `latest`) → same verdicts as the last green matrix.
- [ ] **Step 5:** 3-node e2e smoke at MTU 1408 with GSO on: `tp.sh` with `RUNE_AERON_MTU=1408 … --mtu 1408` using `BROKERDIR=/root/gso/rune-broker/build-rel` and the GSO client binary → errors=0, broker logs contain `rune-gso: UDP GSO active` and `rune-gso: UDP GRO active`, effective config shows `gso=on`.

### Task 6: Perf proof on k3s-node-01 (controller)

- [ ] **Step 1:** ABAB n=5, 3-node acks=all 1 KiB saturate 1.5M records, same config as findings §4.3: arms (a) MTU 1408 `RUNE_AERON_UDP_GSO=0`/`RUNE_CLIENT_AERON_UDP_GSO=0`, (b) MTU 1408 GSO on, (c) MTU 65504 GSO on. Clean box, logs off, PRE-RUN load printed, disk cleaned before/after.
- [ ] **Step 2:** Record medians, CPU busy, and the `rune-gso` log proof in the findings doc §4.4 in the same turn; commit.

### Task 7: Kafka vs Rune over a veth pair at MTU 1500 (controller)

- [ ] **Step 1:** Script `bench/kafka-compare/veth-net.sh up|down`: three network namespaces (`rn0..rn2`) joined to a bridge in the root namespace by veth pairs, MTU 1500, addresses 10.77.0.1-3/24, client in the root namespace on 10.77.0.254; `down` deletes them. Brokers start via `ip netns exec rnX`. Verify `ethtool -k` on the veths shows `tx-udp-segmentation` and `generic-receive-offload` on.
- [ ] **Step 2:** Run Kafka (existing compare harness, same 3-node acks=all 1 KiB workload) and Rune (MTU derived = 1452 -> aligned 1440, GSO on vs off) over it, ABAB n=5 each; record in the findings doc §4.5 with the namespace setup; `veth-net.sh down`; commit.
