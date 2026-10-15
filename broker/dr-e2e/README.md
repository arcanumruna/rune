# Rune disaster-recovery E2E harness (`dr-e2e`)

> **Full suite reference:** [`DR_SUITE.md`](DR_SUITE.md) — comprehensive coverage of all 14
> DR tickets, 6 invariants, the durability contract, Docker Desktop caveats, and the
> DR-BUG-1..6 catalogue (generated from source; authoritative for DR-9).

A py-testinfra-style **pytest + Docker** harness that drives the REAL 3-node
`aeron:udp` Rune cluster (`rune-broker/docker/docker-compose.yml`) plus multiple
`rune-client-cpp` client containers, injects faults, and asserts durability /
ordering / leadership invariants.

The whole point is the **parametrization framework** (`harness/matrix.py`): a
disaster is never one test — it is a MATRIX across node × count × timing ×
intensity × client-state × data-state × repetition × recovery. DR-2..15 declare
dozens-to-hundreds of cases per fault type in one line.

## Layout

```
dr-e2e/
  requirements.txt        pytest + pytest-timeout
  pytest.ini              hard per-test timeout, markers
  conftest.py             cluster / fresh_cluster / netem_ready fixtures
  harness/
    dockerutil.py         bounded docker / docker compose CLI wrappers
    cluster.py            3-node compose lifecycle, wait_for_leader, identify_leader
    driver.py             run_clients(n, ...) — multi-client dr_client driver + parser
    faults.py             kill/pause/unpause/restart, netem, partition/heal
    invariants.py         no-loss / per-key-ordering / exactly-once / single-leader / resume / durable
    matrix.py             THE parametrization framework (matrix() + parametrize())
  tests/
    test_dr1_baseline.py  16-case baseline matrix (clients × keys × from × target)
```

## Prerequisites

* Docker (Docker Desktop on macOS is fine — netem proven, see caveat below).
* The broker + client images built once:
  * `rune-broker-rune-{1,2,3}` — `cd rune-broker && docker compose -f docker/docker-compose.yml build`
  * `rune-dr-client:<client sha>` — built by the harness itself on every `Cluster.up()` (`harness.driver.ensure_dr_client_image`: `docker build -f docker/Dockerfile.dr` with the five build contexts rune-protocol, rune-fr, rune-codec, rune-ql, rune-crypto; a cache hit when nothing changed). A prebuilt `DR_CLIENT_IMAGE` must carry a matching `rune.client.sha` label (`--build-arg RUNE_CLIENT_SHA=<client HEAD>`) or the harness refuses it; `DR_ALLOW_STALE_CLIENT_IMAGE=1` accepts a mismatch explicitly.
  * `rune-netem:dev` — built automatically on first network-fault use.

## Run

```bash
cd rune-broker/dr-e2e
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
pytest -v                 # baseline matrix (16 cases) against the live cluster
DR_E2E_BUILD=1 pytest -v  # also (re)build the compose images on bring-up
```

The session-scoped `cluster` fixture brings the 3 nodes up once, waits for a
leader, and tears down (`down -v --remove-orphans`) at the end — even on failure.
Fault tests that need pristine state per case request the function-scoped
`fresh_cluster` fixture instead.

## The parametrization framework

`harness/matrix.py` turns a dimension declaration into a `pytest.mark.parametrize`
cross-product. One line a later ticket uses:

```python
from harness import matrix

@matrix.parametrize(
    target=["leader", "follower"],       # which node the fault hits
    data_state=[(1, 20), (16, 2000)],    # (keys, records)
    timing=["mid_produce", "mid_subscribe"],
    intensity=[10, 50],                  # e.g. netem loss %
    repetition=3,                        # repeat each point 3x
)
def test_partition(case, fresh_cluster):
    ...   # 2*2*2*2*3 = 48 parametrized cases, ids auto-generated
```

### Dimension vocabulary

| dimension      | meaning                                                        | example values |
|----------------|---------------------------------------------------------------|----------------|
| `target`       | node the fault hits / clients target                          | `"leader"`, `"follower"`, `"any"`, `0/1/2` |
| `count`        | how many nodes affected (1 = quorum-safe, 2 = quorum loss)    | `1`, `2` |
| `timing`       | when the fault fires vs the workload                          | `"before"`, `"mid_produce"`, `"mid_subscribe"`, `"after"`, `"idle"` |
| `intensity`    | fault magnitude (loss/dup %, delay ms, signal, pause ms)      | `10`, `50`, `100` |
| `client_state` | what clients are doing                                        | `"producing"`, `"subscribing"`, `"both"`, `"slow_subscriber"`, `"reconnect_storm"` |
| `data_state`   | data shape `(keys, records)`                                  | `(1, 20)`, `(16, 2000)` |
| `repetition`   | repeat index (int N ⇒ range(N))                               | `3` |
| `recovery`     | how the fault is undone                                       | `"restart"`, `"heal"`, `"unpause"`, `"none"` |

`matrix(**dims)` returns `list[Case]`; `parametrize(**dims)` returns the decorator.
Only supplied dimensions are swept; the rest keep `Case` defaults, so small
matrices stay small and large ones are one edit away.

## Multi-client driver

`harness/driver.run_clients(n, network=..., target_service=..., **cfg)` launches
`n` `rune-dr-client` containers concurrently and parses their output into a
`Results` object. cfg knobs: `mode` (produce/subscribe/produce_subscribe),
`records`, `keys`, `rate_hz`, `from` (earliest/latest/offset:N), `slow_subscriber_ms`,
`expect`, `cid_base`, `timeout_ms`, `subscribe_ms`.

The driver binary is `rune-client-cpp/examples/dr_client.cpp`; it emits
machine-parseable lines:

```
PRODUCED  cid=<c> key=<k> seq=<s> offset=<o>
DELIVERED cid=<c> key=<k> seq=<s> offset=<o>
SUMMARY   produced=<np> delivered=<nd> mode=<m> rc=<rc>
```

## Fault helpers (`harness/faults.py`)

| helper                              | mechanism |
|-------------------------------------|-----------|
| `kill_node(cluster, svc, sig)`      | `docker kill --signal KILL/TERM` |
| `pause/unpause(cluster, svc)`       | `docker pause` (cgroup freeze) / `docker unpause` |
| `restart(cluster, svc)`             | `docker compose restart` |
| `netem(cluster, svc, loss=/delay=/reorder=/duplicate=)` | `tc qdisc replace ... netem` in the broker netns |
| `partition(cluster, svc)` / `heal`  | netem 100% loss / clear |

### Network faults without touching the broker image

netem/partition are applied via a **privileged sidecar** that shares the broker's
network namespace:

```
docker run --rm --net=container:<broker_cid> --cap-add=NET_ADMIN rune-netem:dev \
    tc qdisc replace dev eth0 root netem loss 30%
```

The qdisc installed on `eth0` affects the broker's traffic and persists after the
sidecar exits, so the broker image needs no `iproute2`/`NET_ADMIN` changes.

### Docker-Desktop / macOS netem caveat

* **netem WORKS** on Docker Desktop's LinuxKit VM with `--cap-add=NET_ADMIN`
  (proven live: a 100% loss qdisc turns rune↔rune `ping` from `0%` to `100%`
  packet loss, then heals). This is the mechanism `netem()` and `partition()` use.
* **iptables inside `--net=container:` sidecars is NOT reliable** on Docker
  Desktop (the LinuxKit kernel may not have the required nf modules loaded).
  Therefore `partition()` uses netem 100% loss (portable) rather than
  `iptables -j DROP`. On Linux CI both work; a future *directional* / pairwise
  partition keyed on peer IPs is Linux-CI-only and noted in `faults.py`.
* If you need per-compose `NET_ADMIN` on the brokers directly (Linux CI), add
  `cap_add: [NET_ADMIN]` to each broker service in the compose file; the sidecar
  approach here avoids needing that.
```
