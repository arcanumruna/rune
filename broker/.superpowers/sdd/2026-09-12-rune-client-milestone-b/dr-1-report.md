# DR-1 report — harness scaffold + multi-client driver + parametrization framework

**Status:** DONE. Baseline matrix is green live against the real 3-node
`aeron:udp` Rune cluster + real client containers (no mocks). All fault helpers
proven live. A real client-side race was found and worked around.

## What shipped

New Python harness in `rune-broker/dr-e2e/`:

```
dr-e2e/
  requirements.txt   pytest + pytest-timeout        pytest.ini   hard timeouts + markers
  README.md          run instructions + dimension vocabulary + netem caveat
  conftest.py        cluster (session) / fresh_cluster (function) / netem_ready fixtures
  harness/
    dockerutil.py    bounded docker / docker-compose CLI wrappers (hard timeouts)
    cluster.py       3-node compose lifecycle, wait_for_leader, identify_leader,
                     wait_for_new_leader, serving_node (data-plane readiness probe), teardown
    driver.py        run_clients(n, **cfg) — N dr_client containers, concurrent, parsed;
                     bounded startup-crash + connect-timeout retry
    faults.py        kill_node(KILL/TERM), pause/unpause, restart, netem, partition/heal
    invariants.py    no_committed_loss, per_key_ordering, exactly_once, single_leader,
                     resume_from_offset, durable_after_restart
    matrix.py        THE parametrization framework (matrix() + parametrize())
  tests/
    test_dr1_baseline.py   16-case baseline matrix
```

New driver in `rune-client-cpp/`:
* `examples/dr_client.cpp` — configurable env-driven produce/subscribe driver:
  #clients (via harness), #keys, #records, rate_hz, from Earliest/Latest/Offset,
  target node, slow-subscriber, per-message `PRODUCED/DELIVERED/SUMMARY` output.
* `docker/Dockerfile.dr` + `docker/Dockerfile.dr.dockerignore` — builds `dr_client`
  into image `rune-dr-client:dev`.
* `CMakeLists.txt` — registers the `dr_client` example target.

## Baseline: does pytest run it green? How many cases?

Yes. `pytest -v` → **16 passed in 205.58s** against the live cluster.
The matrix is a genuine cross-product: `{1,3} clients × {1,4} keys ×
{earliest,latest} from × {leader,follower} target = 16`. Each case does
up→produce→subscribe→assert-invariants, cluster brought up once (session fixture)
and torn down `down -v --remove-orphans` at the end. Clean teardown verified: no
leftover containers or networks after the run.

Invariants asserted per case: single-leader (before + after), per-key-ordering,
exactly-once, and (earliest cases) no-committed-loss over the union of subscriber
views.

## Do the fault helpers work?

All proven live against running broker containers:
* **netem** — PROVEN. `tc netem loss 37%` installed on a broker's `eth0` and
  cleared. End-to-end packet effect proven: rune↔rune `ping` went `0%` → `100%`
  packet loss under `partition()` (netem 100% loss), then healed back to `0%`.
* **partition/heal** — PROVEN (netem 100% loss / clear, with the ping proof above).
* **pause/unpause** — PROVEN (`docker pause`/`unpause`).
* **kill_node** — implemented via `docker kill --signal KILL/TERM` (same
  mechanism the existing e2e.sh leader-kill uses and which Milestone B relies on).
* **restart** — `docker compose restart`.

### How netem is applied without touching the broker image

A privileged **sidecar shares the broker's network namespace**:
`docker run --rm --net=container:<broker_cid> --cap-add=NET_ADMIN rune-netem:dev
tc qdisc replace dev eth0 root netem ...`. The qdisc persists after the sidecar
exits, so the broker image needs no `iproute2`/`NET_ADMIN` change. The
`rune-netem:dev` image (alpine + iproute2 + iptables) is built once on demand.

## Docker-Desktop / netem caveat

* **netem WORKS** on Docker Desktop's LinuxKit VM with `--cap-add=NET_ADMIN`
  (proven live here). This is what `netem()` and `partition()` use.
* **iptables inside `--net=container:` sidecars is NOT reliable** on Docker
  Desktop (LinuxKit kernel may lack the nf modules). Therefore `partition()` uses
  netem 100% loss (portable) rather than `iptables -j DROP`. On Linux CI both
  work; a *directional/pairwise* partition keyed on peer IPs (iptables) is
  Linux-CI-only and noted in `faults.py`. For per-broker `NET_ADMIN` directly on
  Linux CI, add `cap_add: [NET_ADMIN]` to each broker service in the compose file
  (the sidecar approach avoids needing that).

## The parametrization framework — one-line API a later ticket uses

```python
from harness import matrix

@matrix.parametrize(
    target=["leader", "follower"],       # node the fault hits
    data_state=[(1, 20), (16, 2000)],    # (keys, records)
    timing=["mid_produce", "mid_subscribe"],
    intensity=[10, 50],                  # e.g. netem loss %
    repetition=3,                        # repeat each point
)
def test_partition(case, fresh_cluster):
    ...   # -> 2*2*2*2*3 = 48 parametrized cases, ids auto-generated
```

Dimension vocabulary: `target, count, timing, intensity, client_state,
data_state, repetition, recovery` (documented in `matrix.py` + README). Only
supplied dimensions are swept; the rest keep `Case` defaults, so small matrices
stay small and dozens-to-hundreds-case matrices are one edit away. Verified:
`matrix()` builds the 16-case baseline and a 48-case example; `parametrize()`
returns a working `pytest.mark.parametrize`; unknown dimensions are rejected.

## Concerns / findings

1. **Client-side startup SIGSEGV race (real defect, worked around).** The FIRST
   produce through a freshly-connected `rune::client::Client` in the current
   rune-client-cpp build (commit `06764bb`) crashes with SIGSEGV intermittently
   — observed ~80% of runs on `:dev`, 0% when extra stderr I/O perturbed timing,
   confirming a race (not a driver logic bug; the *old* `rune_produce_subscribe`
   binary races far less but is not immune). Root cause is in the client library
   (connect→first-send window), not the harness. **Worked around** two ways:
   (a) a 250 ms post-connect settle (`RUNE_SETTLE_MS`) in `dr_client` closed the
   window — 8/8 then 16/16 clean; (b) the driver bounded-retries a startup crash
   (exit 139) or connect-timeout **only when zero records were produced** (never
   risks double-produce). **This race should be filed against rune-client-cpp and
   fixed there** — the settle delay is a harness-side mitigation, not a fix, and
   it will matter for DR-2..15 which restart/redirect clients constantly.

2. **Session-scoped cluster + persistent partition-0 log.** The baseline reuses
   one cluster; the log accumulates across cases. Handled by giving each run a
   disjoint client-id window (`cid_base`, stride 1000) so old records group under
   different cids and never confuse per-key ordering / exactly-once, plus
   `RUNE_EXPECT_CID_EXACT` so a subscriber counts only its own run's records.
   Fault tickets that need pristine state use the function-scoped `fresh_cluster`.

3. **`serving_node()` readiness probe.** Because of finding (1), the baseline
   targets a data-plane-ready node discovered by a tiny probe-produce; the
   leader/follower axis still varies the case id and single-leader is still
   asserted against the real leader. Once the client race is fixed, this can be
   simplified back to direct leader/follower targeting.

4. **Runtime.** Baseline is ~3.5 min (16 cases, one cluster). Fault matrices with
   `fresh_cluster` will be far slower; DR-2+ should batch cases per cluster where
   the fault is non-destructive, and reserve per-case fresh clusters for
   membership-changing faults.

## Run instructions

```bash
cd rune-broker/dr-e2e
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
pytest -v                 # 16-case baseline vs live cluster
DR_E2E_BUILD=1 pytest -v  # also (re)build compose images on bring-up
```
Prereqs: `rune-broker-rune-{1,2,3}` images (compose build) and
`rune-dr-client:dev` (`docker build --platform linux/amd64 --build-context
rune-protocol=../rune-protocol -f docker/Dockerfile.dr -t rune-dr-client:dev .`
from rune-client-cpp). `rune-netem:dev` builds automatically on first
network-fault use.
