# Rune vs Kafka comparison harness

Drives Rune and Apache Kafka over a matched-durability benchmark matrix on one
box, sequentially, and emits `results.json` + `specs.json` that the
`rune-site/compare.html` page renders from.

Spec: `../../docs/superpowers/specs/2026-09-26-rune-vs-kafka-benchmark-design.md`

## Files
- `env.sh` — all paths/ports/knobs, overridable per box.
- `lib.sh` — logging, Rune/Kafka lifecycle, JSON emission, median.
- `run-rune.sh` — Rune cell runners (`rune_produce`, `rune_consume`).
- `run-kafka.sh` — Kafka cell runners (`kafka_produce`, `kafka_consume`, `kafka_latency`).
- `parse_kafka.py` — parse Kafka perf-tool output → key=value (latency → µs).
- `collect.py` — merge `results.jsonl` → `results.json` (median+spread over repeats).
- `install-kafka.sh` — install JDK + Kafka release on the box.
- `smoke.sh` — tiny end-to-end validation (correctness, not measurement).

## Durability tiers (matched on both tools)
| Tier | Rune | Kafka |
|------|------|-------|
| D0 | `topic bench partitions P with (tier = leader)`, acks=leader | acks=1 |
| D1 | `tier=majority`, acks=all | acks=all, RF=3, min.insync=2 |
| D2 | `tier=strict` (fdatasync), acks=all | acks=all, flush.messages=1 |

`tier` is a `with (...)` setting of `topic` DDL. The harness creates the bench topic with the rune CLI
(`$RUNE_CLI`, default `rune` next to `$RUNE_PROD_BIN`) right after the broker/cluster is up
(`rune_create_topics` in `lib.sh`), e.g. `rune ql -e "topic bench partitions 8 with (tier = leader);" --broker 127.0.0.1:40200`.

## Prototype (build-agent-01)
```bash
# on the box, from this dir:
./install-kafka.sh          # JDK + Kafka (once)
./smoke.sh                  # tiny matrix, prints results.json
```
Numbers from a loaded shared box are meaningless — smoke only proves the
harness runs and the JSON schema is correct.

## Real run (ccx43)
Set `CORES_BROKER=0-7 CORES_CLIENT=8-11` (pin broker off client/driver), full
matrix via the (to-be-added) `run-matrix.sh`, then `collect.py`, `scp` results
back, and tear the box down.
