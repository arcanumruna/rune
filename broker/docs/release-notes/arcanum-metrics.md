# Release note: metrics move to the arcanum-metrics file

Rune's own metrics no longer live in the Aeron media driver's counters. The broker writes them to one memory-mapped file
in the arcanum-metrics format, and the driver's `cnc.dat` now holds only the driver's own counters.

## 1. What changed

- **The metrics file.** `RUNE_METRICS_FILE`, default `${RUNE_DATA_DIR}/metrics/rune-metrics.dat` (directory `0750`, file
  `0640`). The broker creates it afresh at every start and allocates every metric before any agent thread runs; a broker
  that cannot create it does not start.
- **Same ids, labels and keys.** Every metric keeps its type id (1000201 to 1000241), its label (`<name> node=<id>`) and
  its 4-byte little-endian node-id key, so a tool that matched `(type id, key)` in `cnc.dat` matches the same pair in the
  new file. Each metric now also has a kind (gauge or counter) and a single writer thread. The table is in
  `docs/operations.md` "Metrics" and in `broker/libs/node/include/rune/node/RuneMetrics.h`.
- **Process metrics.** New: `process_open_fds`, `process_threads`, `process_resident_memory_bytes`,
  `process_virtual_memory_bytes` and `process_metrics_read_errors_total` (type ids 1000301 to 1000305), sampled from
  `/proc/self` every `RUNE_METRICS_SYSTEM_INTERVAL_MS` (default 1000).
- **`rune_role [<metricsFile>]`** reads the metrics file (default `$RUNE_METRICS_FILE`, else
  `$RUNE_DATA_DIR/metrics/rune-metrics.dat`). Its output is unchanged.
- **Rates and windows are the reader's job.** Each slot holds the current value only; the broker keeps no history.

## 2. Upgrading

- Point monitoring at the metrics file instead of `cnc.dat`. A reader that still scans `cnc.dat` for type ids
  1000201 to 1000241 finds nothing.
- In Docker, the file sits in the data volume by default; set `RUNE_METRICS_FILE` to move it (for example to a tmpfs
  shared with a sidecar). The image is built with the `arcanum-metrics` named build context (`broker/docker/README.md`).
- Type id 1000204 (a metadata commit position that was never published) has no metric; 1000236 to 1000239 are
  reserved.
