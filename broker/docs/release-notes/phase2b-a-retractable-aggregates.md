# Phase 2b Part A: retractable Min / Max / Last / LastNonNull

Aggregate views can now maintain `Min`, `Max`, `Last` and `LastNonNull`, exact under update, group move, delete and a broker restart.

## Semantics

- `Min` / `Max`: numeric sources only (Int32, Int64, UInt16, UInt32, UInt64, Float64). Output type = source type. Nulls are ignored; NaN and ±Inf count as NULL (counted in `aggNonFinite`). An empty or all-null group gives NULL.
- `Last`: the value of the most recently written contributor, including NULL. Any fixed-width source type; output type = source type.
- `LastNonNull`: the most recently written contributor value that is not NULL, or NULL if there is none. It is the new `MergeRule::LastNonNull`, wire value **8** (the enum is append-only; rules 0-7 are unchanged).
- Retracting the cached Min/Max scans the group's packed Min/Max value block once (O(group size) but sequential, reported by `BM_ViewFoldExtremeRetract` at 1k and 10k); the typical fold stays inside the §14.0 gate.

## Compatibility

- No catalog or fingerprint format change; `CatalogGoldenRegression.*` passes unchanged. The v2 checkpoint wire format is unchanged too (it already carries a table count), but a view with Min/Max checkpoints one more table in the same v2 record: its per-group value blocks (`blocks`, table id 3), restored offset-atomically with the others. 2a views and views with only Last/LastNonNull keep the 3-table record byte for byte. The links, head and caches are bytes inside the contributor and group rows.
- Views that use none of the four rules keep their 2a layout byte for byte.
- **Upgrade every broker before creating a view with min/max/last (AD1).** `CreateView` events are applied on every broker from `__catalog`, and a broker without Part A cannot install such a plan. There is no version negotiation yet.
- The client's `MergeRule` gains `LastNonNull`, and the client's `deserializeAuthored` accepts it (`rune catalog replay` re-reads stored plans with it).
- Per-pass delta cap: a view with Min/Max dirties up to 12 entries per source record (Last-only: 8, 2a: 5), and the worker's per-pass record cap now follows from that exact bound.
