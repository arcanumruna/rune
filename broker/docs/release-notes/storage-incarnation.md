# Release note: storage identity, lost-disk safety and the truncation guard (ISR protocol v3)

This release fixes a data-loss bug: a broker whose data directory was lost (a replaced or wiped disk) used to rejoin
while the committed assignment still named it the leader of a partition, re-lead at its old epoch with an empty log, and
its followers truncated records that clients had been acknowledged. Every part below is part of that fix. Read sections
2 to 4 before upgrading.

## 1. What changed

- **Every data directory has an identity.** `RUNE_DATA_DIR/storage.meta` is written the first time a directory is
  formatted. The controller keeps the committed storage id of every node; a node that reports a *different* one (a
  replaced disk, a wiped disk, a restored copy) is **fenced**: it is removed from every in-sync replica set, every
  leadership and every retained leader lineage, and only then is its new storage confirmed. Until that happens the node
  logs `storage UNCONFIRMED` (and `rune_role` shows `STORAGE ... state=UNCONFIRMED`), holds any leader role it was
  assigned (its partitions fail over to another in-sync replica), and neither stands for election nor votes.
- **The format CLI.** An empty or unformatted data directory is no longer formatted silently.
  `rune_broker storage format --new` (each founding node of a new cluster), `--join` (a new or replaced disk in a running
  cluster) and `--reassign-join` (a restored directory, see section 5). Without a format the broker refuses to start
  (exit code 78) and prints the command to run.
- **`RUNE_STORAGE_FORMAT`** (`new` | `join` | `auto`) is read only when the directory is empty and unformatted. `auto` is
  for development sandboxes (the repo's compose files set it). On a multi-node cluster `RUNE_STORAGE_FORMAT=join` may stay
  set: a replaced disk then rejoins by itself.
- **`RUNE_STORAGE_RECOVERY_VOTE`** (default `0`) is a disaster-recovery escape hatch only: it lets an unconfirmed node vote
  so a cluster that lost the disks of a majority can elect its one intact node. It can lose metadata that only the lost
  disks held, and logs at critical level.
- **A follower never truncates below the high watermark its own leader told it in the same epoch.** If the leader's
  lineage ends lower (a leader that lost its log), the follower refuses, applies nothing, fetches nothing, logs
  `REFUSED to truncate below the committed offset ...`, and reports to the controller, which fails the leader over. A
  leader whose first record of an epoch starts somewhere else than the follower's copy (two histories under one epoch)
  answers `kErrEpochHistoryMismatch`, and the follower refuses the same way. `RUNE_SURVIVOR_LEADER_ELECTION=1` keeps the
  previous behaviour for user topics (log and truncate) because an unclean election is then a configured choice.
- **The controller records a term** (`ControllerTermRecord`) before it acts, and an empty metadata log on an already
  formatted storage never bootstraps a second cluster: the controller halts and resigns.
- **A `--new` node that finds an established cluster joins it** instead of founding a second one: it logs a warning
  (`R-W3`: `storage of node <n> was formatted for a NEW cluster, but peer <p> reports an established cluster ...`), persists
  as a joiner, and starts `storage UNCONFIRMED` like a replaced disk.
- **Wire (replication schema 112, version 4).** `AlterIsr` carries `proposalId`, per-member storage ids and `sendSeq`;
  `AlterIsrResponse` carries `proposalId`, `controllerEpoch` and `sendSeq`; `FetchRequest` carries `storageId` and
  `lastEpochStartOffset`; `FetchResponse` has the error code `-11` (`kErrEpochHistoryMismatch`). Old receivers read the
  fields they know; absent fields mean 0 / -1.
- **New metrics / output.** Counters `storage gate state`, `leader roles held`, `storage pending nodes`, `lost-committed
  reports`; `rune_role` prints a second line
  `STORAGE nodeId=<n> state=<CONFIRMED|UNCONFIRMED|UNDECIDED|UNKNOWN> heldLeaderRoles=<n> pendingNodes=<n> outOfSync=<n> lostCommittedReports=<n>`.

## 2. Upgrade order and feature level 2

The controller-side behaviour (the strip, the confirmation records and the `StorageReset` mark) is gated by metadata
feature level **2**. The level rises on its own once every node runs this binary.

1. Upgrade the nodes one at a time (a rolling upgrade is supported). Existing data directories are adopted as `legacy`
   on their first start with this binary; nothing needs to be formatted.
2. Production launchers must format (or set `RUNE_STORAGE_FORMAT`) before the **first** start of a *new* directory.
3. When the last node runs the new binary the cluster commits level 2. **There is no downgrade after level 2**: old
   binaries ignore `storage.meta` and skip the new metadata records, but a cluster at level 2 has fenced storages the old
   code cannot honour.
4. Until the roll completes, the below-committed report and the truncation guard already work (they are decided at every
   level), but the controller cannot fence a replaced storage.

## 3. Roll forward only while any node logs `storage UNCONFIRMED`

A node that logs `storage UNCONFIRMED` is gated by the new binary. **Do not roll it back to an old binary while any node
shows `storage UNCONFIRMED`**: the old binary ignores the gate and would lead (and serve) from a storage the cluster has
already fenced. Finish the roll forward (or wait until the node shows `CONFIRMED`) first.

## 4. Do not restart another node while a replaced disk is UNCONFIRMED

Confirming a replaced disk needs a consensus quorum. In a 3-node cluster, one replaced disk plus one stopped node is two
missing voters: the cluster has no quorum until the replaced node is confirmed, and it cannot be confirmed without one.
The gate log names the missing voters. **Do not restart (or stop) another node while a replaced disk shows
`storage UNCONFIRMED`**; it normally confirms within seconds.

## 5. Restoring a directory from a backup

On a multi-node cluster, after restoring a whole data directory from a backup (with or without `storage.meta`), run
`rune_broker storage format --reassign-join --data-dir <D>` **before** starting the node. It assigns the directory a new
storage id and marks it a joiner, so the node rejoins as a replaced disk and catches up from the cluster. Never delete or
copy `storage.meta` on its own: a damaged or missing one makes the broker refuse to start.

## 6. Messages you may see

- **Below feature level 2** (a single-replica disk partition led by a node waiting for storage confirmation): the
  partition is re-granted to that node at each leader-epoch bump (one metadata record per bump interval) until the roll
  completes. This is expected and stops at level 2.
- **`controller: partition <t>/<p> lost its only in-sync replica's storage (node <N>); it stays offline until
  RUNE_SURVIVOR_LEADER_ELECTION=1 elects the most complete live replica (may lose committed records)`**: the partition's
  only in-sync replica lost its disk, so no replica is known to hold the committed records. The partition stays offline
  rather than guessing. Setting `RUNE_SURVIVOR_LEADER_ELECTION=1` on the controller lets it elect the most complete live
  replica; **that can lose committed records**, which is why it is an explicit opt-in. Restoring the lost disk (section 5)
  is the lossless route.
- **`REFUSED to truncate below the committed offset ...`** (follower, critical, repeated every 30 s while it lasts): a
  leader's log is shorter than what the follower knows is committed. The controller fails the leader over; no action is
  needed unless it persists (`no controller path` in the line means the replica has no route to the controller).
- **`storage of node <n> was formatted for a NEW cluster, but peer <p> reports an established cluster`** (`R-W3`): see
  section 1. If `RUNE_PEERS` should name a different cluster, stop the node now.

## 7. Known limits (residuals)

- **Old-controller answer races.** Until the roll completes, an old controller answers a below-committed report with
  `FencedLeaderEpoch` (the reporter then resends slowly, once a second) and does not fence storages. The answer-correlation
  races of the previous protocol (a late answer to a superseded proposal) remain possible until every node runs this
  binary.
- **A cluster id** is not carried on the wire: a broker pointed at the wrong cluster is not detected by identity alone.
- **User partition directories deleted inside an intact storage** (the node's `storage.meta` survives) are not detected
  (Kafka parity).
- **Different-epoch divergence below a floor** (power loss of a sole-ISR leader with fsync off) needs unclean-shutdown
  detection (Kafka KIP-966) and is a follow-up.
- **A node-control path cut while consensus is up** (the node reaches consensus peers but not the controller's control
  path) leaves a refusing follower reporting with `no controller path`.
- **The first data epoch of a partition** is not covered by the history check (both sides start it at offset 0); only the
  gate and the strip protect a wiped leader re-leading that epoch. Closing it needs a per-storage epoch fingerprint
  (a follow-up).
- **Single-node restore.** A restored single-node directory starts as it is; there is no peer to detect a stale copy, so
  records acknowledged after the backup are lost. That is inherent to one replica (see `docs/SELF-HOSTING.md`).
- **`PartitionReplica::publishedReplicaLeo_`** still uses atomics for the cross-thread position publication (engineering
  rule 1); a separate design is scheduled.
