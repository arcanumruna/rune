# Milestone D — Distributed Partition Leadership (write horizontal scale-out)

> **STATUS: DRAFT / IDEA CAPTURE.** Not reviewed, not planned, not scheduled.
> Written to record the design direction discussed on 2026-09-15 so it isn't lost.
> Do NOT begin implementation from this document — it needs a full brainstorming
> + spec pass first. Superseded assumptions are expected.

## Motivation

Today (Milestone B/C) the cluster runs a single simplification: **the cluster
(consensus) leader owns ALL partitions.** All produce traffic funnels through one
node. Partition-hash + per-partition single-writer logs already exist and work,
but per-node partition-*leadership distribution* does not. Distributing partition
leadership across nodes is the key unbuilt piece for horizontal **write** scale-out
(throughput ≈ batch × partitions × nodes → the 100M+ msg/s target).

## Scope

1. **Controller-driven partition-leadership assignment.** Extend the Phase-2
   metadata consensus with a replicated *partition → leader-node* map (balanced
   across the cluster), replacing "cluster leader owns all." The controller
   (metadata quorum) is the authority; the assignment map is durable + versioned.

2. **Per-node multi-role runtime.** Each `BrokerNode` runs `PartitionReplica` as
   **leader** for its assigned partitions and **follower** for the rest.
   `ReplicationMetadataBridge` already carries per-partition role/epoch plumbing
   (DR-scope hardcodes leader = cluster-leader, ISR = all) — generalize it to read
   the controller's assignment map. This is the load-bearing change.

3. **Leadership rebalancing.** On node join/leave/failure, the controller
   reassigns the affected node's partitions to survivors (preferred-leader +
   balance heuristics) with leader-epoch bumps. Rides the failover machinery
   hardened in the DR-E2E epic (epoch-lineage, no committed loss on handoff).

4. **Client metadata + routing.** A `Metadata` request (who leads partition P) +
   client-side routing of produce/subscribe to the owning node. Clients cache the
   partition→leader map and refresh on REDIRECT / epoch change (reuse GB-4
   REDIRECT as the fallback path). Producer partitioner already computes the
   partition; this adds the partition→node hop.

5. **Coordinator placement generalization.** Group coordinators (Milestone C-A)
   and transaction coordinators (C-B) move from "leader-only" to placed by
   `hash(groupId)` / `hash(txnId)` across nodes — i.e. the internal topics
   `__consumer_offsets` / `__transaction_state` become genuinely distributed.
   **This is the direct interaction with Milestone C** and the reason D must be
   sequenced deliberately relative to C.

6. **DR-E2E + benchmarks.** Multi-leader parallel-produce throughput (the scale
   story), leadership-rebalance under node churn, no committed loss across
   leadership handoff, balanced-load + skew benchmarks (all under the 100µs p99
   hot-path gate where applicable).

## Interaction with Milestone C

MC-A (consumer groups) is currently being designed against "cluster leader = sole
coordinator." Under D, coordinator placement becomes hash-based. Sequencing
options captured (decision deferred): (a) D before C (no C rework); (b) C first
with a coordinator-placement seam, then D fills it in; (c) spec both, decide order
later. **User direction 2026-09-15: treat D as its own milestone; this draft is
idea-capture only.**

## Open questions (for the real spec pass)

- Per-partition leader election model: controller-assigns (Kafka-style, ISR-based,
  one metadata consensus) vs. per-partition Raft groups. Rune already has ONE
  consensus group (metadata) + separate Phase-3 replication → controller-assigns
  is the faithful fit, but confirm.
- Assignment/balance policy: preferred-leader, rack/AZ awareness, skew handling,
  min-churn on rebalance.
- How aggressively to co-locate a group's coordinator with the offsets-partition
  it manages.
- Split-brain / fencing across many independent partition leaderships at once
  (the DR-10/11 story multiplied by partition count).
- Materialized-view placement rides on this — see the views draft (co-partitioned
  view shards follow their partition's leader/replica). Cross-reference when that
  spec is written.
