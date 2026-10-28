# Client-side (client-cpp) issue batches — sev:medium + sev:low

Scope: all 474 open issues as of 2026-10-27. Selection: every issue labeled `area:client`, plus
`area:protocol`/`area:auth` issues whose fix lands in client code (client decoders, AuthWireClient, CLI).
Result: 134 issues → 1 extension of the planned M31 + 17 client batches (C1–C17) + 9 held.

Conventions follow `docs/medium-issue-program.md`:
- each batch has at least 4 issues on one code path;
- branch `bugfix/client-batch-<n>`;
- one recipe, one implementer, one fresh reviewer, adversarial pass, one fix round.

Order is bugs → test-gaps → tech-debt/docs/enhancements. "Rider" means the issue is not a bug, but it sits in the
same files and goes into the same PR.

## Phase A — bugs

| Batch | Code path | Issues | Notes |
|---|---|---|---|
| **M31+** | SubscriberHub / SessionMachine / AeronTransport lifecycle | **#514 (med)**, #515, #446, #510 + riders #473 (stale SubscriptionSet status after a gap), #468 (filter e2e gaps) | Already designed in the program doc. Riders touch the same hub. |
| **C1** | Producer claim / Tier-2 slot and credit ownership | #322, #323, #325, #330, #341, #372 + rider #342 | Off-owner-thread abort/destroy, Thread-mode close, Direct abandon, Invoker void send behind Tier-2. One invariant: claim credit/slot is returned exactly once on its owner thread. |
| **C2** | Txn client: coordinator route + abortable/fatal classification | #345, #351, #367, #291, #503 + rider #362 (route → Seed/Remote variant) | #362 is the rich-state fix that #351 needs anyway. |
| **C3** | Group consumer hub | #431, #440, #450, #170, #413 (flaky) + riders #378 (typed exception, which is the #413 root), #356 | #450 needs a client-version gate (protocol). #170: `autoOffsetReset` no-op. |
| **C4** | Connect/login retry: Backoff + AuthUnavailable (48) | #481, #519, #461, #607, #613, #478 | One retry policy for 48/44-at-cap across connect, Subscribe, view reads, group, CLI. #461/#607/#613 are enhancements, but they are the same fix. |
| **C5** | SCRAM handshake / ConnectResponse decode (client + protocol) | #231, #233, #235, #239, #240, #194, #544, #57 | Bounds/charset on decode; PBKDF2 off the duty thread or capped (#235); keepalive for bootstrap sessions (#240). |
| **C6** | Credential hygiene (password lifetime) | #257, #56, #305, #184, #185, #306 | Wipe every plaintext copy (config, agents, control ring, console buffers); passwordFile rules; restore echo after SIGINT. |
| **C7** | Error-code mapping to completions | #22, #222, #226, #225, #310, #638 | 45 → "not appended", retry-after as a field, one name table, detail text on gateway rejects. |
| **C8** | `rune console` / CLI behaviour | #301, #302, #303, #307, #308, #309, #397, #435, #641, #23 | Exit codes, parse before connect, terminal rejects, over-long lines, schema commands, non-atomic `limits --set`. |
| **C9** | Client view reads | #162, #76, #600, #488, #51 (flaky) | Client counterpart of M30 (broker views); run after M30. |

## Phase B — test gaps and flakes

| Batch | Area | Issues |
|---|---|---|
| **C10** | Claim / producer tests | #331, #332, #333, #334, #338, #376 (client bench smoke ctest), #535 (moved from C5) |
| **C11** | Txn tests | #350, #352, #363, #369, #373 |
| **C12** | Console tests + docs | #313, #314, #495 (flaky), #304, #315, #317, #311 |
| **C13** | Test rigs: sleeps, freePort, load flakes | #480 (bug, red on main), #588, #370, #422, #426, #429, #475, #364, #44 |

C13 contains #480, which is red on main. It may need to move ahead of Phase A.

## Phase C — tech debt / enhancements

| Batch | Area | Issues |
|---|---|---|
| **C14** | Claim / producer cleanups + perf | #327, #328, #329, #339, #343, #374, #393, #324, #326, #335 |
| **C15** | Txn machine cleanups + API | #340, #344, #346, #349, #353, #347, #348, #354, #368 |
| **C16** | Group / filter cleanups | #357, #428, #371, #472, #482 |
| **C17** | Misc client debt | #178, #60, #182, #180, #97, #377 |

## Held (not batched)

- **Skip list:** #12, #71.
- **Deferred:** need a design or the owner's call.
  - #610 (med): client ASan failures on main. It is deferred in the program doc, but it is a red sanitizer on main.
    Recommend reopening it as its own task.
  - #361 (med)
  - #617 (med)
  - #68 (med): archived lows batch. Review it before C14–C17, since it may already fix some of them.
  - #41 (med, docs)
  - #102: needs a spec.
  - #8: producer head-of-line stall; a design item.
