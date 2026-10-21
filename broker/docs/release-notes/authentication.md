# Release note: authentication and access control

Every client now proves who it is, every request is checked against explicit grants, and brokers
authenticate and encrypt their links to each other. Users, roles and grants are events in a new
replicated log, `__security`, with full history. This release has breaking changes and no
migration. The reference is [SECURITY.md](../SECURITY.md); how to start a cluster is in
[SELF-HOSTING.md](../SELF-HOSTING.md#authentication-is-on-by-default).

## 1. Breaking: authentication is on by default

A broker **without `RUNE_CLUSTER_KEY_FILE` refuses to start**, on a single node too. Make a key once,
give every node the same file, and bootstrap the first admin once per cluster:

```bash
rune security gen-cluster-key --out secrets/cluster.key
rune security bootstrap --admin admin --cluster-key-file secrets/cluster.key --broker rune-1:40200
```

- A key file that is accessible by group or others, shorter than 32 bytes, missing or unreadable makes the
  broker refuse to start. The docker image entrypoint refuses a missing file with the broker's own message and
  never generates a key.
- `RUNE_AUTH=disabled` is dev mode (no login, every request is `anonymous` with full access). It is for
  single-node development only: a broker with more than one peer refuses to start in dev mode. It is never the
  default.
- Every launcher in the repository now generates a key before it starts the brokers, bootstraps the cluster and
  creates an `e2e` user: `docker/e2e.sh`, `schema-e2e.sh`, `schema-evolution-e2e.sh`, `view-engine-e2e.sh`,
  `transient-failover-e2e.sh`, `catalog-e2e.sh`, the dr-e2e harness and the `bench/kafka-compare` scripts (the
  benchmarks run authenticated, as production does, in shard mode: authentication refuses legacy mode with per-worker
  ingress/egress, and legacy mode in a multi-broker cluster). `docker/auth-e2e.sh` is the new 3-node authentication e2e.
  The compose files mount `./secrets/cluster.key` read-only at `/run/secrets/cluster.key`.

## 2. Breaking: client version 9

The client library and the `rune` CLI speak protocol version 9. **An older client is refused at
`Connect`** by a cluster with authentication enabled (never silently downgraded), and a version 9 client
needs credentials: `ClientConfig::username` with `password` or `passwordFile` (the `rune` CLI and the
examples read `--user` / `--password-file` or `RUNE_USER` / `RUNE_PASSWORD_FILE`). A password is never accepted
on a command line or in the environment.

- `ClientConfig::authToken` is **removed**. It was documented as validated by the broker's existing auth path,
  but no such path existed. The wire field stays reserved and is always sent empty.
- A client with credentials refuses a broker that skips the login (dev mode, an old broker) with
  `AuthenticationFailed`; `ClientConfig::allowUnauthenticatedBroker = true` opts out.
- The `ConnectResponse` carries `authMode` and `authState`; a resumed session keeps its authentication.

## 3. New error codes 44 to 48

| Code | Name | Meaning |
|---|---|---|
| 44 | `AuthenticationFailed` | No credentials, wrong password, unknown or disabled user, or a rate-limited attempt (`retryAfterMs`). The session ends. |
| 45 | `NotAuthorized` | The operation is not granted, or the object is not visible. Fails exactly that request. A revoked `READ` ends the subscription with it. |
| 46 | `SessionRevoked` | The user was dropped or disabled, or `TERMINATE SESSIONS` ran. Terminal for the session. |
| 47 | `PasswordChangeRequired` | The first login with the one-time password; every request except `ALTER USER ... SET PASSWORD` answers it. |
| 48 | `AuthUnavailable` | This broker's `__security` state is not current (catching up after a start, or stale: cut off its leader or the consensus). Retriable: the client backs off and tries another broker. As a session-level notice: the broker stayed stale too long and ended the session. |

## 4. Breaking: inter-broker frames are encrypted

Brokers prove possession of the cluster key to each other (mutual HMAC challenge-response, the key never
crosses the wire) and seal every inter-broker frame (consensus, replication, the admin plane, the workers'
links) with a per-link key. **Each frame carries 32 bytes of overhead, so the maximum produce batch is 32
bytes smaller** than before; the frame limits subtract the 32 bytes in every mode, so they never change with
the mode. All brokers of a cluster must be upgraded together (a broker without the key cannot join). The
cluster key rotates without downtime in three rolling restarts (`RUNE_CLUSTER_KEY_FILE_NEXT`).

## 5. New: users, roles and grants (Rune QL)

`CREATE USER`, `ALTER USER` (add / drop / set a password, `DISABLE`, `ENABLE`), `DROP USER`, `CREATE ROLE`,
`DROP ROLE`, `GRANT`, `REVOKE`, `GRANT ROLE`, `REVOKE ROLE`, `SHOW USERS`, `SHOW ROLES`,
`SHOW GRANTS FOR USER|ROLE ... [AS OF ...]` and `SHOW SECURITY HISTORY`. Only SCRAM-SHA-512 verifiers are
stored. A revoked `READ` ends the affected subscriptions with `NotAuthorized`, a revoked `WRITE` fails the next
produce, and a dropped or disabled user's sessions end with `SessionRevoked`. Catalog history, export, diff and
replay now need `DESCRIBE ON CLUSTER`.

## 6. New: metrics

Aeron counters 1 000 226 to 1 000 235: `rune-security-apply-halted`, `rune-security-applied-offset`,
`rune-auth-mode`, `rune-logins-ok`, `rune-logins-failed`, `rune-logins-rate-limited`, `rune-sessions-revoked`,
`rune-peer-links-established`, `rune-peer-link-refusals`, `rune-peer-frame-drops`. A halted `__security` apply
fails closed; the runbook is in [SECURITY.md](../SECURITY.md#halted-the-__security-apply-halted-d28).

## 7. Upgrading

There is no migration: stop the cluster, generate a key, mount it on every node, start the brokers, bootstrap the
first admin, recreate your services as users with grants, and move the clients to version 9 with credentials.
Set `RUNE_REQUIRE_TLS=true` in production: SCRAM keeps the password off the wire but does not protect the data.
