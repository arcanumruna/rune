# Rune — Authentication and access control

**Status:** approved by the user on 2026-10-13.
**Scheduling:** directly after the catalog-history phase and before Part D, confirmed by the user on 2026-10-13. `__security` reuses that phase's event-log machinery.
**Out of scope:** tenants and multi-tenancy (a separate design), mTLS client certificates, tokens (OAuth/OIDC/JWT), deny rules and quotas.
**Repos:**
- rune-broker: `__security`, enforcement, the SCRAM server, the cluster key, bootstrap
- rune-protocol: the authentication messages and error codes
- rune-client-cpp: the SCRAM client and credentials config
- rune-ql: user, role and grant DDL
- the `rune` CLI: `security bootstrap`

## 1. Goal

Every client proves who it is, and every request is checked against explicit grants. Anything not granted is refused with an explicit error. Users, roles and grants are stored as replayable events with full history, exactly like the catalog. Authorization adds no measurable cost to the produce/consume hot path.

## 2. Model

- **Principal:** a user. Services such as world servers are users too, with no separate kind.
- **Role:** a named set of grants.
- **Grant:** a triple:
  - an **operation**: `READ`, `WRITE`, `CREATE`, `ALTER`, `DROP`, `DESCRIBE` or `ADMIN`;
  - a **resource type**: `CLUSTER`, `TOPIC`, `VIEW`, `GROUP` or `TRANSACTIONAL_ID`;
  - a **pattern**: an exact name, or `PREFIX '<p>'`.
- Users receive roles and can also receive grants directly.
- **Effective permissions** are the union of the user's direct grants and the grants of all their roles.
- **Explicit allow only. Default deny.** v1 has no deny rules.

### 2.1 Which operation each request needs

| Request | Needs |
|---|---|
| Produce, transactional produce | `WRITE` on the topic |
| Transaction init / markers | `WRITE` on the transactional id |
| Subscribe (any form) | `READ` on the topic |
| JoinGroup / Sync / Heartbeat / OffsetFetch / OffsetCommit | `READ` on the group, and `READ` on each topic |
| GetViewRow / openView | `READ` on the view |
| Metadata / topic and view listing | `DESCRIBE` |
| `CreateTopic` / `CreateView` / `CreateSchema` | `CREATE` on the name (an exact or prefix grant), or on `CLUSTER` |
| `AlterSchema` / `AlterView` / `AlterTopicConfig` / `SetCompatibility` | `ALTER` on the object |
| `DropTopic` / `DropView` | `DROP` on the object |
| Catalog history / as-of / diff (`__catalog` reads) | `DESCRIBE` on the objects shown. Objects the reader cannot describe are filtered out of results. |
| User, role and grant DDL; `__security` reads | `ADMIN` on `CLUSTER` (the security admin) |

**Visibility:** objects the user cannot `DESCRIBE` do not appear in metadata or listings. Direct access to such an object answers `NotAuthorized`, whether or not the object exists, so the answer does not reveal existence.

## 3. `__security`

This log has the same design as `__catalog` in the catalog-history spec, sections 3 to 6:
- one partition, replicated to every broker, acks=all, no unclean election;
- never compacted, always disk-backed;
- serialized through its leader's AdminWorker;
- applied by every broker in offset order;
- each event carries a format version, the statement, the principal, a timestamp and the resulting-state fingerprint, and the apply rules are frozen per format version.

The difference is that reads are restricted to brokers and `ADMIN CLUSTER` holders, because events carry credential verifiers.

**Event kinds:**
- users: `CreateUser`, `AlterUser` (add or remove a password, disable, enable), `DropUser`;
- roles: `CreateRole`, `DropRole`;
- grants: `Grant`, `Revoke`, `GrantRole`, `RevokeRole`.

**Credential material:**
- Only SCRAM-SHA-512 verifiers are stored: salt, iteration count, `StoredKey` and `ServerKey`. Never a password, and never anything that can be used to log in directly.
- A user holds **at most two** active verifiers, to allow rotation.

**History reads** (audit, as-of, diff) use the shared replay library and the same commands as the catalog, scoped to `ADMIN` holders: `SHOW SECURITY HISTORY`, `SHOW GRANTS FOR USER u AS OF …`.

## 4. Definition (Rune QL)

```sql
CREATE USER ws1 WITH PASSWORD '...';
ALTER  USER ws1 ADD PASSWORD '...';            -- second active password (rotation)
ALTER  USER ws1 DROP PASSWORD 1;               -- remove the older one
ALTER  USER ws1 SET PASSWORD '...' [TERMINATE SESSIONS];
ALTER  USER ws1 DISABLE | ENABLE;
DROP   USER ws1;

CREATE ROLE world_server;
GRANT READ  ON TOPIC player_events   TO ROLE world_server;
GRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE world_server;
GRANT READ  ON GROUP PREFIX 'world-' TO ROLE world_server;
GRANT ROLE world_server TO USER ws1;
REVOKE WRITE ON TOPIC PREFIX 'world.' FROM ROLE world_server;
DROP ROLE world_server;

SHOW USERS;  SHOW ROLES;  SHOW GRANTS FOR USER ws1 | ROLE world_server;
```

- The minimum password length is 12. It is configurable, and there are no other complexity rules.
- Passwords are turned into verifiers by the client library or the console before sending. The plaintext never reaches the broker.

## 5. Authentication (SCRAM-SHA-512)

- The existing session handshake, which already runs inside client↔broker TLS, gains a SCRAM exchange: client-first, server-first, client-final, server-final. It uses new authentication message types in rune-protocol.
- Clients set `username` / `password` in their config. The SCRAM exchange means the password never crosses the wire.
- **Failure:** a client that sends no credentials, a wrong password, an unknown user or a disabled user gets `AuthenticationFailed`, and the session ends. The answer does not reveal which of these happened.
- **Brute force:** failed attempts are rate-limited per user and per source address, with exponential backoff. There is **no hard lockout**, because a lockout would let an attacker lock out world servers.
- **Version gate:** authentication requires the client version that introduces it, assigned at implementation time. An older client connecting to a cluster with authentication enabled is refused explicitly at the handshake, never silently downgraded.
- **Logins are not events.** Successes, failures and rate limiting go to metrics and structured logs, not to `__security`, because they are high-volume.

## 6. Enforcement (hot path)

- **At authentication,** the broker compiles the user's effective permissions into a **per-session permission table**. It is keyed by topic and view slot: one bitmask of operations per object, plus the group and transactional-id patterns.
  - Prefix grants are resolved against existing names at that point.
  - When a `CreateTopic` or `CreateView` catalog event is applied, the table is extended for sessions whose prefixes match.
- **Checks:**
  - A Subscribe, a group join or a producer's first produce to a topic checks once.
  - Each produce request checks with one bitmask load from the session's table.
  - There is no allocation, no lock and no shared state. Each session's table is owned by the thread that owns the session, and updates arrive through the existing rings.
- **Live changes:**
  - A `Grant` / `Revoke` / role change that affects a user refreshes all their sessions' tables through the rings.
  - A revoked `READ` **ends the affected subscriptions** with `NotAuthorized`.
  - A revoked `WRITE` fails subsequent produces with `NotAuthorized`.
  - Nothing is silent.
- **Credential changes:**
  - `DropUser` / `DISABLE` ends every live session of that user immediately with `SessionRevoked`. Its subscriptions and producers end explicitly.
  - A password change leaves existing sessions running, unless `TERMINATE SESSIONS` is given, in which case they end with `SessionRevoked`.

## 7. First admin (bootstrap)

- `rune security bootstrap --admin <name>` runs once against a fresh cluster:
  - It creates the admin user with `ADMIN CLUSTER` and a generated one-time password, and prints that password once.
  - It records the action as the first `__security` event, with principal `bootstrap`.
  - The admin must change the password on first login. Until then, every request except `ALTER USER … SET PASSWORD` answers `PasswordChangeRequired`.
- **Non-interactive form** for automation, CI and docker: `--password-file <path>` reads the one-time password from a file the caller supplies, never from an environment variable.
- **Run twice:** once a security admin exists, `bootstrap` refuses with an explicit error.

## 8. Broker-to-broker authentication

- Every broker is deployed with the same **cluster key file**, `RUNE_CLUSTER_KEY_FILE`, from a Kubernetes secret or a mounted file.
- On each inter-broker connection, the brokers prove possession of the key by mutual HMAC challenge-response. The key never crosses the wire.
- Inter-broker links use the same transport encryption as clients.
- **Rotation:** `RUNE_CLUSTER_KEY_FILE_NEXT` holds a second key. Brokers accept either key and present the current one.
  1. Add the next key everywhere.
  2. Roll the brokers so the next key becomes current.
  3. Remove the old key.
- **Failure:** a broker without a valid key cannot join. It is refused explicitly and the refusal is logged on both sides.
- **No `__security` dependency:** broker identity does not depend on `__security`, so a new broker can authenticate before it has replicated the log.

## 9. Dev mode

- `RUNE_AUTH=disabled` turns authentication and authorization off, and every request runs as the built-in `anonymous` principal with full access.
- The broker logs a loud warning at startup and every 5 minutes, exposes the mode as a metric, and shows it in the console banner.
- A broker in dev mode **refuses to start as part of a multi-broker cluster**, with an explicit error.
- Dev mode is not the default. Without the setting, authentication is on.

## 10. Errors (every request answered; no silent fallback)

New codes, appended after E2's 43, are assigned at implementation time.

| Condition | Answer |
|---|---|
| No credentials, wrong password, unknown or disabled user | `AuthenticationFailed`, and the session ends |
| Rate-limited login attempt | `AuthenticationFailed`, with a retry-after hint |
| Operation not granted, or the object is not visible | `NotAuthorized` |
| A user dropped or disabled, or `TERMINATE SESSIONS` | `SessionRevoked` on every in-flight request and subscription, then the session ends |
| First login before the one-time password is changed | `PasswordChangeRequired` |
| A client too old for an authenticated cluster | refused at the handshake (version gate) |
| A broker without a valid cluster key | refused at the peer connection, logged on both sides |

## 11. Testing (interaction matrix)

- **Request type × permission state.** Every request in §2.1 is crossed with:
  - no grant;
  - an exact grant;
  - a prefix grant;
  - a grant through a role;
  - a grant revoked mid-request or mid-subscription;
  - the object created after login, for prefix resolution.
- **Session lifecycle:** login with each of the two passwords; rotation; `DISABLE`; `DROP`; password change with and without `TERMINATE SESSIONS`; the bootstrap one-time password flow.
- **Brute force:** the backoff curve, with no lockout of the legitimate user.
- **Brokers:**
  - joining with the right key, a wrong key, or no key;
  - key rotation across a rolling restart;
  - dev mode refusing multi-node.
- **Seeded, model-checked stress:** random grant and revoke sequences interleaved with produce and subscribe, against an independent permission model. Invariants:
  - no request succeeds without a grant at that moment;
  - every revoke ends affected subscriptions;
  - every request gets an answer.
- **Replay:** golden `__security` logs, with fingerprints verified.
- **Docker e2e, 3 nodes:**
  1. bootstrap;
  2. create a role, a user and grants;
  3. a world-server-style client produces and consumes;
  4. revoke, and the client fails explicitly;
  5. rotate the cluster key across a rolling restart.

## 12. Gates

- **Per-produce authorization check:** ≤ 2 ns, measured with a benchmark, with zero allocations in steady state (allocation gate).
- **Produce/consume hot-path benchmarks:** unchanged within noise, compared with authentication disabled.
- **Revoke propagation:** a revoke ends the affected subscriptions within 100 ms on a 3-node cluster.
- **Login latency:** a full SCRAM handshake takes ≤ 5 ms at the default iteration count.
