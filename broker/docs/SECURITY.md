# Rune security: users, roles, grants, the cluster key

Authentication is **on by default**. Every client proves who it is with a SCRAM-SHA-512 login,
every request is checked against explicit grants, and anything not granted is refused with an
explicit error. Users, roles and grants are replayable events in the `__security` log, with full
history, exactly like the catalog ([CATALOG.md](CATALOG.md)). Brokers prove themselves to each
other with a shared **cluster key**, and every inter-broker frame is encrypted.

This page is the reference. To start a cluster, see [SELF-HOSTING.md](SELF-HOSTING.md#authentication-is-on-by-default).
For what changed for existing deployments, see [the release notes](release-notes/authentication.md).

## The model

- **Principal:** a user. Services such as world servers are users too.
- **Role:** a named set of grants.
- **Grant:** an operation on a resource type, matched by an exact name or a prefix.
  - Operations: `READ`, `WRITE`, `CREATE`, `ALTER`, `DROP`, `DESCRIBE`, `ADMIN`.
  - Resource types: `CLUSTER`, `TOPIC`, `VIEW`, `GROUP`, `TRANSACTIONAL_ID`.
  - Pattern: `'name'` (exact) or `PREFIX 'p'`.
- A user receives roles, and can also receive grants directly. The effective permissions are the
  union of the direct grants and the grants of all the user's roles.
- **Explicit allow only. Default deny.** There are no deny rules.

Rules that keep the model small (D15 to D17):

- Any granted operation on an object implies `DESCRIBE` on it.
- Valid pairs: `TOPIC`: `READ WRITE CREATE ALTER DROP DESCRIBE`. `VIEW`: `READ CREATE ALTER DROP DESCRIBE`.
  `GROUP`: `READ DESCRIBE`. `TRANSACTIONAL_ID`: `WRITE DESCRIBE`. Any other pair is refused.
- `CLUSTER` takes `CREATE ALTER DROP DESCRIBE ADMIN` only. `CREATE`, `ALTER`, `DROP` and `DESCRIBE`
  on `CLUSTER` apply to every topic and view (`DESCRIBE` also to groups and transactional ids).
  `READ` and `WRITE` on `CLUSTER` are refused: use `PREFIX ''`.
- `ADMIN ON CLUSTER` makes a security admin. It implies no data access.
- Internal objects (names starting `__`) cannot be granted by pattern.
- User and role names are `[A-Za-z0-9._@-]`, 1 to 128 characters, not starting with `__`.

### Which operation each request needs

| Request | Needs |
|---|---|
| Produce, transactional produce | `WRITE` on the topic |
| Transaction init / markers | `WRITE` on the transactional id |
| `AddOffsetsToTxn` / `TxnOffsetCommit` (D18) | `WRITE` on the transactional id and `READ` on the group |
| Subscribe (any form) | `READ` on the topic |
| JoinGroup / SyncGroup / Heartbeat / OffsetFetch / OffsetCommit | `READ` on the group, and `READ` on each topic |
| LeaveGroup | `READ` on the group |
| Non-transactional InitProducerId (#211, Kafka's rule) | `WRITE` on at least one topic (literal, prefix or wildcard grant); its produces are checked as well |
| GetViewRow / openView | `READ` on the view |
| Metadata, topic and view listings, `GetTopicSchema` | `DESCRIBE` |
| `CreateTopic` / `CreateView` / `CreateSchema` | `CREATE` on the name (exact or prefix grant), or on `CLUSTER` |
| `CreateSchema` on an existing untyped topic (a `DefineTopic` with a descriptor) | `ALTER` on the topic as well: it is a new schema version of a live object |
| `CreateView` / `AlterView` (D19) | also `READ` on every source topic |
| `AlterSchema` / `AlterView` / `AlterTopicConfig` / `SetCompatibility` | `ALTER` on the object |
| `DropTopic` / `DropView` | `DROP` on the object |
| Catalog history, as-of, diff, export, replay (`__catalog` reads) (D21) | `DESCRIBE` on `CLUSTER` |
| User, role and grant DDL; every `SHOW USERS|ROLES|GRANTS|SECURITY HISTORY` (D20) | `ADMIN` on `CLUSTER` |
| `ALTER USER <self> SET|ADD|DROP PASSWORD` (D20) | no grant, but a fresh proof of a current password (the client sends it; a session alone is not enough) |

**Ownership.** A consumer-group member id and a producer id belong to whoever obtained them.
- A member id is bound to the session and user that joined. SyncGroup, Heartbeat, LeaveGroup and
  OffsetCommit naming it are honoured only on that session, and are otherwise answered `UnknownMember`,
  the same answer as for a member that does not exist. A JoinGroup naming it re-joins only for the same
  user (a reconnect); anyone else joins as a new member with a fresh id. Member ids are random.
- A producer id carries the user it was issued to. A Produce, AddPartitionsToTxn, AddOffsetsToTxn,
  TxnOffsetCommit or EndTxn naming another user's producer id is refused (`NotAuthorized` for a Produce, the
  wrong-producer answer otherwise). Two users sharing `WRITE` on a transactional id each get their own
  producer id at InitProducerId; the previous holder is fenced.
- Producer ids are bound only once every broker in the cluster runs a version that supports it (the
  metadata feature level). During a rolling upgrade, ids are issued unbound, as before. An unbound id
  carries no owner and is usable by any user with `WRITE`, so a producer that started before the
  upgrade completed is unprotected until it re-initializes. A transactional id still holding an unbound id
  gets a bound one at its next InitProducerId.

**Visibility.** Objects the user cannot `DESCRIBE` do not appear in metadata or listings. Direct
access to such an object answers `NotAuthorized`, whether or not the object exists, so the answer
does not reveal existence. A request that addresses an object by id (produce, subscribe, view reads)
is checked against the grants on that object. Only when the id is not live (missing or dropped) does it
answer `NotAuthorized`, unless the session holds `DESCRIBE ON CLUSTER`; then it gets the request's normal
`UnknownTopic` answer (D22). A view plan's source ids never get this relaxation: they need `READ`. The detail text is
always `not authorized: <OP> on <TYPE> <requested name or id>` (D23).

## Rune QL statements

All security statements need `ADMIN ON CLUSTER`, except a user changing their own password. That
change carries a SCRAM proof of a current password, bound to the session's login and to the request,
so a hijacked or leaked session cannot add or replace a password. After a `SET PASSWORD`, log in again
before the next change of your own password. A
statement that carries a password literal is always read from a file or stdin with
`rune ql -f` (`-e` refuses it, so the password never shows in `ps` or the shell history). Passwords
are turned into SCRAM verifiers by the client library or the CLI before sending: the plaintext
never reaches a broker, and the log records the statement with the literal replaced by
`'<redacted>'`. The minimum password length is 12 (`ClientConfig::minPasswordLength`).

```sql
CREATE USER ws1 WITH PASSWORD 'correct horse battery';
ALTER  USER ws1 ADD PASSWORD 'a second password 1234';
ALTER  USER ws1 DROP PASSWORD 1;
ALTER  USER ws1 SET PASSWORD 'a brand new password 99' TERMINATE SESSIONS;
ALTER  USER ws1 DISABLE;
ALTER  USER ws1 ENABLE;
DROP   USER ws1;
```

```sql
CREATE ROLE world_server;
GRANT READ  ON TOPIC player_events   TO ROLE world_server;
GRANT WRITE ON TOPIC PREFIX 'world_' TO ROLE world_server;
GRANT READ  ON TOPIC PREFIX 'world_' TO ROLE world_server;
GRANT READ  ON GROUP PREFIX 'world-' TO ROLE world_server;
GRANT WRITE ON TRANSACTIONAL_ID 'ws-1' TO USER ws1;
GRANT CREATE ON CLUSTER TO USER ops;
GRANT ROLE world_server TO USER ws1;
REVOKE WRITE ON TOPIC PREFIX 'world_' FROM ROLE world_server;
REVOKE ROLE world_server FROM USER ws1;
DROP ROLE world_server;
```

```sql
SHOW USERS;
SHOW ROLES;
SHOW GRANTS FOR USER ws1;
SHOW GRANTS FOR ROLE world_server;
SHOW GRANTS FOR USER ws1 AS OF OFFSET 12;
SHOW GRANTS FOR ROLE world_server AS OF TIME '2026-10-13T00:00:00Z';
SHOW SECURITY HISTORY;
SHOW SECURITY HISTORY USER ws1 SINCE '2026-10-13T00:00:00Z';
```

`SHOW GRANTS FOR USER ws1;` lists the direct grants (source `direct`) and the grants each role
brings (source `role:<name>`). The history reads replay `__security` client-side with the shared
library, verifying every event's fingerprint, like the catalog's.

Topic and view names in DDL are identifiers (letters, digits, underscore), so the topics in the
examples are `world_*`; a grant pattern is a string and may contain any allowed character.

## Passwords, SCRAM and rotation

- Only **SCRAM-SHA-512 verifiers** are stored: salt, iteration count, `StoredKey` and `ServerKey`
  (default 4096 iterations, accepted range 4096 to 1 000 000; a full login takes at most 5 ms).
  A password, or anything that logs in directly, is never stored and never crosses the wire.
- A user holds **at most two** active verifiers, numbered by age (1 = oldest). `ADD PASSWORD` with
  two present, and `DROP PASSWORD` of the last one, are refused. `SET PASSWORD` replaces all of them.
- **Two-password rotation:** `ADD PASSWORD` the new one, roll the clients to it, then
  `DROP PASSWORD 1`. Both passwords log in meanwhile: the broker always sends two salts (a user with
  one password gets a deterministic fake second salt), the client answers with one proof per
  verifier, and the broker accepts the one that verifies (D5, D8).
- Logging in with an unknown user, a wrong password, a disabled user or no credentials all give the
  same `AuthenticationFailed`; the answer never reveals which. An unknown user gets a deterministic
  fake salt, the same on every broker and across restarts.
- A client with credentials **refuses a broker that skips the login** (a dev-mode broker, an old
  broker, a cluster with no security admin yet) with `AuthenticationFailed`, so a rogue endpoint
  never receives a credentialed client's data. Set `ClientConfig::allowUnauthenticatedBroker = true`
  to accept such a broker anyway.

### The brute-force throttle (D14)

Failed logins are rate-limited per (user, source address) and per source address. The first 2
failures of a pair, and the first 10 of an address, are free. Each further failure delays the next
attempt by `min(250 ms * 2^(k-1), 30 s)`. An attempt inside the delay is answered
`AuthenticationFailed` with a `retryAfterMs` hint and is not counted. A success resets the pair.
There is **no hard lockout**: a lockout would let an attacker lock out the world servers, and an
attacker at one address never delays the same user at another. A session that has not finished
the login within `RUNE_AUTH_TIMEOUT_MS` (default 10 000) is answered `AuthenticationFailed` and ended
(D13). A client that is rate-limited waits `max(retryAfterMs, its backoff)` and retries.

## Bootstrap: the first admin (D3, D4)

A fresh cluster has no users. `rune security bootstrap` creates the first security admin, once:

```bash
rune security gen-cluster-key --out secrets/cluster.key          # once, before the brokers start
# ... start the brokers (every one mounts the same key) ...
rune security bootstrap --admin admin \
    --cluster-key-file secrets/cluster.key --broker rune-1:40200
# one-time password for admin: Hz3...            (printed once)
```

- The bootstrap proves possession of the **cluster key** (an HMAC over its fields and a nonce), so
  whoever reaches a fresh cluster first cannot take it over. `--cluster-key-file` defaults to
  `RUNE_CLUSTER_KEY_FILE`.
- It records one `Bootstrap` event with the principal `bootstrap`: the admin user with
  `ADMIN ON CLUSTER` and a generated 24-character one-time password, printed once.
- The admin **must change the password on first login.** Until then every request except
  `ALTER USER <admin> SET PASSWORD` answers `PasswordChangeRequired`.
- Automation (CI, docker) supplies the one-time password itself with `--password-file <path>`: it is
  never read from an environment variable and never accepted on the command line.
- A second run refuses: `this cluster is already bootstrapped - it already has a security admin`.
- The last enabled security admin cannot be removed by accident: `ALTER USER .. DISABLE`, `DROP USER`,
  `DROP ROLE` or `REVOKE` that would leave no enabled user holding `ADMIN ON CLUSTER` is refused unless
  the statement ends in `FORCE`. A forced removal reopens the bootstrap window: the other users keep
  logging in, and the cluster-key holder can run `rune security bootstrap` again (D3 recovery).

```bash
rune ql --user admin --password-file otp.txt -f change-password.rql --broker rune-1:40200
# change-password.rql:   ALTER USER admin SET PASSWORD '<a new password of 12 or more characters>';
```

Every `rune` command takes `--user <u>` and `--password-file <path>`, or `RUNE_USER` and
`RUNE_PASSWORD_FILE`. The example clients and probes read the same two variables.

## The cluster key (D32, D35)

Brokers authenticate each other with one shared key file, `RUNE_CLUSTER_KEY_FILE`. On each
inter-broker connection they prove possession by a mutual HMAC challenge-response; the key never
crosses the wire. A broker without a valid key cannot join: it is refused explicitly, and the
refusal is logged on **both** sides (counter 1 000 234). Broker identity does not depend on
`__security`, so a new broker authenticates before it has replicated the log.

**Generating it.** `rune security gen-cluster-key --out <path>` writes 64 hex characters (256 bits)
at mode `0600` and never overwrites an existing file. The contents (trailing whitespace trimmed)
must be at least 32 bytes. The same file goes to every broker.

**File permissions.** A key file that is accessible by group or others makes the broker refuse to
start. Mount it `0400` or `0600`, readable by the broker's user.

**Mounting.**

```bash
# docker compose: a read-only bind mount (docker/docker-compose.yml)
#   volumes: [ "./secrets/cluster.key:/run/secrets/cluster.key:ro" ]
#   environment: { RUNE_CLUSTER_KEY_FILE: /run/secrets/cluster.key }

# Kubernetes: a secret, mounted as a file with a restrictive mode
kubectl create secret generic rune-cluster-key --from-file=cluster.key=secrets/cluster.key
#   volumes:       - name: cluster-key
#                    secret: { secretName: rune-cluster-key, defaultMode: 0400 }
#   volumeMounts:  - { name: cluster-key, mountPath: /run/secrets, readOnly: true }
#   env:           - { name: RUNE_CLUSTER_KEY_FILE, value: /run/secrets/cluster.key }
```

The image entrypoint refuses to start without the file and **never generates a key** (a key made
inside one container would differ from its peers' and split the cluster).

**Inter-broker encryption.** Every inter-broker frame is sealed with a per-link key derived from
the cluster key, the handshake nonces and an X25519 ephemeral secret (HKDF-SHA-512, one key per
direction, a replay window per sender). The ephemeral secret gives forward secrecy: a recording of
inter-broker traffic plus a cluster key stolen later does not yield the link keys. A broker still
links with a peer from before this change when that peer initiates (the lower node id initiates),
without forward secrecy on that link; it never falls back itself. **Upgrade brokers in descending
node id** so that every upgraded initiator meets an upgraded responder. A frame carries 32 bytes of overhead (16-byte header and 16-byte tag); the frame limits
subtract them, so the maximum produce batch is 32 bytes smaller than before authentication.

**Rotation (spec section 8, three rolling restarts, no downtime).** `RUNE_CLUSTER_KEY_FILE_NEXT`
holds a second key. A broker presents its current key and accepts the current or the next (matched
by key id); a hello naming a key id the responder holds neither of is refused `no common cluster key`.

1. **Add the next key everywhere.** Generate `cluster.next.key`; restart each broker, one at a time,
   with `RUNE_CLUSTER_KEY_FILE=<old>` and `RUNE_CLUSTER_KEY_FILE_NEXT=<new>`.
2. **Make it current.** Restart each broker, one at a time, with `RUNE_CLUSTER_KEY_FILE=<new>` and
   `RUNE_CLUSTER_KEY_FILE_NEXT=<old>`.
3. **Remove the old key.** Restart each broker, one at a time, with `RUNE_CLUSTER_KEY_FILE=<new>`
   and no `_NEXT`.

After each restart wait until the node logs `peer link with node N established` for both peers
before touching the next node. `docker/auth-e2e.sh` runs exactly this under a live producer and
consumer, and checks that every acked record arrives. The bootstrap proof is also accepted under
either key.

## What happens when permissions change

A change takes effect on live sessions. Nothing is silent (D24 to D26).

| Change | Effect on live sessions |
|---|---|
| `GRANT`, `GRANT ROLE` | The user's sessions refresh their tables; new requests are allowed. |
| `REVOKE READ` (or the role, or the role's grant) | Every affected subscription **ends** with `NotAuthorized`; a group consumer ends with `NotAuthorized`. Never a hang, never a silent stop. |
| `REVOKE WRITE` | The next produce fails `NotAuthorized`. |
| `DROP ROLE` | Like revoking everything the role granted. |
| `DROP USER`, `ALTER USER ... DISABLE`, `ALTER USER ... SET PASSWORD ... TERMINATE SESSIONS` | Every live session of the user is answered `SessionRevoked` (each subscription on its own correlation id, then a session-level notice) and closed. The client fails every in-flight request with `SessionRevoked` and does **not** reconnect that session. |
| `ALTER USER ... SET PASSWORD` (without `TERMINATE SESSIONS`) | Existing sessions keep running; the change lifts a pending `PasswordChangeRequired`. |
| `ADD PASSWORD`, `DROP PASSWORD`, `CREATE *`, `ENABLE` | Nothing for live sessions. |

A security DDL is answered OK once the broker that served the statement enforces it: its control thread
releases the answer once its view applied the event and **every worker of that broker acknowledged** what
the event changed there (D24): its new permission bitmaps and binds are installed, a revoked session is
bound to nothing, and an ended subscription's cursor is gone. A subscription a worker pushes is ended by
that worker: its `NotAuthorized` follows its last `Records` frame, and nothing follows it. An OK whose
enforcement is not confirmed within `RUNE_SECURITY_GATE_DEADLINE_MS` (default 5 000) is answered `Internal`
"committed at offset N; enforcement on this broker not confirmed" (the change is committed; a retry with
the same request id returns its result). The OK does not mean every broker enforces it. Other brokers
apply the event asynchronously (typically within 100 ms on healthy brokers).
A revoke ends the affected subscriptions within 100 ms on a 3-node cluster.

### A broker serves only a current security state (`#serving`)

A broker serves logins, `__security` reads and security DDL only while its `__security` state is provably
current; otherwise it answers the retriable `AuthUnavailable` (48) and the client tries another broker:

- **Catching up.** After a start (a restart, a wiped or a new broker) it serves nothing until it has
  fetched everything its current `__security` leader has and applied it. A leader serves once its whole
  log is committed and applied.
- **Stale.** A follower whose leader has not answered for `RUNE_SECURITY_MAX_STALENESS_MS` (F, default
  1 000), whose apply lags that long, or any broker that lost contact with the consensus for F (a broker cut
  off the cluster, including a `__security` leader that does not know it was replaced) is stale. F exceeds
  the broker liveness timeout, so a planned leadership move does not make a broker stale. A hard leader
  failure does not either (#460): a follower that has applied everything it knows committed holds its
  verdict while its leader is silent, for as long as it stays in the committed `__security` ISR of a current
  partition map and in contact with the consensus. Nothing commits without an ISR member, so a held follower
  can miss no committed event; a follower cut off is shrunk out of the ISR and goes stale, and a commit it
  learns of but has not applied ends the hold at once.
- **Behind.** A broker that holds committed `__security` events it has not applied yet (its leader told it
  they are committed, or it leads and its own apply lags) refuses every grant-checked request with
  `AuthUnavailable`: produce (through the gateway and straight to a worker), subscribe, view reads, group
  and transaction requests, DDL, logins. Nothing is appended and no subscription is pushed until the events
  are applied; then the new grants decide. A revoke this broker has received is never served around.
- A broker stale for `RUNE_SECURITY_STALE_SESSION_END_MS` (R, default 10 000) ends its authenticated
  sessions with a session-level `AuthUnavailable`; their clients reconnect to a current broker. Only an
  event the broker has not heard of yet can still be missed by those sessions until then: what a broker
  serves is never more than about F behind the cluster.
- A new or wiped broker never advertises the bootstrap window while it is catching up.

On the hot path an authorization check is one bitmask load from the session's own table (at most 2 ns,
zero allocation); tables are owned by the thread that owns the session and updated through the rings.

## Dev mode

`RUNE_AUTH=disabled` turns authentication and authorization off: every request runs as the built-in
`anonymous` principal with full access. It is for **single-node development only**.

- It is never the default: without the setting, authentication is on and the broker needs a cluster key.
- The broker logs a loud warning at startup and every 5 minutes, and publishes the mode as the
  counter `rune-auth-mode` (1 000 228: 0 enabled, 1 dev mode).
- A broker in dev mode **refuses to start as part of a multi-broker cluster**
  (`RUNE_PEERS` lists more than one broker): an unauthenticated node would be an open door into the
  cluster's data and into `__security`.
- A client that has credentials refuses a dev-mode broker unless `allowUnauthenticatedBroker` is set.

## Errors

| Condition | Answer | Code |
|---|---|---|
| No credentials, wrong password, unknown or disabled user | `AuthenticationFailed`, and the session ends | 44 |
| A rate-limited login attempt | `AuthenticationFailed` with `retryAfterMs` | 44 |
| Operation not granted, or the object is not visible | `NotAuthorized`; fails exactly that request and is never re-sent | 45 |
| A user dropped or disabled, or `TERMINATE SESSIONS` | `SessionRevoked` on every in-flight request and subscription, then the session ends; terminal for that session | 46 |
| First login before the one-time password is changed | `PasswordChangeRequired`; fails exactly that request | 47 |
| A client older than version 9 against an authenticated cluster | refused at the handshake (`Connect`), never downgraded | version gate |
| A broker without a valid cluster key | refused at the peer connection, logged on both sides | counter 1 000 234 |
| `__security` apply halted on this broker | logins `AuthUnavailable`, sessions end with a session-level `AuthUnavailable` (retriable: the client reconnects to another broker), security DDL `Internal` | 48, see `#halted` |
| This broker's `__security` state is not current (catching up, stale) | logins, `__security` reads and security DDL `AuthUnavailable`; retriable, try another broker. Stale longer than R: a session-level `AuthUnavailable` ends the authenticated sessions | 48, see `#serving` |
| This broker holds committed `__security` events it has not applied yet (behind) | every grant-checked request `AuthUnavailable`, nothing appended or pushed; retriable, served again once applied | 48, see `#serving` |

### `#halted`: the `__security` apply halted (D28)

Each broker applies `__security` in offset order and checks every event's fingerprint, like the
catalog. If a fingerprint differs or an event cannot be applied, that broker's apply **halts**, and
because a security log that stops applying stops enforcing revocations, it **fails closed**:

- it ends every authenticated session with a session-level `AuthUnavailable` (detail `security log halted on this
  broker - reconnect to another broker`); it is retriable, so the client reconnects to a healthy broker,
- refuses new logins with `AuthUnavailable` (retriable, likewise),
- answers security DDL `Internal`,
- raises counter `rune-security-apply-halted` (1 000 226: 0 while applying, the halted offset plus 1 after a halt),
- and logs a critical line `SecurityApplier: apply HALTED at offset N ...`.

The other brokers are not affected, and no topic data is lost. **Runbook** (the catalog's, see
[CATALOG.md](CATALOG.md#halted)):

1. Find the halted broker from the counter or the critical log line, and note the offset.
2. Stop that broker.
3. Move `dataDir/__security` aside (do not delete it).
4. Start the broker. It refetches the whole log from a healthy broker and applies it from offset 0.
5. If it halts again at the same offset, the log or the software is at fault, not the copy: keep the
   moved-aside directory and compare the histories with `SHOW SECURITY HISTORY` on a healthy broker.

Do not use this on a single-node broker: its local `__security` is the only history; restore it from a backup.

## Metrics

Aeron counters of the broker's media driver (`dataDir/aeron` unless `RUNE_AERON_DIR` is set):

| Counter | Type id | Value |
|---|---|---|
| `rune-security-apply-halted node=<id>` | 1 000 226 | 0 while applying; after a halt, the halted offset plus 1 |
| `rune-security-applied-offset node=<id>` | 1 000 227 | the last applied `__security` offset |
| `rune-auth-mode node=<id>` | 1 000 228 | 0 authentication enabled, 1 dev mode |
| `rune-logins-ok node=<id>` | 1 000 229 | successful logins |
| `rune-logins-failed node=<id>` | 1 000 230 | failed logins |
| `rune-logins-rate-limited node=<id>` | 1 000 231 | logins answered inside a throttle delay |
| `rune-sessions-revoked node=<id>` | 1 000 232 | sessions ended by a drop, disable or `TERMINATE SESSIONS` |
| `rune-peer-links-established node=<id>` | 1 000 233 | peers whose link is currently established |
| `rune-peer-link-refusals node=<id>` | 1 000 234 | peer-link refusals logged by this broker, both directions |
| `rune-peer-frame-drops node=<id>` | 1 000 235 | inter-broker frames dropped for want of a link or because they did not open |

Alert on a non-zero `rune-security-apply-halted`, on `rune-auth-mode` = 1 anywhere in production, on a
rising `rune-logins-failed` or `rune-logins-rate-limited`, and on a rising `rune-peer-link-refusals`.
Successful and failed logins are metrics and structured logs, not `__security` events (they are
high-volume).

## Transport security (M1, replaces D12)

SCRAM keeps the password off the wire, but only per-frame AEAD binds a frame to its session. So **with authentication
on, every client session is TLS**:

- The broker answers a plaintext Connect `HandshakeFailed` (29) and creates no session. A plaintext session could be
  taken over by any UDP sender (session ids are guessable; nothing binds a frame to its session), and its verifiers,
  bootstrap material and `__security` reads would travel in clear. A Connect never resumes a session by its id.
- An authenticating broker needs `RUNE_TLS_CERT_FILE` / `RUNE_TLS_KEY_FILE` or it refuses to start;
  `RUNE_REQUIRE_TLS=false` with authentication refuses to start too.
- TLS needs a broker built with `-DRUNE_TLS=ON`, the default (and the docker image). A build without it refuses to start
  with authentication, and with any `RUNE_TLS_*` / `RUNE_REQUIRE_TLS` set, instead of ignoring them.
- A client with credentials sends them only over TLS: it requires a negotiated cipher, and a refusal that arrives before
  the TLS keys exist is unauthenticated, so it is retried rather than taken as a verdict. Each session checks the
  broker certificate against the host of its own endpoint (a pre-TLS redirect never changes that name); the system
  store with no name to check is refused.
- The `rune` CLI connects over TLS: `--tls-ca` / `RUNE_TLS_CA_FILE`, `--tls-host` / `RUNE_TLS_HOST`.
  `--insecure-plaintext` (dev-mode brokers) never sends credentials or the bootstrap.

Inter-broker links are always encrypted when authentication is on.
