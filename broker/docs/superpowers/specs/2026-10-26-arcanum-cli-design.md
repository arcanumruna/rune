# Arcanum CLI: `arcanum` (platform) and `rune` (service): design

**Status:** draft, 2026-10-26. Split out of
[2026-10-24-rune-namespaces-and-truncate-design.md](2026-10-24-rune-namespaces-and-truncate-design.md) §10, which
keeps only the `rune namespace` and `rune topic truncate` commands.
**Depends on:** namespaces (that spec, phase 1) and the Arcanum control-plane service.

The platform CLI is `arcanum`. Rune is one service under it; more services will follow.

**Two binaries, with one shared login.** The `kubectl`/`gcloud` split.
- **`arcanum`:** the platform CLI, for accounts, projects, deploy and teardown (§1).
- **`rune`:** the Rune service CLI, the existing binary with every command unchanged: `rune schema list`, `rune console` and so on. You never have to type an `arcanum` prefix.
- **Shared context:** both read the same login and current project from the Arcanum config directory. `rune` resolves the endpoint and credentials from there, so `--broker` is not needed after `arcanum login`.
- **Plugins:** `arcanum <service> <args>` runs the service's binary with those args, the way `git` runs its plugins, so `arcanum rune schema list` equals `rune schema list`. Future services ship their own binary the same way.
- **Self-hosted:** `rune` with no login works exactly as today, using `--broker` and `--user`/`--password-file`.

## 1. Platform model
Rune knows nothing of accounts, projects, tiers or billing. The platform model and its commands (`arcanum login`,
`deploy`, `teardown` and so on) are specified with the Arcanum control-plane service, a separate project.

The contract between them is small:
- the control plane acts as a cluster admin, through `rune namespace create|limits|suspend|resume|delete` or the
  same admin requests;
- whatever a plan sells is expressed only as a `NamespaceLimits` record (namespaces spec §5) or as a separate cluster;
- usage shown to a customer comes from the namespace's metrics (namespaces spec §5.3).

## 2. `rune` commands
- **Connection:** when signed in, the control plane hands out the namespace-admin SCRAM credential for the current project, and `rune` caches it with mode 0600.
- **Existing commands, unchanged:**
  - `console`;
  - `ql`;
  - `schema get|list|generate`;
  - `catalog export|replay|diff`;
  - `security bootstrap|gen-cluster-key`.
  - Self-hosted only: `security bootstrap` and `gen-cluster-key`. On the platform these are run by the control plane.
- **Commands from the namespaces spec** (§10 there; they ship with that work): `rune topic truncate` and
  `rune namespace ...`. On the platform the control plane runs `rune namespace`.
- **Review notes:**
  - `schema list` actually lists topics. Add `topic list` as the canonical command and keep `schema list` as an alias.
  - Error messages keep their existing `rune ...:` prefix.

## 3. Selecting and connecting to a deployment
- **Context:** (project, environment) picks the Rune deployment. Start with one Rune deployment per project; environments such as `--env dev|prod` can come later.
  - `arcanum use <project> [--env]` sets the current context.
  - `--project`/`--env` override it for one command, and `RUNE_PROJECT`/`RUNE_ENV` override it for a shell or CI.
- **Resolution:** the first time a context is used, `rune` calls the control plane with the `arcanum` token. It gets back:
  - the bootstrap broker endpoints (the rest of the cluster comes from metadata);
  - the namespace;
  - the cluster TLS identity, which the client pins;
  - a short-lived namespace-admin credential.

  These are cached with mode 0600, per context. On a connect or auth failure, `rune` refreshes once.
- **Precedence:** `--broker`/`--user` flags, then `RUNE_BROKER`/`RUNE_USER`, then the Arcanum context. Self-hosted use needs only the first two.
- **Apps:** apps never use a personal login.
  - `arcanum rune credentials create <name> --grants ...` creates a service user in the namespace and prints `rune://<ns>/<user>@host1:port,host2:port?tls=<fingerprint>` plus a secret.
  - The client library accepts that connection string.
  - `credentials revoke <name>` removes the user and fences its sessions.


## 4. Rune login
Full design of the auth model: arcanum-site `docs/specs/2026-10-24-arcanum-auth-design.md` §5.
- **Two separate logins.**
  - The Arcanum CLI token is used only against the control plane and never reaches the broker.
  - Rune users log in to the broker with SCRAM via `rune login --user`. The CLI stores the derived `ClientKey`/`ServerKey` for each context, never the password. `rune logout` deletes them, and `rune whoami` shows both identities.
