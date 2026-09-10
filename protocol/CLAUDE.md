# Rune — working agreement for AI sessions

Rune streams its development **live** to a public dashboard —
https://rune.regressorspath.com (fed by the `rune-live` server at
https://live.regressorspath.com). **Every session working on Rune must keep it
updated** as it works. Three habits, always:

## 1. Tickets — Planka board (via the `planka` MCP)
- The board is **"Rune / Roadmap"** (board id `1861164569347490826`), lists:
  **Backlog · In Progress · In Review · Done**.
- **Create a card** in Backlog for each unit of work you intend to do *before* you start it.
- **Move the card** as you progress: Backlog → In Progress (on start) → In Review
  (verifying/PR) → Done (finished). Don't leave work untracked.

## 2. Chat — say what you're about to do
- When you start a task or change direction, post a short note:
  ```
  scripts/rune-live.sh chat plan "Scaffolding the Raft metadata log next"
  ```
  kinds: `plan` (intent) · `thought` (reasoning) · `status` (progress).

## 3. Tests & benchmarks — push results as they run
- After running tests/benchmarks, push the result with a **stable `id` per suite**
  so re-runs conflate (the dashboard dedups by id, latest wins):
  ```
  scripts/rune-live.sh test "rune-broker/storage" passed \
    passed=65 failed=0 total=65 commit=$(git rev-parse --short HEAD)
  ```
- Benchmarks: add `kind=benchmark p99Us=87`. Long runs: push `running` first, then the final result.

Push token/URL default to dev values in the helper; override with `RUNE_PUSH_TOKEN` /
`RUNE_LIVE_URL`. API + payload formats: `rune-live/README.md`.
