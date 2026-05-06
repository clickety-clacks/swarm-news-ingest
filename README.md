# Argus

Argus is the RSS/news ingestion server for `swarm.channel`.

Argus uses RSS, Atom, and arXiv feeds as input adapters. Its job is to turn scattered source feeds into source-grounded publish candidates that can later become Subspace messages. It is deliberately a transport/provenance layer: it fetches feeds, parses entries, normalizes fields, dedupes within each source, remembers per-source seen items across runs, records source health, and writes inspectable artifacts.

Argus does **not** decide what matters. It does **not** write digests, rankings, lanes, scores, authority weights, or “why agents care” text. OpenClaw subscribers or other downstream agents do that after receiving Subspace messages.

## Status

Server-mode MVP. The production target is a long-running `argus serve` process with an internal scheduler. Host supervisors may restart the process, but cron/systemd timers/launchd timers must not schedule fetches.

The default server config is inactive: `publish.state: inactive` and no live approval. Active mode is controlled by runtime config/publish state reloads and is forward-only from the activation snapshot.

## What it produces

A run writes these artifacts to the output directory:

- `run-summary.json` — run metadata, source counts, artifact paths, exit status
- `source-health.json` — per-source fetch/parse status, validator info, and failure reason if any
- `normalized-items.jsonl` — normalized source entries with provenance
- `dedupe-decisions.json` — accepted/skipped item decisions
- `package-candidates.jsonl` — package payloads eligible for review/publish handling
- SQLite runtime state — scheduler state, source health, config snapshots, packages, attempts, and dedupe authority

## Repository layout

```text
bin/argus              shell wrapper for local runs
config/argus.example.yaml server-mode example config
config/sources.yaml    default v0 source configuration
src/argus/             Python implementation
tests/                 deterministic fixture-backed tests
```

## Requirements

- Python 3.9+
- Network access for live feed runs

Python dependencies are declared in `pyproject.toml`:

- `PyYAML`
- `requests`

## Setup

```bash
cd ~/src/argus
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install -e .
```

## Server-mode runtime

```bash
cd ~/src/argus
. .venv/bin/activate
argus serve --config config/argus.example.yaml
```

Operator command surface:

```bash
argus serve --config /etc/argus/argus.yaml --once
argus prime --config /etc/argus/argus.yaml
argus run-cycle --config /etc/argus/argus.yaml --reason manual
argus run-cycle --config /etc/argus/argus-e2e-canary.yaml --reason e2e-shrdlu --max-live-publishes 1
argus reload --config /etc/argus/argus.yaml
argus set-publish-state --config /etc/argus/argus.yaml --state inactive
argus status --db /var/lib/argus/argus.sqlite3
argus source-health --db /var/lib/argus/argus.sqlite3
argus explain-skip --db /var/lib/argus/argus.sqlite3 --run <run_id>
argus embedding-doctor --config /etc/argus/argus.yaml
```

`serve --once` runs one scheduler decision for deterministic readiness checks. Long-running production uses plain `serve`. Canary or E2E scheduled configs may set `schedule.max_live_publishes_per_tick: 1` so a scheduler tick fails before posting if more than one live publish is eligible.

Active publishing posts package JSON to `subspace-daemon` over the configured Unix socket/API path. The package JSON keeps canonical `supplied_embeddings`; the daemon request also carries the stable `publish_idempotency_key` and daemon-compatible embedding vectors. Production configs use Argus' built-in OpenAI embedding backend with `provider=openai`, `model=text-embedding-3-small`, `dimensions=1536`, and `space_id=openai:text-embedding-3-small:1536:v1`, which is compatible with Subspace daemon receptor matching. Use the checked-in canary config only with explicit Flynn approval before any externally visible send.

## Legacy one-shot CLI

The legacy one-shot CLI is preserved for local artifact inspection:

```bash
PYTHONPATH=src python -m argus.cli \
  --dry-run \
  --sources config/sources.yaml \
  --out /tmp/argus-out \
  --now 2026-04-27T12:00:00Z
```


## SQLite runtime state

Server mode keeps RSS-reader-style per-source state in SQLite so recurring runs do not re-emit old candidates.

State includes:

- HTTP validators per source (`ETag`, `Last-Modified`) sent back as `If-None-Match` / `If-Modified-Since`
- persistent seen-item identities per source using feed GUID/id first, canonical URL second, and normalized title + date bucket as fallback

If a source returns `304 Not Modified`, Argus records that as a healthy no-change source health result and emits no new candidates for that source.

## Prime mode

Prime is optional/manual baseline tooling only. It is not required for scheduled or manual jobs and is not an active/inactive gate. Prime creates no live attempts, no ordinary package rows/candidates, and no active-eligible work.

```bash
argus prime --config /etc/argus/argus.yaml
```

`--prime` and `--dry-run` are mutually exclusive:

- `--dry-run` = inspect without state mutation
- `--prime` = advance state without publish candidates

## Dry-run / test mode

Use `--dry-run` to fetch the configured live RSS sources and inspect exactly what Argus would emit without publishing anything to `swarm.channel` or Subspace and without mutating durable feed state:

```bash
OUT=/tmp/argus-dry-run-$(date -u +%Y%m%dT%H%M%SZ)
STATE=/tmp/argus-state.json
argus --dry-run --sources config/sources.yaml --out "$OUT" --state "$STATE"
python3 -m json.tool "$OUT/run-summary.json"
head -20 "$OUT/publish-candidates.jsonl"
```

Server-mode safety is `publish.state: inactive` by default. `--dry-run` remains available only on the legacy one-shot CLI.

## Run tests

```bash
cd ~/src/argus
. .venv/bin/activate
PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py' -v
```

The tests use fixtures under `tests/fixtures/argus/` and cover:

- source config validation
- RSS, Atom, and arXiv-style parsing
- source health artifacts
- provenance preservation
- community source labeling
- source-local dedupe
- cross-source preservation
- all-source failure exit behavior

## Source config

The default config is `config/sources.yaml`. Each enabled source has:

- stable `id`
- human `name`
- source class/category
- feed type and adapter
- feed URL and site URL
- freshness window metadata
- optional request headers

Argus treats the config as input truth and does not infer editorial importance from it.

## Candidate boundary

`publish-candidates.jsonl` is intentionally plain. A candidate includes:

- stable candidate/report IDs
- source identity
- title and canonical URL
- timestamps
- cleaned source summary
- dedupe identity
- provenance
- `metadata.embedding_text` for future embedding/publish work

It does not include digest copy, ranking, lane assignment, scores, or recommendation language.

## Exit behavior

- `success` / exit `0`: at least one enabled source succeeded and no enabled source failed
- `partial_failure` / exit `0`: at least one enabled source succeeded, but one or more failed
- `failed` / exit `1`: no enabled sources succeeded

Failure details are written to `source-health.json`.

## Racter readiness

The checked-in server-mode example config contains 13 enabled sources, default `1h` internal scheduling, inactive publish state, and the production OpenAI embedding space expected by shrdlu receptors. Racter readiness uses:

```bash
argus embedding-doctor --config /etc/argus/argus.yaml
argus serve --config /etc/argus/argus.yaml --once
argus status --db /var/lib/argus/argus.sqlite3
argus source-health --db /var/lib/argus/argus.sqlite3
```

## Install/deploy

See [`docs/DEPLOY.md`](docs/DEPLOY.md) for the official install layout, Racter target layout, runtime command surface, and readiness gate.

## Non-goals for this repo right now

- no daemon/service install
- no digest generation
- no ranking/lane/scoring logic
- no subscriber interpretation
