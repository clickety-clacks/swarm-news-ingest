# Argus

Argus is the local-only RSS/news ingestion worker for `swarm.channel`.

Argus uses RSS, Atom, and arXiv feeds as input adapters. Its job is to turn scattered source feeds into source-grounded publish candidates that can later become Subspace messages. It is deliberately a transport/provenance layer: it fetches feeds, parses entries, normalizes fields, dedupes within each source, records source health, and writes inspectable artifacts.

Argus does **not** decide what matters. It does **not** write digests, rankings, lanes, scores, authority weights, or “why agents care” text. OpenClaw subscribers or other downstream agents do that after receiving Subspace messages.

## Status

Early local worker. It is runnable and fixture-tested, and it has an explicit dry-run mode. It does not deploy itself, schedule itself, or publish to Subspace yet.

## What it produces

A run writes these artifacts to the output directory:

- `run-summary.json` — run metadata, source counts, artifact paths, exit status
- `source-health.json` — per-source fetch/parse status and failure reason if any
- `normalized.jsonl` — normalized source entries with provenance
- `clusters.jsonl` — source-local duplicate clusters only
- `publish-candidates.jsonl` — candidate messages for a future Subspace publisher

## Repository layout

```text
bin/argus              shell wrapper for local runs
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

## Run against live feeds

```bash
cd ~/src/argus
. .venv/bin/activate
argus \
  --dry-run \
  --sources config/sources.yaml \
  --out /tmp/argus-out
```

Equivalent module form:

```bash
PYTHONPATH=src python -m argus.cli \
  --dry-run \
  --sources config/sources.yaml \
  --out /tmp/argus-out
```

For deterministic run IDs/timestamps during verification, pass `--now`:

```bash
PYTHONPATH=src python -m argus.cli \
  --dry-run \
  --sources config/sources.yaml \
  --out /tmp/argus-out \
  --now 2026-04-27T12:00:00Z
```

## Dry-run / test mode

Use `--dry-run` to fetch the configured live RSS sources and inspect exactly what Argus would emit without publishing anything to `swarm.channel` or Subspace:

```bash
OUT=/tmp/argus-dry-run-$(date -u +%Y%m%dT%H%M%SZ)
argus --dry-run --sources config/sources.yaml --out "$OUT"
python3 -m json.tool "$OUT/run-summary.json"
head -20 "$OUT/publish-candidates.jsonl"
```

Current code is local-artifact-only, so `--dry-run` is explicit operator intent plus run metadata. Future publisher work must keep this flag as the safe verification path.

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

## Current verified live run

On Racter, the current dry run against `config/sources.yaml` verified:

- 13 configured/enabled/fetched sources
- 0 failed sources
- 1,907 raw entries
- 1,907 normalized entries
- 1,907 publish candidates
- `publish_performed: false`

## Install/deploy

See [`docs/DEPLOY.md`](docs/DEPLOY.md) for the official install layout, Racter target layout, scheduling options, verification gate, upgrade, and rollback notes.

## Non-goals for this repo right now

- no cron or scheduler
- no daemon/service install
- no live Subspace publish
- no digest generation
- no ranking/lane/scoring logic
- no subscriber interpretation
