# Deploy and install runbook

This repo ships a local news-feed ingestion worker. It is not a hosted service yet. A proper deploy means installing the worker on a machine, giving it a source config, choosing an output directory, and scheduling or supervising runs.

The worker currently produces local artifacts only. It does **not** publish to Subspace, run a daemon, install a cron job, or manage credentials by itself.

## Official installation model

Use the platform's normal application data locations instead of cloning into random scratch directories.

### macOS / Linux single-user install

Recommended layout:

```text
~/src/swarm-news-ingest                         # source checkout for operators/developers
~/.local/bin/swarm-news-ingest                  # executable shim or symlink
~/.config/swarm-news-ingest/sources.yaml        # operator config
~/.local/state/swarm-news-ingest/runs/          # run outputs/artifacts
~/.local/state/swarm-news-ingest/logs/          # logs if supervised
```

For a checked-out install:

```bash
cd ~/src
git clone https://github.com/clickety-clacks/swarm-news-ingest.git
cd swarm-news-ingest
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install -e .
mkdir -p ~/.local/bin ~/.config/swarm-news-ingest ~/.local/state/swarm-news-ingest/runs
cp config/sources.yaml ~/.config/swarm-news-ingest/sources.yaml
ln -sf "$PWD/bin/swarm-news-ingest" ~/.local/bin/swarm-news-ingest
```

Run it:

```bash
~/.local/bin/swarm-news-ingest \
  --sources ~/.config/swarm-news-ingest/sources.yaml \
  --out ~/.local/state/swarm-news-ingest/runs/$(date -u +%Y%m%dT%H%M%SZ)
```

### Linux system install

For a shared/server install, use `/opt` for code, `/etc` for config, and `/var/lib` for state:

```text
/opt/swarm-news-ingest/                         # source checkout / release tree
/usr/local/bin/swarm-news-ingest                # executable shim or symlink
/etc/swarm-news-ingest/sources.yaml             # system config
/var/lib/swarm-news-ingest/runs/                # run outputs/artifacts
/var/log/swarm-news-ingest/                     # logs if supervised
```

Suggested service user:

```bash
sudo useradd --system --home /var/lib/swarm-news-ingest --shell /usr/sbin/nologin swarm-news-ingest
```

Install:

```bash
sudo mkdir -p /opt /etc/swarm-news-ingest /var/lib/swarm-news-ingest/runs /var/log/swarm-news-ingest
git clone https://github.com/clickety-clacks/swarm-news-ingest.git /opt/swarm-news-ingest
cd /opt/swarm-news-ingest
sudo python3 -m venv .venv
sudo .venv/bin/python -m pip install -U pip
sudo .venv/bin/python -m pip install -e .
sudo cp config/sources.yaml /etc/swarm-news-ingest/sources.yaml
sudo ln -sf /opt/swarm-news-ingest/bin/swarm-news-ingest /usr/local/bin/swarm-news-ingest
# The wrapper automatically uses /opt/swarm-news-ingest/.venv/bin/python when present.
sudo chown -R swarm-news-ingest:swarm-news-ingest /var/lib/swarm-news-ingest /var/log/swarm-news-ingest
```

Run once:

```bash
sudo -u swarm-news-ingest /usr/local/bin/swarm-news-ingest \
  --sources /etc/swarm-news-ingest/sources.yaml \
  --out /var/lib/swarm-news-ingest/runs/$(date -u +%Y%m%dT%H%M%SZ)
```

## Racter target install

Racter is the intended first always-on runtime host for this project.

Recommended Racter layout:

```text
/opt/swarm-news-ingest/
/etc/swarm-news-ingest/sources.yaml
/var/lib/swarm-news-ingest/runs/
/var/log/swarm-news-ingest/
/usr/local/bin/swarm-news-ingest
```

Racter deploy should be treated as an operator deployment, not a code checkout experiment:

1. Pull or clone `https://github.com/clickety-clacks/swarm-news-ingest.git` into `/opt/swarm-news-ingest`.
2. Create/update the Python virtualenv under `/opt/swarm-news-ingest/.venv`.
3. Install the package editable or from the checked-out tree.
4. Install `/etc/swarm-news-ingest/sources.yaml` from `config/sources.yaml`, then edit only config in `/etc`.
5. Write run artifacts under `/var/lib/swarm-news-ingest/runs/<timestamp>/`.
6. Log supervisor output under `/var/log/swarm-news-ingest/`.
7. Only after the Subspace publisher exists, add publisher config separately; do not overload the scraper config with subscriber interpretation.


## Current Racter install evidence

Current verified Racter install:

```text
code: /opt/swarm-news-ingest
config: /etc/swarm-news-ingest/sources.yaml
output root: /var/lib/swarm-news-ingest/runs/
command: /usr/local/bin/swarm-news-ingest
run user: swarm-news-ingest
verified commit: 646381c
latest verified dry run: /var/lib/swarm-news-ingest/runs/dry-run-20260430T085742Z
```

Latest verified dry run result:

```text
dry_run: true
publish_performed: false
exit_status: success
configured/enabled/fetched sources: 13/13/13
failed sources: 0
normalized entries: 1,907
publish candidates: 1,907
```

No scheduler/timer and no Subspace publisher are installed yet.

## Scheduling options

No scheduler is installed by default. Choose one per platform.

### systemd timer, Linux

Example service:

```ini
# /etc/systemd/system/swarm-news-ingest.service
[Unit]
Description=swarm.channel news feed ingestion

[Service]
Type=oneshot
User=swarm-news-ingest
Group=swarm-news-ingest
WorkingDirectory=/opt/swarm-news-ingest
ExecStart=/usr/local/bin/swarm-news-ingest --sources /etc/swarm-news-ingest/sources.yaml --out /var/lib/swarm-news-ingest/runs/%N-%H
```

Example timer:

```ini
# /etc/systemd/system/swarm-news-ingest.timer
[Unit]
Description=Run swarm.channel news feed ingestion periodically

[Timer]
OnBootSec=5m
OnUnitActiveSec=15m
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now swarm-news-ingest.timer
```

The `ExecStart` output path should usually be replaced with a wrapper script that creates a UTC timestamped directory. The raw unit above is a shape example, not the final Racter service.

### launchd, macOS

Use launchd only for Mac hosts. Keep config in `~/.config/swarm-news-ingest` and output in `~/.local/state/swarm-news-ingest` for single-user installs.

## Dry-run before deploy

Dry-run is the official pre-publish verification mode. It fetches real RSS sources and writes the same local artifacts, but performs no Subspace publish:

```bash
OUT=/var/lib/swarm-news-ingest/runs/dry-run-$(date -u +%Y%m%dT%H%M%SZ)
swarm-news-ingest --dry-run --sources /etc/swarm-news-ingest/sources.yaml --out "$OUT"
python3 -m json.tool "$OUT/run-summary.json"
head -20 "$OUT/publish-candidates.jsonl"
```

For Racter, run this after install and before enabling any scheduler or publisher. The current worker has no publisher, so all runs are non-publishing; the flag exists to make the safety boundary explicit and durable.

## Verification gate

Before calling any install healthy:

```bash
swarm-news-ingest --dry-run --sources <config-path> --out <fresh-output-dir>
```

Then verify:

```bash
test -s <fresh-output-dir>/run-summary.json
test -s <fresh-output-dir>/source-health.json
test -s <fresh-output-dir>/normalized.jsonl
test -s <fresh-output-dir>/publish-candidates.jsonl
python3 -m json.tool <fresh-output-dir>/run-summary.json >/dev/null
```

Expected current live baseline from the default config is roughly 13 enabled sources and about 1.9k publish candidates. Counts will drift as feeds change.

## Upgrade

For source installs:

```bash
cd /opt/swarm-news-ingest   # or ~/src/swarm-news-ingest
git pull --ff-only
. .venv/bin/activate
python -m pip install -e .
```

Run the verification gate after upgrade.

## Rollback

For source installs:

```bash
cd /opt/swarm-news-ingest
git log --oneline --max-count=10
git checkout <known-good-sha>
. .venv/bin/activate
python -m pip install -e .
```

Keep previous run artifacts; do not delete `/var/lib/swarm-news-ingest/runs/` during rollback.

## Current hard boundaries

- No live Subspace publish yet.
- No digest generation here.
- No ranking, scoring, lane assignment, or interpretation here.
- No credentials should be needed for the current public-feed worker.
- Racter deploy needs a concrete scheduler/supervisor decision before it is called production.
