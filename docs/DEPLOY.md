# Deploy and install runbook

This repo ships a local news-feed ingestion worker. It is not a hosted service yet. A proper deploy means installing the worker on a machine, giving it a source config, choosing an output directory, and scheduling or supervising runs.

The worker currently produces local artifacts only. It does **not** publish to Subspace, run a daemon, install a cron job, or manage credentials by itself.

## Official installation model

Use the platform's normal application data locations instead of cloning into random scratch directories.

### macOS / Linux single-user install

Recommended layout:

```text
~/src/argus                         # source checkout for operators/developers
~/.local/bin/argus                  # executable shim or symlink
~/.config/argus/sources.yaml        # operator config
~/.local/state/argus/runs/          # run outputs/artifacts
~/.local/state/argus/logs/          # logs if supervised
```

For a checked-out install:

```bash
cd ~/src
git clone https://github.com/clickety-clacks/argus.git
cd argus
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install -e .
mkdir -p ~/.local/bin ~/.config/argus ~/.local/state/argus/runs
cp config/sources.yaml ~/.config/argus/sources.yaml
ln -sf "$PWD/bin/argus" ~/.local/bin/argus
```

Run it:

```bash
~/.local/bin/argus \
  --sources ~/.config/argus/sources.yaml \
  --out ~/.local/state/argus/runs/$(date -u +%Y%m%dT%H%M%SZ)
```

### Linux system install

For a shared/server install, use `/opt` for code, `/etc` for config, and `/var/lib` for state:

```text
/opt/argus/                         # source checkout / release tree
/usr/local/bin/argus                # executable shim or symlink
/etc/argus/sources.yaml             # system config
/var/lib/argus/runs/                # run outputs/artifacts
/var/log/argus/                     # logs if supervised
```

Suggested service user:

```bash
sudo useradd --system --home /var/lib/argus --shell /usr/sbin/nologin argus
```

Install:

```bash
sudo mkdir -p /opt /etc/argus /var/lib/argus/runs /var/log/argus
git clone https://github.com/clickety-clacks/argus.git /opt/argus
cd /opt/argus
sudo python3 -m venv .venv
sudo .venv/bin/python -m pip install -U pip
sudo .venv/bin/python -m pip install -e .
sudo cp config/sources.yaml /etc/argus/sources.yaml
sudo ln -sf /opt/argus/bin/argus /usr/local/bin/argus
# The wrapper automatically uses /opt/argus/.venv/bin/python when present.
sudo chown -R argus:argus /var/lib/argus /var/log/argus
```

Run once:

```bash
sudo -u argus /usr/local/bin/argus \
  --sources /etc/argus/sources.yaml \
  --out /var/lib/argus/runs/$(date -u +%Y%m%dT%H%M%SZ)
```

## Racter target install

Racter is the intended first always-on runtime host for this project.

Recommended Racter layout:

```text
/opt/argus/
/etc/argus/sources.yaml
/var/lib/argus/runs/
/var/log/argus/
/usr/local/bin/argus
```

Racter deploy should be treated as an operator deployment, not a code checkout experiment:

1. Pull or clone `https://github.com/clickety-clacks/argus.git` into `/opt/argus`.
2. Create/update the Python virtualenv under `/opt/argus/.venv`.
3. Install the package editable or from the checked-out tree.
4. Install `/etc/argus/sources.yaml` from `config/sources.yaml`, then edit only config in `/etc`.
5. Write run artifacts under `/var/lib/argus/runs/<timestamp>/`.
6. Log supervisor output under `/var/log/argus/`.
7. Only after the Subspace publisher exists, add publisher config separately; do not overload the scraper config with subscriber interpretation.


## Current Racter install evidence

Current verified Racter install:

```text
code: /opt/argus
config: /etc/argus/sources.yaml
output root: /var/lib/argus/runs/
command: /usr/local/bin/argus
run user: argus
verified commit: pre-rename 646381c; Argus rename pending Racter migration
latest verified dry run: /var/lib/argus/runs/dry-run-20260430T085742Z
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
# /etc/systemd/system/argus.service
[Unit]
Description=Argus RSS feed ingestion

[Service]
Type=oneshot
User=argus
Group=argus
WorkingDirectory=/opt/argus
ExecStart=/usr/local/bin/argus --sources /etc/argus/sources.yaml --out /var/lib/argus/runs/%N-%H
```

Example timer:

```ini
# /etc/systemd/system/argus.timer
[Unit]
Description=Run Argus RSS feed ingestion periodically

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
sudo systemctl enable --now argus.timer
```

The `ExecStart` output path should usually be replaced with a wrapper script that creates a UTC timestamped directory. The raw unit above is a shape example, not the final Racter service.

### launchd, macOS

Use launchd only for Mac hosts. Keep config in `~/.config/argus` and output in `~/.local/state/argus` for single-user installs.

## Dry-run before deploy

Dry-run is the official pre-publish verification mode. It fetches real RSS sources and writes the same local artifacts, but performs no Subspace publish:

```bash
OUT=/var/lib/argus/runs/dry-run-$(date -u +%Y%m%dT%H%M%SZ)
argus --dry-run --sources /etc/argus/sources.yaml --out "$OUT"
python3 -m json.tool "$OUT/run-summary.json"
head -20 "$OUT/publish-candidates.jsonl"
```

For Racter, run this after install and before enabling any scheduler or publisher. The current worker has no publisher, so all runs are non-publishing; the flag exists to make the safety boundary explicit and durable.

## Verification gate

Before calling any install healthy:

```bash
argus --dry-run --sources <config-path> --out <fresh-output-dir>
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
cd /opt/argus   # or ~/src/argus
git pull --ff-only
. .venv/bin/activate
python -m pip install -e .
```

Run the verification gate after upgrade.

## Rollback

For source installs:

```bash
cd /opt/argus
git log --oneline --max-count=10
git checkout <known-good-sha>
. .venv/bin/activate
python -m pip install -e .
```

Keep previous run artifacts; do not delete `/var/lib/argus/runs/` during rollback.

## Current hard boundaries

- No live Subspace publish yet.
- No digest generation here.
- No ranking, scoring, lane assignment, or interpretation here.
- No credentials should be needed for the current public-feed worker.
- Racter deploy needs a concrete scheduler/supervisor decision before it is called production.
