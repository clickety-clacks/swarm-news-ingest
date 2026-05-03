from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

from .pipeline import PipelineError, SourceConfig, iso_z, parse_now, run_pipeline_for_sources, utc_now


MIN_INTERVAL_SECONDS = 15 * 60
MAX_INTERVAL_SECONDS = 24 * 60 * 60
DEFAULT_INTERVAL_SECONDS = 60 * 60


@dataclasses.dataclass(frozen=True)
class SchedulerConfig:
    mode: str = "interval"
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS
    jitter_seconds: int = 0
    run_on_startup_if_due: bool = True
    missed_tick_policy: str = "coalesce_one"


@dataclasses.dataclass(frozen=True)
class PublishConfig:
    state: str = "inactive"
    live_approval: bool = False
    subspace_endpoint: Optional[str] = None
    require_embeddings: bool = True
    allow_non_embedded_fallback: bool = False


@dataclasses.dataclass(frozen=True)
class RuntimeConfig:
    database_path: Path
    output_dir: Path
    sources: List[SourceConfig]
    scheduler: SchedulerConfig
    publish: PublishConfig
    fixture_dir: Optional[Path] = None
    source_config_path: str = "<config>"
    config_hash: str = ""


@dataclasses.dataclass(frozen=True)
class ServiceRegistration:
    pid: int
    database_path: str
    config_path: str


class Clock:
    def now(self) -> datetime:
        return utc_now()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class FakeClock(Clock):
    def __init__(self, now: datetime) -> None:
        self._now = now
        self.slept: List[float] = []

    def now(self) -> datetime:
        return self._now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self._now += timedelta(seconds=seconds)

    def advance(self, seconds: int) -> None:
        self._now += timedelta(seconds=seconds)


def parse_duration_seconds(value: Any) -> int:
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text.endswith("m"):
        return int(text[:-1]) * 60
    if text.endswith("h"):
        return int(text[:-1]) * 60 * 60
    if text.endswith("s"):
        return int(text[:-1])
    return int(text)


def config_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def event_id(prefix: str, observed_at: datetime, detail: str) -> str:
    basis = "{}\n{}\n{}".format(prefix, iso_z(observed_at), detail)
    return "{}:{}".format(prefix, hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24])


def validate_scheduler(payload: Dict[str, Any]) -> SchedulerConfig:
    mode = str(payload.get("mode", "interval"))
    if mode not in {"interval", "manual"}:
        raise PipelineError("Invalid schedule.mode: {}".format(mode))
    interval_seconds = parse_duration_seconds(payload.get("interval", "1h"))
    jitter_seconds = int(payload.get("jitter_seconds", 0))
    missed_tick_policy = str(payload.get("missed_tick_policy", "coalesce_one"))
    if missed_tick_policy != "coalesce_one":
        raise PipelineError("Invalid schedule.missed_tick_policy: {}".format(missed_tick_policy))
    if jitter_seconds < 0 or jitter_seconds > 3600:
        raise PipelineError("Invalid schedule.jitter_seconds: {}".format(jitter_seconds))
    if mode == "interval":
        if interval_seconds < MIN_INTERVAL_SECONDS or interval_seconds > MAX_INTERVAL_SECONDS:
            raise PipelineError("Invalid schedule.interval: {}".format(interval_seconds))
        if jitter_seconds >= interval_seconds / 2:
            raise PipelineError("schedule.jitter_seconds must be less than half of interval")
    return SchedulerConfig(
        mode=mode,
        interval_seconds=interval_seconds,
        jitter_seconds=jitter_seconds,
        run_on_startup_if_due=bool(payload.get("run_on_startup_if_due", True)),
        missed_tick_policy=missed_tick_policy,
    )


def source_from_runtime(row: Dict[str, Any]) -> SourceConfig:
    source_id = str(row["id"])
    feed_url = row.get("feed_url") or row.get("api_url")
    adapter = str(row.get("adapter", "rss"))
    feed_type = {"rss": "rss", "atom": "atom", "arxiv_atom": "arxiv_atom", "generic_rss": "rss"}.get(adapter, adapter)
    return SourceConfig(
        id=source_id,
        name=str(row.get("display_name") or row.get("name") or source_id),
        source_class=str(row["source_class"]),
        source_category=str(row.get("source_category") or row.get("source_class")),
        feed_type=feed_type,
        feed_url=str(feed_url),
        site_url=str(row.get("site_url") or feed_url),
        enabled=bool(row.get("enabled", True)),
        tier=int(row.get("tier", 1)),
        freshness_window_hours=(int(row["freshness_window_hours"]) if row.get("freshness_window_hours") is not None else None),
        adapter=adapter,
        request_headers={str(k): str(v) for k, v in (row.get("headers") or row.get("request_headers") or {}).items()},
        notes=str(row["notes"]) if row.get("notes") is not None else None,
    )


def load_runtime_config(path: Path) -> RuntimeConfig:
    payload = yaml.safe_load(path.read_text())
    if not isinstance(payload, dict):
        raise PipelineError("Invalid Argus config: {}".format(path))
    runtime = payload.get("runtime") or {}
    if not isinstance(runtime, dict):
        raise PipelineError("Invalid runtime config")
    database_path = runtime.get("database_path") or payload.get("database_path")
    output_dir = runtime.get("output_dir") or payload.get("output_dir")
    if not database_path or not output_dir:
        raise PipelineError("runtime.database_path and runtime.output_dir are required")
    sources_payload = payload.get("sources")
    if not isinstance(sources_payload, list):
        raise PipelineError("sources must be a list")
    seen_source_ids = set()
    sources = []
    for row in sources_payload:
        if not isinstance(row, dict):
            continue
        source = source_from_runtime(row)
        if source.id in seen_source_ids:
            raise PipelineError("Duplicate source id: {}".format(source.id))
        seen_source_ids.add(source.id)
        sources.append(source)
    if not any(source.enabled for source in sources):
        raise PipelineError("at least one enabled source is required")
    publish_payload = payload.get("publish") or {}
    publish = PublishConfig(
        state=str(publish_payload.get("state", "inactive")),
        live_approval=bool(publish_payload.get("live_approval", False)),
        subspace_endpoint=(str(publish_payload["subspace_endpoint"]) if publish_payload.get("subspace_endpoint") else None),
        require_embeddings=bool(publish_payload.get("require_embeddings", True)),
        allow_non_embedded_fallback=bool(publish_payload.get("allow_non_embedded_fallback", False)),
    )
    if publish.state not in {"inactive", "active"}:
        raise PipelineError("Invalid publish.state: {}".format(publish.state))
    return RuntimeConfig(
        database_path=Path(database_path),
        output_dir=Path(output_dir),
        sources=sources,
        scheduler=validate_scheduler(payload.get("schedule") or {}),
        publish=publish,
        fixture_dir=(Path(runtime["fixture_dir"]) if runtime.get("fixture_dir") else None),
        source_config_path=str(path),
        config_hash=config_hash(payload),
    )


def connect_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    apply_migrations(connection)
    return connection


def apply_migrations(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY,
          run_kind TEXT NOT NULL,
          started_at TEXT NOT NULL,
          completed_at TEXT,
          status TEXT NOT NULL,
          output_dir TEXT NOT NULL,
          effective_snapshot_id TEXT,
          summary_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS packages (
          package_id TEXT PRIMARY KEY,
          report_id TEXT NOT NULL,
          run_id TEXT NOT NULL,
          created_at TEXT NOT NULL,
          active_eligible INTEGER NOT NULL,
          package_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS publish_attempts (
          attempt_id TEXT PRIMARY KEY,
          package_id TEXT NOT NULL,
          run_id TEXT NOT NULL,
          snapshot_id TEXT NOT NULL,
          publish_idempotency_key TEXT NOT NULL UNIQUE,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_config_snapshots (
          snapshot_id TEXT PRIMARY KEY,
          config_hash TEXT NOT NULL,
          observed_at TEXT NOT NULL,
          requested_mode TEXT NOT NULL,
          effective_mode TEXT NOT NULL,
          activation_observed_at TEXT,
          live_approval_observed INTEGER NOT NULL,
          require_embeddings INTEGER NOT NULL,
          allow_non_embedded_fallback INTEGER NOT NULL,
          blocked_reason TEXT,
          snapshot_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_events (
          event_id TEXT PRIMARY KEY,
          observed_at TEXT NOT NULL,
          event_type TEXT NOT NULL,
          config_hash TEXT,
          snapshot_id TEXT,
          status TEXT NOT NULL,
          error_class TEXT,
          error_message TEXT
        );
        CREATE TABLE IF NOT EXISTS scheduler_state (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          mode TEXT NOT NULL,
          interval_seconds INTEGER,
          jitter_seconds INTEGER NOT NULL,
          run_on_startup_if_due INTEGER NOT NULL,
          missed_tick_policy TEXT NOT NULL,
          config_hash TEXT NOT NULL,
          last_decision_at TEXT,
          last_started_run_id TEXT,
          last_completed_run_id TEXT,
          last_completed_at TEXT,
          running_run_id TEXT,
          next_due_at TEXT,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS scheduler_events (
          event_id TEXT PRIMARY KEY,
          observed_at TEXT NOT NULL,
          event_type TEXT NOT NULL,
          decision_id TEXT,
          run_id TEXT,
          status TEXT NOT NULL,
          config_hash TEXT,
          detail_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS prime_events (
          event_id TEXT PRIMARY KEY,
          requested_at TEXT NOT NULL,
          completed_at TEXT,
          requested_by TEXT,
          run_id TEXT,
          prime_watermark_run_id TEXT,
          status TEXT NOT NULL,
          error_class TEXT,
          error_message TEXT
        );
        CREATE TABLE IF NOT EXISTS service_state (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          pid INTEGER NOT NULL,
          config_path TEXT NOT NULL,
          started_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    connection.commit()


def build_publish_snapshot(config: RuntimeConfig, observed_at: datetime, force_inactive: bool = False, error: Optional[str] = None) -> Dict[str, Any]:
    requested = "inactive" if force_inactive else config.publish.state
    effective = "inactive"
    blocked_reason = None
    activation_observed_at = None
    if error:
        blocked_reason = error
    elif config.publish.state == "active":
        if not config.publish.live_approval:
            effective = "blocked"
            blocked_reason = "missing_live_approval"
        elif not config.publish.subspace_endpoint:
            effective = "blocked"
            blocked_reason = "missing_subspace_endpoint"
        else:
            effective = "active"
            activation_observed_at = iso_z(observed_at)
    snapshot = {
        "snapshot_id": "sha256:{}:{}".format(config.config_hash, iso_z(observed_at)),
        "config_hash": config.config_hash,
        "observed_at": iso_z(observed_at),
        "requested_mode": requested,
        "effective_mode": effective,
        "activation_observed_at": activation_observed_at,
        "live_approval_observed": bool(config.publish.live_approval),
        "require_embeddings": bool(config.publish.require_embeddings),
        "allow_non_embedded_fallback": bool(config.publish.allow_non_embedded_fallback),
        "blocked_reason": blocked_reason,
    }
    return snapshot


def store_runtime_snapshot(connection: sqlite3.Connection, snapshot: Dict[str, Any], event_type: str, status: str, error: Optional[str] = None) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO runtime_config_snapshots
        (snapshot_id, config_hash, observed_at, requested_mode, effective_mode, activation_observed_at,
         live_approval_observed, require_embeddings, allow_non_embedded_fallback, blocked_reason, snapshot_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot["snapshot_id"],
            snapshot["config_hash"],
            snapshot["observed_at"],
            snapshot["requested_mode"],
            snapshot["effective_mode"],
            snapshot["activation_observed_at"],
            int(snapshot["live_approval_observed"]),
            int(snapshot["require_embeddings"]),
            int(snapshot["allow_non_embedded_fallback"]),
            snapshot["blocked_reason"],
            json.dumps(snapshot, sort_keys=True),
        ),
    )
    connection.execute(
        "INSERT OR REPLACE INTO runtime_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event_id(event_type, parse_now(snapshot["observed_at"]), status + (error or "")),
            snapshot["observed_at"],
            event_type,
            snapshot["config_hash"],
            snapshot["snapshot_id"],
            status,
            "config" if error else None,
            error,
        ),
    )
    connection.commit()


def latest_snapshot(connection: sqlite3.Connection) -> Dict[str, Any]:
    row = connection.execute("SELECT snapshot_json FROM runtime_config_snapshots ORDER BY observed_at DESC, rowid DESC LIMIT 1").fetchone()
    if row is None:
        raise PipelineError("No runtime publish snapshot")
    return json.loads(row["snapshot_json"])


class ArgusServer:
    def __init__(self, config_path: Path, clock: Optional[Clock] = None, register_service: bool = True) -> None:
        self.config_path = config_path
        self.clock = clock or Clock()
        self.register_service = register_service
        self.config = load_runtime_config(config_path)
        self.connection = connect_database(self.config.database_path)
        self.running = False
        self.reload_requested = False
        self._cycle_running = False
        if self.register_service:
            self.start()
        else:
            self._ensure_control_state()

    def _ensure_control_state(self) -> None:
        now = self.clock.now()
        store_runtime_snapshot(self.connection, build_publish_snapshot(self.config, now), "control_start", "ok")
        if self.connection.execute("SELECT 1 FROM scheduler_state WHERE id = 1").fetchone() is None:
            self._record_scheduler_config(self.config.scheduler, now, recompute_next=True)

    def start(self) -> None:
        now = self.clock.now()
        snapshot = build_publish_snapshot(self.config, now)
        store_runtime_snapshot(self.connection, snapshot, "service_start" if self.register_service else "control_start", "ok")
        if self.register_service:
            self.connection.execute(
                "INSERT OR REPLACE INTO service_state VALUES (1, ?, ?, ?, ?)",
                (os.getpid(), str(self.config_path), iso_z(now), iso_z(now)),
            )
            self._write_service_registration()
            stale = self.connection.execute("SELECT running_run_id FROM scheduler_state WHERE id = 1").fetchone()
            if stale and stale["running_run_id"]:
                self.connection.execute(
                    "UPDATE scheduler_state SET running_run_id = NULL, updated_at = ? WHERE id = 1",
                    (iso_z(now),),
                )
                self.connection.commit()
                self._record_scheduler_event("cycle_recovered_after_restart", stale["running_run_id"], "failed", {}, now)
        self._record_scheduler_config(self.config.scheduler, now, recompute_next=True)
        self._record_scheduler_event("scheduler_ready", None, "ok", {"mode": self.config.scheduler.mode}, now)

    def _write_service_registration(self) -> None:
        registration = {
            "pid": os.getpid(),
            "database_path": str(self.config.database_path),
            "config_path": str(self.config_path),
        }
        payload = json.dumps(registration, sort_keys=True) + "\n"
        self._runtime_registration_path().write_text(payload)
        self._temp_registration_path().write_text(payload)
        try:
            self._config_registration_path().write_text(payload)
        except OSError:
            pass

    def _config_registration_path(self) -> Path:
        return self.config_path.with_name(self.config_path.name + ".service.json")

    def _runtime_registration_path(self) -> Path:
        return self.config.database_path.with_name("argus.service.json")

    def _temp_registration_path(self) -> Path:
        name = "argus-{}.service.json".format(hashlib.sha256(str(self.config_path).encode("utf-8")).hexdigest()[:24])
        return Path(tempfile.gettempdir()) / name

    def close(self) -> None:
        self.connection.close()

    def request_reload(self, *_args: Any) -> None:
        self.reload_requested = True

    def reload(self) -> None:
        now = self.clock.now()
        try:
            new_config = load_runtime_config(self.config_path)
        except Exception as exc:
            snapshot = build_publish_snapshot(self.config, now, force_inactive=True, error=str(exc))
            store_runtime_snapshot(self.connection, snapshot, "reload_failed", "failed", str(exc))
            self._record_scheduler_event("scheduler_reload_failed", None, "failed", {"error": str(exc)}, now)
            return
        self.config = new_config
        snapshot = build_publish_snapshot(new_config, now)
        store_runtime_snapshot(self.connection, snapshot, "reload", "ok")
        self._record_scheduler_config(new_config.scheduler, now, recompute_next=True)
        self._record_scheduler_event("scheduler_reload", None, "ok", {"mode": new_config.scheduler.mode}, now)

    def set_publish_state(self, state: str) -> Dict[str, Any]:
        if state not in {"inactive", "active"}:
            raise PipelineError("Invalid publish state: {}".format(state))
        self.config = dataclasses.replace(self.config, publish=dataclasses.replace(self.config.publish, state=state))
        snapshot = build_publish_snapshot(self.config, self.clock.now())
        store_runtime_snapshot(self.connection, snapshot, "set_publish_state", "ok")
        return snapshot

    def prime(self, requested_by: str = "cli") -> Tuple[int, Dict[str, Any]]:
        now = self.clock.now()
        state = self._scheduler_state()
        if self._cycle_running or state["running_run_id"]:
            return self._run_cycle("prime", now, prime=True)
        event = event_id("prime", now, requested_by)
        self.connection.execute("INSERT OR REPLACE INTO prime_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (event, iso_z(now), None, requested_by, None, None, "running", None, None))
        self.connection.commit()
        exit_code, summary = self._run_cycle("prime", now, prime=True)
        completed_at = iso_z(self.clock.now())
        self.connection.execute(
            "UPDATE prime_events SET completed_at = ?, run_id = ?, prime_watermark_run_id = ?, status = ? WHERE event_id = ?",
            (completed_at, summary["run_id"], summary["run_id"], "succeeded" if exit_code == 0 else "failed", event),
        )
        self.connection.commit()
        return exit_code, summary

    def manual_cycle(self) -> Tuple[int, Dict[str, Any]]:
        return self._run_cycle("manual", self.clock.now(), prime=False)

    def tick(self) -> Optional[Tuple[int, Dict[str, Any]]]:
        if self.reload_requested:
            self.reload_requested = False
            self.reload()
        now = self.clock.now()
        state = self._scheduler_state()
        if state["mode"] == "manual":
            self._record_scheduler_event("decision_manual_idle", None, "skipped", {}, now)
            return None
        if self._cycle_running or state["running_run_id"]:
            self._record_scheduler_event("cycle_skipped_already_running", None, "skipped", {}, now)
            return None
        next_due_at = parse_now(state["next_due_at"]) if state["next_due_at"] else now
        if now < next_due_at:
            self._record_scheduler_event("decision_not_due", None, "skipped", {"next_due_at": state["next_due_at"]}, now)
            return None
        return self._run_cycle("scheduled", now, prime=False)

    def serve_forever(self) -> int:
        self.running = True
        signal.signal(signal.SIGHUP, self.request_reload)
        while self.running:
            result = self.tick()
            if result is None:
                state = self._scheduler_state()
                next_due_at = parse_now(state["next_due_at"]) if state["next_due_at"] else self.clock.now() + timedelta(seconds=60)
                delay = max(1.0, min(60.0, (next_due_at - self.clock.now()).total_seconds()))
                self.clock.sleep(delay)
        return 0

    def status(self) -> Dict[str, Any]:
        state = self._scheduler_state()
        return {
            "database_path": str(self.config.database_path),
            "output_dir": str(self.config.output_dir),
            "publish": latest_snapshot(self.connection),
            "scheduler": dict(state),
            "last_run": self._last_run(),
        }

    def _run_cycle(self, run_kind: str, now: datetime, prime: bool = False) -> Tuple[int, Dict[str, Any]]:
        state = self._scheduler_state()
        if self._cycle_running or state["running_run_id"]:
            self._record_scheduler_event("cycle_skipped_already_running", state["running_run_id"], "skipped", {"requested_run_kind": run_kind}, now)
            return 0, {
                "run_id": state["running_run_id"],
                "run_kind": run_kind,
                "exit_status": "skipped",
                "skip_reason": "cycle_already_running",
            }
        self._cycle_running = True
        run_id = "{}-{}-{}".format(
            now.strftime("%Y%m%dT%H%M%SZ"),
            run_kind,
            hashlib.sha256("{}:{}:{}".format(run_kind, iso_z(now), time.monotonic_ns()).encode("utf-8")).hexdigest()[:8],
        )
        snapshot = latest_snapshot(self.connection)
        output_dir = self.config.output_dir / "runs" / run_id
        self.connection.execute(
            "UPDATE scheduler_state SET running_run_id = ?, last_started_run_id = ?, updated_at = ? WHERE id = 1",
            (run_id, run_id, iso_z(now)),
        )
        self.connection.commit()
        try:
            exit_code, summary = run_pipeline_for_sources(
                self.config.sources,
                self.config.source_config_path,
                output_dir,
                now,
                fixture_dir=self.config.fixture_dir,
                prime=prime,
                state_path=self.config.output_dir / "state.json",
                state_write=True,
            )
            summary["run_id"] = run_id
            summary["run_kind"] = run_kind
            if self.reload_requested:
                self.reload_requested = False
                self.reload()
            publish_snapshot = latest_snapshot(self.connection)
            summary["effective_publish_snapshot"] = publish_snapshot
            (output_dir / "run-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            self._store_run(run_id, run_kind, now, exit_code, output_dir, publish_snapshot, summary)
            if prime:
                self._write_prime_artifacts(output_dir)
            else:
                self._store_packages_and_publish(run_id, now, output_dir, publish_snapshot)
            completed_at = self.clock.now()
            self._record_scheduler_completion(run_id, completed_at)
            self._record_scheduler_event("cycle_completed", run_id, "ok" if exit_code == 0 else "failed", {"run_kind": run_kind}, completed_at)
            return exit_code, summary
        finally:
            self._cycle_running = False
            self.connection.execute(
                "UPDATE scheduler_state SET running_run_id = NULL, updated_at = ? WHERE running_run_id = ?",
                (iso_z(self.clock.now()), run_id),
            )
            self.connection.commit()

    def _write_prime_artifacts(self, output_dir: Path) -> None:
        (output_dir / "prime-baseline.json").write_text((output_dir / "run-summary.json").read_text())
        (output_dir / "prime-source-health.json").write_text((output_dir / "source-health.json").read_text())
        (output_dir / "prime-normalized-items.jsonl").write_text((output_dir / "normalized.jsonl").read_text())
        (output_dir / "prime-baseline.md").write_text("# Argus Prime Baseline\n\nNo live publish work was created.\n")
        (output_dir / "package-candidates.jsonl").write_text("")

    def _store_run(self, run_id: str, run_kind: str, now: datetime, exit_code: int, output_dir: Path, snapshot: Dict[str, Any], summary: Dict[str, Any]) -> None:
        status = "succeeded" if exit_code == 0 else "failed"
        self.connection.execute(
            "INSERT OR REPLACE INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, run_kind, iso_z(now), iso_z(self.clock.now()), status, str(output_dir), snapshot["snapshot_id"], json.dumps(summary, sort_keys=True)),
        )
        self.connection.commit()

    def _store_packages_and_publish(self, run_id: str, now: datetime, output_dir: Path, snapshot: Dict[str, Any]) -> None:
        candidates_path = output_dir / "publish-candidates.jsonl"
        rows = [json.loads(line) for line in candidates_path.read_text().splitlines() if line.strip()]
        for candidate in rows:
            package_id = "package:" + hashlib.sha256(json.dumps(candidate, sort_keys=True).encode("utf-8")).hexdigest()
            has_embedding = bool(candidate.get("supplied_embeddings") or candidate.get("embedding"))
            publish_allowed = snapshot["effective_mode"] == "active" and (
                not snapshot["require_embeddings"] or snapshot["allow_non_embedded_fallback"] or has_embedding
            )
            self.connection.execute(
                "INSERT OR IGNORE INTO packages VALUES (?, ?, ?, ?, ?, ?)",
                (package_id, candidate["report_id"], run_id, iso_z(now), int(publish_allowed), json.dumps(candidate, sort_keys=True)),
            )
            if publish_allowed:
                target = hashlib.sha256(str(self.config.publish.subspace_endpoint).encode("utf-8")).hexdigest()
                key = "sha256:" + hashlib.sha256(("publish:v0\n{}\n{}".format(package_id, target)).encode("utf-8")).hexdigest()
                attempt = "sha256:" + hashlib.sha256(("publish-attempt:v0\n{}\n1".format(key)).encode("utf-8")).hexdigest()
                self.connection.execute(
                    "INSERT OR IGNORE INTO publish_attempts VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (attempt, package_id, run_id, snapshot["snapshot_id"], key, "pending", iso_z(now)),
                )
        self.connection.commit()

    def _record_scheduler_config(self, scheduler: SchedulerConfig, now: datetime, recompute_next: bool) -> None:
        existing = self.connection.execute("SELECT mode, last_completed_at FROM scheduler_state WHERE id = 1").fetchone()
        last_completed_at = existing["last_completed_at"] if existing else None
        if scheduler.mode == "manual":
            next_due_at = None
        elif not last_completed_at and scheduler.run_on_startup_if_due:
            next_due_at = iso_z(now)
        elif last_completed_at:
            previous_mode = existing["mode"] if existing else None
            base = now if recompute_next and previous_mode == "manual" else parse_now(last_completed_at)
            next_due_at = iso_z(base + timedelta(seconds=scheduler.interval_seconds + self._jitter_offset("{}:{}".format(iso_z(now), scheduler.interval_seconds))))
        else:
            next_due_at = iso_z(now + timedelta(seconds=scheduler.interval_seconds + self._jitter_offset("{}:startup".format(iso_z(now)))))
        self.connection.execute(
            """
            INSERT INTO scheduler_state
            (id, mode, interval_seconds, jitter_seconds, run_on_startup_if_due, missed_tick_policy, config_hash,
             last_decision_at, last_started_run_id, last_completed_run_id, last_completed_at, running_run_id, next_due_at, updated_at)
            VALUES (1, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, NULL, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              mode=excluded.mode,
              interval_seconds=excluded.interval_seconds,
              jitter_seconds=excluded.jitter_seconds,
              run_on_startup_if_due=excluded.run_on_startup_if_due,
              missed_tick_policy=excluded.missed_tick_policy,
              config_hash=excluded.config_hash,
              next_due_at=excluded.next_due_at,
              updated_at=excluded.updated_at
            """,
            (
                scheduler.mode,
                scheduler.interval_seconds,
                scheduler.jitter_seconds,
                int(scheduler.run_on_startup_if_due),
                scheduler.missed_tick_policy,
                self.config.config_hash,
                last_completed_at,
                next_due_at,
                iso_z(now),
            ),
        )
        self.connection.commit()

    def _record_scheduler_completion(self, run_id: str, now: datetime) -> None:
        next_due_at = None
        if self.config.scheduler.mode == "interval":
            next_due_at = iso_z(now + timedelta(seconds=self.config.scheduler.interval_seconds + self._jitter_offset(run_id)))
        self.connection.execute(
            """
            UPDATE scheduler_state
            SET last_completed_run_id = ?, last_completed_at = ?, running_run_id = NULL, next_due_at = ?, updated_at = ?
            WHERE id = 1
            """,
            (run_id, iso_z(now), next_due_at, iso_z(now)),
        )
        self.connection.commit()

    def _jitter_offset(self, basis: str) -> int:
        jitter = self.config.scheduler.jitter_seconds
        if jitter <= 0:
            return 0
        return int(hashlib.sha256(basis.encode("utf-8")).hexdigest()[:8], 16) % (jitter + 1)

    def _record_scheduler_event(self, event_type: str, run_id: Optional[str], status: str, detail: Dict[str, Any], now: datetime) -> None:
        decision = event_id("decision", now, event_type + (run_id or ""))
        self.connection.execute(
            "INSERT OR REPLACE INTO scheduler_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event_id("scheduler", now, event_type + (run_id or "")),
                iso_z(now),
                event_type,
                decision,
                run_id,
                status,
                self.config.config_hash,
                json.dumps(detail, sort_keys=True),
            ),
        )
        self.connection.execute("UPDATE scheduler_state SET last_decision_at = ?, updated_at = ? WHERE id = 1", (iso_z(now), iso_z(now)))
        self.connection.commit()

    def _scheduler_state(self) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone()
        if row is None:
            raise PipelineError("Scheduler state is not initialized")
        return row

    def _last_run(self) -> Optional[Dict[str, Any]]:
        row = self.connection.execute("SELECT * FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 1").fetchone()
        return dict(row) if row else None


def run_status(db_path: Path) -> Dict[str, Any]:
    connection = connect_database(db_path)
    try:
        snapshot_row = connection.execute("SELECT snapshot_json FROM runtime_config_snapshots ORDER BY observed_at DESC, rowid DESC LIMIT 1").fetchone()
        scheduler_row = connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone()
        run_row = connection.execute("SELECT * FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 1").fetchone()
        return {
            "publish": json.loads(snapshot_row["snapshot_json"]) if snapshot_row else None,
            "scheduler": dict(scheduler_row) if scheduler_row else None,
            "last_run": dict(run_row) if run_row else None,
        }
    finally:
        connection.close()


def request_process_reload(config_path: Path) -> Dict[str, Any]:
    temp_registration_path = Path(tempfile.gettempdir()) / "argus-{}.service.json".format(hashlib.sha256(str(config_path).encode("utf-8")).hexdigest()[:24])
    registration_path = config_path.with_name(config_path.name + ".service.json")
    effective_registration_path = registration_path if registration_path.exists() else temp_registration_path
    if effective_registration_path.exists():
        registration = json.loads(effective_registration_path.read_text())
        pid = int(registration["pid"])
        if not _pid_matches_config(pid, config_path):
            raise PipelineError("Recorded Argus service is not running for {}".format(config_path))
        os.kill(pid, signal.SIGHUP)
        return {"pid": pid, "signal": "SIGHUP", "config_path": str(config_path)}
    config = load_runtime_config(config_path)
    connection = connect_database(config.database_path)
    try:
        row = connection.execute("SELECT * FROM service_state WHERE id = 1").fetchone()
        if row is None:
            raise PipelineError("No running Argus service is recorded")
        if not _pid_matches_config(int(row["pid"]), config_path):
            raise PipelineError("Recorded Argus service is not running for {}".format(config_path))
        os.kill(int(row["pid"]), signal.SIGHUP)
        return {"pid": int(row["pid"]), "signal": "SIGHUP", "config_path": str(config_path)}
    finally:
        connection.close()


def _pid_matches_config(pid: int, config_path: Path) -> bool:
    try:
        completed = subprocess.run(["ps", "-p", str(pid), "-o", "command="], check=False, capture_output=True, text=True)
    except OSError:
        return False
    command = completed.stdout.strip()
    return completed.returncode == 0 and str(config_path) in command and "argus" in command


def run_source_health(db_path: Path) -> List[Dict[str, Any]]:
    status = run_status(db_path)
    last_run = status.get("last_run")
    if not last_run:
        return []
    output_dir = Path(last_run["output_dir"])
    path = output_dir / "source-health.json"
    return json.loads(path.read_text()) if path.exists() else []


def explain_skip(db_path: Path, run_id: str) -> Dict[str, Any]:
    connection = connect_database(db_path)
    try:
        row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise PipelineError("Unknown run: {}".format(run_id))
        output_dir = Path(row["output_dir"])
        summary = json.loads((output_dir / "run-summary.json").read_text())
        return {"run": dict(row), "summary": summary}
    finally:
        connection.close()
