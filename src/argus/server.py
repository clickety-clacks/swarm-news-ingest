from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import re
import signal
import sqlite3
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urlunparse

import yaml
import requests

from .pipeline import PipelineError, SourceConfig, date_bucket, iso_z, normalize_feed_entry_id, normalize_title, parse_now, run_pipeline_for_sources, utc_now


MIN_INTERVAL_SECONDS = 5 * 60
MAX_INTERVAL_SECONDS = 24 * 60 * 60
DEFAULT_INTERVAL_SECONDS = 60 * 60
SOURCE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
SOURCE_CLASSES = {"official", "research", "editorial", "community", "adapter"}
FEED_ADAPTERS = {"rss", "atom"}
API_ADAPTERS = {"api", "arxiv_atom"}
DEFAULT_SUBSPACE_WEBSOCKET_PATH = "/api/firehose/stream/websocket"
DEFAULT_SUBSPACE_AGENT_ID_ENV = "ARGUS_SUBSPACE_AGENT_ID"
DEFAULT_SUBSPACE_SESSION_TOKEN_ENV = "ARGUS_SUBSPACE_SESSION_TOKEN"
LEGACY_DAEMON_PUBLISH_KEYS = {"subspace_daemon_socket", "daemon_socket_path", "subspace_daemon_api_path", "daemon_api_path"}
OPENAI_EMBEDDING_PROVIDER = "openai"
OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"
OPENAI_EMBEDDING_DIMENSIONS = 1536
OPENAI_EMBEDDING_SPACE_ID = "openai:text-embedding-3-small:1536:v1"


@dataclasses.dataclass(frozen=True)
class SchedulerConfig:
    mode: str = "interval"
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS
    jitter_seconds: int = 0
    run_on_startup_if_due: bool = True
    missed_tick_policy: str = "coalesce_one"
    max_live_publishes_per_tick: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class PublishConfig:
    mode: str = "inactive"
    subspace_endpoint: Optional[str] = None
    subspace_agent_id: Optional[str] = None
    subspace_session_token: Optional[str] = None
    subspace_websocket_path: str = DEFAULT_SUBSPACE_WEBSOCKET_PATH
    require_embeddings: bool = True
    allow_non_embedded_fallback: bool = False


@dataclasses.dataclass(frozen=True)
class DeliveryConfig:
    mode: str = "tranche"
    manual_window_seconds: int = DEFAULT_INTERVAL_SECONDS
    max_retry_delay_seconds: int = 15 * 60
    live_send_concurrency: int = 1
    min_slot_seconds: int = 60
    max_messages_per_plan: int = 20


@dataclasses.dataclass(frozen=True)
class EmbeddingConfig:
    backend: Optional[str] = None
    command: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    dimensions: Optional[int] = None
    space_id: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class RuntimeConfig:
    database_path: Path
    output_dir: Path
    sources: List[SourceConfig]
    scheduler: SchedulerConfig
    publish: PublishConfig
    delivery: DeliveryConfig
    embedding: EmbeddingConfig
    source_fetch_concurrency: int = 4
    fixture_dir: Optional[Path] = None
    source_config_path: str = "<config>"
    config_hash: str = ""


@dataclasses.dataclass(frozen=True)
class ServiceRegistration:
    pid: int
    database_path: str
    config_path: str


def runtime_service_pid(config_path: Path) -> Optional[int]:
    temp_registration_path = Path(tempfile.gettempdir()) / "argus-{}.service.json".format(hashlib.sha256(str(config_path).encode("utf-8")).hexdigest()[:24])
    registration_path = config_path.with_name(config_path.name + ".service.json")
    effective_registration_path = registration_path if registration_path.exists() else temp_registration_path
    if effective_registration_path.exists():
        registration = json.loads(effective_registration_path.read_text())
        pid = int(registration["pid"])
        if _pid_matches_config(pid, config_path):
            return pid
    try:
        config = load_runtime_config(config_path)
    except Exception:
        return None
    if not config.database_path.exists():
        return None
    connection = connect_database_readonly(config.database_path)
    try:
        try:
            row = connection.execute("SELECT * FROM service_state WHERE id = 1").fetchone()
        except sqlite3.OperationalError:
            return None
        if row is None:
            return None
        pid = int(row["pid"])
        return pid if _pid_matches_config(pid, config_path) else None
    finally:
        connection.close()


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
    try:
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
    except (TypeError, ValueError):
        raise PipelineError("Invalid schedule.interval: {}".format(value))


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
    interval_seconds = DEFAULT_INTERVAL_SECONDS if mode == "manual" else parse_duration_seconds(payload.get("interval", "1h"))
    try:
        jitter_seconds = int(payload.get("jitter_seconds", 0))
    except (TypeError, ValueError):
        raise PipelineError("Invalid schedule.jitter_seconds: {}".format(payload.get("jitter_seconds")))
    missed_tick_policy = str(payload.get("missed_tick_policy", "coalesce_one"))
    if missed_tick_policy != "coalesce_one":
        raise PipelineError("Invalid schedule.missed_tick_policy: {}".format(missed_tick_policy))
    max_live_publishes_per_tick = None
    raw_max_live_publishes_per_tick = payload.get("max_live_publishes_per_tick")
    if raw_max_live_publishes_per_tick is not None:
        try:
            max_live_publishes_per_tick = int(raw_max_live_publishes_per_tick)
        except (TypeError, ValueError):
            raise PipelineError("Invalid schedule.max_live_publishes_per_tick: {}".format(raw_max_live_publishes_per_tick))
        if max_live_publishes_per_tick < 0:
            raise PipelineError("Invalid schedule.max_live_publishes_per_tick: {}".format(raw_max_live_publishes_per_tick))
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
        max_live_publishes_per_tick=max_live_publishes_per_tick,
    )


def validate_delivery(payload: Dict[str, Any]) -> DeliveryConfig:
    mode = str(payload.get("mode", "tranche"))
    if mode not in {"tranche", "immediate"}:
        raise PipelineError("Invalid delivery.mode: {}".format(mode))
    manual_window_seconds = parse_duration_seconds(payload.get("manual_window", "1h"))
    max_retry_delay_seconds = parse_duration_seconds(payload.get("max_retry_delay", "15m"))
    min_slot_seconds = parse_duration_seconds(payload.get("min_slot", "1m"))
    try:
        max_messages_per_plan = int(payload.get("max_messages_per_plan", 20))
    except (TypeError, ValueError):
        raise PipelineError("Invalid delivery.max_messages_per_plan: {}".format(payload.get("max_messages_per_plan")))
    try:
        live_send_concurrency = int(payload.get("live_send_concurrency", 1))
    except (TypeError, ValueError):
        raise PipelineError("Invalid delivery.live_send_concurrency: {}".format(payload.get("live_send_concurrency")))
    if manual_window_seconds <= 0:
        raise PipelineError("Invalid delivery.manual_window: {}".format(payload.get("manual_window")))
    if max_retry_delay_seconds <= 0:
        raise PipelineError("Invalid delivery.max_retry_delay: {}".format(payload.get("max_retry_delay")))
    if min_slot_seconds <= 0:
        raise PipelineError("Invalid delivery.min_slot: {}".format(payload.get("min_slot")))
    if max_messages_per_plan <= 0:
        raise PipelineError("Invalid delivery.max_messages_per_plan: {}".format(payload.get("max_messages_per_plan")))
    if live_send_concurrency != 1:
        raise PipelineError("Invalid delivery.live_send_concurrency: {}".format(live_send_concurrency))
    return DeliveryConfig(
        mode=mode,
        manual_window_seconds=manual_window_seconds,
        max_retry_delay_seconds=max_retry_delay_seconds,
        live_send_concurrency=live_send_concurrency,
        min_slot_seconds=min_slot_seconds,
        max_messages_per_plan=max_messages_per_plan,
    )


def publish_config_from_payload(payload: Dict[str, Any]) -> PublishConfig:
    legacy_keys = sorted(key for key in LEGACY_DAEMON_PUBLISH_KEYS if key in payload)
    if legacy_keys:
        raise PipelineError(
            "Invalid publish config: Argus publishes directly to Subspace; remove legacy publish topology fields: {}".format(
                ", ".join(legacy_keys)
            )
        )
    if "subspace_session_token" in payload:
        raise PipelineError("publish.subspace_session_token must be supplied via ARGUS_SUBSPACE_SESSION_TOKEN")
    return PublishConfig(
        mode=str(payload.get("mode", payload.get("state", "inactive"))),
        subspace_endpoint=(str(payload["subspace_endpoint"]) if payload.get("subspace_endpoint") else None),
        subspace_agent_id=(str(payload["subspace_agent_id"]) if payload.get("subspace_agent_id") else os.environ.get(DEFAULT_SUBSPACE_AGENT_ID_ENV)),
        subspace_session_token=os.environ.get(DEFAULT_SUBSPACE_SESSION_TOKEN_ENV),
        subspace_websocket_path=str(payload.get("subspace_websocket_path") or DEFAULT_SUBSPACE_WEBSOCKET_PATH),
        require_embeddings=bool(payload.get("require_embeddings", True)),
        allow_non_embedded_fallback=bool(payload.get("allow_non_embedded_fallback", False)),
    )


def source_from_runtime(row: Dict[str, Any]) -> SourceConfig:
    source_id = str(row["id"])
    if not SOURCE_ID_RE.match(source_id):
        raise PipelineError("Invalid source id: {}".format(source_id))
    enabled = bool(row.get("enabled", True))
    has_feed_url = bool(row.get("feed_url"))
    has_api_url = bool(row.get("api_url"))
    required = ["display_name", "source_class", "site_url", "adapter", "freshness_window_hours", "authority_score"]
    missing = [field for field in required if row.get(field) is None or row.get(field) == ""]
    if enabled and missing:
        raise PipelineError("Enabled source missing required fields for {}: {}".format(source_id, ", ".join(missing)))
    if not enabled and not (has_feed_url or has_api_url or row.get("fixture_payload_path")):
        raise PipelineError("Disabled source must set an endpoint or fixture_payload_path: {}".format(source_id))
    if enabled and has_feed_url == has_api_url:
        raise PipelineError("Enabled source must set exactly one fetch endpoint family: {}".format(source_id))
    adapter = str(row.get("adapter", "rss"))
    if adapter in FEED_ADAPTERS and has_api_url:
        raise PipelineError("Feed adapter source must use feed_url: {}".format(source_id))
    if adapter in API_ADAPTERS and has_feed_url:
        raise PipelineError("API adapter source must use api_url: {}".format(source_id))
    if adapter not in FEED_ADAPTERS | API_ADAPTERS:
        raise PipelineError("Invalid source adapter for {}: {}".format(source_id, adapter))
    source_class = str(row["source_class"])
    if source_class not in SOURCE_CLASSES:
        raise PipelineError("Invalid source_class for {}: {}".format(source_id, source_class))
    feed_url = row.get("feed_url") or row.get("api_url")
    feed_type = {"rss": "rss", "atom": "atom", "arxiv_atom": "arxiv_atom", "generic_rss": "rss"}.get(adapter, adapter)
    authority_score = row.get("authority_score")
    if authority_score is not None and not 0 <= float(authority_score) <= 1:
        raise PipelineError("Invalid authority_score for {}: {}".format(row.get("id"), authority_score))
    fixture_payload_path = row.get("fixture_payload_path")
    if fixture_payload_path is not None:
        fixture_payload_path = str(fixture_payload_path)
        if Path(fixture_payload_path).is_absolute() or not fixture_payload_path.startswith("testdata/feeds/"):
            raise PipelineError("Invalid fixture_payload_path for {}: {}".format(row.get("id"), fixture_payload_path))
    return SourceConfig(
        id=source_id,
        name=str(row.get("display_name") or row.get("name") or source_id),
        source_class=source_class,
        source_category=str(row.get("source_category") or source_class),
        feed_type=feed_type,
        feed_url=str(feed_url),
        site_url=str(row.get("site_url") or feed_url),
        enabled=enabled,
        tier=int(row.get("tier", 1)),
        freshness_window_hours=(int(row["freshness_window_hours"]) if row.get("freshness_window_hours") is not None else None),
        adapter=adapter,
        request_headers={str(k): str(v) for k, v in (row.get("headers") or row.get("request_headers") or {}).items()},
        notes=str(row["notes"]) if row.get("notes") is not None else None,
        cadence_interval_seconds=(parse_duration_seconds(row["cadence_override"]) if row.get("cadence_override") is not None else None),
        authority_score=(float(authority_score) if authority_score is not None else None),
        fixture_payload_path=fixture_payload_path,
        max_messages_per_fetch=None,
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
    source_fetch_concurrency = int(runtime.get("source_fetch_concurrency", 4))
    if source_fetch_concurrency < 1 or source_fetch_concurrency > 16:
        raise PipelineError("runtime.source_fetch_concurrency must be in range 1..16")
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
    embedding_payload = payload.get("embedding") or {}
    publish = publish_config_from_payload(publish_payload)
    if publish.mode not in {"inactive", "dry_run", "live"}:
        raise PipelineError("Invalid publish.mode: {}".format(publish.mode))
    embedding = EmbeddingConfig(
        backend=(str(embedding_payload["backend"]) if embedding_payload.get("backend") else None),
        command=(str(embedding_payload["command"]) if embedding_payload.get("command") else None),
        provider=(str(embedding_payload["provider"]) if embedding_payload.get("provider") else None),
        model=(str(embedding_payload["model"]) if embedding_payload.get("model") else None),
        dimensions=(int(embedding_payload["dimensions"]) if embedding_payload.get("dimensions") is not None else None),
        space_id=(str(embedding_payload["space_id"]) if embedding_payload.get("space_id") else None),
    )
    if publish.require_embeddings and not publish.allow_non_embedded_fallback and not embedding_config_valid(embedding):
        raise PipelineError("missing_embedding_config")
    return RuntimeConfig(
        database_path=Path(database_path),
        output_dir=Path(output_dir),
        sources=sources,
        scheduler=validate_scheduler(payload.get("schedule") or {}),
        publish=publish,
        delivery=validate_delivery(payload.get("delivery") or {}),
        embedding=embedding,
        source_fetch_concurrency=source_fetch_concurrency,
        fixture_dir=(Path(runtime["fixture_dir"]) if runtime.get("fixture_dir") else None),
        source_config_path=str(path),
        config_hash=config_hash(payload),
    )


def connect_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA foreign_keys = ON")
    apply_migrations(connection)
    return connection


def connect_database_readonly(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise PipelineError("Argus database does not exist: {}".format(path))
    connection = sqlite3.connect("file:{}?mode=ro".format(path), uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
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
        CREATE TABLE IF NOT EXISTS config_snapshots (
          config_hash TEXT PRIMARY KEY,
          captured_at TEXT NOT NULL,
          config_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_config_snapshots (
          run_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_config_json TEXT NOT NULL,
          PRIMARY KEY (run_id, source_id)
        );
        CREATE TABLE IF NOT EXISTS raw_fetches (
          fetch_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          url TEXT NOT NULL,
          fetched_at TEXT NOT NULL,
          http_status INTEGER,
          duration_ms INTEGER,
          payload_hash TEXT,
          payload_ref TEXT,
          error_class TEXT,
          error_message TEXT
        );
        CREATE TABLE IF NOT EXISTS packages (
          package_id TEXT PRIMARY KEY,
          report_id TEXT NOT NULL,
          schema_version TEXT NOT NULL,
          embedding_space_id TEXT,
          embedding_vector_hash TEXT,
          active_eligible INTEGER NOT NULL,
          package_json TEXT NOT NULL,
          created_run_id TEXT NOT NULL,
          created_at TEXT NOT NULL,
          UNIQUE (report_id, schema_version, embedding_space_id, embedding_vector_hash)
        );
        CREATE TABLE IF NOT EXISTS accepted_reports (
          report_id TEXT PRIMARY KEY,
          first_accepted_run_id TEXT NOT NULL,
          first_effective_mode TEXT NOT NULL,
          first_snapshot_id TEXT NOT NULL,
          first_accepted_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS normalized_reports (
          report_id TEXT PRIMARY KEY,
          source_id TEXT NOT NULL,
          first_seen_run_id TEXT NOT NULL,
          latest_seen_run_id TEXT NOT NULL,
          report_id_input_type TEXT NOT NULL,
          report_id_input_hash TEXT NOT NULL,
          feed_guid TEXT,
          raw_url TEXT,
          canonical_url TEXT,
          title_normalized TEXT NOT NULL,
          published_at TEXT,
          fetched_at TEXT NOT NULL,
          report_json TEXT NOT NULL,
          status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS report_seen_runs (
          report_id TEXT NOT NULL,
          run_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          seen_at TEXT NOT NULL,
          PRIMARY KEY (report_id, run_id)
        );
        CREATE TABLE IF NOT EXISTS dedupe_keys (
          key_scope TEXT NOT NULL,
          key_type TEXT NOT NULL,
          key_hash TEXT NOT NULL,
          report_id TEXT NOT NULL,
          normalized_key_value TEXT NOT NULL,
          PRIMARY KEY (key_type, key_scope, key_hash)
        );
        CREATE TABLE IF NOT EXISTS dedupe_decisions (
          run_id TEXT NOT NULL,
          report_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          decision TEXT NOT NULL,
          duplicate_of_report_id TEXT,
          duplicate_key_type TEXT,
          duplicate_key_scope TEXT,
          duplicate_key_hash TEXT,
          duplicate_key_precedence INTEGER,
          duplicate_key_value_preview TEXT,
          reason TEXT NOT NULL,
          detail_json TEXT NOT NULL,
          PRIMARY KEY (run_id, report_id)
        );
        CREATE TABLE IF NOT EXISTS skipped_items (
          run_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          report_id TEXT,
          reason_code TEXT NOT NULL,
          detail_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_health (
          source_id TEXT PRIMARY KEY,
          last_status TEXT NOT NULL,
          last_run_id TEXT NOT NULL,
          last_checked_at TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,
          parse_status TEXT,
          last_successful_fetch_at TEXT,
          latest_item_published_at TEXT,
          consecutive_failures INTEGER NOT NULL DEFAULT 0,
          total_successes INTEGER NOT NULL DEFAULT 0,
          total_failures INTEGER NOT NULL DEFAULT 0,
          last_error_class TEXT,
          last_error_message TEXT,
          last_error TEXT,
          health_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_run_status (
          run_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          status TEXT NOT NULL,
          fetched_at TEXT,
          http_status INTEGER,
          failure_reason TEXT,
          detail_json TEXT NOT NULL,
          PRIMARY KEY (run_id, source_id)
        );
        CREATE TABLE IF NOT EXISTS embeddings (
          report_id TEXT NOT NULL,
          embedding_space_id TEXT NOT NULL,
          provider TEXT NOT NULL,
          model TEXT NOT NULL,
          dimensions INTEGER NOT NULL,
          embedded_text_hash TEXT NOT NULL,
          embedding_vector_hash TEXT NOT NULL,
          vector_json TEXT NOT NULL,
          backend_request_id TEXT,
          status TEXT NOT NULL,
          failure_json TEXT,
          embedded_at TEXT,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (report_id, embedding_space_id, embedded_text_hash)
        );
        CREATE TABLE IF NOT EXISTS publish_attempts (
          attempt_id TEXT PRIMARY KEY,
          package_id TEXT NOT NULL,
          publish_target_key TEXT NOT NULL,
          publish_idempotency_key TEXT NOT NULL,
          effective_publish_snapshot_id TEXT NOT NULL,
          attempt_number INTEGER NOT NULL,
          mode TEXT NOT NULL,
          status TEXT NOT NULL,
          attempted_at TEXT NOT NULL,
          completed_at TEXT,
          subspace_message_id TEXT,
          response_json TEXT,
          error_class TEXT,
          error_message TEXT,
          UNIQUE (publish_idempotency_key, attempt_number)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_success_once ON publish_attempts(publish_idempotency_key) WHERE status = 'succeeded';
        CREATE TABLE IF NOT EXISTS delivery_plans (
          plan_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          publish_target_key TEXT NOT NULL,
          mode TEXT NOT NULL,
          created_at TEXT NOT NULL,
          window_start TEXT NOT NULL,
          window_end TEXT NOT NULL,
          selected_count INTEGER NOT NULL,
          skipped_over_budget_count INTEGER NOT NULL,
          first_due_at TEXT,
          last_due_at TEXT,
          detail_json TEXT NOT NULL,
          UNIQUE (run_id, publish_target_key)
        );
        CREATE TABLE IF NOT EXISTS delivery_entries (
          entry_id TEXT PRIMARY KEY,
          plan_id TEXT NOT NULL,
          run_id TEXT NOT NULL,
          publish_target_key TEXT NOT NULL,
          package_id TEXT NOT NULL,
          publish_idempotency_key TEXT NOT NULL,
          selected_order_index INTEGER NOT NULL,
          due_at TEXT NOT NULL,
          status TEXT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          last_attempt_at TEXT,
          next_retry_at TEXT,
          last_error_class TEXT,
          last_error_message TEXT,
          subspace_message_id TEXT,
          updated_at TEXT NOT NULL,
          UNIQUE (package_id, publish_target_key),
          UNIQUE (run_id, publish_target_key, selected_order_index)
        );
        CREATE INDEX IF NOT EXISTS idx_delivery_due
          ON delivery_entries(status, due_at, next_retry_at, selected_order_index);
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
        CREATE TABLE IF NOT EXISTS control_requests (
          request_id TEXT PRIMARY KEY,
          requested_at TEXT NOT NULL,
          action TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          status TEXT NOT NULL,
          started_at TEXT,
          completed_at TEXT,
          result_json TEXT,
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
          max_live_publishes_per_tick INTEGER,
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
        CREATE TABLE IF NOT EXISTS source_baselines (
          source_id TEXT PRIMARY KEY,
          requested_at TEXT NOT NULL,
          requested_by TEXT,
          prime_run_id TEXT,
          first_successful_run_id TEXT,
          completed_at TEXT,
          status TEXT NOT NULL,
          detail_json TEXT NOT NULL
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
    publish_columns = {row["name"] for row in connection.execute("PRAGMA table_info(publish_attempts)")}
    if "subspace_message_id" not in publish_columns:
        connection.execute("ALTER TABLE publish_attempts ADD COLUMN subspace_message_id TEXT")
    scheduler_columns = {row["name"] for row in connection.execute("PRAGMA table_info(scheduler_state)")}
    if "max_live_publishes_per_tick" not in scheduler_columns:
        connection.execute("ALTER TABLE scheduler_state ADD COLUMN max_live_publishes_per_tick INTEGER")
    connection.commit()


def build_publish_snapshot(config: RuntimeConfig, observed_at: datetime, force_inactive: bool = False, error: Optional[str] = None) -> Dict[str, Any]:
    requested = "inactive" if force_inactive else config.publish.mode
    effective = "inactive"
    blocked_reason = None
    activation_observed_at = None
    if error:
        blocked_reason = error
    elif config.publish.mode == "dry_run":
        effective = "dry_run"
    elif config.publish.mode == "live":
        if not config.publish.subspace_endpoint:
            effective = "blocked"
            blocked_reason = "missing_subspace_endpoint"
        elif not config.publish.subspace_agent_id or not config.publish.subspace_session_token:
            effective = "blocked"
            blocked_reason = "missing_subspace_credentials"
        elif config.scheduler.max_live_publishes_per_tick is None:
            effective = "blocked"
            blocked_reason = "missing_publish_cap"
        elif config.publish.require_embeddings and not config.publish.allow_non_embedded_fallback and not embedding_config_valid(config.embedding):
            effective = "blocked"
            blocked_reason = "missing_embedding_config"
        elif config.publish.require_embeddings and not config.publish.allow_non_embedded_fallback and not production_embedding_config_allowed(config.embedding):
            effective = "blocked"
            blocked_reason = "non_production_embedding_backend"
        else:
            effective = "live"
            activation_observed_at = iso_z(observed_at)
    snapshot = {
        "snapshot_id": "sha256:{}:{}".format(config.config_hash, iso_z(observed_at)),
        "config_hash": config.config_hash,
        "observed_at": iso_z(observed_at),
        "requested_mode": requested,
        "effective_mode": effective,
        "activation_observed_at": activation_observed_at,
        "max_live_publishes_per_tick": config.scheduler.max_live_publishes_per_tick,
        "require_embeddings": bool(config.publish.require_embeddings),
        "allow_non_embedded_fallback": bool(config.publish.allow_non_embedded_fallback),
        "embedding_space_id": config.embedding.space_id,
        "embedding_backend": config.embedding.backend,
        "delivery_mode": config.delivery.mode,
        "publish_target_key": (
            "sha256:" + hashlib.sha256(("publish-target:v0\n" + str(config.publish.subspace_endpoint)).encode("utf-8")).hexdigest()
            if config.publish.subspace_endpoint
            else None
        ),
        "blocked_reason": blocked_reason,
    }
    return snapshot


def embedding_config_valid(config: EmbeddingConfig) -> bool:
    if config.backend == "disabled":
        return False
    if not config.backend or not config.provider or not config.model or not config.dimensions or not config.space_id:
        return False
    if config.backend == "openai":
        return (
            config.provider == OPENAI_EMBEDDING_PROVIDER
            and config.model == OPENAI_EMBEDDING_MODEL
            and int(config.dimensions) == OPENAI_EMBEDDING_DIMENSIONS
            and config.space_id == OPENAI_EMBEDDING_SPACE_ID
        )
    if not config.command:
        return False
    return True


def production_embedding_config_allowed(config: EmbeddingConfig) -> bool:
    return (
        config.backend == "openai"
        and config.provider == OPENAI_EMBEDDING_PROVIDER
        and config.model == OPENAI_EMBEDDING_MODEL
        and int(config.dimensions or 0) == OPENAI_EMBEDDING_DIMENSIONS
        and config.space_id == OPENAI_EMBEDDING_SPACE_ID
    )


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
            0,
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


def supplied_embedding_for(candidate: Dict[str, Any]) -> Dict[str, Any]:
    supplied = candidate.get("supplied_embeddings") or []
    if isinstance(supplied, list):
        return supplied[0] if supplied else {}
    if isinstance(supplied, dict):
        return supplied
    return {}


def package_id_for(candidate: Dict[str, Any]) -> str:
    supplied_embeddings = supplied_embedding_for(candidate)
    vector_hash = supplied_embeddings.get("vector_hash") or hashlib.sha256(
        json.dumps(supplied_embeddings, sort_keys=True).encode("utf-8")
    ).hexdigest()
    basis = "package:swarm.channel.news.report.v0\n{}\n{}\n{}\n{}".format(
        candidate["report_id"],
        candidate.get("schema_version", 1),
        supplied_embeddings.get("space_id", ""),
        vector_hash,
    )
    return "sha256:" + hashlib.sha256(basis.encode("utf-8")).hexdigest()


def embedding_matches_snapshot(snapshot: Dict[str, Any], supplied_embeddings: Dict[str, Any]) -> bool:
    return bool(supplied_embeddings) and supplied_embeddings.get("space_id") == snapshot.get("embedding_space_id")


def is_scheduler_reload_error(exc: Exception) -> bool:
    text = str(exc)
    return text.startswith("Invalid schedule.") or text.startswith("schedule.")


def selected_dedupe_key_for_report(report: Dict[str, Any]) -> Tuple[str, str, str, str]:
    if report.get("feed_entry_id"):
        key_type = "feed_entry_id"
        normalized_key_value = normalize_feed_entry_id(report["feed_entry_id"])
    elif report.get("canonical_url"):
        key_type = "canonical_url"
        normalized_key_value = report["canonical_url"]
    elif not normalize_title(report["title"]):
        key_type = "missing_identity_key"
        normalized_key_value = ""
    else:
        key_type = "normalized_title_date"
        normalized_key_value = "{}\n{}".format(normalize_title(report["title"]), date_bucket(report.get("published_at") or report.get("fetched_at")))
    key_scope = "source:{}".format(report["source_id"])
    key_hash = "sha256:" + hashlib.sha256("dedupe:v0\n{}\n{}\n{}".format(key_scope, key_type, normalized_key_value).encode("utf-8")).hexdigest()
    return key_type, key_scope, normalized_key_value, key_hash


def canonical_embedding_record(candidate: Dict[str, Any]) -> Dict[str, Any]:
    supplied = supplied_embedding_for(candidate)
    return {
        "space_id": supplied.get("space_id"),
        "provider": supplied.get("provider") or supplied.get("backend"),
        "model": supplied.get("model"),
        "dimensions": supplied.get("dimensions"),
        "input_hash": supplied.get("input_hash") or ("sha256:" + hashlib.sha256((candidate.get("metadata", {}).get("embedding_text") or "").encode("utf-8")).hexdigest()),
        "vector_hash": supplied.get("vector_hash"),
        "vector": supplied.get("vector"),
    }


def embedded_text_hash(candidate: Dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256((candidate.get("metadata", {}).get("embedding_text") or "").encode("utf-8")).hexdigest()


def openai_api_key() -> Optional[str]:
    value = os.environ.get("OPENAI_API_KEY")
    return value.strip() if value and value.strip() else None


def sanitize_openai_error_message(message: str) -> str:
    return re.sub(r"sk-[A-Za-z0-9_.*-]+", "[redacted-openai-key]", message)


def request_openai_embedding(text: str, model: str, dimensions: int, api_key: str) -> Dict[str, Any]:
    response = requests.post(
        "https://api.openai.com/v1/embeddings",
        headers={
            "Authorization": "Bearer {}".format(api_key),
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "input": text,
            "dimensions": dimensions,
            "encoding_format": "float",
        },
        timeout=30,
    )
    try:
        payload = response.json()
    except ValueError:
        payload = {"error": {"message": response.text}}
    if response.status_code < 200 or response.status_code >= 300:
        error = payload.get("error") if isinstance(payload, dict) else None
        message = error.get("message") if isinstance(error, dict) else response.text
        raise PipelineError("OpenAI embedding request failed: {}".format(sanitize_openai_error_message(str(message))))
    if isinstance(payload, dict):
        payload["backend_request_id"] = response.headers.get("x-request-id") or response.headers.get("openai-request-id")
    return payload


def package_payload_for(candidate: Dict[str, Any], package_id: str) -> Dict[str, Any]:
    selected_key_type = candidate.get("dedupe_identity", {}).get("primary")
    selected_key_value = candidate.get("dedupe_identity", {}).get("value")
    selected_key_scope = "source:{}".format(candidate["source_id"])
    selected_key_hash = None
    if selected_key_type and selected_key_value is not None:
        selected_key_hash = "sha256:" + hashlib.sha256("dedupe:v0\n{}\n{}\n{}".format(selected_key_scope, selected_key_type, selected_key_value).encode("utf-8")).hexdigest()
    return {
        "schema": "swarm.channel.news.report.v0",
        "package_id": package_id,
        "report_id": candidate["report_id"],
        "body": {
            "title": candidate["title"],
            "summary": candidate["clean_summary"],
            "url": candidate["canonical_url"],
            "source_name": candidate["source_name"],
            "published_at": candidate["published_at"],
        },
        "provenance": {
            "source_id": candidate["source_id"],
            "source_class": candidate["source_class"],
            "feed_guid": candidate.get("feed_entry_id") if selected_key_type == "feed_entry_id" else None,
            "raw_url": candidate.get("raw_url"),
            "canonical_url": candidate["canonical_url"],
            "fetched_at": candidate["fetched_at"],
        },
        "dedupe": {
            "selected_key_type": selected_key_type,
            "selected_key_scope": selected_key_scope,
            "selected_key_hash": selected_key_hash,
        },
        "supplied_embeddings": [canonical_embedding_record(candidate)] if supplied_embedding_for(candidate) else [],
    }


class PublishTransportError(PipelineError):
    def __init__(self, message: str, response: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.response = response


def canonical_embedding_for_subspace(supplied_embedding: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "space_id": supplied_embedding.get("space_id"),
        "vector": supplied_embedding.get("vector") or [],
    }


def subspace_message_id_from_response(response: Dict[str, Any]) -> Optional[str]:
    for result in response.get("results") or []:
        if result.get("sent") or result.get("duplicate") or result.get("idempotent_duplicate"):
            return result.get("subspace_message_id")
    return None


def subspace_response_is_success(response: Dict[str, Any]) -> bool:
    if not response.get("ok"):
        return False
    results = response.get("results") or []
    return any(result.get("duplicate") or result.get("idempotent_duplicate") for result in results)


def publish_exception_is_permanent(exc: Exception) -> bool:
    if not isinstance(exc, PublishTransportError):
        return False
    response = exc.response if isinstance(exc.response, dict) else {}
    error = response.get("error") if isinstance(response.get("error"), dict) else {}
    reply = response.get("reply") if isinstance(response.get("reply"), dict) else {}
    response_body = reply.get("response") if isinstance(reply.get("response"), dict) else {}
    code = str(error.get("code") or response_body.get("code") or response_body.get("reason") or "")
    return code in {
        "invalid_package_contract",
        "invalid_request",
        "invalid_subspace_message",
        "message_contract_violation",
        "missing_required_embedding",
    }


def subspace_websocket_url(endpoint: str, websocket_path: str) -> str:
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise PublishTransportError("invalid Subspace endpoint: {}".format(endpoint))
    scheme = "wss" if parsed.scheme == "https" else "ws"
    endpoint_path = parsed.path.rstrip("/")
    publish_path = "/" + websocket_path.lstrip("/")
    path = endpoint_path + publish_path if endpoint_path else publish_path
    return urlunparse((scheme, parsed.netloc, path, "", "vsn=2.0.0", ""))


def _read_phoenix_reply(connection: Any, expected_ref: str, operation: str, max_frames: int = 100) -> Dict[str, Any]:
    for _ in range(max_frames):
        raw = connection.recv()
        try:
            frame = json.loads(raw)
        except ValueError as exc:
            raise PublishTransportError("invalid Subspace websocket frame") from exc
        if not isinstance(frame, list) or len(frame) != 5:
            continue
        _join_ref, ref, _topic, event, payload = frame
        if ref != expected_ref or event != "phx_reply":
            continue
        if not isinstance(payload, dict):
            raise PublishTransportError("invalid Subspace {} reply".format(operation))
        if payload.get("status") != "ok":
            response = {"ok": False, "reply": payload}
            message = payload.get("response", {}).get("reason") if isinstance(payload.get("response"), dict) else None
            raise PublishTransportError(message or "Subspace {} failed".format(operation), response)
        return payload
    raise PublishTransportError("Subspace {} reply not received".format(operation))


def post_message_to_subspace(
    endpoint: str,
    websocket_path: str,
    agent_id: str,
    session_token: str,
    text: str,
    embeddings: List[Dict[str, Any]],
    idempotency_key: str,
    *,
    timeout: int = 10,
    create_connection: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    if create_connection is None:
        try:
            import websocket  # type: ignore
        except ImportError as exc:
            raise PublishTransportError("websocket-client dependency is unavailable") from exc
        create_connection = websocket.create_connection
    url = subspace_websocket_url(endpoint, websocket_path)
    try:
        connection = create_connection(url, timeout=timeout, suppress_origin=True)
    except Exception as exc:
        raise PublishTransportError("Subspace websocket connection failed: {}".format(exc)) from exc
    try:
        join_ref = "1"
        post_ref = "2"
        connection.send(
            json.dumps(
                [join_ref, join_ref, "firehose", "phx_join", {"agent_id": agent_id, "session_token": session_token}],
                separators=(",", ":"),
            )
        )
        _read_phoenix_reply(connection, join_ref, "join")
        connection.send(
            json.dumps(
                [
                    join_ref,
                    post_ref,
                    "firehose",
                    "post_message",
                    {"text": text, "embeddings": embeddings, "idempotency_key": idempotency_key},
                ],
                separators=(",", ":"),
            )
        )
        reply = _read_phoenix_reply(connection, post_ref, "post_message")
    except PublishTransportError:
        raise
    except Exception as exc:
        raise PublishTransportError("Subspace websocket publish failed: {}".format(exc)) from exc
    finally:
        try:
            connection.close()
        except Exception:
            pass
    response = reply.get("response") if isinstance(reply.get("response"), dict) else {}
    message_id = response.get("id")
    if not message_id:
        raise PublishTransportError("Subspace response missing subspace_message_id", {"ok": True, "reply": reply})
    return {
        "ok": True,
        "transport": "subspace_websocket",
        "results": [
            {
                "server": endpoint,
                "sent": True,
                "subspace_message_id": message_id,
                "idempotency_key": idempotency_key,
            }
        ],
    }


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
        self._persisted_activation_observed_at: Optional[str] = None
        if self.register_service:
            self.start()
        else:
            self._ensure_control_state()

    def _apply_persisted_publish_state(self) -> None:
        row = self.connection.execute(
            "SELECT snapshot_json FROM runtime_config_snapshots ORDER BY observed_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return
        snapshot = json.loads(row["snapshot_json"])
        self._persisted_activation_observed_at = snapshot.get("activation_observed_at")
        requested_mode = snapshot.get("requested_mode")
        if requested_mode in {"inactive", "dry_run", "live"}:
            self.config = dataclasses.replace(
                self.config,
                publish=dataclasses.replace(self.config.publish, mode=requested_mode),
            )

    def _build_publish_snapshot(self, now: datetime, force_inactive: bool = False, error: Optional[str] = None) -> Dict[str, Any]:
        snapshot = build_publish_snapshot(self.config, now, force_inactive=force_inactive, error=error)
        if not force_inactive and snapshot.get("requested_mode") == "live":
            try:
                previous = latest_snapshot(self.connection)
            except PipelineError:
                previous = None
            activation_observed_at = (previous or {}).get("activation_observed_at") if (previous or {}).get("requested_mode") == "live" else None
            if activation_observed_at:
                snapshot["activation_observed_at"] = activation_observed_at
        return snapshot

    def _ensure_control_state(self) -> None:
        now = self.clock.now()
        self._store_config_snapshot(now)
        self._apply_persisted_publish_state()
        store_runtime_snapshot(self.connection, self._build_publish_snapshot(now), "control_start", "ok")
        if self.connection.execute("SELECT 1 FROM scheduler_state WHERE id = 1").fetchone() is None:
            self._record_scheduler_config(self.config.scheduler, now, recompute_next=True)

    def start(self) -> None:
        now = self.clock.now()
        self._store_config_snapshot(now)
        self._apply_persisted_publish_state()
        snapshot = self._build_publish_snapshot(now)
        store_runtime_snapshot(self.connection, snapshot, "service_start" if self.register_service else "control_start", "ok")
        if self.register_service:
            self.connection.execute(
                "INSERT OR REPLACE INTO service_state VALUES (1, ?, ?, ?, ?)",
                (os.getpid(), str(self.config_path), iso_z(now), iso_z(now)),
            )
            self._write_service_registration()
            stale = self.connection.execute("SELECT running_run_id FROM scheduler_state WHERE id = 1").fetchone()
            if stale and stale["running_run_id"]:
                self._mark_stale_run_failed(stale["running_run_id"], now)
                self.connection.execute(
                    "UPDATE scheduler_state SET running_run_id = NULL, updated_at = ? WHERE id = 1",
                    (iso_z(now),),
                )
                self.connection.commit()
                self._record_scheduler_event("cycle_recovered_after_restart", stale["running_run_id"], "failed", {}, now)
            self._recover_stale_delivery_attempts(now)
        self._record_scheduler_config(self.config.scheduler, now, recompute_next=True)
        self._record_scheduler_event("scheduler_ready", None, "ok", {"mode": self.config.scheduler.mode}, now)

    def _store_config_snapshot(self, now: datetime) -> None:
        config_path = Path(self.config.source_config_path)
        payload = yaml.safe_load(config_path.read_text())
        self.connection.execute(
            "INSERT OR REPLACE INTO config_snapshots VALUES (?, ?, ?)",
            (self.config.config_hash, iso_z(now), json.dumps(payload, sort_keys=True, default=str)),
        )
        self.connection.commit()

    def _mark_stale_run_failed(self, run_id: str, now: datetime) -> None:
        row = self.connection.execute("SELECT summary_json FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            return
        summary = json.loads(row["summary_json"])
        summary["post_pipeline_error"] = {"class": "RuntimeError", "message": "cycle recovered after restart"}
        self.connection.execute(
            "UPDATE runs SET status = ?, completed_at = ?, summary_json = ? WHERE run_id = ?",
            ("failed", iso_z(now), json.dumps(summary, sort_keys=True), run_id),
        )
        self.connection.execute(
            """
            UPDATE publish_attempts
            SET status = ?, completed_at = ?, error_class = ?, error_message = ?
            WHERE status = 'pending'
            """,
            ("unknown", iso_z(now), "RuntimeError", "cycle recovered after restart before publish result was recorded"),
        )
        self.connection.execute(
            """
            UPDATE delivery_entries
            SET status = 'retry_pending',
                next_retry_at = ?,
                last_error_class = ?,
                last_error_message = ?,
                updated_at = ?
            WHERE status = 'attempting'
            """,
            (iso_z(now), "RuntimeError", "cycle recovered after restart before delivery result was recorded", iso_z(now)),
        )

    def _recover_stale_delivery_attempts(self, now: datetime) -> None:
        self.connection.execute(
            """
            UPDATE publish_attempts
            SET status = ?, completed_at = ?, error_class = ?, error_message = ?
            WHERE status = 'pending'
            """,
            ("unknown", iso_z(now), "RuntimeError", "service restarted before publish result was recorded"),
        )
        self.connection.execute(
            """
            UPDATE delivery_entries
            SET status = 'retry_pending',
                next_retry_at = ?,
                last_error_class = ?,
                last_error_message = ?,
                updated_at = ?
            WHERE status = 'attempting'
            """,
            (iso_z(now), "RuntimeError", "service restarted before delivery result was recorded", iso_z(now)),
        )
        self.connection.commit()

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
            if is_scheduler_reload_error(exc) and self._reload_publish_inputs_unchanged():
                self._record_scheduler_event("scheduler_reload_failed", None, "failed", {"error": str(exc)}, now)
                return
            snapshot = self._build_publish_snapshot(now, force_inactive=True, error=str(exc))
            store_runtime_snapshot(self.connection, snapshot, "reload_failed", "failed", str(exc))
            self._record_scheduler_event("scheduler_reload_failed", None, "failed", {"error": str(exc)}, now)
            return
        self.config = new_config
        self._store_config_snapshot(now)
        snapshot = self._build_publish_snapshot(now)
        store_runtime_snapshot(self.connection, snapshot, "reload", "ok")
        self._record_scheduler_config(new_config.scheduler, now, recompute_next=True)
        self._record_scheduler_event("scheduler_reload", None, "ok", {"mode": new_config.scheduler.mode}, now)

    def _reload_publish_inputs_unchanged(self) -> bool:
        try:
            payload = yaml.safe_load(self.config_path.read_text())
        except Exception:
            return False
        if not isinstance(payload, dict):
            return False
        publish_payload = payload.get("publish") or {}
        embedding_payload = payload.get("embedding") or {}
        try:
            publish = publish_config_from_payload(publish_payload)
        except PipelineError:
            return False
        embedding = EmbeddingConfig(
            backend=(str(embedding_payload["backend"]) if embedding_payload.get("backend") else None),
            command=(str(embedding_payload["command"]) if embedding_payload.get("command") else None),
            provider=(str(embedding_payload["provider"]) if embedding_payload.get("provider") else None),
            model=(str(embedding_payload["model"]) if embedding_payload.get("model") else None),
            dimensions=(int(embedding_payload["dimensions"]) if embedding_payload.get("dimensions") is not None else None),
            space_id=(str(embedding_payload["space_id"]) if embedding_payload.get("space_id") else None),
        )
        return publish == self.config.publish and embedding == self.config.embedding

    def set_publish_state(self, state: str) -> Dict[str, Any]:
        if state not in {"inactive", "dry_run", "live"}:
            raise PipelineError("Invalid publish mode: {}".format(state))
        self.config = dataclasses.replace(self.config, publish=dataclasses.replace(self.config.publish, mode=state))
        snapshot = self._build_publish_snapshot(self.clock.now())
        store_runtime_snapshot(self.connection, snapshot, "set_publish_state", "ok")
        return snapshot

    def prime(self, requested_by: str = "cli", source_id: Optional[str] = None) -> Tuple[int, Dict[str, Any]]:
        now = self.clock.now()
        state = self._scheduler_state()
        source_ids = self._validated_prime_source_ids(source_id)
        if self._cycle_running or state["running_run_id"]:
            return self._run_cycle("prime", now, prime=True, source_ids=source_ids)
        event = event_id("prime", now, requested_by)
        self.connection.execute("INSERT OR REPLACE INTO prime_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (event, iso_z(now), None, requested_by, None, None, "running", None, None))
        self.connection.commit()
        try:
            exit_code, summary = self._run_cycle("prime", now, prime=True, source_ids=source_ids)
            completed_at = iso_z(self.clock.now())
            self.connection.execute(
                "UPDATE prime_events SET completed_at = ?, run_id = ?, prime_watermark_run_id = ?, status = ? WHERE event_id = ?",
                (completed_at, summary["run_id"], summary["run_id"], "succeeded" if exit_code == 0 else "failed", event),
            )
            self.connection.commit()
            return exit_code, summary
        except Exception as exc:
            row = self.connection.execute("SELECT run_id FROM runs WHERE run_kind = 'prime' ORDER BY started_at DESC, rowid DESC LIMIT 1").fetchone()
            run_id = row["run_id"] if row else None
            self.connection.execute(
                "UPDATE prime_events SET completed_at = ?, run_id = ?, prime_watermark_run_id = NULL, status = ?, error_class = ?, error_message = ? WHERE event_id = ?",
                (iso_z(self.clock.now()), run_id, "failed", exc.__class__.__name__, str(exc), event),
            )
            self.connection.commit()
            raise

    def manual_cycle(self, max_live_publishes: Optional[int] = None) -> Tuple[int, Dict[str, Any]]:
        effective_max_live_publishes = self.config.scheduler.max_live_publishes_per_tick if max_live_publishes is None else max_live_publishes
        return self._run_cycle("manual", self.clock.now(), prime=False, max_live_publishes=effective_max_live_publishes)

    def _validated_prime_source_ids(self, source_id: Optional[str]) -> Optional[Set[str]]:
        if source_id is None:
            return None
        matching = [source for source in self.config.sources if source.id == source_id]
        if not matching:
            raise PipelineError("Unknown source for prime: {}".format(source_id))
        if not matching[0].enabled:
            raise PipelineError("Cannot prime disabled source: {}".format(source_id))
        return {source_id}

    def tick(self) -> Optional[Tuple[int, Dict[str, Any]]]:
        if self.reload_requested:
            self.reload_requested = False
            self.reload()
        control_result = self._process_control_requests()
        if control_result is not None:
            return control_result
        now = self.clock.now()
        delivery_result = self._drain_due_delivery(now)
        if delivery_result:
            return 0, {"run_kind": "delivery", "exit_status": "ok", "delivery": delivery_result}
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
        return self._run_cycle(
            "scheduled",
            now,
            prime=False,
            max_live_publishes=self.config.scheduler.max_live_publishes_per_tick,
        )

    def serve_forever(self) -> int:
        self.running = True
        signal.signal(signal.SIGHUP, self.request_reload)
        while self.running:
            result = self.tick()
            if result is None:
                state = self._scheduler_state()
                next_due_at = parse_now(state["next_due_at"]) if state["next_due_at"] else self.clock.now() + timedelta(seconds=60)
                delivery_due_at = self._next_delivery_due_at()
                if delivery_due_at is not None and delivery_due_at < next_due_at:
                    next_due_at = delivery_due_at
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
            "delivery": self._delivery_status(self.clock.now()),
            "last_run": self._last_run(),
        }

    def _process_control_requests(self) -> Optional[Tuple[int, Dict[str, Any]]]:
        row = self.connection.execute(
            "SELECT * FROM control_requests WHERE status = 'pending' ORDER BY requested_at, rowid LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        now = self.clock.now()
        self.connection.execute(
            "UPDATE control_requests SET status = ?, started_at = ? WHERE request_id = ?",
            ("running", iso_z(now), row["request_id"]),
        )
        self.connection.commit()
        payload = json.loads(row["payload_json"])
        try:
            if row["action"] == "prime":
                result = self.prime(requested_by=str(payload.get("requested_by", "cli")), source_id=payload.get("source_id"))
            elif row["action"] == "run-cycle":
                max_live_publishes = payload.get("max_live_publishes")
                result = self.manual_cycle(max_live_publishes=int(max_live_publishes) if max_live_publishes is not None else None)
            elif row["action"] == "set-publish-state":
                snapshot = self.set_publish_state(str(payload["state"]))
                result = (0, snapshot)
            else:
                raise PipelineError("Unknown control request action: {}".format(row["action"]))
            self.connection.execute(
                "UPDATE control_requests SET status = ?, completed_at = ?, result_json = ? WHERE request_id = ?",
                ("succeeded", iso_z(self.clock.now()), json.dumps(result[1], sort_keys=True, default=str), row["request_id"]),
            )
            self.connection.commit()
            return result
        except Exception as exc:
            self.connection.execute(
                "UPDATE control_requests SET status = ?, completed_at = ?, error_class = ?, error_message = ? WHERE request_id = ?",
                ("failed", iso_z(self.clock.now()), exc.__class__.__name__, str(exc), row["request_id"]),
            )
            self.connection.commit()
            raise

    def _run_cycle(
        self,
        run_kind: str,
        now: datetime,
        prime: bool = False,
        max_live_publishes: Optional[int] = None,
        source_ids: Optional[Set[str]] = None,
    ) -> Tuple[int, Dict[str, Any]]:
        state = self._scheduler_state()
        if self._cycle_running or state["running_run_id"]:
            self._record_scheduler_event("cycle_skipped_already_running", state["running_run_id"], "skipped", {"requested_run_kind": run_kind}, now)
            return 0, {
                "run_id": state["running_run_id"],
                "run_kind": run_kind,
                "exit_status": "skipped",
                "skip_reason": "cycle_already_running",
            }
        cycle_sources = self._sources_due_for_cycle(run_kind, now)
        if source_ids is not None:
            cycle_sources = [source for source in cycle_sources if source.id in source_ids]
        if not cycle_sources and run_kind == "scheduled":
            self._record_scheduler_event("cycle_skipped_no_sources_due", None, "skipped", {"run_kind": run_kind}, now)
            return 0, {"run_kind": run_kind, "exit_status": "skipped", "skip_reason": "no_sources_due"}
        self._cycle_running = True
        run_id = "{}-{}-{}".format(
            now.strftime("%Y%m%dT%H%M%SZ"),
            run_kind,
            hashlib.sha256("{}:{}:{}".format(run_kind, iso_z(now), time.monotonic_ns()).encode("utf-8")).hexdigest()[:8],
        )
        cycle_snapshot = latest_snapshot(self.connection)
        output_dir = self.config.output_dir / "runs" / run_id
        self.connection.execute(
            "UPDATE scheduler_state SET running_run_id = ?, last_started_run_id = ?, updated_at = ? WHERE id = 1",
            (run_id, run_id, iso_z(now)),
        )
        self._store_source_config_snapshots(run_id)
        self._store_run_started(run_id, run_kind, now, output_dir, cycle_snapshot)
        try:
            if prime:
                self._record_source_baselines_requested(cycle_sources, now, requested_by="prime", prime_run_id=run_id)
            cycle_embedding = self.config.embedding
            exit_code, summary = run_pipeline_for_sources(
                cycle_sources,
                self.config.source_config_path,
                output_dir,
                now,
                fixture_dir=self.config.fixture_dir,
                prime=prime,
                state_path=None,
                state_read=False,
                state_write=False,
                dedupe_in_pipeline=False,
                package_candidates_artifact=".pre-package-candidates.jsonl",
                publish_candidates_artifact=".pre-publish-candidates.jsonl",
            )
            summary["run_id"] = run_id
            summary["run_kind"] = run_kind
            if source_ids is not None:
                summary["source_ids"] = sorted(source_ids)
            if self.reload_requested:
                self.reload_requested = False
                self.reload()
            publish_snapshot = latest_snapshot(self.connection)
            summary["effective_publish_snapshot"] = cycle_snapshot
            source_health_rows = json.loads((output_dir / "source-health.json").read_text()) if (output_dir / "source-health.json").exists() else []
            baseline_source_ids = set() if prime else self._source_ids_requiring_baseline(cycle_sources, cycle_snapshot)
            self._store_source_health(run_id, now, output_dir)
            if prime:
                self._store_normalized_storage(run_id, now, output_dir, None)
                self._mark_successful_source_baselines(run_id, now, source_health_rows)
                self._write_decision_artifacts(run_id, output_dir)
                summary["state"] = {
                    "backend": "sqlite",
                    "path": str(self.config.database_path),
                    "read_enabled": True,
                    "write_enabled": True,
                    "write_performed": True,
                }
                summary["baseline_source_ids"] = sorted(source.id for source in cycle_sources if source.enabled)
                self._store_run(run_id, run_kind, now, exit_code, output_dir, cycle_snapshot, summary, commit=False)
                (output_dir / "run-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
                self._write_prime_artifacts(output_dir)
                self.connection.commit()
            else:
                package_candidates_path = output_dir / ".pre-package-candidates.jsonl"
                package_candidate_rows = [json.loads(line) for line in package_candidates_path.read_text().splitlines() if line.strip()]
                candidate_report_ids = {row["report_id"] for row in package_candidate_rows}
                accepted_report_ids = self._store_normalized_storage(run_id, now, output_dir, candidate_report_ids)
                accepted_candidate_rows = []
                seen_candidate_report_ids = set()
                for row in package_candidate_rows:
                    if row["source_id"] in baseline_source_ids:
                        continue
                    if row["report_id"] in accepted_report_ids and row["report_id"] not in seen_candidate_report_ids:
                        accepted_candidate_rows.append(row)
                        seen_candidate_report_ids.add(row["report_id"])
                package_candidates_path.write_text("".join(json.dumps(row) + "\n" for row in accepted_candidate_rows))
                summary["baseline_source_ids"] = sorted(baseline_source_ids)
                self._mark_successful_source_baselines(run_id, now, source_health_rows, source_ids=baseline_source_ids)
                package_payloads = self._store_packages_and_publish(
                    run_id,
                    now,
                    output_dir,
                    package_candidates_path,
                    cycle_snapshot,
                    publish_snapshot,
                    cycle_embedding,
                    max_live_publishes=max_live_publishes,
                )
                self._rewrite_source_health_final_counts(run_id, output_dir, package_payloads)
                self._store_source_health(run_id, now, output_dir, update_totals=False, commit=False)
                self._write_decision_artifacts(run_id, output_dir)
                (output_dir / "package-candidates.jsonl").write_text("".join(json.dumps(row) + "\n" for row in package_payloads))
                (output_dir / "publish-candidates.jsonl").write_text("".join(json.dumps(row) + "\n" for row in package_payloads))
                summary["counts"]["package_candidates"] = len(package_payloads)
                summary["counts"]["publish_candidates"] = len(package_payloads)
                summary["counts"]["embedding_failures"] = len(json.loads((output_dir / "embedding-failures.json").read_text()))
                summary["artifact_paths"]["embedding_failures"] = "embedding-failures.json"
                self._store_run(run_id, run_kind, now, exit_code, output_dir, cycle_snapshot, summary, commit=False)
                (output_dir / "run-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
                self.connection.commit()
            completed_at = self.clock.now()
            self._record_scheduler_completion(run_id, run_kind, completed_at)
            self._record_scheduler_event("cycle_completed", run_id, "ok" if exit_code == 0 else "failed", {"run_kind": run_kind}, completed_at)
            return exit_code, summary
        except Exception as exc:
            self.connection.rollback()
            self._mark_run_failed(run_id, run_kind, now, output_dir, cycle_snapshot, exc)
            self._record_scheduler_event("cycle_completed", run_id, "failed", {"run_kind": run_kind, "error": str(exc)}, self.clock.now())
            raise
        finally:
            self._cycle_running = False
            self.connection.execute(
                "UPDATE scheduler_state SET running_run_id = NULL, updated_at = ? WHERE running_run_id = ?",
                (iso_z(self.clock.now()), run_id),
            )
            self.connection.commit()

    def _sources_due_for_cycle(self, run_kind: str, now: datetime) -> List[SourceConfig]:
        if run_kind != "scheduled":
            return self.config.sources
        due_sources = []
        for source in self.config.sources:
            if not source.enabled or source.cadence_interval_seconds is None:
                due_sources.append(source)
                continue
            row = self.connection.execute(
                "SELECT fetched_at FROM source_run_status WHERE source_id = ? AND status IN ('ok', 'empty', 'not_modified') ORDER BY fetched_at DESC LIMIT 1",
                (source.id,),
            ).fetchone()
            if row is None or not row["fetched_at"] or now >= parse_now(row["fetched_at"]) + timedelta(seconds=source.cadence_interval_seconds):
                due_sources.append(source)
        return due_sources

    def _source_ids_requiring_baseline(self, cycle_sources: List[SourceConfig], cycle_snapshot: Dict[str, Any]) -> Set[str]:
        baseline_source_ids = self._pending_source_baseline_ids(cycle_sources)
        if cycle_snapshot.get("effective_mode") != "live":
            return baseline_source_ids
        if self.connection.execute("SELECT 1 FROM source_run_status LIMIT 1").fetchone() is None:
            return baseline_source_ids
        for source in cycle_sources:
            if not source.enabled:
                continue
            row = self.connection.execute(
                """
                SELECT 1
                FROM source_run_status
                WHERE source_id = ?
                  AND status IN ('ok', 'empty')
                LIMIT 1
                """,
                (source.id,),
            ).fetchone()
            if row is None:
                baseline_source_ids.add(source.id)
        return baseline_source_ids

    def _pending_source_baseline_ids(self, cycle_sources: List[SourceConfig]) -> Set[str]:
        source_ids = {source.id for source in cycle_sources if source.enabled}
        if not source_ids:
            return set()
        rows = self.connection.execute("SELECT source_id FROM source_baselines WHERE status = 'pending'").fetchall()
        return {row["source_id"] for row in rows if row["source_id"] in source_ids}

    def _record_source_baselines_requested(self, sources: List[SourceConfig], now: datetime, requested_by: str, prime_run_id: str) -> None:
        for source in sources:
            if not source.enabled:
                continue
            self.connection.execute(
                """
                INSERT INTO source_baselines
                (source_id, requested_at, requested_by, prime_run_id, first_successful_run_id, completed_at, status, detail_json)
                VALUES (?, ?, ?, ?, NULL, NULL, 'pending', ?)
                ON CONFLICT(source_id) DO UPDATE SET
                  requested_at=excluded.requested_at,
                  requested_by=excluded.requested_by,
                  prime_run_id=excluded.prime_run_id,
                  first_successful_run_id=NULL,
                  completed_at=NULL,
                  status='pending',
                  detail_json=excluded.detail_json
                """,
                (
                    source.id,
                    iso_z(now),
                    requested_by,
                    prime_run_id,
                    json.dumps({"source_id": source.id, "prime_run_id": prime_run_id}, sort_keys=True),
                ),
            )

    def _mark_successful_source_baselines(
        self,
        run_id: str,
        now: datetime,
        source_health: List[Dict[str, Any]],
        source_ids: Optional[Set[str]] = None,
    ) -> None:
        for row in source_health:
            source_id = row.get("source_id")
            if not source_id or (source_ids is not None and source_id not in source_ids):
                continue
            if row.get("status") not in {"ok", "empty"}:
                continue
            if self.connection.execute(
                "SELECT 1 FROM source_baselines WHERE source_id = ? AND status = 'pending'",
                (source_id,),
            ).fetchone() is None:
                continue
            self.connection.execute(
                """
                UPDATE source_baselines
                SET first_successful_run_id = ?, completed_at = ?, status = 'satisfied',
                    detail_json = ?
                WHERE source_id = ? AND status = 'pending'
                """,
                (
                    run_id,
                    iso_z(now),
                    json.dumps({"source_id": source_id, "satisfied_run_id": run_id, "status": row.get("status")}, sort_keys=True),
                    source_id,
                ),
            )

    def _store_source_config_snapshots(self, run_id: str) -> None:
        for source in self.config.sources:
            self.connection.execute(
                "INSERT OR REPLACE INTO source_config_snapshots VALUES (?, ?, ?)",
                (run_id, source.id, json.dumps(dataclasses.asdict(source), sort_keys=True, default=str)),
            )

    def _store_run_started(self, run_id: str, run_kind: str, now: datetime, output_dir: Path, snapshot: Dict[str, Any]) -> None:
        summary = {"run_id": run_id, "run_kind": run_kind, "exit_status": "running"}
        self.connection.execute(
            "INSERT OR REPLACE INTO runs VALUES (?, ?, ?, NULL, ?, ?, ?, ?)",
            (run_id, run_kind, iso_z(now), "running", str(output_dir), snapshot["snapshot_id"], json.dumps(summary, sort_keys=True)),
        )
        self.connection.commit()

    def _write_prime_artifacts(self, output_dir: Path) -> None:
        (output_dir / "prime-baseline.json").write_text((output_dir / "run-summary.json").read_text())
        (output_dir / "prime-source-health.json").write_text((output_dir / "source-health.json").read_text())
        (output_dir / "prime-normalized-items.jsonl").write_text((output_dir / "normalized-items.jsonl").read_text())
        (output_dir / "prime-baseline.md").write_text("# Argus Prime Baseline\n\nNo live publish work was created.\n")

    def _write_decision_artifacts(self, run_id: str, output_dir: Path) -> None:
        dedupe_rows = [
            {
                **dict(row),
                "detail": json.loads(row["detail_json"]),
            }
            for row in self.connection.execute("SELECT * FROM dedupe_decisions WHERE run_id = ? ORDER BY source_id, report_id", (run_id,))
        ]
        skipped_rows = [
            {
                **dict(row),
                "detail": json.loads(row["detail_json"]),
            }
            for row in self.connection.execute("SELECT * FROM skipped_items WHERE run_id = ? ORDER BY source_id, report_id, reason_code", (run_id,))
        ]
        (output_dir / "dedupe-decisions.json").write_text(json.dumps(dedupe_rows, indent=2, sort_keys=True) + "\n")
        (output_dir / "skipped-items.json").write_text(json.dumps(skipped_rows, indent=2, sort_keys=True) + "\n")

    def _rewrite_source_health_final_counts(self, run_id: str, output_dir: Path, package_payloads: List[Dict[str, Any]]) -> None:
        path = output_dir / "source-health.json"
        health_rows = json.loads(path.read_text()) if path.exists() else []
        counts: Dict[str, int] = {}
        for package in package_payloads:
            source_id = package.get("provenance", {}).get("source_id")
            if source_id:
                counts[source_id] = counts.get(source_id, 0) + 1
        for row in health_rows:
            row["emitted_candidate_count"] = counts.get(row["source_id"], 0)
            row["final_package_candidate_count"] = counts.get(row["source_id"], 0)
        path.write_text(json.dumps(health_rows, indent=2, sort_keys=True) + "\n")

    def _store_run(
        self,
        run_id: str,
        run_kind: str,
        now: datetime,
        exit_code: int,
        output_dir: Path,
        snapshot: Dict[str, Any],
        summary: Dict[str, Any],
        commit: bool = True,
    ) -> None:
        status = "failed" if exit_code != 0 else "succeeded_with_source_errors" if summary.get("counts", {}).get("failed_sources", 0) else "succeeded"
        self.connection.execute(
            "INSERT OR REPLACE INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, run_kind, iso_z(now), iso_z(self.clock.now()), status, str(output_dir), snapshot["snapshot_id"], json.dumps(summary, sort_keys=True)),
        )
        if commit:
            self.connection.commit()

    def _mark_run_failed(self, run_id: str, run_kind: str, now: datetime, output_dir: Path, snapshot: Dict[str, Any], error: Exception) -> None:
        row = self.connection.execute("SELECT summary_json FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        summary = json.loads(row["summary_json"]) if row else {"run_id": run_id, "run_kind": run_kind}
        summary["post_pipeline_error"] = {"class": error.__class__.__name__, "message": str(error)}
        if row is None:
            self.connection.execute(
                "INSERT OR REPLACE INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, run_kind, iso_z(now), iso_z(self.clock.now()), "failed", str(output_dir), snapshot["snapshot_id"], json.dumps(summary, sort_keys=True)),
            )
        else:
            self.connection.execute(
                "UPDATE runs SET status = ?, completed_at = ?, summary_json = ? WHERE run_id = ?",
                ("failed", iso_z(self.clock.now()), json.dumps(summary, sort_keys=True), run_id),
            )
        self.connection.commit()

    def _store_source_health(self, run_id: str, now: datetime, output_dir: Path, update_totals: bool = True, commit: bool = True) -> None:
        path = output_dir / "source-health.json"
        health_rows = json.loads(path.read_text()) if path.exists() else []
        for item in health_rows:
            previous = self.connection.execute("SELECT * FROM source_health WHERE source_id = ?", (item["source_id"],)).fetchone()
            failed = item.get("status") == "failed"
            previous_consecutive_failures = int(previous["consecutive_failures"]) if previous else 0
            consecutive_failures = previous_consecutive_failures + 1 if failed and update_totals else 0 if not failed and update_totals else previous_consecutive_failures
            total_successes = int(previous["total_successes"]) if previous else 0
            total_failures = int(previous["total_failures"]) if previous else 0
            if update_totals:
                if failed:
                    total_failures += 1
                else:
                    total_successes += 1
            last_error = item.get("last_error") or {}
            self.connection.execute(
                """
                INSERT INTO source_health VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                  last_status=excluded.last_status,
                  last_run_id=excluded.last_run_id,
                  last_checked_at=excluded.last_checked_at,
                  enabled=excluded.enabled,
                  parse_status=excluded.parse_status,
                  last_successful_fetch_at=excluded.last_successful_fetch_at,
                  latest_item_published_at=excluded.latest_item_published_at,
                  consecutive_failures=excluded.consecutive_failures,
                  total_successes=excluded.total_successes,
                  total_failures=excluded.total_failures,
                  last_error_class=excluded.last_error_class,
                  last_error_message=excluded.last_error_message,
                  last_error=excluded.last_error,
                  health_json=excluded.health_json
                """,
                (
                    item["source_id"],
                    item.get("status") or "unknown",
                    run_id,
                    item.get("fetched_at") or iso_z(now),
                    int(bool(item.get("enabled", True))),
                    item.get("parse_status"),
                    item.get("last_successful_fetch_at"),
                    item.get("latest_item_published_at"),
                    consecutive_failures,
                    total_successes,
                    total_failures,
                    last_error.get("class"),
                    last_error.get("message"),
                    item.get("failure_reason"),
                    json.dumps(item, sort_keys=True),
                ),
            )
            self.connection.execute(
                "INSERT OR REPLACE INTO source_run_status VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    item["source_id"],
                    item.get("status") or "unknown",
                    item.get("fetched_at"),
                    item.get("http_status"),
                    item.get("failure_reason"),
                    json.dumps(item, sort_keys=True),
                ),
            )
            fetch_id = "sha256:" + hashlib.sha256("fetch:v0\n{}\n{}\n{}".format(run_id, item["source_id"], item.get("feed_url")).encode("utf-8")).hexdigest()
            last_error = item.get("last_error") or {}
            self.connection.execute(
                "INSERT OR REPLACE INTO raw_fetches VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    fetch_id,
                    run_id,
                    item["source_id"],
                    item.get("feed_url") or "",
                    item.get("fetched_at") or iso_z(now),
                    item.get("http_status"),
                    item.get("elapsed_ms"),
                    None,
                    None,
                    last_error.get("class"),
                    last_error.get("message"),
                ),
            )
        if commit:
            self.connection.commit()

    def _embedding_for_candidate(self, candidate: Dict[str, Any], now: datetime, embedding_config: Optional[EmbeddingConfig] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        embedding = embedding_config or self.config.embedding
        input_hash = embedded_text_hash(candidate)
        request_id = "sha256:" + hashlib.sha256(
            "embed-request:v0\n{}\n{}\n{}".format(candidate["report_id"], embedding.space_id, input_hash).encode("utf-8")
        ).hexdigest()
        if embedding.backend == "openai":
            api_key = openai_api_key()
            if not api_key:
                return None, {"class": "embed_backend_unavailable", "message": "OPENAI_API_KEY is missing or empty", "retry_eligible": True}
            try:
                response = request_openai_embedding(
                    candidate.get("metadata", {}).get("embedding_text") or "",
                    embedding.model or "",
                    int(embedding.dimensions or 0),
                    api_key,
                )
            except PipelineError as exc:
                return None, {"class": "embed_backend_unavailable", "message": str(exc), "retry_eligible": True}
            data = response.get("data") if isinstance(response, dict) else None
            vector = data[0].get("embedding") if isinstance(data, list) and data and isinstance(data[0], dict) else None
            if response.get("model") != embedding.model:
                return None, {"class": "embed_invalid_response", "message": "embedding metadata mismatch", "retry_eligible": True}
            if not isinstance(vector, list):
                return None, {"class": "embed_invalid_response", "message": "embedding vector is not a list", "retry_eligible": True}
            if len(vector) != int(embedding.dimensions or 0):
                return None, {"class": "embed_invalid_response", "message": "embedding vector dimensions mismatch", "retry_eligible": True}
            vector_hash = "sha256:" + hashlib.sha256(json.dumps(vector, separators=(",", ":"), sort_keys=True).encode("utf-8")).hexdigest()
            return {
                "space_id": embedding.space_id,
                "provider": embedding.provider,
                "model": embedding.model,
                "dimensions": embedding.dimensions,
                "input_hash": input_hash,
                "vector_hash": vector_hash,
                "vector": vector,
                "backend_request_id": response.get("backend_request_id") or request_id,
                "embedded_at": iso_z(now),
            }, None
        if embedding.command:
            request = {
                "request_id": request_id,
                "space_id": embedding.space_id,
                "provider": embedding.provider,
                "model": embedding.model,
                "dimensions": embedding.dimensions,
                "text": candidate.get("metadata", {}).get("embedding_text") or "",
            }
            try:
                completed = subprocess.run(
                    [embedding.command, "--input-json", "-"],
                    input=json.dumps(request),
                    capture_output=True,
                    check=False,
                    text=True,
                )
                if completed.returncode != 0:
                    failure_payload = None
                    for stream in (completed.stdout, completed.stderr):
                        try:
                            failure_payload = json.loads(stream) if stream.strip() else None
                        except ValueError:
                            failure_payload = None
                        if isinstance(failure_payload, dict):
                            break
                    if isinstance(failure_payload, dict):
                        return None, {
                            "class": str(failure_payload.get("error_class") or failure_payload.get("class") or "embed_backend_unavailable"),
                            "message": str(failure_payload.get("message") or failure_payload.get("error") or "embedding backend exited non-zero"),
                            "retry_eligible": bool(failure_payload.get("retry_eligible", True)),
                        }
                    return None, {
                        "class": "embed_backend_unavailable",
                        "message": completed.stderr.strip() or "embedding backend exited non-zero",
                        "retry_eligible": True,
                    }
                try:
                    response = json.loads(completed.stdout)
                except ValueError as exc:
                    return None, {"class": "embed_invalid_response", "message": str(exc), "retry_eligible": True}
            except OSError as exc:
                return None, {"class": "embed_backend_unavailable", "message": str(exc), "retry_eligible": True}
            if (
                response.get("space_id") != embedding.space_id
                or response.get("provider") != embedding.provider
                or response.get("model") != embedding.model
                or int(response.get("dimensions", -1)) != int(embedding.dimensions or -2)
            ):
                return None, {"class": "embed_invalid_response", "message": "embedding metadata mismatch", "retry_eligible": True}
            vector = response.get("vector")
            if not isinstance(vector, list):
                return None, {"class": "embed_invalid_response", "message": "embedding vector is not a list", "retry_eligible": True}
            backend_request_id = str(response.get("backend_request_id") or "")
            if backend_request_id.startswith("local-deterministic-"):
                return None, {"class": "embed_invalid_response", "message": "deterministic fake embedding backend is not allowed", "retry_eligible": False}
            vector_hash = "sha256:" + hashlib.sha256(json.dumps(vector, separators=(",", ":"), sort_keys=True).encode("utf-8")).hexdigest()
            return {
                "space_id": embedding.space_id,
                "provider": response.get("provider") or embedding.provider or "cli",
                "model": embedding.model,
                "dimensions": embedding.dimensions,
                "input_hash": input_hash,
                "vector_hash": vector_hash,
                "vector": vector,
                "backend_request_id": response.get("backend_request_id"),
                "embedded_at": iso_z(now),
            }, None
        supplied = supplied_embedding_for(candidate)
        if supplied:
            return supplied, None
        return None, {"class": "embed_backend_unavailable", "message": "no supplied embedding", "retry_eligible": True}

    def _record_embedding_failure(self, run_id: str, now: datetime, candidate: Dict[str, Any], failure: Dict[str, Any]) -> Dict[str, Any]:
        failure = {
            **failure,
            "run_id": run_id,
            "source_id": candidate["source_id"],
            "report_id": candidate["report_id"],
        }
        self.connection.execute(
            "INSERT OR REPLACE INTO embeddings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                candidate["report_id"],
                self.config.embedding.space_id or "",
                self.config.embedding.provider or "",
                self.config.embedding.model or "",
                int(self.config.embedding.dimensions or 0),
                embedded_text_hash(candidate),
                "",
                "[]",
                None,
                "failed",
                json.dumps(failure, sort_keys=True),
                None,
                iso_z(now),
            ),
        )
        return failure

    def _store_packages_and_publish(
        self,
        run_id: str,
        now: datetime,
        output_dir: Path,
        candidates_path: Path,
        cycle_snapshot: Dict[str, Any],
        publish_snapshot: Dict[str, Any],
        cycle_embedding: EmbeddingConfig,
        max_live_publishes: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        package_payloads: List[Dict[str, Any]] = []
        embedding_failures: List[Dict[str, Any]] = []
        publishable: List[Tuple[str, str, str, Dict[str, Any], Dict[str, Any], Dict[str, Any]]] = []
        queued_publish_keys: set[str] = set()
        rows = [json.loads(line) for line in candidates_path.read_text().splitlines() if line.strip()]
        for candidate in rows:
            if self.reload_requested:
                self.reload_requested = False
                self.reload()
                publish_snapshot = latest_snapshot(self.connection)
            self.connection.execute(
                "INSERT OR IGNORE INTO accepted_reports VALUES (?, ?, ?, ?, ?)",
                (
                    candidate["report_id"],
                    run_id,
                    cycle_snapshot["effective_mode"],
                    cycle_snapshot["snapshot_id"],
                    iso_z(now),
                ),
            )
            first = self.connection.execute(
                "SELECT first_effective_mode FROM accepted_reports WHERE report_id = ?",
                (candidate["report_id"],),
            ).fetchone()
            supplied_embeddings, embedding_failure = self._embedding_for_candidate(candidate, now, cycle_embedding)
            if supplied_embeddings is None and cycle_snapshot["require_embeddings"] and not cycle_snapshot["allow_non_embedded_fallback"]:
                failure = embedding_failure or {"class": "embed_backend_unavailable", "message": "embedding unavailable", "retry_eligible": True}
                embedding_failures.append(self._record_embedding_failure(run_id, now, candidate, failure))
                self.connection.execute(
                    "INSERT INTO skipped_items VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        candidate["source_id"],
                        candidate["report_id"],
                        failure["class"],
                        json.dumps({**failure, "report_id": candidate["report_id"], "source_id": candidate["source_id"]}, sort_keys=True),
                        iso_z(now),
                    ),
                )
                continue
            if supplied_embeddings is None:
                supplied_embeddings = {}
            candidate = {**candidate, "supplied_embeddings": [supplied_embeddings] if supplied_embeddings else []}
            package_id = package_id_for(candidate)
            if supplied_embeddings:
                self.connection.execute(
                    "INSERT OR REPLACE INTO embeddings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        candidate["report_id"],
                        supplied_embeddings.get("space_id", ""),
                        supplied_embeddings.get("provider", ""),
                        supplied_embeddings.get("model", ""),
                        int(supplied_embeddings.get("dimensions") or 0),
                        supplied_embeddings.get("input_hash") or embedded_text_hash(candidate),
                        supplied_embeddings.get("vector_hash", ""),
                        json.dumps(supplied_embeddings.get("vector") or [], sort_keys=True),
                        supplied_embeddings.get("backend_request_id"),
                        "embedded",
                        None,
                        supplied_embeddings.get("embedded_at") or iso_z(now),
                        iso_z(now),
                    ),
                )
            first_acceptance_allows_publish = first["first_effective_mode"] == "live" and (
                not cycle_snapshot["require_embeddings"] or cycle_snapshot["allow_non_embedded_fallback"] or embedding_matches_snapshot(cycle_snapshot, supplied_embeddings)
            )
            publish_allowed = first_acceptance_allows_publish and bool(cycle_snapshot.get("publish_target_key"))
            package_payload = package_payload_for(candidate, package_id)
            package_payloads.append(package_payload)
            self.connection.execute(
                "INSERT OR IGNORE INTO packages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    package_id,
                    candidate["report_id"],
                    str(candidate.get("schema_version", 1)),
                    supplied_embeddings.get("space_id"),
                    supplied_embeddings.get("vector_hash"),
                    int(first_acceptance_allows_publish),
                    json.dumps(package_payload, sort_keys=True),
                    run_id,
                    iso_z(now),
                ),
            )
            if publish_allowed:
                target = str(cycle_snapshot["publish_target_key"])
                key = "sha256:" + hashlib.sha256(("publish:v0\n{}\n{}".format(package_id, target)).encode("utf-8")).hexdigest()
                if self.connection.execute("SELECT 1 FROM publish_attempts WHERE publish_idempotency_key = ? AND status = 'succeeded'", (key,)).fetchone():
                    continue
                queued_publish_keys.add(key)
                publishable.append((package_id, target, key, package_payload, supplied_embeddings, candidate))
        if publishable:
            delivery_started_at = self.clock.now()
            self._create_delivery_plan(run_id, delivery_started_at, publishable, publish_snapshot, max_live_publishes)
            self.connection.commit()
            if self.config.delivery.mode == "immediate":
                while self._drain_due_delivery(delivery_started_at, max_entries=100000).get("attempted"):
                    pass
            else:
                self._drain_due_delivery(delivery_started_at)
        (output_dir / "embedding-failures.json").write_text(json.dumps(embedding_failures, indent=2, sort_keys=True) + "\n")
        return package_payloads

    def _publish_package_to_subspace(self, package_payload: Dict[str, Any], idempotency_key: str, supplied_embedding: Dict[str, Any]) -> Dict[str, Any]:
        return post_message_to_subspace(
            str(self.config.publish.subspace_endpoint),
            self.config.publish.subspace_websocket_path,
            str(self.config.publish.subspace_agent_id),
            str(self.config.publish.subspace_session_token),
            json.dumps(package_payload, sort_keys=True, separators=(",", ":")),
            [canonical_embedding_for_subspace(supplied_embedding)] if supplied_embedding else [],
            idempotency_key,
        )

    def _delivery_window_end(self, now: datetime, run_id: str) -> datetime:
        state = self._scheduler_state()
        if self.config.scheduler.mode == "manual" or not state["next_due_at"]:
            return now + timedelta(seconds=self.config.delivery.manual_window_seconds)
        next_due_at = parse_now(state["next_due_at"])
        if next_due_at <= now:
            return now + timedelta(seconds=self.config.scheduler.interval_seconds + self._jitter_offset(run_id))
        return next_due_at

    def _create_delivery_plan(
        self,
        run_id: str,
        now: datetime,
        publishable: List[Tuple[str, str, str, Dict[str, Any], Dict[str, Any], Dict[str, Any]]],
        publish_snapshot: Dict[str, Any],
        max_live_publishes: Optional[int],
    ) -> None:
        if not publishable:
            return
        target = publishable[0][1]
        existing = self.connection.execute(
            "SELECT plan_id FROM delivery_plans WHERE run_id = ? AND publish_target_key = ?",
            (run_id, target),
        ).fetchone()
        if existing is not None:
            return
        publishable.sort(
            key=lambda item: (
                item[5].get("published_at") is None,
                item[5].get("published_at") or "",
                item[5].get("fetched_at") or "",
                item[5].get("source_id") or "",
                item[5].get("report_id") or "",
            )
        )
        budget = len(publishable) if max_live_publishes is None else max(0, max_live_publishes)
        selected = publishable[:budget]
        over_budget = publishable[budget:]
        for package_id, _target, key, _payload, _embedding, candidate in over_budget:
            self.connection.execute(
                "INSERT INTO skipped_items VALUES (?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    candidate["source_id"],
                    candidate["report_id"],
                    "over_live_publish_budget",
                    json.dumps({"package_id": package_id, "publish_idempotency_key": key}, sort_keys=True),
                    iso_z(now),
                ),
            )
        if not selected:
            return
        window_start = now
        configured_window_end = self._delivery_window_end(now, run_id)
        window_end = configured_window_end if configured_window_end > window_start else window_start
        plan_id = "sha256:" + hashlib.sha256(("delivery-plan:v0\n{}\n{}".format(run_id, target)).encode("utf-8")).hexdigest()
        duration = max(0.0, (window_end - window_start).total_seconds())
        if self.config.delivery.mode == "tranche" and len(selected) > self.config.delivery.max_messages_per_plan:
            raise PipelineError(
                "delivery_max_messages_exceeded: selected_count={} exceeds max_messages_per_plan={} for window_seconds={}".format(
                    len(selected),
                    self.config.delivery.max_messages_per_plan,
                    int(duration),
                )
            )
        slot_width = duration / len(selected) if selected and duration > 0 else 0
        if self.config.delivery.mode == "tranche" and len(selected) > 1 and duration > 0 and slot_width < self.config.delivery.min_slot_seconds:
            max_items_for_window = int(duration // self.config.delivery.min_slot_seconds)
            raise PipelineError(
                "delivery_density_exceeded: selected_count={} exceeds max_items_for_window={} for window_seconds={} min_slot_seconds={} computed_slot_seconds={:.3f}".format(
                    len(selected),
                    max_items_for_window,
                    int(duration),
                    self.config.delivery.min_slot_seconds,
                    slot_width,
                )
            )
        first_due_at = None
        last_due_at = None
        due_times: List[datetime] = []
        for index, _item in enumerate(selected):
            if self.config.delivery.mode == "immediate" or duration <= 0:
                due_at = window_start
            else:
                due_at = window_start + timedelta(seconds=int(index * (duration / len(selected))))
                if due_at > window_end:
                    due_at = window_end
            due_times.append(due_at)
            first_due_at = due_at if first_due_at is None else min(first_due_at, due_at)
            last_due_at = due_at if last_due_at is None else max(last_due_at, due_at)
        self.connection.execute(
            """
            INSERT INTO delivery_plans
            (plan_id, run_id, publish_target_key, mode, created_at, window_start, window_end,
             selected_count, skipped_over_budget_count, first_due_at, last_due_at, detail_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                plan_id,
                run_id,
                target,
                self.config.delivery.mode,
                iso_z(now),
                iso_z(window_start),
                iso_z(window_end),
                len(selected),
                len(over_budget),
                iso_z(first_due_at or window_start),
                iso_z(last_due_at or window_start),
                json.dumps(
                    {
                        "publish_snapshot_id": publish_snapshot["snapshot_id"],
                        "configured_window_end": iso_z(configured_window_end),
                    },
                    sort_keys=True,
                ),
            ),
        )
        for index, item in enumerate(selected):
            package_id, _target, key, _payload, _embedding, _candidate = item
            entry_id = "sha256:" + hashlib.sha256(("delivery-entry:v0\n{}\n{}".format(plan_id, package_id)).encode("utf-8")).hexdigest()
            self.connection.execute(
                """
                INSERT INTO delivery_entries
                (entry_id, plan_id, run_id, publish_target_key, package_id, publish_idempotency_key,
                 selected_order_index, due_at, status, attempt_count, last_attempt_at, next_retry_at,
                 last_error_class, last_error_message, subspace_message_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, NULL, NULL, NULL, NULL, NULL, ?)
                """,
                (entry_id, plan_id, run_id, target, package_id, key, index, iso_z(due_times[index]), iso_z(now)),
            )

    def _current_publish_target_key(self) -> Optional[str]:
        if not self.config.publish.subspace_endpoint:
            return None
        return "sha256:" + hashlib.sha256(("publish-target:v0\n" + str(self.config.publish.subspace_endpoint)).encode("utf-8")).hexdigest()

    def _mark_delivery_permanent_failure(self, entry_id: str, now: datetime, error_class: str, message: str) -> None:
        self.connection.execute(
            """
            UPDATE delivery_entries
            SET status = 'permanent_failure',
                last_error_class = ?,
                last_error_message = ?,
                updated_at = ?
            WHERE entry_id = ?
            """,
            (error_class, message, iso_z(now), entry_id),
        )
        self.connection.commit()

    def _drain_due_delivery(self, now: datetime, max_entries: Optional[int] = None) -> Dict[str, Any]:
        publish_snapshot = latest_snapshot(self.connection)
        if publish_snapshot["effective_mode"] != "live":
            return {}
        target = self._current_publish_target_key()
        if target is None:
            return {}
        due_filter = """
            (
                status = 'pending' AND due_at <= ?
            ) OR (
                status = 'retry_pending' AND COALESCE(next_retry_at, due_at) <= ?
            ) OR status = 'attempting'
        """
        mismatched_rows = self.connection.execute(
            """
            SELECT entry_id
            FROM delivery_entries
            WHERE ({})
              AND publish_target_key != ?
            ORDER BY COALESCE(next_retry_at, due_at), due_at, selected_order_index
            """.format(due_filter),
            (iso_z(now), iso_z(now), target),
        ).fetchall()
        for row in mismatched_rows:
            self._mark_delivery_permanent_failure(
                row["entry_id"],
                now,
                "publish_target_mismatch",
                "delivery entry publish target does not match current approved target",
            )
        due_rows = self.connection.execute(
            """
            SELECT *
            FROM delivery_entries
            WHERE ({})
              AND publish_target_key = ?
            ORDER BY COALESCE(next_retry_at, due_at), due_at, selected_order_index
            LIMIT ?
            """.format(due_filter),
            (iso_z(now), iso_z(now), target, max_entries or self.config.delivery.live_send_concurrency),
        ).fetchall()
        attempted = 0
        succeeded = 0
        failed = 0
        for entry in due_rows:
            package_row = self.connection.execute("SELECT active_eligible, package_json FROM packages WHERE package_id = ?", (entry["package_id"],)).fetchone()
            if package_row is None:
                self._mark_delivery_permanent_failure(
                    entry["entry_id"],
                    now,
                    "missing_package",
                    "delivery package payload is missing",
                )
                failed += 1
                continue
            if not int(package_row["active_eligible"]):
                self._mark_delivery_permanent_failure(
                    entry["entry_id"],
                    now,
                    "inactive_package",
                    "delivery package is not active-eligible",
                )
                failed += 1
                continue
            package_payload = json.loads(package_row["package_json"])
            supplied_embeddings = supplied_embedding_for(package_payload)
            if package_payload.get("schema") != "swarm.channel.news.report.v0" or package_payload.get("package_id") != entry["package_id"]:
                self._mark_delivery_permanent_failure(
                    entry["entry_id"],
                    now,
                    "invalid_package_contract",
                    "delivery package violates canonical Subspace message contract",
                )
                failed += 1
                continue
            if (
                publish_snapshot["require_embeddings"]
                and not publish_snapshot["allow_non_embedded_fallback"]
                and not embedding_matches_snapshot(publish_snapshot, supplied_embeddings)
            ):
                self._mark_delivery_permanent_failure(
                    entry["entry_id"],
                    now,
                    "missing_required_embedding",
                    "delivery package is missing required embedding for current publish snapshot",
                )
                failed += 1
                continue
            key = entry["publish_idempotency_key"]
            if self.connection.execute("SELECT 1 FROM publish_attempts WHERE publish_idempotency_key = ? AND status = 'succeeded'", (key,)).fetchone():
                message_row = self.connection.execute(
                    "SELECT subspace_message_id FROM publish_attempts WHERE publish_idempotency_key = ? AND status = 'succeeded' ORDER BY completed_at DESC LIMIT 1",
                    (key,),
                ).fetchone()
                self.connection.execute(
                    """
                    UPDATE delivery_entries
                    SET status = 'succeeded', subspace_message_id = ?, updated_at = ?
                    WHERE entry_id = ?
                    """,
                    (message_row["subspace_message_id"] if message_row else None, iso_z(now), entry["entry_id"]),
                )
                self.connection.commit()
                succeeded += 1
                continue
            previous_attempt = self.connection.execute(
                "SELECT MAX(attempt_number) AS attempt_number FROM publish_attempts WHERE publish_idempotency_key = ?",
                (key,),
            ).fetchone()
            attempt_number = int(previous_attempt["attempt_number"] or 0) + 1
            attempt = "sha256:" + hashlib.sha256(("publish-attempt:v0\n{}\n{}".format(key, attempt_number)).encode("utf-8")).hexdigest()
            self.connection.execute(
                """
                UPDATE delivery_entries
                SET status = 'attempting', attempt_count = attempt_count + 1, last_attempt_at = ?, updated_at = ?
                WHERE entry_id = ?
                """,
                (iso_z(now), iso_z(now), entry["entry_id"]),
            )
            self.connection.execute(
                """
                INSERT INTO publish_attempts
                (attempt_id, package_id, publish_target_key, publish_idempotency_key, effective_publish_snapshot_id,
                 attempt_number, mode, status, attempted_at, completed_at, subspace_message_id, response_json, error_class, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL, NULL, NULL, NULL)
                """,
                (attempt, entry["package_id"], entry["publish_target_key"], key, publish_snapshot["snapshot_id"], attempt_number, publish_snapshot["effective_mode"], iso_z(now)),
            )
            self.connection.commit()
            attempted += 1
            try:
                response = self._publish_package_to_subspace(package_payload, key, supplied_embeddings)
                message_id = subspace_message_id_from_response(response)
                if not message_id and not subspace_response_is_success(response):
                    raise PublishTransportError("Subspace response missing subspace_message_id", response)
                completed_at = iso_z(self.clock.now())
                self.connection.execute(
                    """
                    UPDATE publish_attempts
                    SET status = 'succeeded', completed_at = ?, subspace_message_id = ?, response_json = ?, error_class = NULL, error_message = NULL
                    WHERE attempt_id = ?
                    """,
                    (completed_at, message_id, json.dumps(response, sort_keys=True), attempt),
                )
                self.connection.execute(
                    """
                    UPDATE delivery_entries
                    SET status = 'succeeded', subspace_message_id = ?, last_error_class = NULL, last_error_message = NULL, updated_at = ?
                    WHERE entry_id = ?
                    """,
                    (message_id, completed_at, entry["entry_id"]),
                )
                self.connection.commit()
                succeeded += 1
            except Exception as exc:
                response = getattr(exc, "response", None)
                completed_at = iso_z(self.clock.now())
                delay = min(self.config.delivery.max_retry_delay_seconds, 60 * (2 ** max(0, attempt_number - 1)))
                delivery_status = "permanent_failure" if publish_exception_is_permanent(exc) else "retry_pending"
                next_retry_at = None if delivery_status == "permanent_failure" else iso_z(self.clock.now() + timedelta(seconds=delay))
                self.connection.execute(
                    """
                    UPDATE publish_attempts
                    SET status = 'failed', completed_at = ?, response_json = ?, error_class = ?, error_message = ?
                    WHERE attempt_id = ?
                    """,
                    (completed_at, json.dumps(response, sort_keys=True) if response is not None else None, exc.__class__.__name__, str(exc), attempt),
                )
                self.connection.execute(
                    """
                    UPDATE delivery_entries
                    SET status = ?, next_retry_at = ?, last_error_class = ?, last_error_message = ?, updated_at = ?
                    WHERE entry_id = ?
                    """,
                    (delivery_status, next_retry_at, exc.__class__.__name__, str(exc), completed_at, entry["entry_id"]),
                )
                self.connection.commit()
                failed += 1
        return {"attempted": attempted, "succeeded": succeeded, "failed": failed} if due_rows else {}

    def _next_delivery_due_at(self) -> Optional[datetime]:
        try:
            if latest_snapshot(self.connection)["effective_mode"] != "live":
                return None
        except PipelineError:
            return None
        row = self.connection.execute(
            """
            SELECT due_at, next_retry_at, status
            FROM delivery_entries
            WHERE status IN ('pending', 'retry_pending')
            ORDER BY COALESCE(next_retry_at, due_at), due_at, selected_order_index
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        return parse_now(row["next_retry_at"] or row["due_at"])

    def _delivery_status(self, now: datetime) -> Dict[str, Any]:
        plan = self.connection.execute("SELECT * FROM delivery_plans ORDER BY created_at DESC, rowid DESC LIMIT 1").fetchone()
        if plan:
            counts = {
                row["status"]: row["count"]
                for row in self.connection.execute(
                    "SELECT status, COUNT(*) AS count FROM delivery_entries WHERE plan_id = ? GROUP BY status",
                    (plan["plan_id"],),
                )
            }
            overdue = self.connection.execute(
                """
                SELECT COUNT(*)
                FROM delivery_entries
                WHERE plan_id = ?
                  AND ((status = 'pending' AND due_at <= ?)
                    OR (status = 'retry_pending' AND COALESCE(next_retry_at, due_at) <= ?))
                """,
                (plan["plan_id"], iso_z(now), iso_z(now)),
            ).fetchone()[0]
        else:
            counts = {}
            overdue = 0
        last_success = self.connection.execute(
            "SELECT subspace_message_id FROM delivery_entries WHERE status = 'succeeded' AND (? IS NULL OR plan_id = ?) ORDER BY updated_at DESC LIMIT 1",
            (plan["plan_id"] if plan else None, plan["plan_id"] if plan else None),
        ).fetchone()
        last_error = self.connection.execute(
            "SELECT last_error_class, last_error_message FROM delivery_entries WHERE last_error_class IS NOT NULL AND (? IS NULL OR plan_id = ?) ORDER BY updated_at DESC LIMIT 1",
            (plan["plan_id"] if plan else None, plan["plan_id"] if plan else None),
        ).fetchone()
        return {
            "mode": self.config.delivery.mode,
            "active_plan_id": plan["plan_id"] if plan else None,
            "counts_by_status": counts,
            "pending_count": counts.get("pending", 0),
            "overdue_count": overdue,
            "retry_pending_count": counts.get("retry_pending", 0),
            "succeeded_count": counts.get("succeeded", 0),
            "last_successful_subspace_message_id": last_success["subspace_message_id"] if last_success else None,
            "last_delivery_error": dict(last_error) if last_error else None,
        }

    def _store_normalized_storage(self, run_id: str, now: datetime, output_dir: Path, accepted_report_ids: Optional[set[str]]) -> set[str]:
        sqlite_accepted_report_ids: set[str] = set()
        rows = [json.loads(line) for line in (output_dir / "normalized-items.jsonl").read_text().splitlines() if line.strip()]
        skipped_path = output_dir / "skipped-items.json"
        skipped_payload = json.loads(skipped_path.read_text()) if skipped_path.exists() else []
        preexisting_skips = {
            item.get("report_id"): item
            for item in skipped_payload
            if item.get("report_id")
        }
        rows.sort(key=lambda row: (row["source_id"], row.get("published_at") is None, row.get("published_at") or row.get("fetched_at") or "", row["report_id"]))
        for report in rows:
            key_type, key_scope, normalized_key_value, key_hash = selected_dedupe_key_for_report(report)
            existing_report = self.connection.execute("SELECT report_id FROM normalized_reports WHERE report_id = ?", (report["report_id"],)).fetchone()
            existing_key = self.connection.execute(
                "SELECT report_id FROM dedupe_keys WHERE key_type = ? AND key_scope = ? AND key_hash = ?",
                (key_type, key_scope, key_hash),
            ).fetchone()
            existing_seen_in_run = self.connection.execute(
                "SELECT 1 FROM report_seen_runs WHERE report_id = ? AND run_id = ?",
                (report["report_id"], run_id),
            ).fetchone()
            is_accepted = accepted_report_ids is None or report["report_id"] in accepted_report_ids
            retry_failed_embedding = bool(
                existing_report
                and is_accepted
                and self.connection.execute(
                    "SELECT 1 FROM embeddings WHERE report_id = ? AND status = 'failed'",
                    (report["report_id"],),
                ).fetchone()
                and not self.connection.execute(
                    "SELECT 1 FROM packages WHERE report_id = ?",
                    (report["report_id"],),
                ).fetchone()
            )
            if key_type == "missing_identity_key":
                decision = "missing_identity_key"
            elif retry_failed_embedding:
                decision = "seen_existing_report"
                sqlite_accepted_report_ids.add(report["report_id"])
            elif existing_key and existing_seen_in_run:
                decision = "exact_duplicate"
            elif existing_report:
                decision = "seen_existing_report"
            elif existing_key:
                decision = "exact_duplicate"
            elif is_accepted:
                decision = "accepted"
                sqlite_accepted_report_ids.add(report["report_id"])
            elif report["report_id"] in preexisting_skips:
                decision = str(preexisting_skips[report["report_id"]].get("reason_code") or "skipped_not_package_candidate")
            else:
                decision = "skipped_not_package_candidate"
            if decision in {"exact_duplicate", "skipped_not_package_candidate", "missing_identity_key", "stale_by_freshness_window"}:
                detail = preexisting_skips.get(report["report_id"], {}).get("detail") or {
                    "source_id": report["source_id"],
                    "report_id": report["report_id"],
                    "duplicate_of_report_id": existing_key["report_id"] if existing_key else None,
                    "duplicate_key_type": None if decision == "missing_identity_key" else key_type,
                    "duplicate_key_scope": key_scope,
                    "duplicate_key_hash": None if decision == "missing_identity_key" else key_hash,
                    "duplicate_key_precedence": 1 if key_type == "feed_entry_id" else 2 if key_type == "canonical_url" else 3 if key_type == "normalized_title_date" else None,
                    "reason": decision,
                    "title": report["title"],
                    "canonical_url": report["canonical_url"],
                    "feed_entry_id": report.get("feed_entry_id"),
                    "published_at": report.get("published_at"),
                }
                detail.setdefault("duplicate_of_report_id", existing_key["report_id"] if existing_key else None)
                detail.setdefault("duplicate_key_type", None if decision == "missing_identity_key" else key_type)
                detail.setdefault("duplicate_key_hash", None if decision == "missing_identity_key" else key_hash)
                detail.setdefault("duplicate_key_precedence", 1 if key_type == "feed_entry_id" else 2 if key_type == "canonical_url" else 3 if key_type == "normalized_title_date" else None)
                self.connection.execute(
                    "INSERT INTO skipped_items VALUES (?, ?, ?, ?, ?, ?)",
                    (run_id, report["source_id"], report["report_id"], decision, json.dumps(detail, sort_keys=True), iso_z(now)),
                )
                self.connection.execute(
                    "INSERT OR REPLACE INTO dedupe_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        report["report_id"],
                        report["source_id"],
                        decision,
                        detail["duplicate_of_report_id"],
                        detail["duplicate_key_type"],
                        key_scope,
                        detail["duplicate_key_hash"],
                        detail["duplicate_key_precedence"],
                        normalized_key_value.replace("\n", " ")[:160],
                        decision,
                        json.dumps(detail, sort_keys=True),
                    ),
                )
                continue
            report_id_input_hash = "sha256:" + hashlib.sha256(normalized_key_value.encode("utf-8")).hexdigest()
            self.connection.execute(
                """
                INSERT INTO normalized_reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(report_id) DO UPDATE SET latest_seen_run_id = excluded.latest_seen_run_id, report_json = excluded.report_json, status = excluded.status
                """,
                (
                    report["report_id"],
                    report["source_id"],
                    run_id,
                    run_id,
                    key_type,
                    report_id_input_hash,
                    report.get("feed_entry_id"),
                    report.get("raw_url"),
                    report.get("canonical_url"),
                    normalize_title(report["title"]),
                    report.get("published_at"),
                    report["fetched_at"],
                    json.dumps(report, sort_keys=True),
                    "accepted",
                ),
            )
            self.connection.execute(
                "INSERT OR IGNORE INTO report_seen_runs VALUES (?, ?, ?, ?)",
                (report["report_id"], run_id, report["source_id"], iso_z(now)),
            )
            self.connection.execute(
                "INSERT OR IGNORE INTO dedupe_keys VALUES (?, ?, ?, ?, ?)",
                (key_scope, key_type, key_hash, report["report_id"], normalized_key_value),
            )
            self.connection.execute(
                "INSERT OR REPLACE INTO dedupe_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    report["report_id"],
                    report["source_id"],
                    decision,
                    None,
                    key_type,
                    key_scope,
                    key_hash,
                    1 if key_type == "feed_entry_id" else 2 if key_type == "canonical_url" else 3,
                    normalized_key_value.replace("\n", " ")[:160],
                    decision,
                    json.dumps({"key_hash": key_hash, "key_type": key_type}, sort_keys=True),
                ),
            )
        skipped_path = output_dir / "skipped-items.json"
        for item in json.loads(skipped_path.read_text()) if skipped_path.exists() else []:
            if item.get("report_id") in preexisting_skips and item.get("reason_code") == "stale_by_freshness_window":
                continue
            reason_code = item.get("reason_code") or "source_skipped_counts"
            self.connection.execute(
                "INSERT INTO skipped_items VALUES (?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    item["source_id"],
                    item.get("report_id"),
                    reason_code,
                    json.dumps(item.get("detail") or item, sort_keys=True),
                    iso_z(now),
                ),
            )
        return sqlite_accepted_report_ids

    def _record_scheduler_config(self, scheduler: SchedulerConfig, now: datetime, recompute_next: bool) -> None:
        existing = self.connection.execute("SELECT mode, last_completed_at FROM scheduler_state WHERE id = 1").fetchone()
        last_completed_at = existing["last_completed_at"] if existing else None
        previous_mode = existing["mode"] if existing else None
        if scheduler.mode == "manual":
            next_due_at = None
        elif not last_completed_at and recompute_next and previous_mode == "manual":
            next_due_at = iso_z(now + timedelta(seconds=scheduler.interval_seconds + self._jitter_offset("{}:{}".format(iso_z(now), scheduler.interval_seconds))))
        elif not last_completed_at and scheduler.run_on_startup_if_due:
            next_due_at = iso_z(now)
        elif last_completed_at:
            base = now if recompute_next and previous_mode == "manual" else parse_now(last_completed_at)
            next_due_at = iso_z(base + timedelta(seconds=scheduler.interval_seconds + self._jitter_offset("{}:{}".format(iso_z(now), scheduler.interval_seconds))))
        else:
            next_due_at = iso_z(now + timedelta(seconds=scheduler.interval_seconds + self._jitter_offset("{}:startup".format(iso_z(now)))))
        self.connection.execute(
            """
            INSERT INTO scheduler_state
            (id, mode, interval_seconds, jitter_seconds, run_on_startup_if_due, missed_tick_policy,
             max_live_publishes_per_tick, config_hash,
             last_decision_at, last_started_run_id, last_completed_run_id, last_completed_at, running_run_id, next_due_at, updated_at)
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, NULL, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              mode=excluded.mode,
              interval_seconds=excluded.interval_seconds,
              jitter_seconds=excluded.jitter_seconds,
              run_on_startup_if_due=excluded.run_on_startup_if_due,
              missed_tick_policy=excluded.missed_tick_policy,
              max_live_publishes_per_tick=excluded.max_live_publishes_per_tick,
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
                scheduler.max_live_publishes_per_tick,
                self.config.config_hash,
                last_completed_at,
                next_due_at,
                iso_z(now),
            ),
        )
        self.connection.commit()

    def _record_scheduler_completion(self, run_id: str, run_kind: str, now: datetime) -> None:
        next_due_at = None
        if self.config.scheduler.mode == "interval":
            existing = self._scheduler_state()
            if run_kind != "scheduled" and existing["next_due_at"] and parse_now(existing["next_due_at"]) > now:
                next_due_at = existing["next_due_at"]
            else:
                plan_row = self.connection.execute(
                    "SELECT window_start FROM delivery_plans WHERE run_id = ? ORDER BY created_at DESC LIMIT 1",
                    (run_id,),
                ).fetchone()
                base = parse_now(plan_row["window_start"]) if plan_row else now
                next_due_at = iso_z(base + timedelta(seconds=self.config.scheduler.interval_seconds + self._jitter_offset(run_id)))
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
    connection = connect_database_readonly(db_path)
    try:
        snapshot_row = connection.execute("SELECT snapshot_json FROM runtime_config_snapshots ORDER BY observed_at DESC, rowid DESC LIMIT 1").fetchone()
        snapshot = json.loads(snapshot_row["snapshot_json"]) if snapshot_row else None
        scheduler_row = connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone()
        run_row = connection.execute("SELECT * FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 1").fetchone()
        prime_row = connection.execute("SELECT * FROM prime_events ORDER BY requested_at DESC, rowid DESC LIMIT 1").fetchone()
        runtime_event_row = connection.execute("SELECT * FROM runtime_events ORDER BY observed_at DESC, rowid DESC LIMIT 1").fetchone()
        reload_failure_row = connection.execute(
            "SELECT * FROM runtime_events WHERE event_type LIKE '%reload%' AND status = 'failed' ORDER BY observed_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
        scheduler_reload_failure_row = connection.execute(
            "SELECT * FROM scheduler_events WHERE event_type LIKE '%reload%' AND status = 'failed' ORDER BY observed_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
        package_count = connection.execute("SELECT COUNT(*) FROM packages").fetchone()[0]
        delivery_plan_row = connection.execute("SELECT * FROM delivery_plans ORDER BY created_at DESC, rowid DESC LIMIT 1").fetchone()
        now = utc_now()
        if delivery_plan_row:
            delivery_counts = {
                row["status"]: row["count"]
                for row in connection.execute(
                    "SELECT status, COUNT(*) AS count FROM delivery_entries WHERE plan_id = ? GROUP BY status",
                    (delivery_plan_row["plan_id"],),
                )
            }
            overdue_count = connection.execute(
                """
                SELECT COUNT(*)
                FROM delivery_entries
                WHERE plan_id = ?
                  AND ((status = 'pending' AND due_at <= ?)
                    OR (status = 'retry_pending' AND COALESCE(next_retry_at, due_at) <= ?))
                """,
                (delivery_plan_row["plan_id"], iso_z(now), iso_z(now)),
            ).fetchone()[0]
            last_delivery_error_row = connection.execute(
                "SELECT last_error_class, last_error_message FROM delivery_entries WHERE plan_id = ? AND last_error_class IS NOT NULL ORDER BY updated_at DESC LIMIT 1",
                (delivery_plan_row["plan_id"],),
            ).fetchone()
            last_delivery_success_row = connection.execute(
                "SELECT subspace_message_id FROM delivery_entries WHERE plan_id = ? AND status = 'succeeded' ORDER BY updated_at DESC LIMIT 1",
                (delivery_plan_row["plan_id"],),
            ).fetchone()
        else:
            delivery_counts = {}
            overdue_count = 0
            last_delivery_error_row = None
            last_delivery_success_row = None
        all_delivery_counts = {
            row["status"]: row["count"]
            for row in connection.execute("SELECT status, COUNT(*) AS count FROM delivery_entries GROUP BY status")
        }
        publish_counts = {
            row["status"]: row["count"]
            for row in connection.execute("SELECT status, COUNT(*) AS count FROM publish_attempts GROUP BY status")
        }
        skipped_counts = {
            row["reason_code"]: row["count"]
            for row in connection.execute("SELECT reason_code, COUNT(*) AS count FROM skipped_items GROUP BY reason_code")
        }
        embedding_failure_count = connection.execute("SELECT COUNT(*) FROM embeddings WHERE status = 'failed'").fetchone()[0]
        last_summary = json.loads(run_row["summary_json"]) if run_row and run_row["summary_json"] else None
        return {
            "publish": snapshot,
            "scheduler": dict(scheduler_row) if scheduler_row else None,
            "delivery": {
                "mode": snapshot.get("delivery_mode") if snapshot else (delivery_plan_row["mode"] if delivery_plan_row else None),
                "active_plan_id": delivery_plan_row["plan_id"] if delivery_plan_row else None,
                "counts_by_status": delivery_counts,
                "pending_count": delivery_counts.get("pending", 0),
                "overdue_count": overdue_count,
                "retry_pending_count": delivery_counts.get("retry_pending", 0),
                "succeeded_count": delivery_counts.get("succeeded", 0),
                "last_successful_subspace_message_id": last_delivery_success_row["subspace_message_id"] if last_delivery_success_row else None,
                "last_delivery_error": dict(last_delivery_error_row) if last_delivery_error_row else None,
            },
            "last_run": ({**dict(run_row), "summary": last_summary} if run_row else None),
            "counts": {
                "packages": package_count,
                "delivery_entries_by_status": all_delivery_counts,
                "publish_attempts_by_status": publish_counts,
                "skipped_items_by_reason": skipped_counts,
                "embedding_failures": embedding_failure_count,
            },
            "prime": dict(prime_row) if prime_row else None,
            "last_runtime_event": dict(runtime_event_row) if runtime_event_row else None,
            "last_reload_failure": dict(reload_failure_row or scheduler_reload_failure_row) if (reload_failure_row or scheduler_reload_failure_row) else None,
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


def request_control_action(config_path: Path, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    pid = runtime_service_pid(config_path)
    if pid is None:
        raise PipelineError("No running Argus service is recorded")
    config = load_runtime_config(config_path)
    connection = connect_database(config.database_path)
    now = utc_now()
    request_id = "sha256:" + hashlib.sha256(
        "control:v0\n{}\n{}\n{}".format(action, iso_z(now), time.monotonic_ns()).encode("utf-8")
    ).hexdigest()
    try:
        connection.execute(
            "INSERT INTO control_requests VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL)",
            (request_id, iso_z(now), action, json.dumps(payload, sort_keys=True), "pending"),
        )
        connection.commit()
    finally:
        connection.close()
    os.kill(pid, signal.SIGHUP)
    return {"request_id": request_id, "action": action, "status": "pending", "pid": pid}


def _pid_matches_config(pid: int, config_path: Path) -> bool:
    try:
        completed = subprocess.run(["ps", "-p", str(pid), "-o", "command="], check=False, capture_output=True, text=True)
    except OSError:
        return False
    command = completed.stdout.strip()
    return completed.returncode == 0 and str(config_path) in command and "argus" in command


def run_source_health(db_path: Path) -> List[Dict[str, Any]]:
    connection = connect_database_readonly(db_path)
    try:
        return [
            {
                **json.loads(row["health_json"]),
                "last_run_id": row["last_run_id"],
                "consecutive_failures": row["consecutive_failures"],
                "total_successes": row["total_successes"],
                "total_failures": row["total_failures"],
            }
            for row in connection.execute("SELECT * FROM source_health ORDER BY source_id")
        ]
    finally:
        connection.close()


def run_embedding_doctor(config_path: Path, text: str) -> Dict[str, Any]:
    server = ArgusServer(config_path, register_service=False)
    try:
        candidate = {
            "report_id": "sha256:" + hashlib.sha256(("embedding-doctor:v0\n" + text).encode("utf-8")).hexdigest(),
            "metadata": {"embedding_text": text},
            "supplied_embeddings": [],
        }
        embedding, failure = server._embedding_for_candidate(candidate, server.clock.now())
        if failure:
            raise PipelineError("embedding doctor failed: {}: {}".format(failure.get("class"), failure.get("message")))
        if embedding is None:
            raise PipelineError("embedding doctor failed: no embedding returned")
        real_model_backed = (
            server.config.embedding.backend == "openai"
            and embedding.get("provider") == OPENAI_EMBEDDING_PROVIDER
            and embedding.get("model") == OPENAI_EMBEDDING_MODEL
            and int(embedding.get("dimensions") or 0) == OPENAI_EMBEDDING_DIMENSIONS
            and embedding.get("space_id") == OPENAI_EMBEDDING_SPACE_ID
            and bool(embedding.get("vector"))
        )
        if not real_model_backed:
            raise PipelineError("embedding doctor failed: embedding is not OpenAI model-backed")
        return {
            "ok": True,
            "real_model_backed": True,
            "provider": embedding.get("provider"),
            "model": embedding.get("model"),
            "dimensions": embedding.get("dimensions"),
            "space_id": embedding.get("space_id"),
            "vector_hash": embedding.get("vector_hash"),
            "input_hash": embedding.get("input_hash"),
            "backend_request_id": embedding.get("backend_request_id"),
        }
    finally:
        server.close()


def explain_skip(db_path: Path, run_id: str) -> Dict[str, Any]:
    connection = connect_database_readonly(db_path)
    try:
        row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise PipelineError("Unknown run: {}".format(run_id))
        summary = json.loads(row["summary_json"])
        skipped_rows = connection.execute(
            "SELECT * FROM skipped_items WHERE run_id = ? ORDER BY source_id, report_id, reason_code",
            (run_id,),
        ).fetchall()
        duplicate_rows = connection.execute(
            "SELECT * FROM dedupe_decisions WHERE run_id = ? AND decision = 'exact_duplicate' ORDER BY source_id, report_id",
            (run_id,),
        ).fetchall()
        return {
            "run": dict(row),
            "summary": summary,
            "skipped_items": [
                {
                    **dict(skipped),
                    "detail": json.loads(skipped["detail_json"]),
                }
                for skipped in skipped_rows
            ],
            "exact_duplicates": [
                {
                    "source_id": duplicate["source_id"],
                    "report_id": duplicate["report_id"],
                    "duplicate_of_report_id": duplicate["duplicate_of_report_id"],
                    "selected_key_type": duplicate["duplicate_key_type"],
                    "selected_key_scope": duplicate["duplicate_key_scope"],
                    "selected_key_hash": duplicate["duplicate_key_hash"],
                    "selected_key_precedence": duplicate["duplicate_key_precedence"],
                    "normalized_value_preview": duplicate["duplicate_key_value_preview"],
                    "reason": duplicate["reason"],
                    "candidate": json.loads(duplicate["detail_json"]),
                    "cross_source_note": "source-local exact dedupe only; cross-source overlap is not suppressed",
                }
                for duplicate in duplicate_rows
            ],
        }
    finally:
        connection.close()
