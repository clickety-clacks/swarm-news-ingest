from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

import argus.server as server_module
from argus.pipeline import command_main, run_pipeline_for_sources
from argus.server import ArgusServer, FakeClock, load_runtime_config

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "argus"
FEEDS = FIXTURE_ROOT / "feeds"
NOW = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)


def write_config(root: Path, *, mode: str = "interval", interval: str = "1h", publish: dict | None = None, embedding: dict | bool | None = None, schedule: dict | None = None) -> Path:
    fixture_dir = root / "feeds"
    fixture_dir.mkdir(exist_ok=True)
    shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "stateful-source.xml")
    config = {
        "runtime": {
            "database_path": str(root / "argus.sqlite3"),
            "output_dir": str(root / "out"),
            "fixture_dir": str(fixture_dir),
        },
        "schedule": {
            "mode": mode,
            "interval": interval,
            "jitter_seconds": 0,
            "run_on_startup_if_due": True,
            "missed_tick_policy": "coalesce_one",
        },
        "publish": publish or {"state": "inactive"},
        "embedding": (
            {
                "backend": "openai",
                "provider": "openai",
                "model": "text-embedding-3-small",
                "dimensions": 1536,
                "space_id": "openai:text-embedding-3-small:1536:v1",
            }
            if embedding is None
            else ({} if embedding is False else embedding)
        ),
        "sources": [
            {
                "id": "stateful-source",
                "display_name": "Stateful Source",
                "source_class": "editorial",
                "feed_url": "https://fixture.invalid/stateful-source.xml",
                "site_url": "https://stateful.example/",
                "adapter": "rss",
                "enabled": True,
                "freshness_window_hours": 72,
                "authority_score": 0.75,
            }
        ],
    }
    if schedule:
        config["schedule"].update(schedule)
    path = root / "argus.yaml"
    path.write_text(yaml.safe_dump(config))
    return path


def rows(db_path: Path, table: str):
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(f"SELECT * FROM {table}")]
    finally:
        connection.close()


def write_fake_embedder(root: Path) -> Path:
    script = root / "fake_embedder.py"
    script.write_text(
        """#!/usr/bin/env python3
import json
import sys

request = json.loads(sys.stdin.read())
vector_seed = sum(ord(ch) for ch in request["request_id"])
print(json.dumps({
    "request_id": request["request_id"],
    "space_id": request["space_id"],
    "provider": request["provider"],
    "model": request["model"],
    "dimensions": request["dimensions"],
    "vector": [float(vector_seed % 17)],
    "backend_request_id": "fake-" + request["request_id"][-8:],
}))
"""
    )
    script.chmod(0o755)
    return script


def write_failing_embedder(root: Path) -> Path:
    script = root / "failing_embedder.py"
    script.write_text(
        """#!/usr/bin/env python3
import sys

sys.stderr.write("embedding failed\\n")
sys.exit(2)
"""
    )
    script.chmod(0o755)
    return script


def write_json_failing_embedder(root: Path) -> Path:
    script = root / "json_failing_embedder.py"
    script.write_text(
        """#!/usr/bin/env python3
import json
import sys

print(json.dumps({"error_class": "embed_invalid_response", "message": "bad vector", "retry_eligible": True}))
sys.exit(2)
"""
    )
    script.chmod(0o755)
    return script


class ServerTests(unittest.TestCase):
    def setUp(self):
        self._original_request_openai_embedding = server_module.request_openai_embedding
        self._original_openai_api_key = server_module.openai_api_key
        server_module.openai_api_key = lambda: "test-openai-key"
        server_module.request_openai_embedding = self._fake_openai_embedding

    def tearDown(self):
        server_module.request_openai_embedding = self._original_request_openai_embedding
        server_module.openai_api_key = self._original_openai_api_key

    @staticmethod
    def _fake_openai_embedding(text, model, dimensions, api_key):
        return {
            "object": "list",
            "model": model,
            "backend_request_id": "req_test_openai",
            "data": [
                {
                    "object": "embedding",
                    "index": 0,
                    "embedding": [float((sum(ord(ch) for ch in text) + index) % 23) / 23.0 for index in range(dimensions)],
                }
            ],
        }

    def test_default_scheduler_is_interval_one_hour_inactive(self):
        with TemporaryDirectory() as tmpdir:
            path = write_config(Path(tmpdir), mode="interval", publish={"state": "inactive"})
            config = load_runtime_config(path)
            self.assertEqual(config.scheduler.mode, "interval")
            self.assertEqual(config.scheduler.interval_seconds, 3600)
            self.assertEqual(config.publish.state, "inactive")

    def test_top_level_help_exposes_server_commands(self):
        output = StringIO()
        with self.assertRaises(SystemExit) as raised:
            with redirect_stdout(output):
                command_main(["--help"])
        self.assertEqual(raised.exception.code, 0)
        self.assertIn("serve", output.getvalue())
        self.assertIn("run-cycle", output.getvalue())
        self.assertIn("embedding-doctor", output.getvalue())

    def test_checked_in_server_config_is_racter_ready_and_inactive(self):
        config = load_runtime_config(Path("config/argus.example.yaml"))
        self.assertEqual(config.database_path, Path("/var/lib/argus/argus.sqlite3"))
        self.assertEqual(config.output_dir, Path("/var/lib/argus"))
        self.assertEqual(config.scheduler.interval_seconds, 3600)
        self.assertEqual(config.source_fetch_concurrency, 4)
        self.assertEqual(config.publish.state, "inactive")
        self.assertFalse(config.publish.live_approval)
        self.assertTrue(config.publish.require_embeddings)
        self.assertEqual(config.publish.subspace_daemon_socket, "~/.openclaw/subspace-daemon/daemon.sock")
        self.assertEqual(config.publish.subspace_daemon_api_path, "/v1/messages")
        self.assertEqual(config.embedding.backend, "openai")
        self.assertEqual(config.embedding.provider, "openai")
        self.assertEqual(config.embedding.model, "text-embedding-3-small")
        self.assertEqual(config.embedding.dimensions, 1536)
        self.assertEqual(config.embedding.space_id, "openai:text-embedding-3-small:1536:v1")
        self.assertEqual(len(config.sources), 13)
        self.assertTrue(all(server_module.SOURCE_ID_RE.match(source.id) for source in config.sources))
        self.assertTrue(any(source.cadence_interval_seconds == 7200 for source in config.sources))
        self.assertTrue(all(source.authority_score is not None for source in config.sources))
        self.assertTrue(all(source.fixture_payload_path for source in config.sources))
        self.assertTrue(all(Path(source.fixture_payload_path).exists() for source in config.sources if source.fixture_payload_path))

    def test_checked_in_recommended_sources_are_fixture_backed(self):
        with TemporaryDirectory() as tmpdir:
            config = load_runtime_config(Path("config/argus.example.yaml"))
            exit_code, summary = run_pipeline_for_sources(
                config.sources,
                "config/argus.example.yaml",
                Path(tmpdir),
                NOW,
                fixture_dir=Path("unused-fixture-dir"),
                state_path=None,
                state_read=False,
                state_write=False,
                dedupe_in_pipeline=False,
            )
            self.assertEqual(exit_code, 0)
            self.assertGreaterEqual(summary["counts"]["normalized_entries"], 30)

    def test_checked_in_e2e_canary_config_is_single_item_inactive(self):
        config = load_runtime_config(Path("config/argus.e2e-canary.example.yaml"))
        self.assertEqual(config.scheduler.mode, "manual")
        self.assertEqual(config.source_fetch_concurrency, 1)
        self.assertEqual(config.publish.state, "inactive")
        self.assertFalse(config.publish.live_approval)
        self.assertEqual(config.publish.subspace_endpoint, "https://subspace.swarm.channel")
        self.assertEqual(config.publish.subspace_daemon_socket, "~/.openclaw/subspace-daemon/daemon.sock")
        self.assertEqual(config.publish.subspace_daemon_api_path, "/v1/messages")
        self.assertEqual(config.embedding.backend, "openai")
        self.assertEqual(config.embedding.provider, "openai")
        self.assertEqual(config.embedding.model, "text-embedding-3-small")
        self.assertEqual(config.embedding.dimensions, 1536)
        self.assertEqual(config.embedding.space_id, "openai:text-embedding-3-small:1536:v1")
        self.assertEqual(config.fixture_dir, Path("testdata/feeds"))
        self.assertEqual(len(config.sources), 1)
        self.assertEqual(config.sources[0].fixture_payload_path, "testdata/feeds/argus-e2e-canary.rss.xml")
        with TemporaryDirectory() as tmpdir:
            exit_code, summary = run_pipeline_for_sources(
                config.sources,
                "config/argus.e2e-canary.example.yaml",
                Path(tmpdir),
                NOW,
                fixture_dir=Path("unused-fixture-dir"),
                state_path=None,
                state_read=False,
                state_write=False,
                dedupe_in_pipeline=False,
            )
        self.assertEqual(exit_code, 0)
        self.assertEqual(summary["counts"]["normalized_entries"], 1)

    def test_checked_in_shrdlu_receptor_pack_uses_openai_space_and_veto(self):
        pack = json.loads(Path("config/receptors/argus-shrdlu-e2e-receptors.json").read_text())
        receptors = pack["receptors"]
        self.assertEqual({receptor["space_id"] for receptor in receptors}, {"openai:text-embedding-3-small:1536:v1"})
        self.assertIn("veto", {receptor["class"] for receptor in receptors})
        self.assertIn("broad", {receptor["class"] for receptor in receptors})
        self.assertTrue(all("query" in receptor and "threshold" in receptor for receptor in receptors))
        self.assertFalse(any("negative_examples" in receptor for receptor in receptors))
        self.assertNotIn("anti_receptor", {receptor["class"] for receptor in receptors})

    def test_e2e_canary_config_can_emit_exactly_one_guarded_live_attempt(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = yaml.safe_load(Path("config/argus.e2e-canary.example.yaml").read_text())
            config["runtime"]["database_path"] = str(root / "argus.sqlite3")
            config["runtime"]["output_dir"] = str(root / "out")
            config["publish"]["state"] = "active"
            config["publish"]["live_approval"] = True
            path = root / "canary.yaml"
            path.write_text(yaml.safe_dump(config))
            calls = []
            original_post = server_module.post_json_over_unix_socket

            def fake_post(socket_path, api_path, payload):
                calls.append(payload)
                return {
                    "ok": True,
                    "results": [
                        {
                            "server": payload["server"],
                            "sent": True,
                            "subspace_message_id": "canary-message",
                            "idempotency_key": payload["idempotency_key"],
                        }
                    ],
                }

            server_module.post_json_over_unix_socket = fake_post
            server = ArgusServer(path, clock=FakeClock(NOW), register_service=False)
            original_cwd = os.getcwd()
            try:
                os.chdir(root)
                exit_code, summary = server.manual_cycle(max_live_publishes=1)
            finally:
                os.chdir(original_cwd)
                server_module.post_json_over_unix_socket = original_post
                server.close()
            self.assertEqual(exit_code, 0)
            self.assertEqual(summary["counts"]["normalized_entries"], 1)
            self.assertEqual(len(calls), 1)
            package = json.loads(calls[0]["text"])
            self.assertEqual(package["body"]["title"], "Argus Subetha shrdlu E2E canary")
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 1)

    def test_embedding_text_uses_canonical_report_fields(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            config = load_runtime_config(path)
            exit_code, _ = run_pipeline_for_sources(
                config.sources,
                str(path),
                root / "out",
                NOW,
                fixture_dir=root / "feeds",
                state_path=None,
                state_read=False,
                state_write=False,
            )
            self.assertEqual(exit_code, 0)
            candidate = json.loads((root / "out" / "publish-candidates.jsonl").read_text().splitlines()[0])
            embedding_text = candidate["metadata"]["embedding_text"]
            self.assertIn("Title: ", embedding_text)
            self.assertIn("Source: Stateful Source", embedding_text)
            self.assertIn("Published: ", embedding_text)
            self.assertIn("URL: ", embedding_text)
            self.assertIn("Summary: ", embedding_text)

    def test_runtime_config_validates_source_fetch_concurrency(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            config = yaml.safe_load(path.read_text())
            config["runtime"]["source_fetch_concurrency"] = 16
            path.write_text(yaml.safe_dump(config))
            self.assertEqual(load_runtime_config(path).source_fetch_concurrency, 16)
            config["runtime"]["source_fetch_concurrency"] = 17
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "source_fetch_concurrency"):
                load_runtime_config(path)

    def test_enabled_sources_require_exactly_one_endpoint_family(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            config = yaml.safe_load(path.read_text())
            del config["sources"][0]["feed_url"]
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "exactly one fetch endpoint"):
                load_runtime_config(path)
            config["sources"][0]["feed_url"] = "https://fixture.invalid/stateful-source.xml"
            config["sources"][0]["api_url"] = "https://fixture.invalid/api"
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "exactly one fetch endpoint"):
                load_runtime_config(path)
            del config["sources"][0]["feed_url"]
            config["sources"][0]["adapter"] = "rss"
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "Feed adapter source"):
                load_runtime_config(path)
            config["sources"][0]["adapter"] = "api"
            config["sources"][0]["source_class"] = "nonsense"
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "Invalid source_class"):
                load_runtime_config(path)
            config["sources"][0]["source_class"] = "research"
            config["sources"][0]["adapter"] = "arxiv_atom"
            config["sources"][0]["api_url"] = "https://fixture.invalid/arxiv"
            path.write_text(yaml.safe_dump(config))
            self.assertEqual(load_runtime_config(path).sources[0].adapter, "arxiv_atom")
            config["sources"][0]["adapter"] = "generic_rss"
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "Invalid source adapter"):
                load_runtime_config(path)
            config["sources"][0]["adapter"] = "api"
            del config["sources"][0]["authority_score"]
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "authority_score"):
                load_runtime_config(path)

    def test_sqlite_uses_wal_and_busy_timeout(self):
        with TemporaryDirectory() as tmpdir:
            connection = server_module.connect_database(Path(tmpdir) / "argus.sqlite3")
            try:
                self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal")
                self.assertEqual(connection.execute("PRAGMA busy_timeout").fetchone()[0], 5000)
            finally:
                connection.close()

    def test_empty_db_inactive_startup_runs_without_prime_or_publish(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                result = server.tick()
            finally:
                server.close()
            self.assertIsNotNone(result)
            self.assertEqual(len(rows(root / "argus.sqlite3", "prime_events")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)
            self.assertEqual(len(list((root / "out" / "runs").glob("20260429T120000Z-scheduled-*/publish-candidates.jsonl"))), 1)

    def test_manual_mode_does_not_run_until_manual_cycle(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual")
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                self.assertIsNone(server.tick())
                clock.advance(7200)
                self.assertIsNone(server.tick())
                exit_code, summary = server.manual_cycle()
            finally:
                server.close()
            self.assertEqual(exit_code, 0)
            self.assertEqual(summary["run_kind"], "manual")
            self.assertEqual(len(rows(root / "argus.sqlite3", "runs")), 1)
            self.assertEqual(len(rows(root / "argus.sqlite3", "prime_events")), 0)

    def test_manual_to_interval_reload_waits_one_interval_without_backfill(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual")
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                config = yaml.safe_load(path.read_text())
                config["schedule"]["mode"] = "interval"
                config["schedule"]["interval"] = "1h"
                path.write_text(yaml.safe_dump(config))
                server.reload()
                state = dict(server.connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone())
                self.assertEqual(state["next_due_at"], "2026-04-29T13:00:00Z")
                self.assertIsNone(server.tick())
                clock.advance(3600)
                self.assertIsNotNone(server.tick())
            finally:
                server.close()

    def test_prime_creates_baseline_only_no_packages_or_attempts(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                exit_code, summary = server.prime()
            finally:
                server.close()
            self.assertEqual(exit_code, 0)
            self.assertTrue(summary["prime"])
            run_dir = next((root / "out" / "runs").glob("20260429T120000Z-prime-*"))
            self.assertTrue((run_dir / "prime-baseline.json").exists())
            self.assertFalse((run_dir / "publish-candidates.jsonl").exists())
            self.assertFalse((run_dir / "package-candidates.jsonl").exists())
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

    def test_blocked_active_reload_and_approved_active_forward_only(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            fixture_dir = root / "feeds"
            fixture_dir.mkdir(exist_ok=True)
            (fixture_dir / "stateful-source.xml").write_text((FEEDS / "stateful_source.xml").read_text())
            config = yaml.safe_load(path.read_text())
            config["runtime"]["fixture_dir"] = str(fixture_dir)
            path.write_text(yaml.safe_dump(config))

            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

                config["publish"] = {"state": "active", "live_approval": False, "subspace_endpoint": "https://subspace.invalid"}
                path.write_text(yaml.safe_dump(config))
                server.reload()
                snapshot = json.loads(rows(root / "argus.sqlite3", "runtime_config_snapshots")[-1]["snapshot_json"])
                self.assertEqual(snapshot["effective_mode"], "blocked")

                config["publish"] = {
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                }
                path.write_text(yaml.safe_dump(config))
                server.reload()
                clock.advance(3600)
                (fixture_dir / "stateful-source.xml").write_text(
                    (FEEDS / "stateful_source.xml").read_text().replace(
                        "  </channel>\n</rss>",
                        """    <item>\n      <guid>stateful-guid-3</guid>\n      <title>Third story</title>\n      <link>https://stateful.example/story-3</link>\n      <pubDate>Wed, 30 Apr 2026 10:00:00 +0000</pubDate>\n      <description>Brand new story.</description>\n    </item>\n  </channel>\n</rss>""",
                    )
                )
                server.tick()
            finally:
                server.close()
            packages = rows(root / "argus.sqlite3", "packages")
            attempts = rows(root / "argus.sqlite3", "publish_attempts")
            self.assertEqual(sum(row["active_eligible"] for row in packages), 1)
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "failed")

    def test_invalid_reload_fails_closed_from_active(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding=False,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                },
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                self.assertEqual(server.status()["publish"]["effective_mode"], "active")
                path.write_text("publish: [")
                server.reload()
                self.assertNotEqual(server.status()["publish"]["effective_mode"], "active")
            finally:
                server.close()

    def test_set_publish_state_survives_service_restart(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "inactive",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                },
            )
            first = ArgusServer(path, clock=FakeClock(NOW))
            try:
                first.set_publish_state("active")
                self.assertEqual(first.status()["publish"]["effective_mode"], "active")
                activation_observed_at = first.status()["publish"]["activation_observed_at"]
            finally:
                first.close()
            second_clock = FakeClock(NOW)
            second_clock.advance(3600)
            second = ArgusServer(path, clock=second_clock)
            try:
                self.assertEqual(second.status()["publish"]["requested_mode"], "active")
                self.assertEqual(second.status()["publish"]["effective_mode"], "active")
                self.assertEqual(second.status()["publish"]["activation_observed_at"], activation_observed_at)
            finally:
                second.close()

    def test_reactivation_after_inactive_gets_new_activation_watermark(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "inactive",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                },
            )
            clock = FakeClock(NOW)
            first = ArgusServer(path, clock=clock)
            try:
                active_one = first.set_publish_state("active")
            finally:
                first.close()
            clock.advance(60)
            second = ArgusServer(path, clock=clock)
            try:
                second.set_publish_state("inactive")
                clock.advance(60)
                active_two = second.set_publish_state("active")
            finally:
                second.close()
            self.assertNotEqual(active_one["activation_observed_at"], active_two["activation_observed_at"])

    def test_no_overlap_decision_records_skip(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.connection.execute("UPDATE scheduler_state SET running_run_id = ?", ("in-flight",))
                server.connection.commit()
                self.assertIsNone(server.tick())
            finally:
                server.close()
            events = rows(root / "argus.sqlite3", "scheduler_events")
            self.assertIn("cycle_skipped_already_running", [row["event_type"] for row in events])

    def test_manual_and_prime_obey_no_overlap(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.connection.execute("UPDATE scheduler_state SET running_run_id = ?", ("in-flight",))
                server.connection.commit()
                _, manual_summary = server.manual_cycle()
                _, prime_summary = server.prime()
            finally:
                server.close()
            self.assertEqual(manual_summary["skip_reason"], "cycle_already_running")
            self.assertEqual(prime_summary["skip_reason"], "cycle_already_running")
            self.assertEqual(len(rows(root / "argus.sqlite3", "runs")), 0)

    def test_restart_recovers_stale_running_marker(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            first = ArgusServer(path, clock=FakeClock(NOW))
            try:
                first.connection.execute("UPDATE scheduler_state SET running_run_id = ?", ("stale-run",))
                first.connection.commit()
            finally:
                first.close()

            second = ArgusServer(path, clock=FakeClock(NOW))
            try:
                state = dict(second.connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone())
            finally:
                second.close()
            self.assertIsNone(state["running_run_id"])
            events = rows(root / "argus.sqlite3", "scheduler_events")
            self.assertIn("cycle_recovered_after_restart", [row["event_type"] for row in events])

    def test_restart_marks_started_run_failed_after_crash_before_pipeline_completion(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            original_run_pipeline = server_module.run_pipeline_for_sources

            def crash_after_cycle_start(*args, **kwargs):
                raise KeyboardInterrupt("simulated process crash")

            server_module.run_pipeline_for_sources = crash_after_cycle_start
            first = ArgusServer(path, clock=FakeClock(NOW))
            try:
                with self.assertRaises(KeyboardInterrupt):
                    first.tick()
            finally:
                server_module.run_pipeline_for_sources = original_run_pipeline
                first.close()
            run_id = rows(root / "argus.sqlite3", "runs")[0]["run_id"]
            connection = sqlite3.connect(root / "argus.sqlite3")
            try:
                connection.execute("UPDATE scheduler_state SET running_run_id = ?", (run_id,))
                connection.commit()
            finally:
                connection.close()

            second = ArgusServer(path, clock=FakeClock(NOW))
            try:
                state = dict(second.connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone())
            finally:
                second.close()
            self.assertIsNone(state["running_run_id"])
            run_row = rows(root / "argus.sqlite3", "runs")[0]
            self.assertEqual(run_row["status"], "failed")
            self.assertIn("cycle recovered after restart", json.loads(run_row["summary_json"])["post_pipeline_error"]["message"])
            self.assertEqual(len(rows(root / "argus.sqlite3", "source_config_snapshots")), 1)

    def test_restart_marks_pending_publish_attempt_unknown(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            first = ArgusServer(path, clock=FakeClock(NOW))
            try:
                run_id = "run-with-pending-publish"
                snapshot = latest = rows(root / "argus.sqlite3", "runtime_config_snapshots")[-1]
                first.connection.execute(
                    "INSERT OR REPLACE INTO runs VALUES (?, ?, ?, NULL, ?, ?, ?, ?)",
                    (
                        run_id,
                        "manual",
                        "2026-04-29T12:00:00Z",
                        "running",
                        str(root / "out" / "runs" / run_id),
                        snapshot["snapshot_id"],
                        json.dumps({"run_id": run_id, "run_kind": "manual", "exit_status": "running"}),
                    ),
                )
                first.connection.execute(
                    "INSERT INTO packages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "package-1",
                        "report-1",
                        "1",
                        "argus-local-v0",
                        "vector-hash",
                        1,
                        json.dumps({"package_id": "package-1", "supplied_embeddings": [{"space_id": "argus-local-v0", "vector": [1.0]}]}),
                        run_id,
                        "2026-04-29T12:00:00Z",
                    ),
                )
                first.connection.execute(
                    """
                    INSERT INTO publish_attempts
                    (attempt_id, package_id, publish_target_key, publish_idempotency_key, effective_publish_snapshot_id,
                     attempt_number, mode, status, attempted_at, completed_at, subspace_message_id, response_json, error_class, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "attempt-1",
                        "package-1",
                        "target-1",
                        "key-1",
                        snapshot["snapshot_id"],
                        1,
                        "active",
                        "pending",
                        "2026-04-29T12:00:00Z",
                        None,
                        None,
                        None,
                        None,
                        None,
                    ),
                )
                first.connection.execute("UPDATE scheduler_state SET running_run_id = ?", (run_id,))
                first.connection.commit()
            finally:
                first.close()

            second = ArgusServer(path, clock=FakeClock(NOW))
            try:
                attempt = rows(root / "argus.sqlite3", "publish_attempts")[0]
            finally:
                second.close()
            self.assertEqual(attempt["status"], "unknown")
            self.assertEqual(attempt["error_class"], "RuntimeError")
            self.assertIn("before publish result was recorded", attempt["error_message"])

    def test_same_second_manual_runs_get_distinct_run_ids(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual")
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                first = server.manual_cycle()[1]["run_id"]
                second = server.manual_cycle()[1]["run_id"]
            finally:
                server.close()
            self.assertNotEqual(first, second)

    def test_require_embeddings_does_not_queue_unembedded_publish_attempts(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding=False,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "require_embeddings": True,
                },
            )
            with self.assertRaisesRegex(Exception, "missing_embedding_config"):
                ArgusServer(path, clock=FakeClock(NOW))

    def test_fake_embedding_backend_blocks_active_live_config(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "require_embeddings": True,
                },
            )
            config = yaml.safe_load(path.read_text())
            config["embedding"] = {
                "backend": "cli",
                "command": "/definitely/not/invoked",
                "provider": "fake",
                "model": "text-embedding-3-small",
                "dimensions": 1536,
                "space_id": "openai:text-embedding-3-small:1536:v1",
            }
            path.write_text(yaml.safe_dump(config))
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                self.assertEqual(server.status()["publish"]["effective_mode"], "blocked")
                self.assertEqual(server.status()["publish"]["blocked_reason"], "non_production_embedding_backend")
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

    def test_cli_embedding_requires_provider(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "require_embeddings": True,
                },
            )
            config = yaml.safe_load(path.read_text())
            del config["embedding"]["provider"]
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "missing_embedding_config"):
                ArgusServer(path, clock=FakeClock(NOW))

    def test_openai_embedding_backend_records_model_backed_vector(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive", "require_embeddings": True})
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            embedding_rows = rows(root / "argus.sqlite3", "embeddings")
            self.assertEqual({row["provider"] for row in embedding_rows}, {"openai"})
            self.assertEqual({row["model"] for row in embedding_rows}, {"text-embedding-3-small"})
            self.assertEqual({row["embedding_space_id"] for row in embedding_rows}, {"openai:text-embedding-3-small:1536:v1"})
            self.assertEqual({row["dimensions"] for row in embedding_rows}, {1536})
            self.assertTrue(all(len(json.loads(row["vector_json"])) == 1536 for row in embedding_rows))

    def test_embedding_doctor_proves_real_model_backed_openai_metadata(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive", "require_embeddings": True})
            result = server_module.run_embedding_doctor(path, "Argus receptor-compatible embedding doctor")
            self.assertTrue(result["ok"])
            self.assertTrue(result["real_model_backed"])
            self.assertEqual(result["provider"], "openai")
            self.assertEqual(result["model"], "text-embedding-3-small")
            self.assertEqual(result["dimensions"], 1536)
            self.assertEqual(result["space_id"], "openai:text-embedding-3-small:1536:v1")
            self.assertTrue(result["vector_hash"].startswith("sha256:"))

    def test_openai_embedding_errors_redact_api_key_material(self):
        class Response:
            status_code = 401
            text = "Incorrect API key provided: sk-proj-sensitive-fragment"
            headers = {}

            @staticmethod
            def json():
                return {"error": {"message": "Incorrect API key provided: sk-proj-sensitive-fragment"}}

        original_post = server_module.requests.post
        server_module.requests.post = lambda *args, **kwargs: Response()
        try:
            with self.assertRaisesRegex(Exception, r"\[redacted-openai-key\]"):
                self._original_request_openai_embedding("text", "text-embedding-3-small", 1536, "sk-proj-sensitive-fragment")
        finally:
            server_module.requests.post = original_post

    def test_embedding_doctor_rejects_fake_cli_backend(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding={
                    "backend": "subspace-embedding-cli",
                    "command": str(write_fake_embedder(root)),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            with self.assertRaisesRegex(Exception, "not OpenAI model-backed"):
                server_module.run_embedding_doctor(path, "fake")

    def test_bin_wrapper_reports_pipeline_errors_without_traceback(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding={
                    "backend": "subspace-embedding-cli",
                    "command": str(write_fake_embedder(root)),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            result = subprocess.run(
                ["bin/argus", "embedding-doctor", "--config", str(path)],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("not OpenAI model-backed", result.stderr)
            self.assertNotIn("Traceback", result.stderr)

    def test_local_deterministic_backend_response_is_rejected(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            script = root / "deterministic_stub.py"
            script.write_text(
                """#!/usr/bin/env python3
import json
import sys
request = json.loads(sys.stdin.read())
print(json.dumps({
    "space_id": request["space_id"],
    "provider": request["provider"],
    "model": request["model"],
    "dimensions": request["dimensions"],
    "vector": [0.0],
    "backend_request_id": "local-deterministic-test"
}))
"""
            )
            script.chmod(0o755)
            path = write_config(
                root,
                embedding={
                    "backend": "subspace-embedding-cli",
                    "command": str(script),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            failures = [json.loads(row["failure_json"]) for row in rows(root / "argus.sqlite3", "embeddings")]
            self.assertEqual({failure["message"] for failure in failures}, {"deterministic fake embedding backend is not allowed"})

    def test_embedding_failure_records_failed_embedding_and_artifact(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding={
                    "backend": "cli",
                    "command": str(write_failing_embedder(root)),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual({row["status"] for row in rows(root / "argus.sqlite3", "embeddings")}, {"failed"})
            run_dir = next((root / "out" / "runs").glob("20260429T120000Z-scheduled-*"))
            failures = json.loads((run_dir / "embedding-failures.json").read_text())
            summary = json.loads((run_dir / "run-summary.json").read_text())
            digest = json.loads((run_dir / "digest.json").read_text())
            self.assertEqual(len(failures), 2)
            self.assertEqual({failure["class"] for failure in failures}, {"embed_backend_unavailable"})
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            self.assertEqual(summary["counts"]["package_candidates"], 0)
            self.assertEqual(summary["counts"]["publish_candidates"], 0)
            self.assertEqual(digest["candidate_count"], 0)

    def test_embedding_failure_retries_on_later_cycle(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding={
                    "backend": "cli",
                    "command": str(write_failing_embedder(root)),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                config = yaml.safe_load(path.read_text())
                config["embedding"]["command"] = str(write_fake_embedder(root))
                path.write_text(yaml.safe_dump(config))
                server.reload()
                clock.advance(3600)
                server.tick()
            finally:
                server.close()
            self.assertEqual({row["status"] for row in rows(root / "argus.sqlite3", "embeddings")}, {"embedded"})
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)

    def test_embedding_failure_honors_backend_failure_json(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding={
                    "backend": "cli",
                    "command": str(write_json_failing_embedder(root)),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            failure = json.loads(rows(root / "argus.sqlite3", "embeddings")[0]["failure_json"])
            self.assertEqual(failure["class"], "embed_invalid_response")
            self.assertEqual(failure["message"], "bad vector")

    def test_active_no_backfill_even_if_historical_report_reprocessed(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                inactive_package_ids = {row["package_id"] for row in rows(root / "argus.sqlite3", "packages")}
                config = yaml.safe_load(path.read_text())
                config["publish"] = {
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                }
                path.write_text(yaml.safe_dump(config))
                server.reload()
                clock.advance(3600)
                server.tick()
            finally:
                server.close()
            packages = rows(root / "argus.sqlite3", "packages")
            self.assertEqual({row["package_id"] for row in packages}, inactive_package_ids)
            self.assertEqual(sum(row["active_eligible"] for row in packages), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

    def test_invalid_scheduler_reload_keeps_publish_snapshot_when_publish_inputs_unchanged(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                },
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                self.assertEqual(server.status()["publish"]["effective_mode"], "active")
                config = yaml.safe_load(path.read_text())
                for invalid_interval in ("1m", "bogus"):
                    config["schedule"]["interval"] = invalid_interval
                    path.write_text(yaml.safe_dump(config))
                    server.reload()
                    self.assertEqual(server.status()["publish"]["effective_mode"], "active")
                config["schedule"]["interval"] = "1h"
                config["schedule"]["jitter_seconds"] = "bogus"
                path.write_text(yaml.safe_dump(config))
                server.reload()
                self.assertEqual(server.status()["publish"]["effective_mode"], "active")
            finally:
                server.close()
            self.assertIn("scheduler_reload_failed", [row["event_type"] for row in rows(root / "argus.sqlite3", "scheduler_events")])

    def test_invalid_scheduler_reload_with_publish_change_fails_inactive(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                },
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                self.assertEqual(server.status()["publish"]["effective_mode"], "active")
                config = yaml.safe_load(path.read_text())
                config["publish"] = {"state": "inactive"}
                config["schedule"]["interval"] = "1m"
                path.write_text(yaml.safe_dump(config))
                server.reload()
                self.assertEqual(server.status()["publish"]["effective_mode"], "inactive")
            finally:
                server.close()

    def test_reload_persists_new_full_config_snapshot(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual")
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                initial_count = server.connection.execute("SELECT COUNT(*) FROM config_snapshots").fetchone()[0]
                config = yaml.safe_load(path.read_text())
                config["schedule"]["mode"] = "interval"
                config["schedule"]["interval"] = "2h"
                path.write_text(yaml.safe_dump(config))
                server.reload()
                latest_hash = server.connection.execute(
                    "SELECT config_hash FROM runtime_config_snapshots ORDER BY observed_at DESC, rowid DESC LIMIT 1"
                ).fetchone()[0]
                config_snapshot = server.connection.execute(
                    "SELECT config_json FROM config_snapshots WHERE config_hash = ?",
                    (latest_hash,),
                ).fetchone()
                final_count = server.connection.execute("SELECT COUNT(*) FROM config_snapshots").fetchone()[0]
            finally:
                server.close()
            self.assertEqual(final_count, initial_count + 1)
            self.assertIsNotNone(config_snapshot)
            self.assertEqual(json.loads(config_snapshot["config_json"])["schedule"]["interval"], "2h")

    def test_server_config_rejects_invalid_source_id(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            config = yaml.safe_load(path.read_text())
            config["sources"][0]["id"] = "stateful_source"
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(Exception, "Invalid source id"):
                load_runtime_config(path)

    def test_manual_mode_ignores_invalid_interval_value(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual", interval="bogus")
            config = load_runtime_config(path)
            self.assertEqual(config.scheduler.mode, "manual")

    def test_inactive_run_writes_review_artifacts_and_sqlite_state(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            run_dir = next((root / "out" / "runs").glob("20260429T120000Z-scheduled-*"))
            for artifact in [
                "digest.md",
                "digest.json",
                "normalized-items.jsonl",
                "dedupe-decisions.json",
                "skipped-items.json",
                "package-candidates.jsonl",
            ]:
                self.assertTrue((run_dir / artifact).exists(), artifact)
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "report_seen_runs")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "dedupe_keys")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "dedupe_decisions")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "embeddings")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)
            self.assertEqual(rows(root / "argus.sqlite3", "source_health")[0]["total_successes"], 1)
            self.assertEqual(len(rows(root / "argus.sqlite3", "config_snapshots")), 1)
            self.assertEqual(len(rows(root / "argus.sqlite3", "source_config_snapshots")), 1)
            self.assertEqual(len(rows(root / "argus.sqlite3", "raw_fetches")), 1)
            self.assertEqual(len(json.loads((run_dir / "dedupe-decisions.json").read_text())), 2)
            self.assertEqual(json.loads((run_dir / "skipped-items.json").read_text()), [])

    def test_read_only_operator_commands_do_not_create_database(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            missing_db = root / "missing-parent" / "argus.sqlite3"
            with self.assertRaisesRegex(Exception, "does not exist"):
                server_module.run_status(missing_db)
            with self.assertRaisesRegex(Exception, "does not exist"):
                server_module.run_source_health(missing_db)
            with self.assertRaisesRegex(Exception, "does not exist"):
                server_module.explain_skip(missing_db, "missing-run")
            self.assertFalse(missing_db.parent.exists())

    def test_server_ignores_legacy_json_seen_state_for_sqlite_authority(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            legacy_state = root / "out" / "runs" / "state.json"
            legacy_state.parent.mkdir(parents=True)
            legacy_state.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "sources": {
                            "stateful-source": {
                                "validators": {},
                                "seen_identities": [
                                    "feed_entry_id|stateful-guid-1",
                                    "feed_entry_id|stateful-guid-2",
                                ],
                            }
                        },
                    }
                )
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)

    def test_post_pipeline_artifact_failure_records_failed_run(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            original_write_text = Path.write_text
            fail_after_pipeline = {"enabled": False}

            def flaky_write_text(self, data, *args, **kwargs):
                if fail_after_pipeline["enabled"] and self.name == "run-summary.json":
                    raise OSError("simulated artifact failure")
                return original_write_text(self, data, *args, **kwargs)

            server = ArgusServer(path, clock=FakeClock(NOW))
            fail_after_pipeline["enabled"] = True
            Path.write_text = flaky_write_text
            try:
                with self.assertRaisesRegex(OSError, "simulated artifact failure"):
                    server.tick()
            finally:
                Path.write_text = original_write_text
                server.close()
            run_rows = rows(root / "argus.sqlite3", "runs")
            self.assertEqual(len(run_rows), 1)
            self.assertEqual(run_rows[0]["status"], "failed")
            self.assertIn("post_pipeline_error", json.loads(run_rows[0]["summary_json"]))

    def test_post_pipeline_storage_rolls_back_when_decision_artifact_fails(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            original_write_text = Path.write_text
            fail_decision_artifact = {"enabled": False}

            def flaky_write_text(self, data, *args, **kwargs):
                if fail_decision_artifact["enabled"] and self.name == "dedupe-decisions.json":
                    raise OSError("simulated decision artifact failure")
                return original_write_text(self, data, *args, **kwargs)

            server = ArgusServer(path, clock=FakeClock(NOW))
            fail_decision_artifact["enabled"] = True
            Path.write_text = flaky_write_text
            try:
                with self.assertRaisesRegex(OSError, "simulated decision artifact failure"):
                    server.tick()
            finally:
                Path.write_text = original_write_text
                server.close()
            self.assertEqual(rows(root / "argus.sqlite3", "runs")[0]["status"], "failed")
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "dedupe_keys")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "embeddings")), 0)

    def test_post_pipeline_storage_rolls_back_when_package_artifact_fails(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            original_write_text = Path.write_text
            fail_package_artifact = {"enabled": False}

            def flaky_write_text(self, data, *args, **kwargs):
                if fail_package_artifact["enabled"] and self.name == "package-candidates.jsonl":
                    raise OSError("simulated package artifact failure")
                return original_write_text(self, data, *args, **kwargs)

            server = ArgusServer(path, clock=FakeClock(NOW))
            fail_package_artifact["enabled"] = True
            Path.write_text = flaky_write_text
            try:
                with self.assertRaisesRegex(OSError, "simulated package artifact failure"):
                    server.tick()
            finally:
                Path.write_text = original_write_text
                server.close()
            self.assertEqual(rows(root / "argus.sqlite3", "runs")[0]["status"], "failed")
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "dedupe_keys")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "embeddings")), 0)

    def test_post_pipeline_storage_rolls_back_when_summary_artifact_fails(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            original_write_text = Path.write_text
            fail_summary_artifact = {"enabled": False}

            def flaky_write_text(self, data, *args, **kwargs):
                if fail_summary_artifact["enabled"] and self.name == "run-summary.json":
                    raise OSError("simulated summary artifact failure")
                return original_write_text(self, data, *args, **kwargs)

            server = ArgusServer(path, clock=FakeClock(NOW))
            fail_summary_artifact["enabled"] = True
            Path.write_text = flaky_write_text
            try:
                with self.assertRaisesRegex(OSError, "simulated summary artifact failure"):
                    server.tick()
            finally:
                Path.write_text = original_write_text
                server.close()
            self.assertEqual(rows(root / "argus.sqlite3", "runs")[0]["status"], "failed")
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "dedupe_keys")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "embeddings")), 0)

    def test_prime_artifact_failure_marks_prime_event_failed(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            original_write_text = Path.write_text
            fail_prime_artifact = {"enabled": False}

            def flaky_write_text(self, data, *args, **kwargs):
                if fail_prime_artifact["enabled"] and self.name == "prime-baseline.md":
                    raise OSError("simulated prime artifact failure")
                return original_write_text(self, data, *args, **kwargs)

            server = ArgusServer(path, clock=FakeClock(NOW))
            fail_prime_artifact["enabled"] = True
            Path.write_text = flaky_write_text
            try:
                with self.assertRaisesRegex(OSError, "simulated prime artifact failure"):
                    server.prime()
            finally:
                Path.write_text = original_write_text
                server.close()
            event = rows(root / "argus.sqlite3", "prime_events")[0]
            self.assertEqual(event["status"], "failed")
            self.assertIsNotNone(event["completed_at"])
            self.assertIsNotNone(event["run_id"])
            self.assertEqual(rows(root / "argus.sqlite3", "runs")[0]["status"], "failed")

    def test_prime_persists_baseline_dedupe_state_without_packages(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.prime()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "dedupe_keys")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

    def test_source_cadence_override_skips_source_until_due_without_wall_clock_sleep(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            fixture_dir = root / "feeds"
            shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "slow-source.xml")
            config = yaml.safe_load(path.read_text())
            config["sources"].append(
                {
                    "id": "slow-source",
                    "display_name": "Slow Source",
                    "source_class": "editorial",
                    "feed_url": "https://fixture.invalid/slow-source.xml",
                    "site_url": "https://slow.example/",
                    "adapter": "rss",
                    "enabled": True,
                    "freshness_window_hours": 72,
                    "authority_score": 0.75,
                    "cadence_override": "2h",
                }
            )
            path.write_text(yaml.safe_dump(config))

            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                clock.advance(3600)
                server.tick()
            finally:
                server.close()
            source_rows = rows(root / "argus.sqlite3", "source_run_status")
            self.assertEqual(sum(row["source_id"] == "stateful-source" for row in source_rows), 2)
            self.assertEqual(sum(row["source_id"] == "slow-source" for row in source_rows), 1)

    def test_control_commands_request_running_service_instead_of_local_cycle(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual")
            calls = []
            original_pid = server_module.runtime_service_pid
            original_request = server_module.request_control_action
            server_module.runtime_service_pid = lambda config_path: 12345
            server_module.request_control_action = lambda config_path, action, payload: calls.append((action, payload)) or {"status": "pending", "action": action}
            try:
                with redirect_stdout(StringIO()):
                    self.assertEqual(command_main(["prime", "--config", str(path)]), 0)
                with redirect_stdout(StringIO()):
                    self.assertEqual(command_main(["run-cycle", "--config", str(path), "--reason", "manual", "--max-live-publishes", "1"]), 0)
                with redirect_stdout(StringIO()):
                    self.assertEqual(command_main(["set-publish-state", "--config", str(path), "--state", "active"]), 0)
            finally:
                server_module.runtime_service_pid = original_pid
                server_module.request_control_action = original_request
            self.assertEqual([call[0] for call in calls], ["prime", "run-cycle", "set-publish-state"])
            self.assertEqual(calls[1][1]["max_live_publishes"], 1)
            self.assertFalse((root / "argus.sqlite3").exists())

    def test_server_process_executes_pending_manual_control_request(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, mode="manual")
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.connection.execute(
                    "INSERT INTO control_requests VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL)",
                    ("request-1", "2026-04-29T12:00:00Z", "run-cycle", json.dumps({"reason": "manual"}), "pending"),
                )
                server.connection.commit()
                result = server.tick()
            finally:
                server.close()
            self.assertIsNotNone(result)
            self.assertEqual(rows(root / "argus.sqlite3", "control_requests")[0]["status"], "succeeded")
            self.assertEqual(rows(root / "argus.sqlite3", "runs")[0]["run_kind"], "manual")

    def test_stale_items_are_not_inserted_as_accepted_reports(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            config = yaml.safe_load(path.read_text())
            config["sources"][0]["freshness_window_hours"] = 1
            path.write_text(yaml.safe_dump(config))
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            reasons = {row["reason_code"] for row in rows(root / "argus.sqlite3", "skipped_items")}
            self.assertIn("stale_by_freshness_window", reasons)
            self.assertNotIn("skipped_not_package_candidate", reasons)
            stale_rows = [row for row in rows(root / "argus.sqlite3", "skipped_items") if row["reason_code"] == "stale_by_freshness_window"]
            self.assertEqual(len(stale_rows), 2)

    def test_same_canonical_url_different_feed_ids_are_distinct_candidates(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            fixture = root / "feeds" / "stateful-source.xml"
            fixture.write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Stateful Source</title>
    <item>
      <guid>guid-a</guid>
      <title>First item</title>
      <link>https://stateful.example/shared</link>
      <pubDate>Wed, 29 Apr 2026 10:00:00 +0000</pubDate>
      <description>First item.</description>
    </item>
    <item>
      <guid>guid-b</guid>
      <title>Second item</title>
      <link>https://stateful.example/shared</link>
      <pubDate>Wed, 29 Apr 2026 10:05:00 +0000</pubDate>
      <description>Second item.</description>
    </item>
  </channel>
</rss>"""
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)

    def test_feed_identity_rerun_updates_seen_report_not_duplicate_report(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            fixture = root / "feeds" / "stateful-source.xml"
            clock = FakeClock(NOW)
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                fixture.write_text(
                    fixture.read_text().replace("<title>First story</title>", "<title>First story revised</title>")
                )
                clock.advance(3600)
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 2)
            self.assertEqual(len(rows(root / "argus.sqlite3", "report_seen_runs")), 4)
            self.assertIn("seen_existing_report", {row["decision"] for row in rows(root / "argus.sqlite3", "dedupe_decisions")})
            second_run = sorted(rows(root / "argus.sqlite3", "runs"), key=lambda row: row["started_at"])[1]
            second_health = json.loads((Path(second_run["output_dir"]) / "source-health.json").read_text())[0]
            self.assertEqual(second_health["emitted_candidate_count"], 0)
            self.assertEqual(second_health["final_package_candidate_count"], 0)

    def test_package_json_uses_canonical_shape_and_embeddings(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "require_embeddings": True,
                },
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            package = json.loads(rows(root / "argus.sqlite3", "packages")[0]["package_json"])
            self.assertEqual(package["schema"], "swarm.channel.news.report.v0")
            self.assertTrue(package["package_id"].startswith("sha256:"))
            self.assertIsInstance(package["supplied_embeddings"], list)
            self.assertEqual(package["supplied_embeddings"][0]["space_id"], "openai:text-embedding-3-small:1536:v1")
            self.assertNotIn("suppliedEmbeddings", package)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 2)

    def test_live_publisher_success_records_response_and_message_id(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.swarm.channel",
                    "require_embeddings": True,
                },
            )
            calls = []
            original_post = server_module.post_json_over_unix_socket

            def fake_post(socket_path, api_path, payload):
                calls.append({"socket_path": socket_path, "api_path": api_path, "payload": payload})
                return {
                    "ok": True,
                    "results": [
                        {
                            "server": payload["server"],
                            "sent": True,
                            "subspace_message_id": "msg-" + payload["idempotency_key"][-8:],
                            "idempotency_key": payload["idempotency_key"],
                        }
                    ],
                }

            server_module.post_json_over_unix_socket = fake_post
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server_module.post_json_over_unix_socket = original_post
                server.close()
            attempts = rows(root / "argus.sqlite3", "publish_attempts")
            self.assertEqual({row["status"] for row in attempts}, {"succeeded"})
            self.assertEqual(len(calls), 2)
            self.assertTrue(all(row["subspace_message_id"] for row in attempts))
            self.assertTrue(all(json.loads(row["response_json"])["ok"] for row in attempts))

    def test_live_publisher_forwards_embeddings_and_stable_idempotency_key(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.swarm.channel",
                    "require_embeddings": True,
                },
            )
            calls = []
            original_post = server_module.post_json_over_unix_socket

            def fake_post(socket_path, api_path, payload):
                calls.append(payload)
                return {
                    "ok": True,
                    "results": [
                        {
                            "server": payload["server"],
                            "sent": True,
                            "subspace_message_id": "subspace-message",
                            "idempotency_key": payload["idempotency_key"],
                        }
                    ],
                }

            server_module.post_json_over_unix_socket = fake_post
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server_module.post_json_over_unix_socket = original_post
                server.close()
            attempts = rows(root / "argus.sqlite3", "publish_attempts")
            self.assertEqual({call["idempotency_key"] for call in calls}, {row["publish_idempotency_key"] for row in attempts})
            self.assertTrue(all(call["embeddings"] for call in calls))
            self.assertEqual({call["embeddings"][0]["space_id"] for call in calls}, {"openai:text-embedding-3-small:1536:v1"})
            self.assertTrue(all(isinstance(call["embeddings"][0]["vector"], list) for call in calls))
            packages = [json.loads(call["text"]) for call in calls]
            self.assertTrue(all(package["schema"] == "swarm.channel.news.report.v0" for package in packages))
            self.assertTrue(all(package["supplied_embeddings"][0]["space_id"] == "openai:text-embedding-3-small:1536:v1" for package in packages))
            self.assertTrue(all(package["supplied_embeddings"][0]["vector"] == call["embeddings"][0]["vector"] for package, call in zip(packages, calls)))

    def test_live_publisher_daemon_failure_records_failed_attempt(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.swarm.channel",
                    "require_embeddings": True,
                },
            )
            original_post = server_module.post_json_over_unix_socket

            def fake_post(socket_path, api_path, payload):
                raise server_module.PublishTransportError(
                    "daemon refused send",
                    {"ok": False, "error": {"code": "subspace_unavailable", "message": "daemon refused send"}},
                )

            server_module.post_json_over_unix_socket = fake_post
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server_module.post_json_over_unix_socket = original_post
                server.close()
            attempts = rows(root / "argus.sqlite3", "publish_attempts")
            self.assertEqual({row["status"] for row in attempts}, {"failed"})
            self.assertEqual({row["error_class"] for row in attempts}, {"PublishTransportError"})
            self.assertTrue(all(json.loads(row["response_json"])["ok"] is False for row in attempts))

    def test_failed_live_publish_retries_with_next_attempt_number(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.swarm.channel",
                    "require_embeddings": True,
                },
            )
            calls = []
            original_post = server_module.post_json_over_unix_socket

            def failing_post(socket_path, api_path, payload):
                calls.append(("failed", payload["idempotency_key"]))
                raise server_module.PublishTransportError("daemon down", {"ok": False, "error": {"message": "daemon down"}})

            def succeeding_post(socket_path, api_path, payload):
                calls.append(("succeeded", payload["idempotency_key"]))
                return {
                    "ok": True,
                    "results": [
                        {
                            "server": payload["server"],
                            "sent": True,
                            "subspace_message_id": "msg-" + payload["idempotency_key"][-8:],
                            "idempotency_key": payload["idempotency_key"],
                        }
                    ],
                }

            clock = FakeClock(NOW)
            server_module.post_json_over_unix_socket = failing_post
            server = ArgusServer(path, clock=clock)
            try:
                server.tick()
                first_attempts = rows(root / "argus.sqlite3", "publish_attempts")
                self.assertEqual({row["attempt_number"] for row in first_attempts}, {1})
                self.assertEqual({row["status"] for row in first_attempts}, {"failed"})
                server_module.post_json_over_unix_socket = succeeding_post
                clock.advance(3600)
                server.manual_cycle()
            finally:
                server_module.post_json_over_unix_socket = original_post
                server.close()
            attempts = rows(root / "argus.sqlite3", "publish_attempts")
            self.assertEqual(len(attempts), 4)
            by_key = {}
            for attempt in attempts:
                by_key.setdefault(attempt["publish_idempotency_key"], []).append(attempt)
            self.assertEqual(len(by_key), 2)
            for key, key_attempts in by_key.items():
                self.assertEqual([row["attempt_number"] for row in sorted(key_attempts, key=lambda row: row["attempt_number"])], [1, 2])
                self.assertEqual({row["status"] for row in key_attempts}, {"failed", "succeeded"})
                self.assertEqual(calls.count(("failed", key)), 1)
                self.assertEqual(calls.count(("succeeded", key)), 1)

    def test_succeeded_live_publish_survives_later_artifact_failure(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.swarm.channel",
                    "require_embeddings": True,
                },
            )
            original_post = server_module.post_json_over_unix_socket
            original_write_text = Path.write_text
            fail_summary_artifact = {"enabled": False}

            def fake_post(socket_path, api_path, payload):
                fail_summary_artifact["enabled"] = True
                return {
                    "ok": True,
                    "results": [
                        {
                            "server": payload["server"],
                            "sent": True,
                            "subspace_message_id": "msg-" + payload["idempotency_key"][-8:],
                            "idempotency_key": payload["idempotency_key"],
                        }
                    ],
                }

            def flaky_write_text(self, data, *args, **kwargs):
                if fail_summary_artifact["enabled"] and self.name == "run-summary.json":
                    raise OSError("simulated summary artifact failure after publish")
                return original_write_text(self, data, *args, **kwargs)

            server_module.post_json_over_unix_socket = fake_post
            Path.write_text = flaky_write_text
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                with self.assertRaisesRegex(OSError, "simulated summary artifact failure after publish"):
                    server.tick()
            finally:
                server_module.post_json_over_unix_socket = original_post
                Path.write_text = original_write_text
                server.close()
            attempts = rows(root / "argus.sqlite3", "publish_attempts")
            self.assertEqual(len(attempts), 2)
            self.assertEqual({row["status"] for row in attempts}, {"succeeded"})
            self.assertTrue(all(row["subspace_message_id"] for row in attempts))
            self.assertEqual(rows(root / "argus.sqlite3", "runs")[0]["status"], "failed")

    def test_inactive_and_blocked_publish_do_not_send(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            fixture = root / "feeds" / "stateful-source.xml"
            calls = []
            original_post = server_module.post_json_over_unix_socket
            server_module.post_json_over_unix_socket = lambda *args: calls.append(args) or {"ok": True, "results": []}
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
                config = yaml.safe_load(path.read_text())
                config["publish"] = {"state": "active", "live_approval": False, "subspace_endpoint": "https://subspace.swarm.channel"}
                path.write_text(yaml.safe_dump(config))
                server.reload()
                server.clock.advance(3600)
                fixture.write_text(
                    fixture.read_text().replace(
                        "  </channel>\n</rss>",
                        """    <item>\n      <guid>stateful-guid-3</guid>\n      <title>Third story</title>\n      <link>https://stateful.example/story-3</link>\n      <pubDate>Wed, 30 Apr 2026 10:00:00 +0000</pubDate>\n      <description>Brand new story.</description>\n    </item>\n  </channel>\n</rss>""",
                    )
                )
                server.tick()
            finally:
                server_module.post_json_over_unix_socket = original_post
                server.close()
            self.assertEqual(calls, [])
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

    def test_canary_live_publish_limit_fails_before_any_send(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.swarm.channel",
                    "require_embeddings": True,
                },
            )
            calls = []
            original_post = server_module.post_json_over_unix_socket
            server_module.post_json_over_unix_socket = lambda *args: calls.append(args) or {"ok": True, "results": []}
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                with self.assertRaisesRegex(Exception, "canary publish limit exceeded"):
                    server.manual_cycle(max_live_publishes=1)
            finally:
                server_module.post_json_over_unix_socket = original_post
                server.close()
            self.assertEqual(calls, [])
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)

    def test_scheduled_live_publish_limit_fails_before_any_send(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.swarm.channel",
                    "require_embeddings": True,
                },
                schedule={"max_live_publishes_per_tick": 1},
            )
            calls = []
            original_post = server_module.post_json_over_unix_socket
            server_module.post_json_over_unix_socket = lambda *args: calls.append(args) or {"ok": True, "results": []}
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                with self.assertRaisesRegex(Exception, "canary publish limit exceeded"):
                    server.tick()
                state = dict(server.connection.execute("SELECT * FROM scheduler_state WHERE id = 1").fetchone())
            finally:
                server_module.post_json_over_unix_socket = original_post
                server.close()
            self.assertEqual(state["max_live_publishes_per_tick"], 1)
            self.assertEqual(calls, [])
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)

    def test_schedule_live_publish_limit_rejects_invalid_values(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, schedule={"max_live_publishes_per_tick": -1})
            with self.assertRaisesRegex(Exception, "Invalid schedule.max_live_publishes_per_tick"):
                load_runtime_config(path)

    def test_daemon_chunked_response_and_missing_message_id_failure(self):
        ok_response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n{\"ok\r\n14\r\n\":true,\"results\":[]}\r\n0\r\n\r\n"
        original_socket = server_module.socket.socket

        class FakeSocket:
            def __init__(self, *args, **kwargs):
                self._chunks = [ok_response, b""]

            def settimeout(self, timeout):
                self.timeout = timeout

            def connect(self, path):
                self.path = path

            def sendall(self, request):
                self.request = request

            def recv(self, size):
                return self._chunks.pop(0)

            def close(self):
                pass

        server_module.socket.socket = FakeSocket
        try:
            response = server_module.post_json_over_unix_socket(Path("/tmp/daemon.sock"), "/v1/messages", {"text": "x"})
        finally:
            server_module.socket.socket = original_socket
        self.assertEqual(response, {"ok": True, "results": []})

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                publish={
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.swarm.channel",
                    "require_embeddings": True,
                },
            )
            original_post = server_module.post_json_over_unix_socket
            server_module.post_json_over_unix_socket = lambda *args: {"ok": True, "results": []}
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server_module.post_json_over_unix_socket = original_post
                server.close()
            attempts = rows(root / "argus.sqlite3", "publish_attempts")
            self.assertEqual({row["status"] for row in attempts}, {"failed"})
            self.assertEqual({row["error_message"] for row in attempts}, {"daemon response missing subspace_message_id"})

    def test_spec_named_cli_backend_invokes_embedding_command(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(
                root,
                embedding={
                    "backend": "subspace-embedding-cli",
                    "command": str(write_fake_embedder(root)),
                    "provider": "fake",
                    "model": "argus-local-v0",
                    "dimensions": 1,
                    "space_id": "argus-local-v0",
                },
                publish={"state": "inactive", "require_embeddings": True},
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual({row["status"] for row in rows(root / "argus.sqlite3", "embeddings")}, {"embedded"})
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)

    def test_source_health_command_reads_sqlite_state(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            health = server_module.run_source_health(root / "argus.sqlite3")
            self.assertEqual(health[0]["source_id"], "stateful-source")
            self.assertEqual(health[0]["last_run_id"], rows(root / "argus.sqlite3", "runs")[0]["run_id"])
            self.assertEqual(len(rows(root / "argus.sqlite3", "source_run_status")), 1)

    def test_run_status_records_source_errors(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            config = yaml.safe_load(path.read_text())
            config["sources"].append(
                {
                    "id": "missing-source",
                    "display_name": "Missing Source",
                    "source_class": "editorial",
                    "feed_url": "https://fixture.invalid/missing-source.xml",
                    "site_url": "https://missing.example/",
                    "adapter": "rss",
                    "enabled": True,
                    "freshness_window_hours": 72,
                    "authority_score": 0.75,
                }
            )
            path.write_text(yaml.safe_dump(config))
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                exit_code, _ = server.tick()
            finally:
                server.close()
            self.assertEqual(exit_code, 0)
            self.assertEqual(rows(root / "argus.sqlite3", "runs")[0]["status"], "succeeded_with_source_errors")
            self.assertEqual({row["status"] for row in rows(root / "argus.sqlite3", "source_run_status")}, {"ok", "failed"})

    def test_explain_skip_includes_exact_duplicate_details_from_sqlite(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            (root / "feeds" / "stateful-source.xml").write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Stateful Source</title>
    <item>
      <title>Canonical duplicate story</title>
      <link>https://stateful.example/story?utm_source=rss</link>
      <pubDate>Tue, 29 Apr 2026 07:00:00 +0000</pubDate>
      <description>First copy.</description>
    </item>
    <item>
      <title>Canonical duplicate story</title>
      <link>https://stateful.example/story?utm_medium=email</link>
      <pubDate>Tue, 29 Apr 2026 07:00:00 +0000</pubDate>
      <description>Second copy.</description>
    </item>
  </channel>
</rss>
"""
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                _, summary = server.tick()
            finally:
                server.close()
            run_dir = Path(rows(root / "argus.sqlite3", "runs")[0]["output_dir"])
            (run_dir / "run-summary.json").unlink()
            explanation = server_module.explain_skip(root / "argus.sqlite3", summary["run_id"])
            self.assertEqual(len(explanation["exact_duplicates"]), 1)
            duplicate = explanation["exact_duplicates"][0]
            self.assertEqual(duplicate["selected_key_type"], "canonical_url")
            self.assertEqual(duplicate["candidate"]["canonical_url"], "https://stateful.example/story")
            self.assertIsNotNone(duplicate["duplicate_of_report_id"])
            self.assertIn("cross-source", duplicate["cross_source_note"])

    def test_missing_identity_key_is_skipped_before_storage_and_package(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            (root / "feeds" / "stateful-source.xml").write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Stateful Source</title>
    <item>
      <description>No identity fields.</description>
    </item>
  </channel>
</rss>
"""
            )
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.tick()
            finally:
                server.close()
            self.assertEqual(len(rows(root / "argus.sqlite3", "normalized_reports")), 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 0)
            skipped = rows(root / "argus.sqlite3", "skipped_items")
            self.assertEqual(skipped[0]["reason_code"], "missing_identity_key")
            run_dir = next((root / "out" / "runs").glob("20260429T120000Z-scheduled-*"))
            artifact = json.loads((run_dir / "skipped-items.json").read_text())
            self.assertEqual(artifact[0]["reason_code"], "missing_identity_key")

    def test_activation_reload_during_cycle_does_not_backfill_cycle_started_inactive(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive"})
            config = yaml.safe_load(path.read_text())
            original_run_pipeline = server_module.run_pipeline_for_sources
            server = ArgusServer(path, clock=FakeClock(NOW))

            def activate_during_fetch(*args, **kwargs):
                result = original_run_pipeline(*args, **kwargs)
                config["publish"] = {
                    "state": "active",
                    "live_approval": True,
                    "subspace_endpoint": "https://subspace.invalid",
                    "allow_non_embedded_fallback": True,
                }
                path.write_text(yaml.safe_dump(config))
                server.reload_requested = True
                return result

            server_module.run_pipeline_for_sources = activate_during_fetch
            try:
                server.tick()
            finally:
                server_module.run_pipeline_for_sources = original_run_pipeline
                server.close()
            self.assertEqual(rows(root / "argus.sqlite3", "packages")[0]["active_eligible"], 0)
            self.assertEqual(len(rows(root / "argus.sqlite3", "publish_attempts")), 0)

    def test_reload_during_cycle_does_not_change_embedding_config_for_packages(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root, publish={"state": "inactive", "require_embeddings": True})
            config = yaml.safe_load(path.read_text())
            original_run_pipeline = server_module.run_pipeline_for_sources
            server = ArgusServer(path, clock=FakeClock(NOW))

            def change_embedding_during_fetch(*args, **kwargs):
                result = original_run_pipeline(*args, **kwargs)
                config["embedding"]["command"] = "/definitely/not/used/after/cycle/start"
                path.write_text(yaml.safe_dump(config))
                server.reload_requested = True
                return result

            server_module.run_pipeline_for_sources = change_embedding_during_fetch
            try:
                server.tick()
            finally:
                server_module.run_pipeline_for_sources = original_run_pipeline
                server.close()
            self.assertEqual({row["status"] for row in rows(root / "argus.sqlite3", "embeddings")}, {"embedded"})
            self.assertEqual(len(rows(root / "argus.sqlite3", "packages")), 2)

    def test_status_reports_prime_and_reload_failure_observability(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = write_config(root)
            server = ArgusServer(path, clock=FakeClock(NOW))
            try:
                server.prime()
                path.write_text("publish: [")
                server.reload()
            finally:
                server.close()
            status = server_module.run_status(root / "argus.sqlite3")
            self.assertEqual(status["prime"]["status"], "succeeded")
            self.assertIsNotNone(status["prime"]["prime_watermark_run_id"])
            self.assertIsNotNone(status["last_reload_failure"])
            self.assertIn("packages", status["counts"])
            self.assertIn("skipped_items_by_reason", status["counts"])


if __name__ == "__main__":
    unittest.main()
