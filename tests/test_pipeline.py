from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from swarm_news_ingest.pipeline import read_source_config, run_pipeline

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "swarm-news"
FEEDS = FIXTURE_ROOT / "feeds"
NOW = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)


def read_json(path: Path):
    return json.loads(path.read_text())


def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class PipelineTests(unittest.TestCase):
    def test_source_config_validation_and_pipeline(self):
        sources = read_source_config(FIXTURE_ROOT / "test-sources.yaml")
        self.assertEqual(len(sources), 11)
        with self.subTest("pipeline"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmpdir:
                exit_code, summary = run_pipeline(
                    FIXTURE_ROOT / "test-sources.yaml",
                    Path(tmpdir),
                    now=NOW,
                    fixture_dir=FEEDS,
                )
                self.assertEqual(exit_code, 0)
                self.assertEqual(summary["exit_status"], "partial_failure")
                self.assertEqual(summary["counts"]["failed_sources"], 1)
                self.assertEqual(summary["counts"]["source_local_duplicates"], 2)
                for name in [
                    "run-summary.json",
                    "source-health.json",
                    "normalized.jsonl",
                    "clusters.jsonl",
                    "publish-candidates.jsonl",
                ]:
                    self.assertTrue((Path(tmpdir) / name).exists(), name)

    def test_artifacts_preserve_provenance_and_community_labeling(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            run_pipeline(FIXTURE_ROOT / "test-sources.yaml", Path(tmpdir), now=NOW, fixture_dir=FEEDS)
            health = read_json(Path(tmpdir) / "source-health.json")
            normalized = read_jsonl(Path(tmpdir) / "normalized.jsonl")
            candidates = read_jsonl(Path(tmpdir) / "publish-candidates.jsonl")
            health_by_id = {row["source_id"]: row for row in health}
            normalized_by_id = {row["report_id"]: row for row in normalized}

            self.assertEqual(len(health), 11)
            self.assertEqual(health_by_id["malformed_source"]["status"], "failed")
            self.assertEqual(health_by_id["empty_source"]["status"], "empty")

            reddit = next(row for row in normalized if row["source_id"] == "reddit_localllama")
            self.assertEqual(reddit["source_class"], "community")
            self.assertIn("Community thread", reddit["raw_summary"])

            for row in candidates:
                self.assertIn(row["report_id"], normalized_by_id)
                self.assertEqual(
                    row["provenance"]["source_class"],
                    normalized_by_id[row["report_id"]]["source_class"],
                )
                self.assertTrue(row["metadata"]["embedding_text"])

    def test_source_local_dedupe_and_cross_source_preservation(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            run_pipeline(FIXTURE_ROOT / "test-sources.yaml", Path(tmpdir), now=NOW, fixture_dir=FEEDS)
            clusters = read_jsonl(Path(tmpdir) / "clusters.jsonl")
            candidates = read_jsonl(Path(tmpdir) / "publish-candidates.jsonl")

            dup_cluster = next(row for row in clusters if row["source_id"] == "dup_source")
            self.assertEqual(dup_cluster["dedupe_reasons"], ["canonical_url_exact"])
            self.assertEqual(len(dup_cluster["report_ids"]), 2)

            title_cluster = next(row for row in clusters if row["source_id"] == "dup_title_source")
            self.assertEqual(title_cluster["dedupe_reasons"], ["normalized_title_date_bucket"])
            self.assertEqual(len(title_cluster["report_ids"]), 2)

            shared_candidates = [row for row in candidates if row["canonical_url"] == "https://shared.example/story"]
            self.assertEqual(len(shared_candidates), 2)
            self.assertEqual({row["source_id"] for row in shared_candidates}, {"cross_source_one", "cross_source_two"})

    def test_all_source_failure_exits_nonzero(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            exit_code, summary = run_pipeline(
                FIXTURE_ROOT / "all-fail-sources.yaml",
                Path(tmpdir),
                now=NOW,
                fixture_dir=FEEDS,
            )
            self.assertEqual(exit_code, 1)
            self.assertEqual(summary["exit_status"], "failed")


if __name__ == "__main__":
    unittest.main()
