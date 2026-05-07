from __future__ import annotations

import json
import shutil
import unittest
from datetime import datetime, timezone
from pathlib import Path

from argus.pipeline import SourceConfig, canonicalize_url, default_state_path, normalize_title, parse_feed, read_source_config, run_pipeline

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "argus"
FEEDS = FIXTURE_ROOT / "feeds"
NOW = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)


def read_json(path: Path):
    return json.loads(path.read_text())


def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class PipelineTests(unittest.TestCase):
    def test_rss_gmt_pubdate_normalizes_before_freshness_filter(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            fixture_dir = Path(tmpdir) / "fixtures"
            fixture_dir.mkdir()
            (fixture_dir / "openai_gmt.xml").write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>OpenAI</title>
    <item>
      <guid>openai-gmt-guid</guid>
      <title>OpenAI GMT story</title>
      <link>https://openai.example/gmt-story</link>
      <pubDate>Wed, 06 May 2026 19:06:55 GMT</pubDate>
      <description>Representative OpenAI RSS date.</description>
    </item>
  </channel>
</rss>
"""
            )
            sources = Path(tmpdir) / "sources.yaml"
            sources.write_text(
                """sources:
  - id: openai_gmt
    name: OpenAI GMT
    source_class: official
    source_category: official_lab
    feed_type: rss
    feed_url: https://fixture.invalid/openai_gmt.xml
    site_url: https://openai.example/news
    enabled: true
    tier: 1
    freshness_window_hours: 72
    adapter: generic_rss
    request_headers: {}
"""
            )
            exit_code, summary = run_pipeline(sources, Path(tmpdir) / "out", now=datetime(2026, 5, 7, 12, 0, 0, tzinfo=timezone.utc), fixture_dir=fixture_dir)
            self.assertEqual(exit_code, 0)
            self.assertEqual(summary["counts"]["publish_candidates"], 1)
            report = read_jsonl(Path(tmpdir) / "out" / "normalized-items.jsonl")[0]
            self.assertEqual(report["published_at"], "2026-05-06T19:06:55Z")

    def test_the_verge_and_reddit_atom_snippets_parse_as_atom(self):
        for source_id, xml_text, expected_url in [
            (
                "the-verge-ai",
                """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>tag:theverge.com,2026:ai-story</id>
    <title>The Verge Atom story</title>
    <updated>2026-05-06T20:01:02Z</updated>
    <link rel="alternate" type="text/html" href="https://www.theverge.com/ai-artificial-intelligence/atom-story" />
    <summary>Representative The Verge Atom item.</summary>
  </entry>
</feed>
""",
                "https://www.theverge.com/ai-artificial-intelligence/atom-story",
            ),
            (
                "reddit-localllama",
                """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>t3_reddit_atom_story</id>
    <title>Reddit Atom story</title>
    <updated>2026-05-06T21:02:03Z</updated>
    <link rel="alternate" href="https://www.reddit.com/r/LocalLLaMA/comments/reddit_atom_story/" />
    <content>Representative Reddit Atom item.</content>
  </entry>
</feed>
""",
                "https://www.reddit.com/r/LocalLLaMA/comments/reddit_atom_story/",
            ),
        ]:
            source = SourceConfig(
                id=source_id,
                name=source_id,
                source_class="community" if source_id.startswith("reddit") else "editorial",
                source_category="community_reddit" if source_id.startswith("reddit") else "trusted_editorial",
                feed_type="atom",
                feed_url="https://fixture.invalid/{}.xml".format(source_id),
                site_url=expected_url,
                enabled=True,
                tier=1,
                freshness_window_hours=72,
                adapter="atom",
                request_headers={},
            )
            entries = parse_feed(xml_text, source)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].canonical_url, expected_url)
            self.assertTrue(entries[0].title.endswith("Atom story"))

    def test_canonical_url_normalizes_specified_dedupe_shape(self):
        self.assertEqual(
            canonicalize_url("HTTPS://Example.COM:443/a/b/../c/%7eone?b=2&a=2&a=1&utm_source=x&igshid=y#frag"),
            "https://example.com/a/c/~one?a=1&a=2&b=2",
        )
        self.assertEqual(canonicalize_url("https://example.com/a%2Fb?x=%7e&y=%2f"), "https://example.com/a%2Fb?x=~&y=%2F")
        self.assertNotEqual(canonicalize_url("https://example.com/a"), canonicalize_url("https://example.com/a/"))
        self.assertIsNone(canonicalize_url("/relative/path"))

    def test_title_key_normalization_preserves_internal_punctuation(self):
        self.assertEqual(normalize_title("  ...AI/ML: shipping now!!!  "), "ai/ml: shipping now")
        self.assertNotEqual(normalize_title("AI/ML"), normalize_title("AI ML"))

    def test_title_date_fallback_uses_fetched_date_when_published_missing(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            fixture_dir = Path(tmpdir) / "fixtures"
            fixture_dir.mkdir()
            fixture = fixture_dir / "stateful_source.xml"
            fixture.write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Stateful Source</title>
    <item>
      <title>No published date</title>
      <description>First day.</description>
    </item>
  </channel>
</rss>
"""
            )
            shutil.copy(FIXTURE_ROOT / "stateful-sources.yaml", Path(tmpdir) / "stateful-sources.yaml")
            _, summary1 = run_pipeline(Path(tmpdir) / "stateful-sources.yaml", Path(tmpdir) / "run1", now=NOW, fixture_dir=fixture_dir)
            _, summary2 = run_pipeline(
                Path(tmpdir) / "stateful-sources.yaml",
                Path(tmpdir) / "run2",
                now=NOW.replace(day=30),
                fixture_dir=fixture_dir,
            )
            self.assertEqual(summary1["counts"]["publish_candidates"], 1)
            self.assertEqual(summary2["counts"]["publish_candidates"], 1)
            report1 = read_jsonl(Path(tmpdir) / "run1" / "normalized-items.jsonl")[0]
            report2 = read_jsonl(Path(tmpdir) / "run2" / "normalized-items.jsonl")[0]
            self.assertNotEqual(report1["report_id"], report2["report_id"])
            self.assertTrue(report1["report_id_input_value"].endswith("2026-04-29"))
            self.assertTrue(report2["report_id_input_value"].endswith("2026-04-30"))


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
                    "normalized-items.jsonl",
                    "clusters.jsonl",
                    "dedupe-decisions.json",
                    "skipped-items.json",
                    "package-candidates.jsonl",
                    "publish-candidates.jsonl",
                ]:
                    self.assertTrue((Path(tmpdir) / name).exists(), name)
                self.assertNotIn("digest_json", summary["artifact_paths"])
                self.assertNotIn("digest_markdown", summary["artifact_paths"])
                self.assertFalse((Path(tmpdir) / "digest.json").exists())
                self.assertFalse((Path(tmpdir) / "digest.md").exists())
                self.assertEqual(summary["state"]["path"], str(default_state_path(Path(tmpdir))))
                self.assertTrue(summary["state"]["write_performed"])

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
            self.assertEqual(dup_cluster["dedupe_reasons"], ["canonical_url"])
            self.assertEqual(len(dup_cluster["report_ids"]), 2)

            title_cluster = next(row for row in clusters if row["source_id"] == "dup_title_source")
            self.assertEqual(title_cluster["dedupe_reasons"], ["normalized_title_date"])
            self.assertEqual(len(title_cluster["report_ids"]), 2)

            shared_candidates = [row for row in candidates if row["canonical_url"] == "https://shared.example/story"]
            self.assertEqual(len(shared_candidates), 2)
            self.assertEqual({row["source_id"] for row in shared_candidates}, {"cross_source_one", "cross_source_two"})

    def test_all_source_failure_is_source_scoped_degradation(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            exit_code, summary = run_pipeline(
                FIXTURE_ROOT / "all-fail-sources.yaml",
                Path(tmpdir),
                now=NOW,
                fixture_dir=FEEDS,
            )
            self.assertEqual(exit_code, 0)
            self.assertEqual(summary["exit_status"], "partial_failure")

    def test_state_filters_repeated_items_and_emits_only_new_additions(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            fixture_dir = Path(tmpdir) / "fixtures"
            fixture_dir.mkdir()
            shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "stateful_source.xml")
            shutil.copy(FIXTURE_ROOT / "stateful-sources.yaml", Path(tmpdir) / "stateful-sources.yaml")
            state_path = Path(tmpdir) / "argus-state.json"
            out1 = Path(tmpdir) / "run1"
            out2 = Path(tmpdir) / "run2"
            out3 = Path(tmpdir) / "run3"

            _, summary1 = run_pipeline(Path(tmpdir) / "stateful-sources.yaml", out1, now=NOW, fixture_dir=fixture_dir, state_path=state_path)
            self.assertEqual(summary1["counts"]["publish_candidates"], 2)
            self.assertTrue(state_path.exists())

            _, summary2 = run_pipeline(Path(tmpdir) / "stateful-sources.yaml", out2, now=NOW, fixture_dir=fixture_dir, state_path=state_path)
            self.assertEqual(summary2["counts"]["publish_candidates"], 0)
            health2 = read_json(out2 / "source-health.json")
            self.assertEqual(health2[0]["skipped_previously_seen_count"], 2)

            xml_path = fixture_dir / "stateful_source.xml"
            xml_text = xml_path.read_text()
            xml_path.write_text(
                xml_text.replace(
                    "</channel>\n</rss>",
                    """    <item>\n      <guid>stateful-guid-3</guid>\n      <title>Third story</title>\n      <link>https://stateful.example/story-3?utm_source=rss</link>\n      <pubDate>Wed, 30 Apr 2026 10:00:00 +0000</pubDate>\n      <description>Brand new story.</description>\n    </item>\n  </channel>\n</rss>""",
                )
            )
            _, summary3 = run_pipeline(Path(tmpdir) / "stateful-sources.yaml", out3, now=NOW, fixture_dir=fixture_dir, state_path=state_path)
            self.assertEqual(summary3["counts"]["publish_candidates"], 1)
            candidates3 = read_jsonl(out3 / "publish-candidates.jsonl")
            self.assertEqual([row["title"] for row in candidates3], ["Third story"])

    def test_state_supports_http_304_without_re_emitting(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            fixture_dir = Path(tmpdir) / "fixtures"
            fixture_dir.mkdir()
            shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "stateful_source.xml")
            shutil.copy(FIXTURE_ROOT / "stateful-sources.yaml", Path(tmpdir) / "stateful-sources.yaml")
            meta_path = fixture_dir / "stateful_source.meta.json"
            meta_path.write_text(json.dumps({"etag": "\"stateful-etag-v1\"", "last_modified": "Wed, 30 Apr 2026 09:00:00 GMT"}))
            state_path = Path(tmpdir) / "argus-state.json"

            run_pipeline(Path(tmpdir) / "stateful-sources.yaml", Path(tmpdir) / "run1", now=NOW, fixture_dir=fixture_dir, state_path=state_path)
            meta_path.write_text(
                json.dumps(
                    {
                        "status_code": 304,
                        "etag": "\"stateful-etag-v1\"",
                        "last_modified": "Wed, 30 Apr 2026 09:00:00 GMT",
                        "if_none_match": "\"stateful-etag-v1\"",
                    }
                )
            )
            _, summary = run_pipeline(Path(tmpdir) / "stateful-sources.yaml", Path(tmpdir) / "run2", now=NOW, fixture_dir=fixture_dir, state_path=state_path)
            self.assertEqual(summary["counts"]["publish_candidates"], 0)
            health = read_json(Path(tmpdir) / "run2" / "source-health.json")
            self.assertEqual(health[0]["status"], "not_modified")
            self.assertEqual(health[0]["http_status"], 304)

    def test_dry_run_reads_state_but_does_not_write_it(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            fixture_dir = Path(tmpdir) / "fixtures"
            fixture_dir.mkdir()
            shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "stateful_source.xml")
            shutil.copy(FIXTURE_ROOT / "stateful-sources.yaml", Path(tmpdir) / "stateful-sources.yaml")
            state_path = Path(tmpdir) / "argus-state.json"

            _, summary = run_pipeline(
                Path(tmpdir) / "stateful-sources.yaml",
                Path(tmpdir) / "run1",
                now=NOW,
                fixture_dir=fixture_dir,
                state_path=state_path,
                dry_run=True,
                state_write=False,
            )
            self.assertFalse(state_path.exists())
            self.assertFalse(summary["state"]["write_enabled"])
            self.assertFalse(summary["state"]["write_performed"])

    def test_prime_advances_state_without_emitting_publish_candidates(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            fixture_dir = Path(tmpdir) / "fixtures"
            fixture_dir.mkdir()
            shutil.copy(FEEDS / "stateful_source.xml", fixture_dir / "stateful_source.xml")
            shutil.copy(FIXTURE_ROOT / "stateful-sources.yaml", Path(tmpdir) / "stateful-sources.yaml")
            state_path = Path(tmpdir) / "argus-state.json"

            _, summary1 = run_pipeline(
                Path(tmpdir) / "stateful-sources.yaml",
                Path(tmpdir) / "prime",
                now=NOW,
                fixture_dir=fixture_dir,
                state_path=state_path,
                prime=True,
            )
            self.assertTrue(state_path.exists())
            self.assertTrue(summary1["prime"])
            self.assertTrue(summary1["state"]["write_performed"])
            self.assertEqual(summary1["counts"]["publish_candidates"], 0)
            self.assertEqual(summary1["counts"]["primed_candidates"], 2)
            self.assertFalse((Path(tmpdir) / "prime" / "publish-candidates.jsonl").exists())
            self.assertFalse((Path(tmpdir) / "prime" / "package-candidates.jsonl").exists())
            self.assertFalse((Path(tmpdir) / "prime" / "digest.json").exists())
            self.assertFalse((Path(tmpdir) / "prime" / "digest.md").exists())

            _, summary2 = run_pipeline(
                Path(tmpdir) / "stateful-sources.yaml",
                Path(tmpdir) / "after",
                now=NOW,
                fixture_dir=fixture_dir,
                state_path=state_path,
            )
            self.assertEqual(summary2["counts"]["publish_candidates"], 0)


if __name__ == "__main__":
    unittest.main()
