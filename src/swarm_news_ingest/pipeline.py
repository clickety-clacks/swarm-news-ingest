from __future__ import annotations

import argparse
import dataclasses
import hashlib
import html
import json
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from xml.etree import ElementTree as ET

import requests
import yaml

SCHEMA_VERSION = 1
REQUEST_TIMEOUT_SECONDS = 20
DROP_QUERY_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "utm_name",
    "utm_cid",
    "utm_reader",
    "utm_viz_id",
    "utm_pubreferrer",
    "utm_swu",
    "ref",
    "ref_src",
    "source",
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ocid",
    "guccounter",
    "guce_referrer",
    "guce_referrer_sig",
    "ncid",
    "trk",
    "mkt_tok",
}
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
ARXIV_NS = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}


class HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        HTMLParser.__init__(self)
        self.parts: List[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)

    def get_text(self) -> str:
        return " ".join(self.parts)


@dataclasses.dataclass
class SourceConfig:
    id: str
    name: str
    source_class: str
    source_category: str
    feed_type: str
    feed_url: str
    site_url: str
    enabled: bool
    tier: int
    freshness_window_hours: Optional[int]
    adapter: str
    request_headers: Dict[str, str]
    notes: Optional[str] = None


@dataclasses.dataclass
class RawEntry:
    source: SourceConfig
    feed_entry_id: Optional[str]
    title: str
    canonical_url: Optional[str]
    raw_url: Optional[str]
    published_at: Optional[str]
    raw_summary: str
    extra_provenance: Dict[str, Any]


class PipelineError(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_now(value: Optional[str]) -> datetime:
    if not value:
        return utc_now()
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_source_config(path: Path) -> List[SourceConfig]:
    payload = yaml.safe_load(path.read_text())
    if not isinstance(payload, dict) or not isinstance(payload.get("sources"), list):
        raise PipelineError("Invalid sources config: {}".format(path))
    required = {
        "id",
        "name",
        "source_class",
        "source_category",
        "feed_type",
        "feed_url",
        "site_url",
        "enabled",
        "tier",
        "adapter",
    }
    seen_ids = set()
    sources: List[SourceConfig] = []
    for row in payload["sources"]:
        if not isinstance(row, dict):
            raise PipelineError("Invalid source row in config")
        missing = sorted(required - set(row.keys()))
        if missing:
            raise PipelineError(
                "Source config missing fields for {}: {}".format(row.get("id", "<unknown>"), ", ".join(missing))
            )
        source_id = str(row["id"])
        if source_id in seen_ids:
            raise PipelineError("Duplicate source id: {}".format(source_id))
        seen_ids.add(source_id)
        sources.append(
            SourceConfig(
                id=source_id,
                name=str(row["name"]),
                source_class=str(row["source_class"]),
                source_category=str(row["source_category"]),
                feed_type=str(row["feed_type"]),
                feed_url=str(row["feed_url"]),
                site_url=str(row["site_url"]),
                enabled=bool(row["enabled"]),
                tier=int(row["tier"]),
                freshness_window_hours=(int(row["freshness_window_hours"]) if row.get("freshness_window_hours") is not None else None),
                adapter=str(row["adapter"]),
                request_headers={str(k): str(v) for k, v in (row.get("request_headers") or {}).items()},
                notes=str(row["notes"]) if row.get("notes") is not None else None,
            )
        )
    return sources


def fetch_feed(source: SourceConfig) -> Tuple[str, Optional[int], Optional[str], int]:
    started = time.perf_counter()
    headers = {"User-Agent": "swarm-news-ingest/0.1 (+local-only prototype)"}
    headers.update(source.request_headers)
    response = requests.get(source.feed_url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    response.raise_for_status()
    return response.text, response.status_code, response.headers.get("content-type"), elapsed_ms


def strip_html(text: str) -> str:
    stripper = HTMLStripper()
    stripper.feed(text)
    stripper.close()
    return stripper.get_text()


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    unescaped = html.unescape(text)
    plain = strip_html(unescaped)
    return re.sub(r"\s+", " ", plain).strip()


def text_or_none(node: Optional[ET.Element]) -> Optional[str]:
    if node is None or node.text is None:
        return None
    value = clean_text(node.text)
    return value or None


def child_text(node: ET.Element, name: str) -> Optional[str]:
    return text_or_none(node.find(name))


def atom_child_text(node: ET.Element, name: str, ns: Dict[str, str]) -> Optional[str]:
    return text_or_none(node.find(name, ns))


def normalize_title(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text.lower())).strip()


def canonicalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    parsed = urlparse(url.strip())
    if not parsed.scheme or not parsed.netloc:
        return url.strip()
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() not in DROP_QUERY_PARAMS]
    netloc = parsed.netloc.lower()
    if (parsed.scheme == "https" and netloc.endswith(":443")) or (parsed.scheme == "http" and netloc.endswith(":80")):
        netloc = netloc.rsplit(":", 1)[0]
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return urlunparse((parsed.scheme.lower(), netloc, path, "", urlencode(query), ""))


def choose_link(item: ET.Element, feed_type: str) -> Optional[str]:
    if feed_type in ("atom", "arxiv_atom"):
        namespace = ARXIV_NS if feed_type == "arxiv_atom" else ATOM_NS
        links = item.findall("atom:link", namespace)
        for link in links:
            rel = link.attrib.get("rel", "alternate")
            href = link.attrib.get("href")
            if href and rel == "alternate":
                return href
        if links:
            return links[0].attrib.get("href")
        return None
    link_text = child_text(item, "link")
    if link_text:
        return link_text
    for link in item.findall("link"):
        href = link.attrib.get("href")
        if href:
            return href
    return None


def parse_rss(xml_text: str, source: SourceConfig) -> List[RawEntry]:
    root = ET.fromstring(xml_text)
    items = root.findall("./channel/item")
    if root.tag == "item":
        items = [root]
    entries: List[RawEntry] = []
    for item in items:
        raw_summary = child_text(item, "description") or child_text(item, "summary") or child_text(item, "content") or ""
        link = choose_link(item, "rss")
        entries.append(
            RawEntry(
                source=source,
                feed_entry_id=child_text(item, "guid") or child_text(item, "id"),
                title=child_text(item, "title") or "(untitled)",
                canonical_url=link,
                raw_url=link,
                published_at=child_text(item, "pubDate") or child_text(item, "published") or child_text(item, "dc:date"),
                raw_summary=raw_summary,
                extra_provenance={},
            )
        )
    return entries


def parse_atom(xml_text: str, source: SourceConfig) -> List[RawEntry]:
    root = ET.fromstring(xml_text)
    entries_xml = root.findall("atom:entry", ATOM_NS)
    entries: List[RawEntry] = []
    for item in entries_xml:
        entries.append(
            RawEntry(
                source=source,
                feed_entry_id=atom_child_text(item, "atom:id", ATOM_NS),
                title=atom_child_text(item, "atom:title", ATOM_NS) or "(untitled)",
                canonical_url=choose_link(item, "atom"),
                raw_url=choose_link(item, "atom"),
                published_at=atom_child_text(item, "atom:published", ATOM_NS) or atom_child_text(item, "atom:updated", ATOM_NS),
                raw_summary=atom_child_text(item, "atom:summary", ATOM_NS) or atom_child_text(item, "atom:content", ATOM_NS) or "",
                extra_provenance={},
            )
        )
    return entries


def parse_arxiv_atom(xml_text: str, source: SourceConfig) -> List[RawEntry]:
    root = ET.fromstring(xml_text)
    entries_xml = root.findall("atom:entry", ARXIV_NS)
    entries: List[RawEntry] = []
    for item in entries_xml:
        authors = [clean_text(author.findtext("atom:name", default="", namespaces=ARXIV_NS)) for author in item.findall("atom:author", ARXIV_NS)]
        categories = [category.attrib.get("term") for category in item.findall("atom:category", ARXIV_NS) if category.attrib.get("term")]
        entries.append(
            RawEntry(
                source=source,
                feed_entry_id=atom_child_text(item, "atom:id", ARXIV_NS),
                title=atom_child_text(item, "atom:title", ARXIV_NS) or "(untitled)",
                canonical_url=choose_link(item, "arxiv_atom"),
                raw_url=choose_link(item, "arxiv_atom"),
                published_at=atom_child_text(item, "atom:published", ARXIV_NS) or atom_child_text(item, "atom:updated", ARXIV_NS),
                raw_summary=atom_child_text(item, "atom:summary", ARXIV_NS) or "",
                extra_provenance={"authors": authors, "categories": categories},
            )
        )
    return entries


def parse_feed(xml_text: str, source: SourceConfig) -> List[RawEntry]:
    if source.feed_type == "rss":
        return parse_rss(xml_text, source)
    if source.feed_type == "atom":
        return parse_atom(xml_text, source)
    if source.feed_type == "arxiv_atom":
        return parse_arxiv_atom(xml_text, source)
    raise PipelineError("Unsupported feed_type {} for {}".format(source.feed_type, source.id))


def normalize_timestamp(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = value.strip()
    formats = [
        None,
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
    ]
    for fmt in formats:
        try:
            if fmt is None:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            else:
                parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return iso_z(parsed.astimezone(timezone.utc))
        except ValueError:
            continue
    return text


def report_id_for(source_id: str, feed_entry_id: Optional[str], canonical_url: Optional[str], title: str, published_at: Optional[str]) -> str:
    basis = "|".join([source_id, feed_entry_id or "", canonical_url or "", normalize_title(title), published_at or ""])
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24]


def date_bucket(timestamp: Optional[str]) -> str:
    return timestamp[:10] if timestamp else "unknown"


def dedupe_key(report: Dict[str, Any]) -> Tuple[str, str, str]:
    source_id = report["source_id"]
    if report.get("feed_entry_id"):
        return source_id, "feed_entry_id_exact", report["feed_entry_id"]
    if report.get("canonical_url"):
        return source_id, "canonical_url_exact", report["canonical_url"]
    return source_id, "normalized_title_date_bucket", "{}|{}".format(normalize_title(report["title"]), date_bucket(report.get("published_at")))


def normalize_entry(entry: RawEntry, fetched_at: str) -> Dict[str, Any]:
    canonical_url = canonicalize_url(entry.canonical_url or entry.raw_url)
    raw_url = canonicalize_url(entry.raw_url) if entry.raw_url else canonical_url
    clean_summary_value = clean_text(entry.raw_summary)
    published_at = normalize_timestamp(entry.published_at)
    report_id = report_id_for(entry.source.id, entry.feed_entry_id, canonical_url, entry.title, published_at)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_id": report_id,
        "source_id": entry.source.id,
        "source_name": entry.source.name,
        "source_class": entry.source.source_class,
        "source_category": entry.source.source_category,
        "feed_url": entry.source.feed_url,
        "feed_entry_id": entry.feed_entry_id,
        "title": clean_text(entry.title) or "(untitled)",
        "canonical_url": canonical_url,
        "raw_url": raw_url,
        "published_at": published_at,
        "fetched_at": fetched_at,
        "raw_summary": clean_text(entry.raw_summary),
        "clean_summary": clean_summary_value,
        "provenance": {
            "source_class": entry.source.source_class,
            "adapter": entry.source.adapter,
            "feed_type": entry.source.feed_type,
            "source_category": entry.source.source_category,
            **entry.extra_provenance,
        },
    }


def cluster_id_for(source_id: str, identity_type: str, identity_value: str) -> str:
    return hashlib.sha256("{}|{}|{}".format(source_id, identity_type, identity_value).encode("utf-8")).hexdigest()[:24]


def candidate_for(report: Dict[str, Any], dedupe_identity: Dict[str, str]) -> Dict[str, Any]:
    candidate_basis = "{}|{}".format(report["report_id"], report["canonical_url"] or "")
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_id": hashlib.sha256(candidate_basis.encode("utf-8")).hexdigest()[:24],
        "report_id": report["report_id"],
        "source_id": report["source_id"],
        "source_name": report["source_name"],
        "source_class": report["source_class"],
        "title": report["title"],
        "canonical_url": report["canonical_url"],
        "published_at": report["published_at"],
        "fetched_at": report["fetched_at"],
        "clean_summary": report["clean_summary"],
        "dedupe_identity": dedupe_identity,
        "provenance": {
            "feed_url": report["feed_url"],
            "adapter": report["provenance"]["adapter"],
            "source_class": report["source_class"],
            "source_category": report["source_category"],
        },
        "metadata": {
            "embedding_text": " ".join(part for part in [report["title"], report["clean_summary"]] if part).strip(),
        },
    }


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def read_fixture_feed(fixture_dir: Path, source: SourceConfig) -> Tuple[str, Optional[int], Optional[str], int]:
    feed_path = fixture_dir / "{}.xml".format(source.id)
    if not feed_path.exists():
        raise PipelineError("Missing fixture for {}: {}".format(source.id, feed_path))
    return feed_path.read_text(), 200, "application/xml", 1


def run_pipeline(sources_path: Path, output_dir: Path, now: datetime, fixture_dir: Optional[Path] = None) -> Tuple[int, Dict[str, Any]]:
    sources = read_source_config(sources_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    source_health: List[Dict[str, Any]] = []
    normalized_rows: List[Dict[str, Any]] = []
    clusters: List[Dict[str, Any]] = []
    publish_candidates: List[Dict[str, Any]] = []
    duplicate_count = 0
    fetched_sources = 0
    failed_sources = 0
    raw_entries = 0
    succeeded_sources = 0

    for source in sources:
        if not source.enabled:
            source_health.append(
                {
                    "source_id": source.id,
                    "source_name": source.name,
                    "status": "disabled",
                    "feed_url": source.feed_url,
                    "fetched_at": None,
                    "http_status": None,
                    "content_type": None,
                    "raw_entry_count": 0,
                    "normalized_entry_count": 0,
                    "failure_reason": None,
                    "elapsed_ms": 0,
                }
            )
            continue

        fetched_at = iso_z(now)
        try:
            if fixture_dir is not None:
                xml_text, status_code, content_type, elapsed_ms = read_fixture_feed(fixture_dir, source)
            else:
                xml_text, status_code, content_type, elapsed_ms = fetch_feed(source)
            fetched_sources += 1
            entries = parse_feed(xml_text, source)
            raw_entries += len(entries)
            seen: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
            source_clusters: List[Dict[str, Any]] = []
            normalized_count = 0
            for entry in entries:
                report = normalize_entry(entry, fetched_at)
                identity = dedupe_key(report)
                if identity in seen:
                    duplicate_count += 1
                    cluster_id = cluster_id_for(source.id, identity[1], identity[2])
                    existing_report = seen[identity]
                    cluster = None
                    for row in source_clusters:
                        if row["cluster_id"] == cluster_id:
                            cluster = row
                            break
                    if cluster is None:
                        cluster = {
                            "schema_version": SCHEMA_VERSION,
                            "cluster_id": cluster_id,
                            "scope": "source_local",
                            "canonical_report_id": existing_report["report_id"],
                            "source_id": source.id,
                            "report_ids": [existing_report["report_id"]],
                            "dedupe_reasons": [identity[1]],
                        }
                        source_clusters.append(cluster)
                    cluster["report_ids"].append(report["report_id"])
                    continue

                seen[identity] = report
                normalized_rows.append(report)
                normalized_count += 1
                publish_candidates.append(
                    candidate_for(
                        report,
                        {"primary": identity[1], "value": identity[2]},
                    )
                )

            clusters.extend(source_clusters)
            status = "ok" if entries else "empty"
            succeeded_sources += 1
            source_health.append(
                {
                    "source_id": source.id,
                    "source_name": source.name,
                    "status": status,
                    "feed_url": source.feed_url,
                    "fetched_at": fetched_at,
                    "http_status": status_code,
                    "content_type": content_type,
                    "raw_entry_count": len(entries),
                    "normalized_entry_count": normalized_count,
                    "failure_reason": None,
                    "elapsed_ms": elapsed_ms,
                }
            )
        except Exception as exc:
            failed_sources += 1
            source_health.append(
                {
                    "source_id": source.id,
                    "source_name": source.name,
                    "status": "failed",
                    "feed_url": source.feed_url,
                    "fetched_at": fetched_at,
                    "http_status": getattr(getattr(exc, "response", None), "status_code", None),
                    "content_type": None,
                    "raw_entry_count": 0,
                    "normalized_entry_count": 0,
                    "failure_reason": str(exc),
                    "elapsed_ms": 0,
                }
            )

    enabled_sources = [source for source in sources if source.enabled]
    if succeeded_sources == 0:
        exit_status = "failed"
        exit_code = 1
    elif failed_sources > 0:
        exit_status = "partial_failure"
        exit_code = 0
    else:
        exit_status = "success"
        exit_code = 0

    run_summary = {
        "schema_version": SCHEMA_VERSION,
        "run_id": "{}-local".format(now.strftime("%Y%m%dT%H%M%SZ")),
        "started_at": iso_z(now),
        "finished_at": iso_z(utc_now()),
        "now": iso_z(now),
        "source_config_path": str(sources_path),
        "output_dir": str(output_dir),
        "exit_status": exit_status,
        "counts": {
            "configured_sources": len(sources),
            "enabled_sources": len(enabled_sources),
            "fetched_sources": fetched_sources,
            "failed_sources": failed_sources,
            "raw_entries": raw_entries,
            "normalized_entries": len(normalized_rows),
            "source_local_duplicates": duplicate_count,
            "publish_candidates": len(publish_candidates),
        },
        "artifact_paths": {
            "source_health": "source-health.json",
            "normalized": "normalized.jsonl",
            "clusters": "clusters.jsonl",
            "publish_candidates": "publish-candidates.jsonl",
        },
    }

    write_json(output_dir / "run-summary.json", run_summary)
    write_json(output_dir / "source-health.json", source_health)
    write_jsonl(output_dir / "normalized.jsonl", normalized_rows)
    write_jsonl(output_dir / "clusters.jsonl", clusters)
    write_jsonl(output_dir / "publish-candidates.jsonl", publish_candidates)
    return exit_code, run_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local-only swarm.channel news ingestion")
    parser.add_argument("--sources", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--now", type=str)
    parser.add_argument("--fixture-dir", type=Path)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    now = parse_now(args.now)
    try:
        exit_code, run_summary = run_pipeline(args.sources, args.out, now, fixture_dir=args.fixture_dir)
    except PipelineError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(run_summary, indent=2))
    return exit_code
