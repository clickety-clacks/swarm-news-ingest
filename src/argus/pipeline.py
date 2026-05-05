from __future__ import annotations

import argparse
import dataclasses
import hashlib
import html
import json
import re
import sys
import time
import posixpath
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse
from xml.etree import ElementTree as ET

import requests
import yaml

SCHEMA_VERSION = 1
STATE_SCHEMA_VERSION = 1
REQUEST_TIMEOUT_SECONDS = 20
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DROP_QUERY_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
    "igshid",
    "ref",
    "ref_src",
}
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
ARXIV_NS = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}
UNRESERVED_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")


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
    cadence_interval_seconds: Optional[int] = None
    authority_score: Optional[float] = None
    fixture_payload_path: Optional[str] = None


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


@dataclasses.dataclass
class FetchResult:
    body: Optional[str]
    status_code: Optional[int]
    content_type: Optional[str]
    elapsed_ms: int
    etag: Optional[str]
    last_modified: Optional[str]


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


def time_delta_hours(hours: int) -> timedelta:
    return timedelta(hours=hours)


def default_state_path(output_dir: Path) -> Path:
    if output_dir.parent.name == "runs" or re.match(r"^(dry-run-)?\d{8}T\d{6}Z$", output_dir.name):
        return output_dir.parent / "state.json"
    return output_dir / "state.json"


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
                authority_score=(float(row["authority_score"]) if row.get("authority_score") is not None else None),
                fixture_payload_path=(str(row["fixture_payload_path"]) if row.get("fixture_payload_path") is not None else None),
            )
        )
    return sources


def empty_state(path: Optional[Path]) -> Dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "state_path": str(path) if path is not None else None,
        "sources": {},
    }


def load_state(path: Optional[Path]) -> Dict[str, Any]:
    if path is None or not path.exists():
        return empty_state(path)
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise PipelineError("Invalid state file: {}".format(path))
    if payload.get("schema_version") != STATE_SCHEMA_VERSION:
        raise PipelineError("Unsupported state schema in {}".format(path))
    if not isinstance(payload.get("sources"), dict):
        raise PipelineError("Invalid state sources in {}".format(path))
    payload["state_path"] = str(path)
    return payload


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": STATE_SCHEMA_VERSION,
        "state_path": str(path),
        "sources": state.get("sources", {}),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def source_state_view(state: Dict[str, Any], source_id: str) -> Dict[str, Any]:
    sources = state.setdefault("sources", {})
    view = sources.setdefault(source_id, {})
    view.setdefault("validators", {})
    view.setdefault("seen_identities", [])
    return view


def fetch_feed(source: SourceConfig, validators: Optional[Dict[str, str]] = None) -> FetchResult:
    started = time.perf_counter()
    headers = {"User-Agent": "argus/0.1 (+local-only worker)"}
    headers.update(source.request_headers)
    if validators:
        if validators.get("etag"):
            headers["If-None-Match"] = validators["etag"]
        if validators.get("last_modified"):
            headers["If-Modified-Since"] = validators["last_modified"]
    response = requests.get(source.feed_url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    if response.status_code == 304:
        return FetchResult(
            body=None,
            status_code=response.status_code,
            content_type=response.headers.get("content-type"),
            elapsed_ms=elapsed_ms,
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
        )
    response.raise_for_status()
    return FetchResult(
        body=response.text,
        status_code=response.status_code,
        content_type=response.headers.get("content-type"),
        elapsed_ms=elapsed_ms,
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
    )


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
    collapsed = re.sub(r"\s+", " ", text.lower()).strip()
    return re.sub(r"^[^\w]+|[^\w]+$", "", collapsed).strip()


def normalize_percent_escapes(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        raw = match.group(0)
        char = chr(int(raw[1:], 16))
        return char if char in UNRESERVED_CHARS else raw.upper()

    return re.sub(r"%[0-9A-Fa-f]{2}", replace, value)


def canonicalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    parsed = urlparse(url.strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None
    query = sorted((k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() not in DROP_QUERY_PARAMS)
    netloc = parsed.netloc.lower()
    if (parsed.scheme == "https" and netloc.endswith(":443")) or (parsed.scheme == "http" and netloc.endswith(":80")):
        netloc = netloc.rsplit(":", 1)[0]
    path = posixpath.normpath(normalize_percent_escapes(parsed.path or "/"))
    if not path.startswith("/"):
        path = "/" + path
    if (parsed.path or "/").endswith("/") and not path.endswith("/"):
        path += "/"
    path = normalize_percent_escapes(quote(path, safe="/-._~%"))
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
                title=child_text(item, "title") or "",
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
                title=atom_child_text(item, "atom:title", ATOM_NS) or "",
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
                title=atom_child_text(item, "atom:title", ARXIV_NS) or "",
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
    formats = [None, "%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"]
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


def normalize_feed_entry_id(value: str) -> str:
    return re.sub(r"[ \t\r\n]+", " ", value.strip())


def report_identity_input(feed_entry_id: Optional[str], canonical_url: Optional[str], title: str, published_at: Optional[str]) -> Tuple[str, str]:
    if feed_entry_id:
        return "feed_entry_id", normalize_feed_entry_id(feed_entry_id)
    if canonical_url:
        return "canonical_url", canonical_url
    if not normalize_title(title):
        return "missing_identity_key", ""
    return "normalized_title_date", "{}\n{}".format(normalize_title(title), date_bucket(published_at))


def report_id_for(source_id: str, feed_entry_id: Optional[str], canonical_url: Optional[str], title: str, identity_timestamp: Optional[str]) -> str:
    input_type, input_value = report_identity_input(feed_entry_id, canonical_url, title, identity_timestamp)
    basis = "report:v0\n{}\n{}\n{}".format(source_id, input_type, input_value)
    return "sha256:" + hashlib.sha256(basis.encode("utf-8")).hexdigest()


def date_bucket(timestamp: Optional[str]) -> str:
    return timestamp[:10] if timestamp else "unknown"


def dedupe_key(report: Dict[str, Any]) -> Tuple[str, str, str]:
    source_id = report["source_id"]
    if report.get("report_id_input_type") == "missing_identity_key":
        return source_id, "missing_identity_key", ""
    if report.get("feed_entry_id"):
        return source_id, "feed_entry_id", normalize_feed_entry_id(report["feed_entry_id"])
    if report.get("canonical_url"):
        return source_id, "canonical_url", report["canonical_url"]
    return source_id, "normalized_title_date", "{}\n{}".format(normalize_title(report["title"]), date_bucket(report.get("published_at") or report.get("fetched_at")))


def persistent_identities_for(report: Dict[str, Any]) -> List[Tuple[str, str]]:
    identities: List[Tuple[str, str]] = []
    if report.get("feed_entry_id"):
        identities.append(("feed_entry_id", normalize_feed_entry_id(report["feed_entry_id"])))
    if report.get("canonical_url"):
        identities.append(("canonical_url", report["canonical_url"]))
    if normalize_title(report["title"]) and report.get("report_id_input_type") != "missing_identity_key":
        identities.append(
            (
                "normalized_title_date",
                "{}\n{}".format(normalize_title(report["title"]), date_bucket(report.get("published_at") or report.get("fetched_at"))),
            )
        )
    return identities


def persistent_identity_keys(report: Dict[str, Any]) -> List[str]:
    return ["{}|{}".format(kind, value) for kind, value in persistent_identities_for(report)]


def normalize_entry(entry: RawEntry, fetched_at: str) -> Dict[str, Any]:
    canonical_url = canonicalize_url(entry.canonical_url or entry.raw_url)
    raw_url = canonicalize_url(entry.raw_url) if entry.raw_url else canonical_url
    clean_summary_value = clean_text(entry.raw_summary)
    published_at = normalize_timestamp(entry.published_at)
    identity_timestamp = published_at or fetched_at
    report_input_type, report_input_value = report_identity_input(entry.feed_entry_id, canonical_url, entry.title, identity_timestamp)
    report_id = report_id_for(entry.source.id, entry.feed_entry_id, canonical_url, entry.title, identity_timestamp)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_id": report_id,
        "report_id_input_type": report_input_type,
        "report_id_input_value": report_input_value,
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
    embedding_text = "\n".join(
        "{}: {}".format(label, value)
        for label, value in [
            ("Title", report["title"]),
            ("Source", report["source_name"]),
            ("Published", report["published_at"]),
            ("URL", report["canonical_url"]),
            ("Summary", report["clean_summary"]),
        ]
        if value
    ).strip()
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_id": hashlib.sha256(candidate_basis.encode("utf-8")).hexdigest()[:24],
        "report_id": report["report_id"],
        "source_id": report["source_id"],
        "source_name": report["source_name"],
        "source_class": report["source_class"],
        "title": report["title"],
        "canonical_url": report["canonical_url"],
        "raw_url": report["raw_url"],
        "feed_entry_id": report["feed_entry_id"],
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
            "embedding_text": embedding_text,
        },
    }


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def read_fixture_feed(fixture_dir: Path, source: SourceConfig, validators: Optional[Dict[str, str]] = None) -> FetchResult:
    resolved_fixture_dir = fixture_dir if fixture_dir.is_absolute() else PROJECT_ROOT / fixture_dir
    if source.fixture_payload_path:
        feed_path = Path(source.fixture_payload_path)
        if not feed_path.is_absolute():
            feed_path = PROJECT_ROOT / feed_path
    else:
        feed_path = resolved_fixture_dir / "{}.xml".format(source.id)
    meta_path = resolved_fixture_dir / "{}.meta.json".format(source.id)
    if not feed_path.exists():
        raise PipelineError("Missing fixture for {}: {}".format(source.id, feed_path))
    metadata: Dict[str, Any] = {}
    if meta_path.exists():
        metadata = json.loads(meta_path.read_text())
    status_code = int(metadata.get("status_code", 200))
    etag = metadata.get("etag")
    last_modified = metadata.get("last_modified")
    if status_code == 304:
        expected_etag = metadata.get("if_none_match")
        expected_last_modified = metadata.get("if_modified_since")
        if expected_etag and (validators or {}).get("etag") != expected_etag:
            status_code = 200
        elif expected_last_modified and (validators or {}).get("last_modified") != expected_last_modified:
            status_code = 200
    if status_code == 304:
        return FetchResult(body=None, status_code=304, content_type=metadata.get("content_type"), elapsed_ms=1, etag=etag, last_modified=last_modified)
    return FetchResult(body=feed_path.read_text(), status_code=status_code, content_type=metadata.get("content_type", "application/xml"), elapsed_ms=1, etag=etag, last_modified=last_modified)


def run_pipeline(
    sources_path: Path,
    output_dir: Path,
    now: datetime,
    fixture_dir: Optional[Path] = None,
    dry_run: bool = False,
    prime: bool = False,
    state_path: Optional[Path] = None,
    state_read: bool = True,
    state_write: bool = True,
    dedupe_in_pipeline: bool = True,
    package_candidates_artifact: str = "package-candidates.jsonl",
    publish_candidates_artifact: str = "publish-candidates.jsonl",
) -> Tuple[int, Dict[str, Any]]:
    sources = read_source_config(sources_path)
    return run_pipeline_for_sources(
        sources,
        str(sources_path),
        output_dir,
        now,
        fixture_dir=fixture_dir,
        dry_run=dry_run,
        prime=prime,
        state_path=state_path,
        state_read=state_read,
        state_write=state_write,
        dedupe_in_pipeline=dedupe_in_pipeline,
        package_candidates_artifact=package_candidates_artifact,
        publish_candidates_artifact=publish_candidates_artifact,
    )


def run_pipeline_for_sources(
    sources: List[SourceConfig],
    source_config_path: str,
    output_dir: Path,
    now: datetime,
    fixture_dir: Optional[Path] = None,
    dry_run: bool = False,
    prime: bool = False,
    state_path: Optional[Path] = None,
    state_read: bool = True,
    state_write: bool = True,
    dedupe_in_pipeline: bool = True,
    package_candidates_artifact: str = "package-candidates.jsonl",
    publish_candidates_artifact: str = "publish-candidates.jsonl",
) -> Tuple[int, Dict[str, Any]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    effective_state_path = state_path or default_state_path(output_dir)
    state = load_state(effective_state_path) if state_read else {"schema_version": STATE_SCHEMA_VERSION, "sources": {}}
    next_state = json.loads(json.dumps(state))

    source_health: List[Dict[str, Any]] = []
    normalized_rows: List[Dict[str, Any]] = []
    clusters: List[Dict[str, Any]] = []
    publish_candidates: List[Dict[str, Any]] = []
    skipped_item_details: List[Dict[str, Any]] = []
    primed_candidates = 0
    duplicate_count = 0
    fetched_sources = 0
    failed_sources = 0
    raw_entries = 0
    succeeded_sources = 0

    for source in sources:
        state_before = source_state_view(state, source.id)
        state_after = source_state_view(next_state, source.id)
        validator_view = state_before.get("validators", {})
        seen_identities: Set[str] = set(state_before.get("seen_identities", []))

        if not source.enabled:
            source_health.append(
                {
                    "source_id": source.id,
                    "source_name": source.name,
                    "status": "disabled",
                    "enabled": False,
                    "parse_status": "skipped",
                    "last_successful_fetch_at": None,
                    "latest_item_published_at": None,
                    "feed_url": source.feed_url,
                    "fetched_at": None,
                    "http_status": None,
                    "content_type": None,
                    "raw_entry_count": 0,
                    "normalized_entry_count": 0,
                    "emitted_candidate_count": 0,
                    "skipped_previously_seen_count": 0,
                    "failure_reason": None,
                    "last_error": None,
                    "elapsed_ms": 0,
                    "etag": validator_view.get("etag"),
                    "last_modified": validator_view.get("last_modified"),
                }
            )
            continue

        fetched_at = iso_z(now)
        try:
            if fixture_dir is not None:
                fetch_result = read_fixture_feed(fixture_dir, source, validators=validator_view)
            else:
                fetch_result = fetch_feed(source, validators=validator_view)
            fetched_sources += 1

            if fetch_result.status_code == 304:
                succeeded_sources += 1
                if fetch_result.etag:
                    state_after["validators"]["etag"] = fetch_result.etag
                if fetch_result.last_modified:
                    state_after["validators"]["last_modified"] = fetch_result.last_modified
                source_health.append(
                    {
                        "source_id": source.id,
                        "source_name": source.name,
                        "status": "not_modified",
                        "enabled": True,
                        "parse_status": "not_modified",
                        "last_successful_fetch_at": fetched_at,
                        "latest_item_published_at": None,
                        "feed_url": source.feed_url,
                        "fetched_at": fetched_at,
                        "http_status": fetch_result.status_code,
                        "content_type": fetch_result.content_type,
                        "raw_entry_count": 0,
                        "normalized_entry_count": 0,
                        "emitted_candidate_count": 0,
                        "skipped_previously_seen_count": 0,
                        "failure_reason": None,
                        "last_error": None,
                        "elapsed_ms": fetch_result.elapsed_ms,
                        "etag": state_after["validators"].get("etag"),
                        "last_modified": state_after["validators"].get("last_modified"),
                    }
                )
                continue

            entries = parse_feed(fetch_result.body or "", source)
            raw_entries += len(entries)
            seen: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
            source_clusters: List[Dict[str, Any]] = []
            normalized_count = 0
            emitted_count = 0
            skipped_seen_count = 0
            skipped_stale_count = 0
            for entry in entries:
                report = normalize_entry(entry, fetched_at)
                identity = dedupe_key(report)
                if identity[1] == "missing_identity_key":
                    skipped_item_details.append(
                        {
                            "source_id": source.id,
                            "report_id": report["report_id"],
                            "reason_code": "missing_identity_key",
                            "detail": {
                                "source_id": source.id,
                                "title": report["title"],
                                "canonical_url": report.get("canonical_url"),
                                "feed_entry_id": report.get("feed_entry_id"),
                                "published_at": report.get("published_at"),
                            },
                        }
                    )
                    continue
                normalized_rows.append(report)
                normalized_count += 1
                if identity in seen:
                    duplicate_count += 1
                    cluster_id = cluster_id_for(source.id, identity[1], identity[2])
                    existing_report = seen[identity]
                    cluster = next((row for row in source_clusters if row["cluster_id"] == cluster_id), None)
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
                    if dedupe_in_pipeline:
                        continue

                seen[identity] = report

                identity_key = "{}|{}".format(identity[1], identity[2])
                if identity_key in seen_identities:
                    skipped_seen_count += 1
                    continue
                if source.freshness_window_hours is not None and report.get("published_at"):
                    published_at = parse_now(report["published_at"])
                    if published_at < now - time_delta_hours(source.freshness_window_hours):
                        skipped_stale_count += 1
                        skipped_item_details.append(
                            {
                                "source_id": source.id,
                                "report_id": report["report_id"],
                                "reason_code": "stale_by_freshness_window",
                                "detail": {
                                    "source_id": source.id,
                                    "title": report["title"],
                                    "canonical_url": report.get("canonical_url"),
                                    "feed_entry_id": report.get("feed_entry_id"),
                                    "published_at": report.get("published_at"),
                                    "freshness_window_hours": source.freshness_window_hours,
                                },
                            }
                        )
                        seen_identities.add(identity_key)
                        continue

                if prime:
                    primed_candidates += 1
                else:
                    emitted_count += 1
                    publish_candidates.append(candidate_for(report, {"primary": identity[1], "value": identity[2]}))
                seen_identities.add(identity_key)

            clusters.extend(source_clusters)
            status = "ok" if entries else "empty"
            succeeded_sources += 1
            if fetch_result.etag:
                state_after["validators"]["etag"] = fetch_result.etag
            if fetch_result.last_modified:
                state_after["validators"]["last_modified"] = fetch_result.last_modified
            state_after["seen_identities"] = sorted(seen_identities)
            source_health.append(
                {
                    "source_id": source.id,
                    "source_name": source.name,
                    "status": status,
                    "enabled": True,
                    "parse_status": "ok",
                    "last_successful_fetch_at": fetched_at,
                    "latest_item_published_at": max((row.get("published_at") for row in normalized_rows if row["source_id"] == source.id and row.get("published_at")), default=None),
                    "feed_url": source.feed_url,
                    "fetched_at": fetched_at,
                    "http_status": fetch_result.status_code,
                    "content_type": fetch_result.content_type,
                    "raw_entry_count": len(entries),
                    "normalized_entry_count": normalized_count,
                    "emitted_candidate_count": emitted_count,
                    "skipped_previously_seen_count": skipped_seen_count,
                    "skipped_stale_count": skipped_stale_count,
                    "failure_reason": None,
                    "last_error": None,
                    "elapsed_ms": fetch_result.elapsed_ms,
                    "etag": state_after["validators"].get("etag"),
                    "last_modified": state_after["validators"].get("last_modified"),
                }
            )
        except Exception as exc:
            failed_sources += 1
            source_health.append(
                {
                    "source_id": source.id,
                    "source_name": source.name,
                    "status": "failed",
                    "enabled": True,
                    "parse_status": "failed",
                    "last_successful_fetch_at": None,
                    "latest_item_published_at": None,
                    "feed_url": source.feed_url,
                    "fetched_at": fetched_at,
                    "http_status": getattr(getattr(exc, "response", None), "status_code", None),
                    "content_type": None,
                    "raw_entry_count": 0,
                    "normalized_entry_count": 0,
                    "emitted_candidate_count": 0,
                    "skipped_previously_seen_count": 0,
                    "failure_reason": str(exc),
                    "last_error": {"class": exc.__class__.__name__, "message": str(exc)},
                    "elapsed_ms": 0,
                    "etag": validator_view.get("etag"),
                    "last_modified": validator_view.get("last_modified"),
                }
            )

    enabled_sources = [source for source in sources if source.enabled]
    if failed_sources > 0:
        exit_status = "partial_failure"
        exit_code = 0
    else:
        exit_status = "success"
        exit_code = 0

    if state_write and exit_code == 0:
        save_state(effective_state_path, next_state)

    run_summary = {
        "schema_version": SCHEMA_VERSION,
        "run_id": "{}-local".format(now.strftime("%Y%m%dT%H%M%SZ")),
        "started_at": iso_z(now),
        "finished_at": iso_z(utc_now()),
        "now": iso_z(now),
        "source_config_path": source_config_path,
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
            "primed_candidates": primed_candidates,
        },
        "artifact_paths": {
            "source_health": "source-health.json",
            "normalized": "normalized-items.jsonl",
            "clusters": "clusters.jsonl",
            "dedupe_decisions": "dedupe-decisions.json",
            "skipped_items": "skipped-items.json",
            "digest_json": "digest.json",
            "digest_markdown": "digest.md",
            **({} if prime else {"package_candidates": "package-candidates.jsonl", "publish_candidates": "publish-candidates.jsonl"}),
        },
        "dry_run": dry_run,
        "prime": prime,
        "publish_performed": False,
        "state": {
            "path": str(effective_state_path),
            "loaded": bool(state_read and effective_state_path.exists()),
            "read_enabled": state_read,
            "write_enabled": state_write,
            "write_performed": bool(state_write and exit_code == 0),
        },
    }

    write_json(output_dir / "run-summary.json", run_summary)
    write_json(output_dir / "source-health.json", source_health)
    write_jsonl(output_dir / "normalized.jsonl", normalized_rows)
    write_jsonl(output_dir / "normalized-items.jsonl", normalized_rows)
    write_jsonl(output_dir / "clusters.jsonl", clusters)
    write_json(output_dir / "dedupe-decisions.json", clusters)
    skipped_items = skipped_item_details + [
        {
            "source_id": row["source_id"],
            "reason_code": "source_skipped_counts",
            "skipped_previously_seen_count": row.get("skipped_previously_seen_count", 0),
            "skipped_stale_count": row.get("skipped_stale_count", 0),
        }
        for row in source_health
        if row.get("skipped_previously_seen_count", 0) or row.get("skipped_stale_count", 0)
    ]
    write_json(output_dir / "skipped-items.json", skipped_items)
    digest = {
        "run_id": run_summary["run_id"],
        "candidate_count": len(publish_candidates),
        "source_health": source_health,
    }
    write_json(output_dir / "digest.json", digest)
    (output_dir / "digest.md").write_text("# Argus Digest\n\n{} package candidates.\n".format(len(publish_candidates)))
    if not prime:
        write_jsonl(output_dir / publish_candidates_artifact, publish_candidates)
        write_jsonl(output_dir / package_candidates_artifact, publish_candidates)
    return exit_code, run_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Argus local-only RSS ingestion")
    parser.add_argument("--sources", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--state", type=Path, help="Path to persistent Argus feed state. Defaults to <out parent>/state.json.")
    parser.add_argument("--no-state-write", action="store_true", help="Read existing state but do not mutate durable state for this run.")
    parser.add_argument("--now", type=str)
    parser.add_argument("--fixture-dir", type=Path)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch sources and write local artifacts only. Dry runs read state for filtering but do not write state unless future behavior explicitly changes.",
    )
    parser.add_argument(
        "--prime",
        action="store_true",
        help="Fetch sources, advance durable seen-state/validators, and emit no publish candidates. Mutually exclusive with --dry-run.",
    )
    return parser


def build_command_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Argus server-mode RSS ingestion")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="Start the long-running Argus server")
    serve.add_argument("--config", required=True, type=Path)
    serve.add_argument("--once", action="store_true", help="Run one scheduler decision and exit. Intended for deterministic tests.")

    prime = subparsers.add_parser("prime", help="Run an explicit baseline prime cycle")
    prime.add_argument("--config", required=True, type=Path)

    run_cycle = subparsers.add_parser("run-cycle", help="Run one normal manual cycle")
    run_cycle.add_argument("--config", required=True, type=Path)
    run_cycle.add_argument("--reason", default="manual")
    run_cycle.add_argument("--max-live-publishes", type=int, default=None, help="Fail the cycle before publishing if more than this many live sends would be emitted.")

    set_publish = subparsers.add_parser("set-publish-state", help="Record a publish-state snapshot")
    set_publish.add_argument("--config", required=True, type=Path)
    set_publish.add_argument("--state", required=True, choices=["inactive", "active"])

    reload_parser = subparsers.add_parser("reload", help="Reload scheduler and publish config")
    reload_parser.add_argument("--config", required=True, type=Path)

    status = subparsers.add_parser("status", help="Print runtime status")
    status.add_argument("--db", required=True, type=Path)

    health = subparsers.add_parser("source-health", help="Print last source-health artifact")
    health.add_argument("--db", required=True, type=Path)

    explain = subparsers.add_parser("explain-skip", help="Print run explanation details")
    explain.add_argument("--db", required=True, type=Path)
    explain.add_argument("--run", required=True)
    return parser


def command_main(argv: List[str]) -> int:
    from .server import ArgusServer, explain_skip, request_control_action, request_process_reload, run_source_health, run_status, runtime_service_pid

    args = build_command_parser().parse_args(argv)
    try:
        if args.command == "serve":
            server = ArgusServer(args.config)
            try:
                if args.once:
                    result = server.tick()
                    print(json.dumps(result[1] if result else server.status(), indent=2))
                    return result[0] if result else 0
                return server.serve_forever()
            finally:
                server.close()
        if args.command == "prime":
            if runtime_service_pid(args.config) is not None:
                print(json.dumps(request_control_action(args.config, "prime", {"requested_by": "cli"}), indent=2))
                return 0
            server = ArgusServer(args.config, register_service=False)
            try:
                exit_code, summary = server.prime()
                print(json.dumps(summary, indent=2))
                return exit_code
            finally:
                server.close()
        if args.command == "run-cycle":
            if runtime_service_pid(args.config) is not None:
                payload = {"reason": args.reason}
                if args.max_live_publishes is not None:
                    payload["max_live_publishes"] = args.max_live_publishes
                print(json.dumps(request_control_action(args.config, "run-cycle", payload), indent=2))
                return 0
            server = ArgusServer(args.config, register_service=False)
            try:
                exit_code, summary = server.manual_cycle(max_live_publishes=args.max_live_publishes)
                print(json.dumps(summary, indent=2))
                return exit_code
            finally:
                server.close()
        if args.command == "set-publish-state":
            if runtime_service_pid(args.config) is not None:
                print(json.dumps(request_control_action(args.config, "set-publish-state", {"state": args.state}), indent=2))
                return 0
            server = ArgusServer(args.config, register_service=False)
            try:
                print(json.dumps(server.set_publish_state(args.state), indent=2))
                return 0
            finally:
                server.close()
        if args.command == "reload":
            print(json.dumps(request_process_reload(args.config), indent=2))
            return 0
        if args.command == "status":
            print(json.dumps(run_status(args.db), indent=2))
            return 0
        if args.command == "source-health":
            print(json.dumps(run_source_health(args.db), indent=2))
            return 0
        if args.command == "explain-skip":
            print(json.dumps(explain_skip(args.db, args.run), indent=2))
            return 0
    except PipelineError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 2


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(argv if argv is not None else sys.argv[1:])
    if not argv or argv[0] in {"-h", "--help", "serve", "prime", "run-cycle", "set-publish-state", "reload", "status", "source-health", "explain-skip"}:
        return command_main(argv)
    args = build_parser().parse_args(argv)
    if args.dry_run and args.prime:
        print("--prime and --dry-run are mutually exclusive", file=sys.stderr)
        return 2
    now = parse_now(args.now)
    state_write = not args.no_state_write and not args.dry_run
    try:
        exit_code, run_summary = run_pipeline(
            args.sources,
            args.out,
            now,
            fixture_dir=args.fixture_dir,
            dry_run=args.dry_run,
            prime=args.prime,
            state_path=args.state,
            state_write=state_write,
        )
    except PipelineError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(run_summary, indent=2))
    return exit_code
