#!/usr/bin/env python3
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
SOURCE_DB = STATE_DIR / "source_registry.db"
EVENT_DB = STATE_DIR / "event_memory.db"
CONFIG_PATH = ROOT / "config.json"


@dataclass
class SourceRecord:
    source_id: str
    name: str
    base_url: str
    kind: str
    family: str
    platform: str
    content_mode: str
    adapter: str
    enabled: int = 1
    tier: int = 2
    priority: int = 100
    language: str | None = None
    region: str | None = None
    notes: str | None = None


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text())


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_source_registry(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sources (
            source_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            base_url TEXT NOT NULL,
            kind TEXT NOT NULL,
            family TEXT NOT NULL,
            platform TEXT NOT NULL,
            content_mode TEXT NOT NULL,
            adapter TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            tier INTEGER NOT NULL DEFAULT 2,
            priority INTEGER NOT NULL DEFAULT 100,
            language TEXT,
            region TEXT,
            notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS source_topics (
            source_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            PRIMARY KEY (source_id, topic),
            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS source_fetch_config (
            source_id TEXT PRIMARY KEY,
            index_url TEXT,
            feed_url TEXT,
            api_url TEXT,
            path_hints_json TEXT,
            max_items INTEGER,
            lookback_hours INTEGER,
            requires_browser INTEGER DEFAULT 0,
            requires_auth INTEGER DEFAULT 0,
            rate_limit_hint TEXT,
            custom_config_json TEXT,
            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS source_health (
            source_id TEXT PRIMARY KEY,
            last_success_at TEXT,
            last_failure_at TEXT,
            consecutive_failures INTEGER DEFAULT 0,
            last_http_status INTEGER,
            last_error TEXT,
            avg_items_per_run REAL,
            last_item_count INTEGER,
            disabled_reason TEXT,
            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
        );
        """
    )
    conn.commit()


def init_event_memory(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS observations (
            observation_id TEXT PRIMARY KEY,
            source_id TEXT,
            observed_at TEXT NOT NULL,
            title TEXT,
            url TEXT,
            content_snippet TEXT,
            published_at TEXT,
            raw_hash TEXT,
            normalized_hash TEXT,
            category TEXT,
            score_total REAL,
            event_id TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            canonical_title TEXT,
            canonical_url TEXT,
            category TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_reported_at TEXT,
            last_score REAL,
            status TEXT,
            is_active INTEGER DEFAULT 1,
            material_update_flag INTEGER DEFAULT 0,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS event_observations (
            event_id TEXT NOT NULL,
            observation_id TEXT NOT NULL,
            source_id TEXT,
            role TEXT,
            PRIMARY KEY (event_id, observation_id)
        );

        CREATE TABLE IF NOT EXISTS event_reports (
            event_id TEXT NOT NULL,
            report_date TEXT NOT NULL,
            channel TEXT NOT NULL,
            reported_score REAL,
            reported_summary TEXT,
            push_sent INTEGER DEFAULT 0,
            PRIMARY KEY (event_id, report_date, channel)
        );

        CREATE INDEX IF NOT EXISTS idx_observations_source_id ON observations(source_id);
        CREATE INDEX IF NOT EXISTS idx_observations_event_id ON observations(event_id);
        CREATE INDEX IF NOT EXISTS idx_events_last_reported_at ON events(last_reported_at);
        CREATE INDEX IF NOT EXISTS idx_event_reports_report_date ON event_reports(report_date);
        """
    )
    conn.commit()


def infer_topics(text: str) -> list[str]:
    text = text.lower()
    topics = []
    mapping = {
        "wallet_aa_ux": ["wallet", "metamask", "safe", "trust", "aa", "account abstraction"],
        "protocol_infra": ["ethereum", "eip", "protocol", "infra", "rollup"],
        "security_risk_compliance": ["security", "rekt", "chainalysis", "trm", "compliance"],
        "tron_stablecoin_payments": ["tron", "circle", "tether", "fireblocks", "stablecoin", "payments"],
        "competitors": ["metamask", "safe", "trust"],
        "market_structure": ["paradigm", "a16z", "research"],
    }
    for topic, hints in mapping.items():
        if any(h in text for h in hints):
            topics.append(topic)
    return sorted(set(topics))


def seed_source_registry(conn: sqlite3.Connection, config: dict) -> None:
    ts = now_iso()
    source_rows: list[tuple[SourceRecord, dict, list[str]]] = []

    for name, url, item_type in config.get("rss_feeds", []):
        rec = SourceRecord(
            source_id=f"rss::{name.lower().replace(' ', '_')}",
            name=name,
            base_url=url,
            kind="rss",
            family="news" if item_type == "news" else "official_protocol",
            platform="web",
            content_mode="news" if item_type == "news" else "blog",
            adapter="generic_rss",
            tier=3 if item_type == "news" else 1,
            priority=100,
        )
        fetch_cfg = {"feed_url": url, "max_items": config.get("limits", {}).get("rss_items_per_feed", 12), "lookback_hours": config.get("lookback_hours", 30)}
        topics = infer_topics(f"{name} {url} {item_type}")
        source_rows.append((rec, fetch_cfg, topics))

    for name, url, item_type in config.get("github_endpoints", []):
        rec = SourceRecord(
            source_id=f"github::{name.lower().replace(' ', '_')}",
            name=name,
            base_url=url,
            kind="github_api",
            family="official_protocol",
            platform="github",
            content_mode="release",
            adapter="github_eips",
            tier=1,
            priority=90,
        )
        fetch_cfg = {"api_url": url, "max_items": config.get("limits", {}).get("github_items", 12), "lookback_hours": config.get("lookback_hours", 30)}
        topics = ["protocol_infra"]
        source_rows.append((rec, fetch_cfg, topics))

    for name, index_url, path_hints, item_type in config.get("html_sources", []):
        base = f"{name} {index_url} {item_type}".lower()
        family = "research"
        content_mode = "blog"
        tier = 2
        if item_type == "wallet_blog":
            family = "wallet_official"
            tier = 1
        elif item_type == "security_blog":
            family = "security"
            content_mode = "alert"
            tier = 1
        elif item_type == "payments_blog":
            family = "payments"
            tier = 1
        rec = SourceRecord(
            source_id=f"html::{name.lower().replace(' ', '_')}",
            name=name,
            base_url=index_url,
            kind="html_index",
            family=family,
            platform="web",
            content_mode=content_mode,
            adapter="generic_html_index",
            tier=tier,
            priority=95,
        )
        fetch_cfg = {"index_url": index_url, "path_hints_json": json.dumps(path_hints, ensure_ascii=False), "max_items": 6, "lookback_hours": config.get("lookback_hours", 30)}
        topics = infer_topics(base)
        source_rows.append((rec, fetch_cfg, topics))

    for rec, fetch_cfg, topics in source_rows:
        conn.execute(
            """
            INSERT INTO sources (source_id, name, base_url, kind, family, platform, content_mode, adapter, enabled, tier, priority, language, region, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                name=excluded.name,
                base_url=excluded.base_url,
                kind=excluded.kind,
                family=excluded.family,
                platform=excluded.platform,
                content_mode=excluded.content_mode,
                adapter=excluded.adapter,
                enabled=excluded.enabled,
                tier=excluded.tier,
                priority=excluded.priority,
                updated_at=excluded.updated_at
            """,
            (rec.source_id, rec.name, rec.base_url, rec.kind, rec.family, rec.platform, rec.content_mode, rec.adapter, rec.enabled, rec.tier, rec.priority, rec.language, rec.region, rec.notes, ts, ts),
        )
        conn.execute(
            """
            INSERT INTO source_fetch_config (source_id, index_url, feed_url, api_url, path_hints_json, max_items, lookback_hours, requires_browser, requires_auth, rate_limit_hint, custom_config_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, NULL, NULL)
            ON CONFLICT(source_id) DO UPDATE SET
                index_url=excluded.index_url,
                feed_url=excluded.feed_url,
                api_url=excluded.api_url,
                path_hints_json=excluded.path_hints_json,
                max_items=excluded.max_items,
                lookback_hours=excluded.lookback_hours
            """,
            (
                rec.source_id,
                fetch_cfg.get("index_url"),
                fetch_cfg.get("feed_url"),
                fetch_cfg.get("api_url"),
                fetch_cfg.get("path_hints_json"),
                fetch_cfg.get("max_items"),
                fetch_cfg.get("lookback_hours"),
            ),
        )
        conn.execute("DELETE FROM source_topics WHERE source_id = ?", (rec.source_id,))
        for topic in topics:
            conn.execute("INSERT OR IGNORE INTO source_topics (source_id, topic) VALUES (?, ?)", (rec.source_id, topic))
        conn.execute("INSERT OR IGNORE INTO source_health (source_id) VALUES (?)", (rec.source_id,))
    conn.commit()


def list_sources(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT s.*, f.index_url, f.feed_url, f.api_url, f.path_hints_json, f.max_items, f.lookback_hours
        FROM sources s
        LEFT JOIN source_fetch_config f ON f.source_id = s.source_id
        WHERE s.enabled = 1
        ORDER BY s.tier ASC, s.priority ASC, s.name ASC
        """
    ).fetchall()


def load_runtime_sources() -> dict:
    if not SOURCE_DB.exists():
        return {"rss_feeds": [], "github_endpoints": [], "html_sources": []}
    with connect(SOURCE_DB) as conn:
        rows = list_sources(conn)
    rss_feeds, github_endpoints, html_sources = [], [], []
    for row in rows:
        if row["kind"] == "rss" and row["feed_url"]:
            item_type = "blog" if row["family"] == "official_protocol" else "news"
            if row["name"] == "EIPs":
                item_type = "eip"
            rss_feeds.append((row["name"], row["feed_url"], item_type))
        elif row["kind"] == "github_api" and row["api_url"]:
            item_type = "github_pull" if "pull" in (row["api_url"] or "") else "github_event"
            github_endpoints.append((row["name"], row["api_url"], item_type))
        elif row["kind"] == "html_index" and row["index_url"]:
            path_hints = json.loads(row["path_hints_json"] or "[]")
            item_type = "research"
            if row["family"] == "wallet_official":
                item_type = "wallet_blog"
            elif row["family"] == "security":
                item_type = "security_blog"
            elif row["family"] == "payments":
                item_type = "payments_blog"
            html_sources.append((row["name"], row["index_url"], path_hints, item_type))
    return {"rss_feeds": rss_feeds, "github_endpoints": github_endpoints, "html_sources": html_sources}


def canonical_hash(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def source_id_for_name(name: str) -> str | None:
    if not SOURCE_DB.exists():
        return None
    with connect(SOURCE_DB) as conn:
        row = conn.execute("SELECT source_id FROM sources WHERE name = ? LIMIT 1", (name,)).fetchone()
        return row[0] if row else None


def persist_observations(items: list[dict]) -> int:
    if not EVENT_DB.exists():
        with connect(EVENT_DB) as conn:
            init_event_memory(conn)
    inserted = 0
    observed_at = now_iso()
    with connect(EVENT_DB) as conn:
        for item in items:
            raw_basis = f"{item.get('source','')}|{item.get('url','')}|{item.get('title','')}|{item.get('timestamp','')}"
            obs_id = canonical_hash(raw_basis)
            raw_hash = canonical_hash(f"{item.get('title','')}|{item.get('content','')}|{item.get('url','')}")
            norm_hash = canonical_hash(f"{item.get('category','')}|{item.get('score',{}).get('total',0)}|{item.get('title','')}")
            conn.execute(
                """
                INSERT OR REPLACE INTO observations (
                    observation_id, source_id, observed_at, title, url, content_snippet,
                    published_at, raw_hash, normalized_hash, category, score_total, event_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    obs_id,
                    source_id_for_name(item.get('source','')),
                    observed_at,
                    item.get('title',''),
                    item.get('url',''),
                    item.get('content',''),
                    item.get('timestamp',''),
                    raw_hash,
                    norm_hash,
                    item.get('category',''),
                    float(item.get('score',{}).get('total',0) or 0),
                    item.get('event_id'),
                ),
            )
            inserted += 1
        conn.commit()
    return inserted


def recently_reported_keys(hours: int = 48, channel: str = 'discord') -> set[str]:
    if not EVENT_DB.exists():
        return set()
    query = """
        SELECT e.category, e.canonical_title, e.canonical_url
        FROM event_reports r
        JOIN events e ON e.event_id = r.event_id
        WHERE r.channel = ?
          AND e.last_reported_at IS NOT NULL
          AND datetime(e.last_reported_at) >= datetime('now', ?)
    """
    keys = set()
    with connect(EVENT_DB) as conn:
        for row in conn.execute(query, (channel, f'-{hours} hours')):
            keys.add(canonical_hash(f"event|{row['category'] or ''}|{row['canonical_title'] or ''}|{row['canonical_url'] or ''}"))
    return keys


def event_key_for_item(item: dict) -> str:
    return canonical_hash(f"event|{item.get('category','')}|{item.get('title','')}|{item.get('url','')}")


def reset_repeat_memory() -> dict:
    if not EVENT_DB.exists():
        return {"event_reports_deleted": 0, "events_cleared": 0}
    with connect(EVENT_DB) as conn:
        reports_deleted = conn.execute("DELETE FROM event_reports").rowcount
        events_cleared = conn.execute("UPDATE events SET last_reported_at = NULL").rowcount
        conn.commit()
    return {"event_reports_deleted": reports_deleted, "events_cleared": events_cleared}


def persist_report_snapshot(report_date: str, channel: str, items: list[dict]) -> int:
    if not EVENT_DB.exists():
        with connect(EVENT_DB) as conn:
            init_event_memory(conn)
    written = 0
    now = now_iso()
    with connect(EVENT_DB) as conn:
        for item in items:
            event_id = item.get('event_id') or event_key_for_item(item)
            conn.execute(
                """
                INSERT OR IGNORE INTO events (
                    event_id, canonical_title, canonical_url, category, first_seen_at, last_seen_at,
                    last_reported_at, last_score, status, is_active, material_update_flag, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, NULL)
                """,
                (
                    event_id,
                    item.get('title',''),
                    item.get('url',''),
                    item.get('category',''),
                    now,
                    now,
                    now,
                    float(item.get('score',{}).get('total',0) or 0),
                    'observed',
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO event_reports (
                    event_id, report_date, channel, reported_score, reported_summary, push_sent
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    report_date,
                    channel,
                    float(item.get('score',{}).get('total',0) or 0),
                    item.get('title',''),
                    1 if item.get('urgency') else 0,
                ),
            )
            conn.execute(
                "UPDATE events SET last_seen_at = ?, last_reported_at = ?, last_score = ? WHERE event_id = ?",
                (now, now, float(item.get('score',{}).get('total',0) or 0), event_id),
            )
            written += 1
        conn.commit()
    return written


def init_all() -> dict:
    config = load_config()
    with connect(SOURCE_DB) as source_conn:
        init_source_registry(source_conn)
        seed_source_registry(source_conn, config)
        source_count = source_conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
    with connect(EVENT_DB) as event_conn:
        init_event_memory(event_conn)
        table_counts = {
            "observations": event_conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
            "events": event_conn.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "event_reports": event_conn.execute("SELECT COUNT(*) FROM event_reports").fetchone()[0],
        }
    return {"source_db": str(SOURCE_DB), "event_db": str(EVENT_DB), "source_count": source_count, "event_counts": table_counts}
