#!/usr/bin/env python3
import email.utils
import json
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path

from crypto_daily_dose.db import (
    SOURCE_DB,
    event_key_for_item,
    load_runtime_sources,
    persist_observations,
    persist_report_snapshot,
    recently_reported_keys,
    reset_repeat_memory,
    get_active_tracked_events,
    start_tracking,
    update_tracking_check,
    archive_stale_tracked_events,
    update_source_health,
    get_unhealthy_sources,
)
from crypto_daily_dose.llm import llm_filter_and_summarize, is_llm_available, check_material_update, generate_quality_assessment
from crypto_daily_dose.prices import fetch_price_changes, build_price_alert_items
from crypto_daily_dose.twitter import fetch_tweets as fetch_tweets_x, is_available as twitter_available, check_cookie_expiry

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "config.json"
API_PUSHOVER = "https://api.pushover.net/1/messages.json"
DEFAULT_CONFIG = {
    "user_agent": "Mozilla/5.0 OpenClaw-CryptoDailyDose/0.3",
    "lookback_hours": 30,
    "limits": {"rss_items_per_feed": 12, "github_items": 12, "discord_items": 6},
    "thresholds": {"top": 8, "secondary": 6, "discord_min": 7, "urgent": 9},
    "paths": {
        "state_dir": "state",
        "state_file": "crypto_daily_dose.json",
        "output_file": "crypto_daily_dose_report.md",
        "pushover_cfg": "state/pushover.json",
    },
    "rss_feeds": [],
    "github_endpoints": [],
    "html_sources": [],
    "priority_topics": {},
    "exclusions": {"hard_drop": [], "low_signal_github": [], "price_only": []},
    "categories": [],
}


def deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config() -> dict:
    if CONFIG_PATH.exists():
        return deep_merge(DEFAULT_CONFIG, json.loads(CONFIG_PATH.read_text()))
    return DEFAULT_CONFIG


CONFIG = load_config()
STATE_DIR = ROOT / CONFIG["paths"]["state_dir"]
STATE_FILE = STATE_DIR / CONFIG["paths"]["state_file"]
OUTPUT_FILE = STATE_DIR / CONFIG["paths"]["output_file"]
PUSHOVER_CFG = Path(CONFIG["paths"]["pushover_cfg"])
if not PUSHOVER_CFG.is_absolute():
    PUSHOVER_CFG = ROOT / PUSHOVER_CFG
USER_AGENT = CONFIG["user_agent"]
LOOKBACK_HOURS = int(CONFIG["lookback_hours"])
MAX_RSS_ITEMS_PER_FEED = int(CONFIG["limits"]["rss_items_per_feed"])
MAX_GITHUB_ITEMS = int(CONFIG["limits"]["github_items"])
MAX_DISCORD_ITEMS = int(CONFIG["limits"]["discord_items"])
TOP_THRESHOLD = int(CONFIG["thresholds"]["top"])
SECONDARY_THRESHOLD = int(CONFIG["thresholds"]["secondary"])
DISCORD_MIN_THRESHOLD = int(CONFIG["thresholds"]["discord_min"])
URGENT_THRESHOLD = int(CONFIG["thresholds"]["urgent"])
MAX_HTML_LINKS_PER_SOURCE = 3
PUSHOVER_MIN_ITEMS = 2  # Minimum discord_items count to send Pushover
if SOURCE_DB.exists():
    RUNTIME_SOURCES = load_runtime_sources()
else:
    RUNTIME_SOURCES = {
        "rss_feeds": [tuple(x) for x in CONFIG["rss_feeds"]],
        "github_endpoints": [tuple(x) for x in CONFIG["github_endpoints"]],
        "html_sources": [tuple(x) for x in CONFIG.get("html_sources", [])],
    }
RSS_FEEDS = RUNTIME_SOURCES["rss_feeds"]
GITHUB_ENDPOINTS = RUNTIME_SOURCES["github_endpoints"]
HTML_SOURCES = RUNTIME_SOURCES["html_sources"]
CATEGORY_MAP = [(x[0], x[1]) for x in CONFIG["categories"]]
PRIORITY_TOPICS = {k: [t.lower() for t in v] for k, v in CONFIG["priority_topics"].items()}
EXCLUSIONS = {k: [t.lower() for t in v] for k, v in CONFIG["exclusions"].items()}


CATEGORY_TO_TOPIC = {
    "Wallet / AA / UX": "wallet_aa_ux",
    "Protocol / EIP / Infra": "protocol_infra",
    "Security / Risk / Compliance": "security_risk_compliance",
    "TRON / Stablecoin / Payments": "tron_stablecoin_payments",
    "Competitor Intelligence": "competitors",
    "Market Structure / Narrative": "market_structure",
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return default
    return default


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def fetch(url: str, accept: str | None = None) -> str:
    headers = {"User-Agent": USER_AGENT}
    if accept:
        headers["Accept"] = accept
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def strip_html(text: str) -> str:
    text = re.sub(r"<script.*?>.*?</script>", " ", text or "", flags=re.I | re.S)
    text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(text)).strip()


def compact(text: str, limit: int = 240) -> str:
    text = re.sub(r"\s+", " ", (text or "").strip())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    try:
        return email.utils.parsedate_to_datetime(value).astimezone(timezone.utc)
    except Exception:
        pass
    for candidate in [value.replace("Z", "+00:00"), value]:
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    # M/D/YYYY or M/D/YY format (Rekt, some blogs)
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{2,4})$', value)
    if m:
        try:
            month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if year < 100:
                year += 2000
            return datetime(year, month, day, tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def canonical_url(url: str) -> str:
    url = (url or "").strip()
    url = re.sub(r"#.*$", "", url)
    url = re.sub(r"\?.*$", "", url)
    return url.rstrip("/")


def norm_title(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (title or "").lower()).strip()


def title_similarity(a: str, b: str) -> float:
    sa = set(norm_title(a).split())
    sb = set(norm_title(b).split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(len(sa), len(sb))


# Short terms that are prone to substring false-positives need word-boundary matching.
# e.g. "tron" matches inside "strong", "sec" inside "second", "aa" inside "aave".
_WORD_BOUNDARY_TERMS = frozenset(["tron", "sec", "aa", "l2", "erc"])

def _term_match(term: str, text: str) -> bool:
    """Match term in text, using word boundaries for short ambiguous terms."""
    if term not in text:
        return False
    if term in _WORD_BOUNDARY_TERMS:
        return bool(re.search(rf'\b{re.escape(term)}\b', text))
    return True


def topic_hits(text: str) -> dict:
    hits = {}
    for key, terms in PRIORITY_TOPICS.items():
        matched = [t for t in terms if _term_match(t, text)]
        if matched:
            hits[key] = matched
    return hits


def hard_drop_reason(item: dict, text: str) -> str | None:
    # Price alert items are always kept
    if item.get("type") == "price_alert":
        return None
    if any(term in text for term in EXCLUSIONS.get("hard_drop", [])):
        return "hard_drop"
    if any(term in text for term in EXCLUSIONS.get("price_only", [])) and not any(
        cue in text for cue in ["approval", "regulation", "upgrade", "fork", "sec", "ofac", "sanction"]
    ):
        return "price_only"
    if item.get("type") in {"github_pull", "github_event", "eip"}:
        if any(term in text for term in EXCLUSIONS.get("low_signal_github", [])):
            return "low_signal_github"
    return None


def classify(item: dict) -> str:
    text = f"{item.get('title','')} {item.get('content','')}".lower()
    for name, terms in CATEGORY_MAP:
        if any(term in text for term in terms):
            return name
    if item.get("type") in {"eip", "github_pull", "github_event"}:
        return "Protocol / EIP / Infra"
    return "Market Structure / Narrative"


def html_strong_topic_evidence(text: str, hits: dict) -> bool:
    strong_terms = [
        "exploit", "hack", "vulnerability", "phishing", "approval draining", "signature abuse",
        "sanction", "ofac", "aml", "regulator", "sec",
        "eip-", "erc-", "hard fork", "ethereum upgrade", "rollup", "sequencer",
        "smart account", "account abstraction", "eip-4337", "eip-7702", "eip-8141",
        "stablecoin", "payment rail", "cross-border payment", "settlement", "merchant",
        "tron", "trc20", "usdt", "usdc",
    ]
    if any(_term_match(term, text) for term in strong_terms):
        return True
    non_competitor_hits = {k: v for k, v in hits.items() if k != "competitors"}
    matched_terms = sum(len(v) for v in non_competitor_hits.values())
    return matched_terms >= 2 and len(non_competitor_hits) >= 1


def passes_topic_gate(item: dict) -> tuple[bool, list[str]]:
    # Use content-only text for topic matching to avoid source name false-positives
    # (e.g. "TRON Weekly" source name matching the "tron" keyword for unrelated articles)
    content_text = f"{item.get('title','')} {item.get('content','')}".lower()
    hits = topic_hits(content_text)
    passed = any(k in hits for k in [
        "wallet_aa_ux",
        "protocol_infra",
        "security_risk_compliance",
        "tron_stablecoin_payments",
        "competitors",
        "market_structure",
    ])
    if passed and item.get("type") in {"wallet_blog", "research", "security_blog", "payments_blog"}:
        passed = html_strong_topic_evidence(content_text, hits)
    return passed, sorted(hits.keys())


def score_item(item: dict, _gate_result: tuple[bool, list[str]] | None = None) -> dict:
    text = f"{item.get('title','')} {item.get('content','')}".lower()
    category = item.get("category") or classify(item)
    passed_gate, gate_hits = _gate_result if _gate_result is not None else passes_topic_gate(item)
    direct_relevance = 0
    impact = 0
    novelty = 0
    actionability = 0

    if not passed_gate:
        return {
            "direct_relevance": 0,
            "impact": 0,
            "novelty": 0,
            "actionability": 0,
            "total": 0,
            "bucket": "Discard",
            "gate_hits": gate_hits,
        }

    topic_key = CATEGORY_TO_TOPIC.get(category)
    matched_terms = [t for t in PRIORITY_TOPICS.get(topic_key, []) if t in text]
    direct_relevance = 3 if matched_terms else 2
    if category in {"Wallet / AA / UX", "TRON / Stablecoin / Payments", "Security / Risk / Compliance"}:
        direct_relevance = min(4, direct_relevance + 1)

    impact_terms = [
        "launch", "upgrade", "proposal", "approved", "merged", "mainnet", "exploit", "funding",
        "integration", "release", "status change", "final", "review", "fork", "regulation", "sanction",
    ]
    impact = min(3, sum(1 for t in impact_terms if t in text))
    if any(t in text for t in ["exploit", "hack", "drain", "critical", "regulation", "final", "mainnet", "launch"]):
        impact = max(impact, 2)

    novelty = 2 if item.get("type") in {"github_pull", "github_event", "eip", "blog"} else 1
    if item.get("hours_ago", 999) <= 12:
        novelty = min(3, novelty + 1)

    actionable_terms = ["signing", "migration", "review", "roadmap", "wallet", "payment", "compliance", "strategy", "partnership"]
    actionability = min(2, sum(1 for t in actionable_terms if t in text))
    if category in {"Wallet / AA / UX", "Security / Risk / Compliance", "TRON / Stablecoin / Payments", "Competitor Intelligence"}:
        actionability = max(actionability, 1)

    total = direct_relevance + impact + novelty + actionability
    if total >= TOP_THRESHOLD:
        bucket = "Top"
    elif total >= SECONDARY_THRESHOLD:
        bucket = "Secondary"
    else:
        bucket = "Discard"

    return {
        "direct_relevance": direct_relevance,
        "impact": impact,
        "novelty": novelty,
        "actionability": actionability,
        "total": total,
        "bucket": bucket,
        "gate_hits": gate_hits,
    }


def urgency_reason(item: dict) -> str | None:
    text = f"{item.get('title','')} {item.get('content','')}".lower()
    category = item.get("category")
    total = item.get("score", {}).get("total", 0)
    if total < URGENT_THRESHOLD:
        return None
    if category == "Security / Risk / Compliance" and any(t in text for t in ["exploit", "hack", "drain", "critical", "phishing"]):
        return "security"
    if category == "Wallet / AA / UX" and any(t in text for t in ["launch", "signing", "eip-7702", "eip-4337", "eip-8141", "smart account"]):
        return "wallet"
    if category == "TRON / Stablecoin / Payments" and any(t in text for t in ["tron", "usdt", "usdc", "stablecoin", "settlement", "payment"]):
        return "payments"
    if category in {"Protocol / EIP / Infra", "Competitor Intelligence"} and any(t in text for t in ["launch", "mainnet", "regulation", "partnership", "strategy"]):
        return "high_impact"
    return None


def parse_feed_entries(source_name: str, url: str, item_type: str, cutoff: datetime) -> list[dict]:
    raw = fetch(url, accept="application/rss+xml, application/atom+xml, text/xml, application/xml")
    root = ET.fromstring(raw)
    entries = []
    channel = root.find("channel")
    if channel is not None:
        nodes = channel.findall("item")
        for node in nodes[:MAX_RSS_ITEMS_PER_FEED]:
            title = (node.findtext("title") or "").strip()
            link = (node.findtext("link") or "").strip()
            desc = node.findtext("description") or node.findtext("{http://purl.org/rss/1.0/modules/content/}encoded") or ""
            dt = parse_dt(node.findtext("pubDate") or node.findtext("published") or node.findtext("updated"))
            if dt and dt < cutoff:
                continue
            entries.append({"title": strip_html(title), "content": compact(strip_html(desc), 280), "url": link, "source": source_name, "type": item_type, "timestamp": dt.isoformat() if dt else ""})
    else:
        ns = {"a": "http://www.w3.org/2005/Atom"}
        for node in root.findall("a:entry", ns)[:MAX_RSS_ITEMS_PER_FEED]:
            title = (node.findtext("a:title", default="", namespaces=ns) or "").strip()
            link = next((ln.attrib.get("href") for ln in node.findall("a:link", ns) if ln.attrib.get("href")), "")
            summary = node.findtext("a:summary", default="", namespaces=ns) or node.findtext("a:content", default="", namespaces=ns) or ""
            dt = parse_dt(node.findtext("a:published", default="", namespaces=ns) or node.findtext("a:updated", default="", namespaces=ns))
            if dt and dt < cutoff:
                continue
            entries.append({"title": strip_html(title), "content": compact(strip_html(summary), 280), "url": link, "source": source_name, "type": item_type, "timestamp": dt.isoformat() if dt else ""})
    return entries


def fetch_json(url: str):
    return json.loads(fetch(url, accept="application/json"))


def absolute_url(base: str, href: str) -> str:
    return urllib.parse.urljoin(base, href)


def extract_meta(html: str, prop: str) -> str:
    patterns = [
        rf'<meta[^>]+property=["\']{re.escape(prop)}["\'][^>]+content=["\']([^"\']+)',
        rf'<meta[^>]+name=["\']{re.escape(prop)}["\'][^>]+content=["\']([^"\']+)',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']{re.escape(prop)}["\']',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']{re.escape(prop)}["\']',
    ]
    for pat in patterns:
        m = re.search(pat, html, flags=re.I)
        if m:
            return m.group(1).strip()
    return ""


def article_link_rank(url: str) -> tuple[int, int, int]:
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.lower()
    articleish = 1 if re.search(r'/20\d\d/|/\d{4}/\d{2}/|[a-z0-9]+-[a-z0-9-]+', path) else 0
    depth = path.count('/')
    return (articleish, depth, len(path))


def extract_links(index_url: str, html: str, path_hints: list[str], limit: int = 6) -> list[str]:
    candidates = []
    seen = set()
    domain = urllib.parse.urlparse(index_url).netloc
    canon_index = canonical_url(index_url)  # computed once outside loop
    for href in re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.I):
        full = absolute_url(index_url, href)
        parsed = urllib.parse.urlparse(full)
        if not parsed.scheme.startswith('http'):
            continue
        if parsed.netloc != domain and parsed.netloc.replace('www.', '') != domain.replace('www.', ''):
            continue
        if any(h in full for h in ['/tag/', '/author/', '/category/', '/cdn-cgi/', '/page/']):
            continue
        if path_hints and not any(h in full for h in path_hints):
            continue
        canon = canonical_url(full)
        if canon in seen or canon == canon_index:
            continue
        seen.add(canon)
        candidates.append(full)
    return sorted(candidates, key=article_link_rank, reverse=True)[:limit]


def parse_html_sources(cutoff: datetime) -> tuple[list[dict], dict, list[str]]:
    items = []
    stats = {
        'sources': len(HTML_SOURCES),
        'index_fetch_failed': 0,
        'zero_link_count': 0,
        'article_fetch_failed': 0,
        'missing_timestamp_count': 0,
        'stale_dropped': 0,
        'accepted': 0,
        'links_considered': 0,
    }
    errors = []
    for source_name, index_url, path_hints, item_type in HTML_SOURCES:
        source_id = f"html::{source_name.lower().replace(' ', '_')}"
        try:
            index_html = fetch(index_url, accept='text/html,application/xhtml+xml')
        except Exception as e:
            stats['index_fetch_failed'] += 1
            errors.append(f"HTML {source_name} index fetch failed: {e}")
            update_source_health(source_id, success=False, error=str(e))
            continue

        article_links = extract_links(index_url, index_html, list(path_hints), limit=MAX_HTML_LINKS_PER_SOURCE)
        stats['links_considered'] += len(article_links)
        if not article_links:
            stats['zero_link_count'] += 1
            errors.append(f"HTML {source_name} extracted 0 candidate links")
            update_source_health(source_id, success=False, error="0 candidate links extracted")
            continue

        for link in article_links:
            try:
                html = fetch(link, accept='text/html,application/xhtml+xml')
            except Exception as e:
                stats['article_fetch_failed'] += 1
                errors.append(f"HTML {source_name} article fetch failed: {link} ({e})")
                continue

            title_match = re.search(r'<title[^>]*>(.*?)</title>', html, flags=re.I | re.S)
            title = extract_meta(html, 'og:title') or (title_match.group(1) if title_match else '')
            title = strip_html(title)
            desc = extract_meta(html, 'description') or extract_meta(html, 'og:description')
            desc = compact(strip_html(desc), 280)
            pub = extract_meta(html, 'article:published_time') or extract_meta(html, 'og:published_time') or extract_meta(html, 'publication_date')
            # Fallback: extract date from __NEXT_DATA__ (Rekt, Next.js sites)
            if not pub:
                import json as _json
                nd = re.search(r'__NEXT_DATA__[^>]*>(.*?)</script>', html, re.S)
                if nd:
                    try:
                        _d = _json.loads(nd.group(1))
                        _pp = _d.get('props',{}).get('pageProps',{})
                        pub = (_pp.get('data') or _pp.get('article') or _pp.get('post') or {}).get('date', '')
                    except Exception:
                        pass
            dt = parse_dt(pub)
            if not dt:
                stats['missing_timestamp_count'] += 1
                continue
            if dt < cutoff:
                stats['stale_dropped'] += 1
                continue
            items.append({
                'title': title,
                'content': desc,
                'url': link,
                'source': source_name,
                'type': item_type,
                'timestamp': dt.isoformat(),
            })
            stats['accepted'] += 1
        # Update health: success if links were found (even if 0 accepted after cutoff filter)
        # This distinguishes "source is reachable" from "no new content today"
        source_accepted = sum(1 for item in items if item.get('source') == source_name)
        update_source_health(source_id, success=True, item_count=source_accepted)
    return items, stats, errors


def parse_github(cutoff: datetime) -> list[dict]:
    items = []
    for source_name, url, item_type in GITHUB_ENDPOINTS:
        payload = fetch_json(url)
        if item_type == "github_pull":
            for pr in payload[:MAX_GITHUB_ITEMS]:
                dt = parse_dt(pr.get("updated_at") or pr.get("created_at"))
                if dt and dt < cutoff:
                    continue
                items.append({
                    "title": f"PR #{pr.get('number')}: {pr.get('title') or ''}",
                    "content": compact(f"{(pr.get('state') or '').upper()} | draft={pr.get('draft')}. {strip_html(pr.get('body') or '')}", 280),
                    "url": pr.get("html_url") or "",
                    "source": source_name,
                    "type": item_type,
                    "timestamp": dt.isoformat() if dt else "",
                })
        else:
            for ev in payload[:MAX_GITHUB_ITEMS]:
                dt = parse_dt(ev.get("created_at"))
                if dt and dt < cutoff:
                    continue
                actor = (ev.get("actor") or {}).get("login") or "unknown"
                event = ev.get("event") or "event"
                issue = ev.get("issue") or {}
                if event in {"referenced", "mentioned", "subscribed"}:
                    continue
                title = f"Issue/PR #{issue.get('number')}: {issue.get('title') or event}" if issue.get("number") else f"{event} by {actor}"
                items.append({
                    "title": title,
                    "content": compact(f"GitHub {event}. actor={actor}. commit={(ev.get('commit_id') or '')[:8]}", 240),
                    "url": issue.get("html_url") or "https://github.com/ethereum/EIPs",
                    "source": source_name,
                    "type": item_type,
                    "timestamp": dt.isoformat() if dt else "",
                })
    return items


def _source_pref(item: dict) -> int:
    return 1 if any(k in item.get("source", "") for k in ["Ethereum", "EIPs", "GitHub"]) else 0


def dedup(items: list[dict]) -> list[dict]:
    # url_index: canonical_url -> index in out_list (for O(1) lookup)
    # title_index: list of (norm_title_tokens_set, index) for similarity scan
    out: list[dict] = []
    url_index: dict[str, int] = {}
    title_tokens: list[set] = []

    for item in sorted(items, key=lambda x: x.get("timestamp", ""), reverse=True):
        u = canonical_url(item.get("url", ""))
        tokens = set(norm_title(item.get("title", "")).split())

        # O(1) URL exact match
        if u and u in url_index:
            continue

        # Title similarity scan (still O(n) but only for titles, no list.remove)
        replaced_idx = None
        drop = False
        for idx, kept_tokens in enumerate(title_tokens):
            if not tokens or not kept_tokens:
                continue
            sim = len(tokens & kept_tokens) / max(len(tokens), len(kept_tokens))
            if sim >= 0.8:
                kept = out[idx]
                if _source_pref(item) > _source_pref(kept):
                    replaced_idx = idx
                else:
                    drop = True
                break

        if drop:
            continue

        if replaced_idx is not None:
            old = out[replaced_idx]
            old_u = canonical_url(old.get("url", ""))
            if old_u in url_index:
                del url_index[old_u]
            out[replaced_idx] = item
            title_tokens[replaced_idx] = tokens
            if u:
                url_index[u] = replaced_idx
        else:
            if u:
                url_index[u] = len(out)
            title_tokens.append(tokens)
            out.append(item)

    return out


STOP_WORDS = frozenset({
    'the','a','an','is','are','was','were','be','been','has','have','had',
    'will','would','could','should','may','might','in','on','at','to','for',
    'of','and','or','but','with','by','from','as','it','this','that','its',
    'after','before','over','says','said','new','more','than','how','what',
    'who','when','why','into','also','its','which','their','there','about',
})


def _cluster_items(items: list[dict], threshold_tokens: int, use_content: bool = True) -> list[list[dict]]:
    """Cluster items by semantic similarity. Returns list of clusters."""
    if not items:
        return []
    clusters: list[list[dict]] = []
    assigned = [False] * len(items)

    for i, item in enumerate(items):
        if assigned[i]:
            continue
        cluster = [item]
        assigned[i] = True
        text_i = f"{item.get('title','')}{' ' + item.get('content','') if use_content else ''}".lower()
        tokens_i = set(norm_title(text_i).split()) - STOP_WORDS

        for j, other in enumerate(items):
            if assigned[j] or i == j:
                continue
            text_j = f"{other.get('title','')}{' ' + other.get('content','') if use_content else ''}".lower()
            tokens_j = set(norm_title(text_j).split()) - STOP_WORDS
            if not tokens_i or not tokens_j:
                continue
            common = tokens_i & tokens_j
            if len(common) >= threshold_tokens:
                cluster.append(other)
                assigned[j] = True
        clusters.append(cluster)
    return clusters


def _best_in_cluster(cluster: list[dict]) -> dict:
    """Pick best item from a cluster (prefer LLM-summarized, then longest content)."""
    # Prefer items that have LLM summaries
    llm_items = [x for x in cluster if x.get("title_zh") and x.get("summary_zh")]
    candidates = llm_items if llm_items else cluster
    return max(candidates, key=lambda x: len(x.get("summary_zh") or x.get("content", "") or ""))


def dedup_tweets(items: list[dict]) -> list[dict]:
    """
    Merge X/Twitter items that describe the same event across multiple accounts.
    Uses 3+ meaningful shared tokens (low threshold for short tweet text).
    Non-tweet items are returned unchanged.
    """
    tweets = [x for x in items if x.get("type") == "tweet"]
    non_tweets = [x for x in items if x.get("type") != "tweet"]

    if len(tweets) <= 1:
        return items

    clusters = _cluster_items(tweets, threshold_tokens=3, use_content=True)
    merged = []
    for cluster in clusters:
        if len(cluster) == 1:
            merged.append(cluster[0])
            continue
        best = dict(_best_in_cluster(cluster))
        authors = []
        seen = set()
        for c in cluster:
            a = c.get("author", "")
            if a and a not in seen:
                authors.append(f"@{a}")
                seen.add(a)
        if len(authors) > 1:
            best["source"] = f"X/{', '.join(authors)}"
        merged.append(best)
    return non_tweets + merged


def dedup_cross_source(items: list[dict]) -> list[dict]:
    """
    Merge RSS/HTML items that describe the same event across multiple outlets.
    Uses stricter Jaccard threshold (5+ shared tokens) since article titles are more uniform.
    Keeps best-quality item; merges source attribution with · separator.
    Stores all source URLs in extra_urls for multi-link display.
    Tweet items pass through unchanged (handled by dedup_tweets).
    """
    non_news = [x for x in items if x.get("type") == "tweet"]
    news = [x for x in items if x.get("type") != "tweet"]

    if len(news) <= 1:
        return items

    clusters = _cluster_items(news, threshold_tokens=5, use_content=False)
    merged = []
    for cluster in clusters:
        if len(cluster) == 1:
            merged.append(cluster[0])
            continue
        best = dict(_best_in_cluster(cluster))
        sources = []
        extra_urls = []
        seen_sources = set()
        for c in sorted(cluster, key=lambda x: -x.get("score", {}).get("total", 0) if x.get("score") else 0):
            src = c.get("source", "")
            url = c.get("url", "")
            if src and src not in seen_sources:
                sources.append(src)
                seen_sources.add(src)
                if url and url != best.get("url"):
                    extra_urls.append({"source": src, "url": url})
        if len(sources) > 1:
            best["source"] = " · ".join(sources)
            best["extra_urls"] = extra_urls
        merged.append(best)
    return non_news + merged


def enrich(items: list[dict], repeat_suppression: bool = True, use_llm: bool = False) -> tuple[list[dict], dict]:
    enriched = []
    dropped = {"hard_drop": 0, "topic_gate": 0, "low_signal": 0, "recent_repeat": 0}
    now = now_utc()
    recent_event_keys = recently_reported_keys(hours=48, channel='discord') if repeat_suppression else set()

    # Phase 1: hard drop filter (always runs, fast)
    candidates = []
    for item in items:
        dt = parse_dt(item.get("timestamp"))
        item["hours_ago"] = round((now - dt).total_seconds() / 3600, 1) if dt else 999
        text = f"{item.get('title','')} {item.get('content','')} {item.get('source','')}".lower()
        reason = hard_drop_reason(item, text)
        if reason:
            dropped["low_signal" if reason in {"low_signal_github", "price_only"} else "hard_drop"] += 1
            continue
        candidates.append(item)

    # Phase 2: topic gate (LLM or keyword)
    if use_llm and is_llm_available() and candidates:
        # Price alerts bypass LLM (already pre-judged relevant)
        pre_approved = [x for x in candidates if x.get("type") == "price_alert"]
        to_filter = [x for x in candidates if x.get("type") != "price_alert"]
        # LLM mode: batch filter + summarize in one call
        llm_results = llm_filter_and_summarize(to_filter) if to_filter else []
        candidates = pre_approved + llm_results
        for item in candidates:
            if not item.get("llm_relevant", True):
                dropped["topic_gate"] += 1
                continue
            item["category"] = item.get("llm_category") or classify(item)
            item["gate_hits"] = []
            # In LLM mode, guarantee a minimum score so relevant items pass discord threshold
            base_score = score_item(item)
            if base_score.get("total", 0) < DISCORD_MIN_THRESHOLD:
                base_score["total"] = DISCORD_MIN_THRESHOLD
                base_score["bucket"] = "Secondary"
            item["score"] = base_score
            item["event_id"] = event_key_for_item(item)
            if item["event_id"] in recent_event_keys:
                dropped["recent_repeat"] += 1
                continue
            item["urgency"] = urgency_reason(item)
            # Start tracking if LLM flagged it
            if item.get("llm_track") and item.get("llm_track_reason"):
                start_tracking(item["event_id"], item["llm_track_reason"])
            enriched.append(item)
        return enriched, dropped

    # Keyword mode (V1 fallback)
    for item in candidates:
        # Price alerts bypass keyword gate
        if item.get("type") == "price_alert":
            item["category"] = "价格"
            item["gate_hits"] = []
            item["score"] = {"direct_relevance": 3, "impact": 2, "novelty": 2, "actionability": 1, "total": DISCORD_MIN_THRESHOLD, "bucket": "Secondary", "gate_hits": []}
            item["event_id"] = event_key_for_item(item)
            item["urgency"] = None
            enriched.append(item)
            continue
        item["category"] = classify(item)
        gate_result = passes_topic_gate(item)
        passed, gate_hits = gate_result
        if not passed:
            dropped["topic_gate"] += 1
            continue
        item["score"] = score_item(item, _gate_result=gate_result)
        item["gate_hits"] = gate_hits
        item["event_id"] = event_key_for_item(item)
        if item["event_id"] in recent_event_keys:
            dropped["recent_repeat"] += 1
            continue
        item["urgency"] = urgency_reason(item)
        enriched.append(item)
    return enriched, dropped


def sort_key(item: dict):
    score = item.get("score", {})
    return (
        score.get("direct_relevance", 0),
        score.get("impact", 0),
        score.get("novelty", 0),
        score.get("actionability", 0),
        -item.get("hours_ago", 999),
    )


def zh_category(category: str) -> str:
    mapping = {
        "Wallet / AA / UX": "💼 钱包支付",
        "Protocol / EIP / Infra": "🧱 协议",
        "Security / Risk / Compliance": "🛡️ 安全",
        "TRON / Stablecoin / Payments": "💸 TRON/稳定币",
        "Competitor Intelligence": "🧭 竞品",
        "Market Structure / Narrative": "📊 市场",
        # LLM category names
        "监管": "🧭 监管",
        "安全": "🛡️ 安全",
        "协议": "🧱 协议",
        "钱包支付": "💼 钱包支付",
        "机构": "💼 机构",
        "行业": "📊 行业",
        "宏观": "📊 宏观",
        "价格": "📊 价格",
        "TRON/稳定币": "💸 TRON/稳定币",
        "竞品情报": "🧭 竞品",
    }
    return mapping.get(category, category)


def why_it_matters(item: dict) -> str:
    category = item.get("category")
    score = item.get("score", {})
    if category == "Wallet / AA / UX":
        return "直接影响钱包交互、签名流程或账户抽象方向，可能关系到产品设计与路线图。"
    if category == "TRON / Stablecoin / Payments":
        return "与稳定币/支付基础设施相关，可能影响支付路径、成本模型或市场方向。"
    if category == "Security / Risk / Compliance":
        return "安全或合规变化通常影响大，而且往往需要快速响应。"
    if category == "Competitor Intelligence":
        return "可能揭示竞品的产品方向、增长策略或市场定位变化。"
    if category == "Protocol / EIP / Infra":
        return "若该协议变化继续推进，可能影响钱包或基础设施侧的中长期规划。"
    return f"高信号基础设施/叙事信息，当前评分 {score.get('total', 0)}。"


def push_emoji(category: str) -> str:
    mapping = {
        "Wallet / AA / UX": "💼",
        "Protocol / EIP / Infra": "🧱",
        "Security / Risk / Compliance": "🛡️",
        "TRON / Stablecoin / Payments": "💸",
        "Competitor Intelligence": "🧭",
        "Market Structure / Narrative": "📊",
    }
    return mapping.get(category, "📌")


def summarize_title_zh(item: dict) -> str:
    title = item.get("title", "")
    category = item.get("category")
    text = f"{title} {item.get('content','')}".lower()

    if category == "Wallet / AA / UX":
        if "eip-8141" in text and "atomic" in text:
            return "EIP-8141 新增原子批处理"
        if "eip-8141" in text:
            return "EIP-8141 影响签名AA"
        if "eip-7702" in text:
            return "EIP-7702 有新进展"
        if "eip-4337" in text:
            return "EIP-4337 路径更新"
        if "ledger" in text:
            return "Ledger 推进 IPO 布局"

    elif category == "TRON / Stablecoin / Payments":
        if "stablecoin" in text or "稳定币" in title:
            return "稳定币规则出现变化"
        if "trc20" in text:
            return "TRON 支付模型有变化"
        if "usdc" in text or "usdt" in text:
            return "稳定币结算场景扩大"

    elif category == "Security / Risk / Compliance":
        if any(t in text for t in ["exploit", "hack", "phishing", "drain", "critical"]):
            return "出现高风险安全事件"
        return "监管合规出现重要变化"

    elif category == "Protocol / EIP / Infra":
        return "协议基础设施有更新"

    elif category == "Competitor Intelligence":
        return "竞品产品方向有变化"

    # Tweet fallback: generate a minimal Chinese label from source + content
    if item.get("type") == "tweet":
        source = item.get("source", "")
        content = compact(item.get("content", "") or title, 40)
        if source:
            return f"【{source}】{content}"
        return content

    # Fallback: use original title so items remain distinguishable
    return compact(title, 60)


def summarize_body_zh(item: dict) -> str:
    title = item.get("title", "")
    text = f"{title} {item.get('content','')}".lower()
    category = item.get("category")

    if category == "Wallet / AA / UX":
        if "eip-8141" in text and "atomic" in text:
            return "该提案新增原子批处理能力，连续操作可作为一组回滚，关系到钱包批量操作与交互体验。"
        if "eip-8141" in text:
            return "该提案围绕账户抽象与签名能力调整，涉及钱包默认代码与签名方案设计。"
        if "ledger" in text:
            return "Ledger 正强化管理层并推进上市准备，反映硬件钱包赛道仍在加速成熟。"

    elif category == "TRON / Stablecoin / Payments":
        if "stablecoin" in text:
            return "稳定币相关规则若继续推进，可能影响产品设计、收益结构与支付基础设施路径。"
        if "usdc" in text or "usdt" in text:
            return "稳定币正在扩展更多结算与交易场景，值得关注支付基础设施变化。"
        if "trc20" in text or "tron network" in text or "tron energy" in text:
            return "TRON 网络或费率模型变化，可能影响稳定币与跨境支付使用场景。"

    elif category == "Security / Risk / Compliance":
        if any(t in text for t in ["exploit", "hack", "phishing", "drain", "critical"]):
            return "该事件可能直接影响资产安全、签名风险或钱包风控策略。"
        return "监管或合规变化可能影响业务边界、产品设计或风险判断。"

    elif category == "Protocol / EIP / Infra":
        return "这类协议或基础设施变化，后续可能传导到钱包与产品路线图。"

    elif category == "Competitor Intelligence":
        return "竞品的新动作可能透露其产品方向、增长策略或集成重点。"

    # Fallback: use original content snippet so items remain distinguishable
    return compact(item.get("content", ""), 120) or compact(item.get("title", ""), 80) or "见原文。"


def build_report(items: list[dict]) -> tuple[str, str | None, list[dict]]:
    today = datetime.now().astimezone().strftime("%Y-%m-%d")
    discord_items = [x for x in items if x["score"]["total"] >= DISCORD_MIN_THRESHOLD][:MAX_DISCORD_ITEMS]
    urgent_items = [x for x in discord_items if x.get("urgency")]
    urgent_urls = {x.get("url") for x in urgent_items}  # O(1) membership test

    lines = [f"# Crypto Daily Dose — {today}", ""]
    if not discord_items:
        lines += ["简报：", "- 今天没有高价值的钱包 / 基础设施 / 支付 / 安全相关情报。"]
        return "\n".join(lines) + "\n", None, discord_items

    # Group items by category to avoid repeated section headers
    from collections import OrderedDict
    grouped: OrderedDict[str, list] = OrderedDict()
    for item in discord_items:
        raw_cat = item.get("llm_category") or item.get("category", "")
        cat_display = zh_category(raw_cat) if raw_cat else "📊 其他"
        grouped.setdefault(cat_display, []).append(item)

    def _one_sentence(text: str, limit: int = 60) -> str:
        """Trim summary to one sentence (first sentence or first limit chars)."""
        text = (text or "").strip()
        for sep in ["。", "；", ". "]:
            idx = text.find(sep)
            if 10 < idx < limit:
                return text[:idx + len(sep)].strip()
        return compact(text, limit)

    for cat_display, cat_items in grouped.items():
        lines.append(f"## {cat_display}")
        for item in cat_items:
            if item.get("title_zh") and item.get("summary_zh"):
                title_display = item["title_zh"]
                summary_display = _one_sentence(item["summary_zh"])
            else:
                title_display = summarize_title_zh(item)
                summary_display = _one_sentence(summarize_body_zh(item))

            # Add tracking label if this is a material update
            tracking_prefix = "📌 **[事件追踪]** " if item.get("is_tracked_update") else ""
            is_high = item.get("llm_significance") == "high"
            source_link = f"[{item['source']}]({item['url']})" if item.get("url") else item.get("source", "")

            if is_high:
                # High significance: expanded format, emoji + [重大]
                lines.append(f"- 🔴 **[重大]** {tracking_prefix}**{title_display}**")
                lines.append(f"  {summary_display}")
                extra_urls = item.get("extra_urls", [])
                if extra_urls:
                    # Each source on its own line, no first-line aggregation
                    lines.append(f"  来源：{source_link}")
                    for eu in extra_urls:
                        eu_link = f"[{eu['source']}]({eu['url']})" if eu.get("url") else eu.get("source", "")
                        lines.append(f"  来源：{eu_link}")
                else:
                    lines.append(f"  来源：{source_link}")
            else:
                # Normal format: compact, hyperlink source
                lines += [
                    f"- {tracking_prefix}**{title_display}**",
                    f"  {summary_display}",
                    f"  来源：{source_link}",
                ]
        lines.append("")

    push_candidates = urgent_items + [x for x in discord_items if x.get("url") not in urgent_urls]
    if push_candidates:
        selected = []
        seen_push_texts = set()
        for item in push_candidates:
            text = item.get("title_zh") or summarize_title_zh(item)
            key = norm_title(text)
            if key in seen_push_texts:
                continue
            seen_push_texts.add(key)
            selected.append((item, text))
            if len(selected) >= 5:
                break
        if selected:
            push_lines = [
                f"{push_emoji(item['category'])} <b>{text}</b>"
                for item, text in selected
            ]
            discord_url = "https://discord.com/channels/1483493776260333709/1485747708496183481"
            push_lines.append(f'\n<a href="{discord_url}">→ 查看完整日报</a>')
            push = "\n".join(push_lines)
        else:
            push = None
    else:
        push = None
    return "\n".join(lines).strip() + "\n", push, discord_items


COOKIE_ALERT_STATE_FILE = STATE_DIR / "cookie_alert_sent.json"
COOKIE_ALERT_DAYS_THRESHOLD = 7


def check_and_build_cookie_alert() -> str | None:
    """
    Check X cookie expiry. If ≤7 days, return alert message string (once per day).
    Returns None if no alert needed or already sent today.
    """
    today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    state = load_json(COOKIE_ALERT_STATE_FILE, {})
    if state.get("last_alert_date") == today:
        return None
    try:
        days_left = check_cookie_expiry()
    except Exception:
        return None
    if days_left is None or days_left > COOKIE_ALERT_DAYS_THRESHOLD:
        return None
    save_json(COOKIE_ALERT_STATE_FILE, {"last_alert_date": today})
    return (
        f"⚠️ **X/Twitter cookies 将在 {days_left} 天后过期**\n"
        f"请运行：`cd ~/.openclaw/workspace-mini/crypto-daily-dose && python3 scripts/refresh_x_cookies.py`"
    )


def run(send_pushover: bool = True, repeat_suppression: bool = True, reset_repeat: bool = False, use_llm: bool = False, lookback_hours: int | None = None, window: str = "auto") -> int:
    if reset_repeat:
        reset_repeat_memory()
    effective_lookback = lookback_hours if lookback_hours is not None else LOOKBACK_HOURS
    cutoff = now_utc() - timedelta(hours=effective_lookback)
    items, errors = [], []
    html_stats = {}

    # Resolve session window for conditional source skipping
    _now_sgt = datetime.now(timezone(timedelta(hours=8)))
    if window == "morning":
        _session_window = "morning"
    elif window == "afternoon":
        _session_window = "afternoon"
    else:
        _session_window = "morning" if _now_sgt.hour < 14 else "afternoon"

    # Sources to skip in morning window (rate-limited, fetch only in afternoon)
    MORNING_SKIP_SOURCES = {"Optimism Blog"}

    for source_name, url, item_type in RSS_FEEDS:
        # Skip rate-limited sources in morning window
        if _session_window == "morning" and source_name in MORNING_SKIP_SOURCES:
            errors.append(f"RSS {source_name}: skipped in morning window (rate limit mitigation)")
            continue
        source_id = f"rss::{source_name.lower().replace(' ', '_')}"
        try:
            new_items = parse_feed_entries(source_name, url, item_type, cutoff)
            items.extend(new_items)
            update_source_health(source_id, success=True, item_count=len(new_items))
        except Exception as e:
            errors.append(f"RSS {source_name}: {e}")
            update_source_health(source_id, success=False, error=str(e))
    try:
        html_items, html_stats, html_errors = parse_html_sources(cutoff)
        items.extend(html_items)
        errors.extend(html_errors)
    except Exception as e:
        errors.append(f"HTML sources: {e}")
    try:
        github_items = parse_github(cutoff)
        items.extend(github_items)
    except Exception as e:
        errors.append(f"GitHub: {e}")

    # Price monitoring: inject alert items if any asset moved ≥5%
    try:
        price_changes = fetch_price_changes(USER_AGENT)
        price_items = build_price_alert_items(price_changes)
        items.extend(price_items)
    except Exception as e:
        errors.append(f"Price monitor: {e}")

    # X/Twitter: fetch recent tweets from tracked accounts
    if twitter_available():
        try:
            tweet_items, tweet_errors = fetch_tweets_x(cutoff)
            items.extend(tweet_items)
            errors.extend(tweet_errors)
        except Exception as e:
            errors.append(f"X/Twitter: {e}")
        # Cookie expiry check — if ≤7 days, inject as COOKIE_ALERT error
        # The cron agent will see this in WARNINGS and can route to #📋产品讨论
        cookie_alert = check_and_build_cookie_alert()
        if cookie_alert:
            errors.append(f"COOKIE_EXPIRY_ALERT: {cookie_alert}")

    # Archive stale tracked events (no updates in 30 days)
    archive_stale_tracked_events(max_days=30)

    # Tracking check: for each active tracked event, check if new items contain material updates
    tracked_update_items = []
    if use_llm and is_llm_available():
        active_tracked = get_active_tracked_events()
        if active_tracked:
            deduped_for_tracking = dedup(list(items))  # Don't modify original
            for tracked_event in active_tracked:
                for candidate in deduped_for_tracking:
                    # Skip if candidate is the event itself (same URL)
                    if canonical_url(candidate.get("url", "")) == canonical_url(tracked_event.get("canonical_url", "")):
                        continue
                    # Quick text relevance check before LLM call
                    tracked_keywords = set(tracked_event.get("canonical_title", "").lower().split())
                    candidate_text = f"{candidate.get('title','')} {candidate.get('content','')}".lower()
                    keyword_overlap = sum(1 for kw in tracked_keywords if len(kw) > 4 and kw in candidate_text)
                    if keyword_overlap < 2:
                        continue
                    # LLM material update check
                    is_update, reason = check_material_update(tracked_event, candidate)
                    # Always record the check (fixes: last_checked_at not updated on no-update)
                    update_tracking_check(tracked_event["event_id"], had_update=is_update)
                    if is_update:
                        # Clone item with tracking label
                        update_item = dict(candidate)
                        update_item["is_tracked_update"] = True
                        update_item["tracked_event_title"] = tracked_event.get("canonical_title", "")
                        update_item["tracked_update_reason"] = reason
                        tracked_update_items.append(update_item)
                        break  # One update per tracked event per run

    # Add tracked update items to the pool
    items = items + tracked_update_items

    filtered, dropped = enrich(dedup_cross_source(dedup_tweets(dedup(items))), repeat_suppression=repeat_suppression, use_llm=use_llm)
    ranked = sorted(filtered, key=sort_key, reverse=True)
    report, push, discord_items = build_report(ranked)

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    observations_written = persist_observations(ranked)
    report_date = datetime.now().astimezone().strftime("%Y-%m-%d")
    reports_written = persist_report_snapshot(report_date, 'discord', discord_items)
    OUTPUT_FILE.write_text(report)
    # NOTE: STATE_FILE is written at end of run() so errors from Pushover/quality/health
    # are all captured. Do not move this save_json call earlier.

    if send_pushover and push and len(discord_items) < PUSHOVER_MIN_ITEMS:
        errors.append(f"Pushover skipped: only {len(discord_items)} item(s), minimum is {PUSHOVER_MIN_ITEMS}")
    if send_pushover and push and len(discord_items) >= PUSHOVER_MIN_ITEMS:
        # Dedup: send once per session (morning / afternoon) per calendar day (SGT)
        _now_sgt = datetime.now(timezone(timedelta(hours=8)))
        today_sgt = _now_sgt.strftime("%Y-%m-%d")
        # Resolve session slot: explicit window arg overrides time-based auto-detect
        if window == "morning":
            _session_slot = "morning"
            _session_label = "早报"
        elif window == "afternoon":
            _session_slot = "afternoon"
            _session_label = "下午报"
        else:  # "auto"
            _session_slot = "morning" if _now_sgt.hour < 14 else "afternoon"
            _session_label = "早报" if _session_slot == "morning" else "下午报"
        _dedup_key = f"{today_sgt}_{_session_slot}"
        pushover_state_file = STATE_DIR / "pushover_sent.json"
        pushover_state = load_json(pushover_state_file, {})
        if pushover_state.get(_dedup_key):
            errors.append(f"Pushover already sent for {_dedup_key}, skipping")
        else:
            cfg = load_json(PUSHOVER_CFG, {})
            token, user = cfg.get("app_token"), cfg.get("user_key")
            if token and user:
                # Build title with date, session label, and item count
                _item_count = len(discord_items)
                _push_title = f"💊 Crypto Daily Dose — {today_sgt} {_session_label}（{_item_count}条）"
                payload = urllib.parse.urlencode({
                    "token": token,
                    "user": user,
                    "title": _push_title,
                    "message": push,
                    "html": "1",
                }).encode()
                req = urllib.request.Request(API_PUSHOVER, data=payload, method="POST")
                with urllib.request.urlopen(req, timeout=20) as resp:
                    parsed = json.loads(resp.read().decode("utf-8", errors="replace"))
                if parsed.get("status") == 1:
                    pushover_state[_dedup_key] = True
                    save_json(pushover_state_file, pushover_state)
                else:
                    errors.append(f"Pushover API error: {parsed}")
            else:
                errors.append("Pushover config missing")

    # Quality report — write to state/ and notify Chronos
    if use_llm and is_llm_available():
        try:
            quality_assessment = generate_quality_assessment(report)
            twitter_count = sum(1 for x in discord_items if x.get("type") == "tweet")
            rss_count = len(discord_items) - twitter_count
            # Normalize category names to Chinese for consistent quality report
            _cat_map = {
                "Wallet / AA / UX": "钱包/AA",
                "Protocol / EIP / Infra": "协议",
                "Security / Risk / Compliance": "安全",
                "TRON / Stablecoin / Payments": "TRON/稳定币",
                "Competitor Intelligence": "竞品情报",
                "Market Structure / Narrative": "市场/叙事",
            }
            def _normalize_cat(c: str) -> str:
                return _cat_map.get(c, c) if c else "未分类"

            category_counts: dict[str, int] = {}
            for item in discord_items:
                raw = item.get("llm_category") or item.get("category", "未分类")
                cat = _normalize_cat(raw)
                category_counts[cat] = category_counts.get(cat, 0) + 1
            quality_report = {
                "date": report_date,
                "run": datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M"),
                "total_items": len(discord_items),
                "categories": category_counts,
                "missing_categories": quality_assessment.get("missing", []),
                "source_breakdown": {"RSS/GitHub/HTML": rss_count, "X/Twitter": twitter_count},
                "x_ratio": round(twitter_count / max(len(discord_items), 1), 2),
                "llm_assessment": quality_assessment.get("assessment", ""),
            }
            quality_file = STATE_DIR / f"quality_report_{report_date}.json"
            save_json(quality_file, quality_report)
        except Exception as e:
            errors.append(f"Quality report: {e}")

    # Source health check — alert if any source has consecutive_failures >= 2
    unhealthy = get_unhealthy_sources(min_failures=2)
    if unhealthy:
        names = ", ".join(f"{s['name']}({s['consecutive_failures']}次)" for s in unhealthy)
        errors.append(f"SOURCE_HEALTH_ALERT: 以下源连续失败 ≥2 次：{names}。请检查或更换。")

    # Write state file last so all errors (Pushover, quality, health) are captured
    save_json(STATE_FILE, {
        "lastRunAt": datetime.now().astimezone().isoformat(),
        "itemCount": len(ranked),
        "discordCount": len(discord_items),
        "urgentCount": len([x for x in ranked if x.get('urgency')]),
        "db": {
            "observationsWritten": observations_written,
            "reportRowsWritten": reports_written
        },
        "dropped": dropped,
        "html": html_stats,
        "errors": errors,
        "sample": ranked[:10],
    })

    sys.stdout.write(report)
    if errors:
        sys.stderr.write("\nWARNINGS:\n- " + "\n- ".join(errors) + "\n")
    return 0


if __name__ == "__main__":
    _window_arg = "auto"
    if "--window" in sys.argv:
        _window_arg = sys.argv[sys.argv.index("--window") + 1]
    raise SystemExit(
        run(
            send_pushover=("--no-pushover" not in sys.argv),
            repeat_suppression=("--disable-repeat-suppression" not in sys.argv),
            reset_repeat=("--reset-repeat-memory" in sys.argv),
            use_llm=("--use-llm" in sys.argv),
            lookback_hours=int(sys.argv[sys.argv.index("--lookback") + 1]) if "--lookback" in sys.argv else None,
            window=_window_arg,
        )
    )
