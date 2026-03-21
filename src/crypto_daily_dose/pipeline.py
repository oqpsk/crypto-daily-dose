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
RSS_FEEDS = [tuple(x) for x in CONFIG["rss_feeds"]]
GITHUB_ENDPOINTS = [tuple(x) for x in CONFIG["github_endpoints"]]
HTML_SOURCES = [tuple(x) for x in CONFIG.get("html_sources", [])]
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


def topic_hits(text: str) -> dict:
    hits = {}
    for key, terms in PRIORITY_TOPICS.items():
        matched = [t for t in terms if t in text]
        if matched:
            hits[key] = matched
    return hits


def hard_drop_reason(item: dict, text: str) -> str | None:
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
    if any(term in text for term in strong_terms):
        return True
    non_competitor_hits = {k: v for k, v in hits.items() if k != "competitors"}
    matched_terms = sum(len(v) for v in non_competitor_hits.values())
    return matched_terms >= 2 and len(non_competitor_hits) >= 1


def passes_topic_gate(item: dict) -> tuple[bool, list[str]]:
    text = f"{item.get('title','')} {item.get('content','')} {item.get('source','')}".lower()
    hits = topic_hits(text)
    passed = any(k in hits for k in [
        "wallet_aa_ux",
        "protocol_infra",
        "security_risk_compliance",
        "tron_stablecoin_payments",
        "competitors",
        "market_structure",
    ])
    if passed and item.get("type") in {"wallet_blog", "research", "security_blog", "payments_blog"}:
        passed = html_strong_topic_evidence(text, hits)
    return passed, sorted(hits.keys())


def score_item(item: dict) -> dict:
    text = f"{item.get('title','')} {item.get('content','')} {item.get('source','')}".lower()
    category = item.get("category") or classify(item)
    passed_gate, gate_hits = passes_topic_gate(item)
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
        if canon in seen or canon == canonical_url(index_url):
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
        try:
            index_html = fetch(index_url, accept='text/html,application/xhtml+xml')
        except Exception as e:
            stats['index_fetch_failed'] += 1
            errors.append(f"HTML {source_name} index fetch failed: {e}")
            continue

        article_links = extract_links(index_url, index_html, list(path_hints), limit=MAX_HTML_LINKS_PER_SOURCE)
        stats['links_considered'] += len(article_links)
        if not article_links:
            stats['zero_link_count'] += 1
            errors.append(f"HTML {source_name} extracted 0 candidate links")
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


def dedup(items: list[dict]) -> list[dict]:
    out = []
    for item in sorted(items, key=lambda x: x.get("timestamp", ""), reverse=True):
        drop = False
        u = canonical_url(item.get("url", ""))
        for kept in list(out):
            ku = canonical_url(kept.get("url", ""))
            if u and ku and u == ku:
                drop = True
                break
            if title_similarity(item.get("title", ""), kept.get("title", "")) >= 0.8:
                preferred = 1 if any(k in kept.get("source", "") for k in ["Ethereum", "EIPs", "GitHub"]) else 0
                current_pref = 1 if any(k in item.get("source", "") for k in ["Ethereum", "EIPs", "GitHub"]) else 0
                if current_pref > preferred:
                    out.remove(kept)
                    break
                drop = True
                break
        if not drop:
            out.append(item)
    return out


def enrich(items: list[dict]) -> tuple[list[dict], dict]:
    enriched = []
    dropped = {"hard_drop": 0, "topic_gate": 0, "low_signal": 0}
    now = now_utc()
    for item in items:
        dt = parse_dt(item.get("timestamp"))
        item["hours_ago"] = round((now - dt).total_seconds() / 3600, 1) if dt else 999
        text = f"{item.get('title','')} {item.get('content','')} {item.get('source','')}".lower()
        reason = hard_drop_reason(item, text)
        if reason:
            dropped["low_signal" if reason in {"low_signal_github", "price_only"} else "hard_drop"] += 1
            continue
        item["category"] = classify(item)
        passed, gate_hits = passes_topic_gate(item)
        if not passed:
            dropped["topic_gate"] += 1
            continue
        item["score"] = score_item(item)
        item["gate_hits"] = gate_hits
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
        "Wallet / AA / UX": "💼 钱包 / AA / 交互体验",
        "Protocol / EIP / Infra": "🧱 协议 / EIP / 基础设施",
        "Security / Risk / Compliance": "🛡️ 安全 / 风险 / 合规",
        "TRON / Stablecoin / Payments": "💸 TRON / 稳定币 / 支付",
        "Competitor Intelligence": "🧭 竞品情报",
        "Market Structure / Narrative": "📊 市场结构 / 叙事",
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
        return "钱包与AA方向有新进展"

    if category == "TRON / Stablecoin / Payments":
        if "stablecoin" in text or "稳定币" in title:
            return "稳定币规则出现变化"
        if "tron" in text or "trc20" in text:
            return "TRON 支付模型有变化"
        if "usdc" in text or "usdt" in text:
            return "稳定币结算场景扩大"
        return "支付基础设施有新动向"

    if category == "Security / Risk / Compliance":
        if any(t in text for t in ["exploit", "hack", "phishing", "drain", "critical"]):
            return "出现高风险安全事件"
        return "监管合规出现重要变化"

    if category == "Protocol / EIP / Infra":
        return "协议基础设施有更新"

    if category == "Competitor Intelligence":
        return "竞品产品方向有变化"

    return compact(title, 20)


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
        return "该更新与钱包交互、签名流程或账户抽象路径相关。"

    if category == "TRON / Stablecoin / Payments":
        if "stablecoin" in text:
            return "稳定币相关规则若继续推进，可能影响产品设计、收益结构与支付基础设施路径。"
        if "usdc" in text or "usdt" in text:
            return "稳定币正在扩展更多结算与交易场景，值得关注支付基础设施变化。"
        if "tron" in text:
            return "TRON 网络或费率模型变化，可能影响稳定币与跨境支付使用场景。"
        return "支付与稳定币基础设施出现值得关注的新动向。"

    if category == "Security / Risk / Compliance":
        if any(t in text for t in ["exploit", "hack", "phishing", "drain", "critical"]):
            return "该事件可能直接影响资产安全、签名风险或钱包风控策略。"
        return "监管或合规变化可能影响业务边界、产品设计或风险判断。"

    if category == "Protocol / EIP / Infra":
        return "这类协议或基础设施变化，后续可能传导到钱包与产品路线图。"

    if category == "Competitor Intelligence":
        return "竞品的新动作可能透露其产品方向、增长策略或集成重点。"

    return compact(item.get("content", ""), 80) or "见原文。"


def build_report(items: list[dict]) -> tuple[str, str | None]:
    today = datetime.now().astimezone().strftime("%Y-%m-%d")
    discord_items = [x for x in items if x["score"]["total"] >= DISCORD_MIN_THRESHOLD][:MAX_DISCORD_ITEMS]
    urgent_items = [x for x in discord_items if x.get("urgency")]

    lines = [f"# Crypto Daily Dose — {today}", ""]
    if not discord_items:
        lines += ["简报：", "- 今天没有高价值的钱包 / 基础设施 / 支付 / 安全相关情报。"]
        return "\n".join(lines) + "\n", None

    for item in discord_items:
        summary = summarize_body_zh(item)
        importance = why_it_matters(item)
        merged = summary if importance in summary else f"{summary} {importance}"
        lines += [
            f"## {zh_category(item['category'])}",
            f"- **{summarize_title_zh(item)}**",
            f"  - 摘要：{merged}",
            f"  - 来源：{item['source']} — {item['url']}",
            "",
        ]

    push_candidates = urgent_items + [x for x in discord_items if x not in urgent_items]
    if push_candidates:
        selected = []
        seen_push_texts = set()
        for item in push_candidates:
            text = summarize_title_zh(item)
            key = norm_title(text)
            if key in seen_push_texts:
                continue
            seen_push_texts.add(key)
            selected.append((item, text))
            if len(selected) >= 2:
                break
        push = "\n".join(f"{push_emoji(item['category'])} {text}" for item, text in selected) if selected else None
    else:
        push = None
    return "\n".join(lines).strip() + "\n", push


def run(send_pushover: bool = True) -> int:
    cutoff = now_utc() - timedelta(hours=LOOKBACK_HOURS)
    items, errors = [], []
    html_stats = {}
    for source_name, url, item_type in RSS_FEEDS:
        try:
            items.extend(parse_feed_entries(source_name, url, item_type, cutoff))
        except Exception as e:
            errors.append(f"RSS {source_name}: {e}")
    try:
        html_items, html_stats, html_errors = parse_html_sources(cutoff)
        items.extend(html_items)
        errors.extend(html_errors)
    except Exception as e:
        errors.append(f"HTML sources: {e}")
    try:
        items.extend(parse_github(cutoff))
    except Exception as e:
        errors.append(f"GitHub: {e}")

    filtered, dropped = enrich(dedup(items))
    ranked = sorted(filtered, key=sort_key, reverse=True)
    report, push = build_report(ranked)

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(report)
    save_json(STATE_FILE, {
        "lastRunAt": datetime.now().astimezone().isoformat(),
        "itemCount": len(ranked),
        "discordCount": len([x for x in ranked if x['score']['total'] >= DISCORD_MIN_THRESHOLD]),
        "urgentCount": len([x for x in ranked if x.get('urgency')]),
        "dropped": dropped,
        "html": html_stats,
        "errors": errors,
        "sample": ranked[:10],
    })

    if send_pushover and push:
        cfg = load_json(PUSHOVER_CFG, {})
        token, user = cfg.get("app_token"), cfg.get("user_key")
        if token and user:
            payload = urllib.parse.urlencode({"token": token, "user": user, "title": "加密情报", "message": push}).encode()
            req = urllib.request.Request(API_PUSHOVER, data=payload, method="POST")
            with urllib.request.urlopen(req, timeout=20) as resp:
                parsed = json.loads(resp.read().decode("utf-8", errors="replace"))
            if parsed.get("status") != 1:
                errors.append(f"Pushover API error: {parsed}")
        else:
            errors.append("Pushover config missing")

    sys.stdout.write(report)
    if errors:
        sys.stderr.write("\nWARNINGS:\n- " + "\n- ".join(errors) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(run(send_pushover=("--no-pushover" not in sys.argv)))
