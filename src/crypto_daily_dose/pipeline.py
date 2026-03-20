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
RSS_FEEDS = [tuple(x) for x in CONFIG["rss_feeds"]]
GITHUB_ENDPOINTS = [tuple(x) for x in CONFIG["github_endpoints"]]
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


def why_it_matters(item: dict) -> str:
    category = item.get("category")
    score = item.get("score", {})
    if category == "Wallet / AA / UX":
        return "Directly touches wallet UX, signing, or account abstraction decisions."
    if category == "TRON / Stablecoin / Payments":
        return "Relevant to stablecoin/payment rails and could affect product or market direction."
    if category == "Security / Risk / Compliance":
        return "Security/compliance changes are high-impact and often immediately actionable."
    if category == "Competitor Intelligence":
        return "May reveal competitor product direction, adoption strategy, or positioning."
    if category == "Protocol / EIP / Infra":
        return "Could affect wallet/infra roadmap if the protocol change becomes meaningful."
    return f"High-signal infra narrative with score {score.get('total', 0)}."


def build_report(items: list[dict]) -> tuple[str, str | None]:
    today = datetime.now().astimezone().strftime("%Y-%m-%d")
    discord_items = [x for x in items if x["score"]["total"] >= DISCORD_MIN_THRESHOLD][:MAX_DISCORD_ITEMS]
    urgent_items = [x for x in discord_items if x.get("urgency")]

    lines = [f"# Crypto Daily Dose — {today}", ""]
    if not discord_items:
        lines += ["Minimal report:", "- No high-value wallet / infra / payments / security items today."]
        return "\n".join(lines) + "\n", None

    for item in discord_items:
        lines += [
            f"## {item['category']}",
            f"- **{item['title']}**",
            f"  - What happened: {compact(item['content'], 180) or 'See source.'}",
            f"  - Why it matters: {why_it_matters(item)}",
            f"  - Source: {item['source']} — {item['url']}",
            "",
        ]

    if urgent_items:
        top = urgent_items[0]
        push = f"Crypto urgent: {compact(top['title'], 90)}"
    else:
        push = None
    return "\n".join(lines).strip() + "\n", push


def run(send_pushover: bool = True) -> int:
    cutoff = now_utc() - timedelta(hours=LOOKBACK_HOURS)
    items, errors = [], []
    for source_name, url, item_type in RSS_FEEDS:
        try:
            items.extend(parse_feed_entries(source_name, url, item_type, cutoff))
        except Exception as e:
            errors.append(f"RSS {source_name}: {e}")
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
        "errors": errors,
        "sample": ranked[:10],
    })

    if send_pushover and push:
        cfg = load_json(PUSHOVER_CFG, {})
        token, user = cfg.get("app_token"), cfg.get("user_key")
        if token and user:
            payload = urllib.parse.urlencode({"token": token, "user": user, "title": "Crypto Daily Dose", "message": push}).encode()
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
