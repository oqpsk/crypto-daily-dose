#!/usr/bin/env python3
import email.utils
import json
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path

WORKDIR = Path(__file__).resolve().parents[1]
STATE_DIR = WORKDIR / "state"
STATE_FILE = STATE_DIR / "crypto_daily_dose.json"
OUTPUT_FILE = STATE_DIR / "crypto_daily_dose_report.md"
PUSHOVER_CFG = STATE_DIR / "pushover.json"
API_PUSHOVER = "https://api.pushover.net/1/messages.json"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) OpenClaw-CryptoDailyDose/0.1"
LOOKBACK_HOURS = 30
MAX_RSS_ITEMS_PER_FEED = 12
MAX_GITHUB_ITEMS = 12
MAX_EIP_ITEMS = 8

RSS_FEEDS = [
    ("The Block", "https://www.theblock.co/rss.xml", "news"),
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/", "news"),
    ("Blockworks", "https://blockworks.co/feed/", "news"),
    ("Ethereum Blog", "https://blog.ethereum.org/feed.xml", "blog"),
    ("EIPs", "https://eips.ethereum.org/feed.xml", "eip"),
]

GITHUB_ENDPOINTS = [
    ("Ethereum/EIPs Pulls", "https://api.github.com/repos/ethereum/EIPs/pulls?state=all&sort=updated&direction=desc&per_page=10", "github_pull"),
    ("Ethereum/EIPs Events", "https://api.github.com/repos/ethereum/EIPs/events?per_page=20", "github_event"),
]

CATEGORY_MAP = [
    ("Security", ["security", "exploit", "hack", "vulnerability", "breach", "attack", "phishing", "drain", "bug bounty"]),
    ("Protocol / EIP", ["eip-", "eips", "ethereum improvement proposal", "hard fork", "pectra", "fusaka", "ethereum", "opcodes", "erc-"]),
    ("Wallet / Infra", ["wallet", "custody", "rpc", "node", "validator", "rollup", "sequencer", "bridge", "infrastructure", "client", "l2", "interop"]),
    ("Payments", ["payment", "payments", "stablecoin", "merchant", "remittance", "settlement"]),
    ("Competitor", ["solana", "sui", "aptos", "avalanche", "berachain", "base", "tron"]),
]


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
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def compact(text: str, limit: int = 240) -> str:
    text = re.sub(r"\s+", " ", (text or "").strip())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


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


def score_item(item: dict) -> dict:
    text = f"{item.get('title','')} {item.get('content','')} {item.get('source','')}".lower()
    relevance = 0
    impact = 0
    novelty = 0
    actionability = 0

    relevance_terms = [
        "ethereum", "eip", "l2", "rollup", "security", "wallet", "stablecoin", "infrastructure",
        "validator", "bridge", "protocol", "payments", "custody", "client", "github", "erc-",
    ]
    relevance = min(3, sum(1 for t in relevance_terms if t in text and len(t) > 1))
    if relevance > 0:
        relevance = min(3, 1 + relevance // 2)

    impact_terms = ["launch", "upgrade", "proposal", "approved", "merged", "mainnet", "security", "exploit", "funding", "integration", "release"]
    impact = min(3, sum(1 for t in impact_terms if t in text))
    if impact > 0:
        impact = min(3, 1 + impact // 2)

    novelty = 2 if item.get("type") in {"github_pull", "github_event", "eip"} else 1
    if item.get("hours_ago", 999) <= 12:
        novelty = min(2, novelty + 1)

    actionable_terms = ["what this means", "breaking", "guide", "proposal", "draft", "review", "deadline", "migration"]
    actionability = min(2, sum(1 for t in actionable_terms if t in text))
    if item.get("type") in {"github_pull", "eip"}:
        actionability = max(actionability, 1)

    penalty = 0
    if any(t in text for t in ["price", "etf inflows", "market wrap", "meme coin", "memecoin", "trading"]):
        penalty += 3
    if any(t in text for t in ["bitcoin price", "ether price"]):
        penalty += 3

    total = max(0, relevance + impact + novelty + actionability - penalty)
    if total >= 8:
        bucket = "Top"
    elif total >= 6:
        bucket = "Secondary"
    else:
        bucket = "Discard"

    return {
        "relevance": relevance,
        "impact": impact,
        "novelty": novelty,
        "actionability": actionability,
        "total": total,
        "bucket": bucket,
    }


def classify(item: dict) -> str:
    text = f"{item.get('title','')} {item.get('content','')}".lower()
    for name, terms in CATEGORY_MAP:
        if any(term in text for term in terms):
            return name
    if item.get("type") in {"eip", "github_pull", "github_event"}:
        return "Protocol / EIP"
    return "Competitor"


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
            pub = node.findtext("pubDate") or node.findtext("published") or node.findtext("updated")
            dt = parse_dt(pub)
            if dt and dt < cutoff:
                continue
            entries.append({
                "title": strip_html(title),
                "content": compact(strip_html(desc), 280),
                "url": link,
                "source": source_name,
                "type": item_type,
                "timestamp": dt.isoformat() if dt else "",
            })
    else:
        ns = {"a": "http://www.w3.org/2005/Atom"}
        nodes = root.findall("a:entry", ns)
        for node in nodes[:MAX_RSS_ITEMS_PER_FEED]:
            title = (node.findtext("a:title", default="", namespaces=ns) or "").strip()
            link = ""
            for ln in node.findall("a:link", ns):
                href = ln.attrib.get("href")
                if href:
                    link = href
                    break
            summary = node.findtext("a:summary", default="", namespaces=ns) or node.findtext("a:content", default="", namespaces=ns) or ""
            pub = node.findtext("a:published", default="", namespaces=ns) or node.findtext("a:updated", default="", namespaces=ns)
            dt = parse_dt(pub)
            if dt and dt < cutoff:
                continue
            entries.append({
                "title": strip_html(title),
                "content": compact(strip_html(summary), 280),
                "url": link,
                "source": source_name,
                "type": item_type,
                "timestamp": dt.isoformat() if dt else "",
            })
    return entries


def fetch_json(url: str):
    text = fetch(url, accept="application/json")
    return json.loads(text)


def parse_github(cutoff: datetime) -> list[dict]:
    items = []
    for source_name, url, item_type in GITHUB_ENDPOINTS:
        payload = fetch_json(url)
        if item_type == "github_pull":
            for pr in payload[:MAX_GITHUB_ITEMS]:
                dt = parse_dt(pr.get("updated_at") or pr.get("created_at"))
                if dt and dt < cutoff:
                    continue
                title = pr.get("title") or ""
                body = pr.get("body") or ""
                state = pr.get("state") or ""
                draft = pr.get("draft")
                items.append({
                    "title": f"PR #{pr.get('number')}: {title}",
                    "content": compact(f"{state.upper()} | draft={draft}. {strip_html(body)}", 280),
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
                commit_id = (ev.get("commit_id") or "")[:8]
                issue = ev.get("issue") or {}
                issue_no = issue.get("number")
                issue_title = issue.get("title") or ""
                title = f"{event} by {actor}"
                if issue_no:
                    title = f"Issue/PR #{issue_no}: {issue_title or event}"
                body = f"GitHub {event}. actor={actor}. commit={commit_id}"
                if event in {"referenced", "mentioned", "subscribed"}:
                    continue
                items.append({
                    "title": title,
                    "content": compact(body, 240),
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
        for kept in out:
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


def enrich(items: list[dict]) -> list[dict]:
    enriched = []
    now = now_utc()
    for item in items:
        dt = parse_dt(item.get("timestamp"))
        hours = round((now - dt).total_seconds() / 3600, 1) if dt else 999
        item["hours_ago"] = hours
        score = score_item(item)
        item["score"] = score
        item["category"] = classify(item)
        enriched.append(item)
    return enriched


def build_report(items: list[dict]) -> tuple[str, str]:
    today = datetime.now().astimezone().strftime("%Y-%m-%d")
    top_items = [x for x in items if x["score"]["bucket"] == "Top"][:6]
    secondary = [x for x in items if x["score"]["bucket"] == "Secondary"][:8]
    grouped = {k: [] for k, _ in CATEGORY_MAP}
    for item in top_items + secondary:
        grouped.setdefault(item["category"], []).append(item)

    lines = [f"# Crypto Daily Dose — {today}", ""]
    lines.append("## Top Priority")
    if top_items:
        for item in top_items:
            lines.append(f"- **{item['title']}**")
            lines.append(f"  - What happened: {compact(item['content'], 180) or 'See source.'}")
            lines.append(f"  - Why it matters: score {item['score']['total']}/10 · {item['category']}")
            lines.append(f"  - Source: {item['source']} — {item['url']}")
    else:
        lines.append("- No clear top-priority crypto intel today.")
    lines.append("")

    for section in ["Wallet / Infra", "Protocol / EIP", "Security", "Payments", "Competitor"]:
        lines.append(f"## {section}")
        entries = grouped.get(section, [])[:3]
        if entries:
            for item in entries:
                lines.append(f"- {item['title']} ({item['source']})")
        else:
            lines.append("- N/A")
        lines.append("")

    lines.append("## What matters today")
    if top_items or secondary:
        summary_items = (top_items + secondary)[:5]
        for item in summary_items:
            lines.append(f"- {item['category']}: {compact(item['title'], 110)}")
    else:
        lines.append("- Quiet day. Mostly low-signal or repetitive coverage.")

    if top_items:
        push = "Crypto Brief:\n" + "\n".join(
            f"{idx}) {compact(item['title'], 80)}" for idx, item in enumerate(top_items[:2], 1)
        )
    else:
        push = "No high-priority crypto intel today."

    return "\n".join(lines).strip() + "\n", push


def run(send_pushover: bool = True) -> int:
    cutoff = now_utc() - timedelta(hours=LOOKBACK_HOURS)
    items = []
    errors = []

    for source_name, url, item_type in RSS_FEEDS:
        try:
            items.extend(parse_feed_entries(source_name, url, item_type, cutoff))
        except Exception as e:
            errors.append(f"RSS {source_name}: {e}")

    try:
        items.extend(parse_github(cutoff))
    except Exception as e:
        errors.append(f"GitHub: {e}")

    items = dedup(items)
    items = enrich(items)
    ranked = sorted(items, key=lambda x: (x["score"]["total"], -x.get("hours_ago", 999)), reverse=True)
    report, push = build_report(ranked)

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(report)
    save_json(STATE_FILE, {
        "lastRunAt": datetime.now().astimezone().isoformat(),
        "itemCount": len(ranked),
        "topCount": len([x for x in ranked if x['score']['bucket'] == 'Top']),
        "secondaryCount": len([x for x in ranked if x['score']['bucket'] == 'Secondary']),
        "errors": errors,
        "sample": ranked[:10],
    })

    if send_pushover:
        cfg = load_json(PUSHOVER_CFG, {})
        token = cfg.get("app_token")
        user = cfg.get("user_key")
        if token and user:
            payload = urllib.parse.urlencode({
                "token": token,
                "user": user,
                "title": "Crypto Daily Dose",
                "message": push,
            }).encode()
            req = urllib.request.Request(API_PUSHOVER, data=payload, method="POST")
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = resp.read().decode("utf-8", errors="replace")
            parsed = json.loads(body)
            if parsed.get("status") != 1:
                errors.append(f"Pushover API error: {body}")
        else:
            errors.append("Pushover config missing")

    sys.stdout.write(report)
    if errors:
        sys.stderr.write("\nWARNINGS:\n- " + "\n- ".join(errors) + "\n")
    return 0


if __name__ == "__main__":
    send = "--no-pushover" not in sys.argv
    raise SystemExit(run(send_pushover=send))
