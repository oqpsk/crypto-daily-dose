"""
Price monitoring for Crypto Daily Dose V2.

Fetches BTC/ETH/BNB/SOL 24h price change from CoinGecko (free API).
Injects a price alert item into the pipeline when any asset moves ≥5% in 24h.
"""
import json
import urllib.request
from datetime import datetime, timezone

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=bitcoin,ethereum,binancecoin,solana"
    "&vs_currencies=usd"
    "&include_24hr_change=true"
)

ASSET_MAP = {
    "bitcoin":     ("BTC", "$BTC"),
    "ethereum":    ("ETH", "$ETH"),
    "binancecoin": ("BNB", "$BNB"),
    "solana":      ("SOL", "$SOL"),
}

THRESHOLD_PCT = 5.0


def fetch_price_changes(user_agent: str = "CryptoDailyDose/0.3") -> dict:
    """Return {coin_id: {symbol, price_usd, change_24h}} for all tracked assets."""
    req = urllib.request.Request(COINGECKO_URL, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = json.loads(resp.read().decode("utf-8"))
    result = {}
    for coin_id, (symbol, tag) in ASSET_MAP.items():
        if coin_id in raw:
            result[coin_id] = {
                "symbol": symbol,
                "tag": tag,
                "price_usd": raw[coin_id].get("usd", 0),
                "change_24h": raw[coin_id].get("usd_24h_change", 0),
            }
    return result


def build_price_alert_items(changes: dict) -> list[dict]:
    """
    Build synthetic pipeline items for assets that moved ≥ THRESHOLD_PCT.
    Multiple triggers are merged into one item.
    """
    triggers = [
        v for v in changes.values()
        if abs(v.get("change_24h", 0)) >= THRESHOLD_PCT
    ]
    if not triggers:
        return []

    now_iso = datetime.now(timezone.utc).isoformat()

    # Sort by absolute change descending
    triggers.sort(key=lambda x: abs(x["change_24h"]), reverse=True)

    tags = " ".join(t["tag"] for t in triggers)
    symbols_str = "、".join(
        f"{t['symbol']} {'↑' if t['change_24h'] > 0 else '↓'}{abs(t['change_24h']):.1f}%"
        for t in triggers
    )
    direction = "大幅上涨" if all(t["change_24h"] > 0 for t in triggers) else \
                "大幅下跌" if all(t["change_24h"] < 0 for t in triggers) else "出现大幅波动"

    title = f"主流资产 24h {direction}：{symbols_str}"
    content = (
        f"过去 24 小时内，{symbols_str}，单资产波动幅度超过 {THRESHOLD_PCT}%。"
        f" 主流资产大幅波动通常预示流动性环境或市场情绪出现结构性变化。"
    )

    return [{
        "title": title,
        "content": content,
        "url": "https://www.coingecko.com",
        "source": "CoinGecko",
        "type": "price_alert",
        "timestamp": now_iso,
        # Pre-fill LLM fields so it bypasses LLM filter (price alerts are always relevant)
        "llm_relevant": True,
        "title_zh": title,
        "summary_zh": f"过去24小时：{symbols_str}。",
        "why_matters_zh": "主流资产大幅波动通常反映市场情绪或流动性的结构性变化，值得关注。",
        "llm_category": "价格",
    }]
