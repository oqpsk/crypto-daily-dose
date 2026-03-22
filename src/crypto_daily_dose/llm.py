"""
LLM-based topic filtering and summarization for Crypto Daily Dose V2.

Replaces keyword-based topic gate with a single LLM call that:
1. Judges relevance against V2 scope definition
2. If relevant, generates Chinese title, summary, and "why it matters"

Single call per batch (8-10 items) to minimize cost.
Uses claude-haiku via Anthropic API.
"""
import json
import re
import urllib.request
import urllib.parse
from pathlib import Path


ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MODEL = "claude-haiku-4-5"
MAX_TOKENS = 2048
BATCH_SIZE = 8

# V2 scope definition injected into the system prompt
SCOPE_DEFINITION = """
你是加密行业情报过滤器。判断每条内容是否值得纳入每日情报简报。

【纳入标准】以下类型的内容应纳入：
- 监管/政策：稳定币立法、SEC/CFTC 动作、各国合规变化、AML/制裁
- 机构/资本：ETF 审批、百亿级机构入场、重大融资（>$1亿）
- 安全/风险：协议漏洞、重大被盗（>$100万）、新型攻击路径
- 协议/基础设施：EIP 状态变化、AA 进展、主网升级/分叉、L2 重大变化
- 钱包/支付/TRON：主流钱包重大发布、稳定币基础设施变化、TRON 网络变化
- 行业结构性事件：主网故障、重大合规事件、行业格局性变化
- 宏观→加密传导：有明确传导链的宏观事件（如 FOMC 利率决议影响流动性）
- 主流资产价格：BTC/ETH/BNB/SOL 任意 24h 波动 ≥5%

【不纳入】以下内容直接排除：
- 常规行情分析、技术分析、交易策略、价格预测
- meme coin、山寨币、无实质内容的炒作
- 鲸鱼搬砖、链上数据分析（无重大结构性意义）
- 地缘政治本身（除非有明确加密传导链）
- AI/半导体本身（除非直接影响加密基础设施）
- 重复报道同一事件（多源同一新闻只保留一条）
""".strip()

BATCH_PROMPT_TEMPLATE = """
以下是 {n} 条加密行业候选内容，请逐一判断是否符合纳入标准。

{items}

请严格按以下 JSON 格式输出，数组长度必须等于输入条目数：
[
  {{
    "index": 0,
    "relevant": true,
    "title_zh": "中文标题（20字以内）",
    "summary_zh": "发生了什么（1-2句话）",
    "why_matters_zh": "为什么重要（1句话）",
    "category": "监管/安全/协议/钱包支付/机构/行业/宏观/价格"
  }},
  {{
    "index": 1,
    "relevant": false,
    "title_zh": "",
    "summary_zh": "",
    "why_matters_zh": "",
    "category": ""
  }}
]

注意：
- relevant=false 时，其他字段留空字符串即可
- title_zh 必须简洁，不超过 20 个汉字
- 只输出 JSON 数组，不要有其他文字
"""


def _load_api_key() -> str | None:
    """Load Anthropic API key from openclaw auth profiles."""
    import os
    # Try environment first
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    # Try openclaw agent auth profiles
    candidates = [
        Path.home() / ".openclaw" / "agents" / "mini" / "agent" / "auth-profiles.json",
        Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json",
    ]
    for path in candidates:
        if path.exists():
            try:
                data = json.loads(path.read_text())
                for profile in data.get("profiles", {}).values():
                    token = profile.get("token", "")
                    if token and token.startswith("sk-ant-"):
                        return token
            except Exception:
                pass
    return None


def _call_anthropic(messages: list[dict], system: str, model: str = DEFAULT_MODEL) -> str:
    """Make a single Anthropic API call, return response text."""
    api_key = _load_api_key()
    if not api_key:
        raise RuntimeError("Anthropic API key not found")

    payload = json.dumps({
        "model": model,
        "max_tokens": MAX_TOKENS,
        "system": system,
        "messages": messages,
    }).encode("utf-8")

    req = urllib.request.Request(
        ANTHROPIC_API_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_VERSION,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read().decode("utf-8"))
    return result["content"][0]["text"]


def _format_item(idx: int, item: dict) -> str:
    title = (item.get("title") or "").strip()
    content = (item.get("content") or "")[:200].strip()
    source = (item.get("source") or "").strip()
    return f"[{idx}] 来源：{source}\n标题：{title}\n摘要：{content}"


def _parse_llm_response(text: str, n: int) -> list[dict]:
    """Parse LLM JSON response, return list of result dicts."""
    # Extract JSON array from response
    match = re.search(r'\[.*\]', text, re.S)
    if not match:
        return []
    try:
        results = json.loads(match.group(0))
        # Validate structure
        if not isinstance(results, list):
            return []
        return results[:n]
    except Exception:
        return []


def llm_filter_and_summarize(items: list[dict], model: str = DEFAULT_MODEL) -> list[dict]:
    """
    Filter and summarize a list of items using LLM.

    Returns items with LLM-generated fields added:
    - llm_relevant: bool
    - title_zh: str
    - summary_zh: str
    - why_matters_zh: str
    - llm_category: str

    Items where llm_relevant=False are returned with the flag set but not removed
    (caller decides what to do).
    """
    if not items:
        return []

    enriched = []
    # Process in batches
    for batch_start in range(0, len(items), BATCH_SIZE):
        batch = items[batch_start:batch_start + BATCH_SIZE]
        items_text = "\n\n".join(_format_item(i, item) for i, item in enumerate(batch))
        prompt = BATCH_PROMPT_TEMPLATE.format(n=len(batch), items=items_text)

        try:
            response_text = _call_anthropic(
                messages=[{"role": "user", "content": prompt}],
                system=SCOPE_DEFINITION,
                model=model,
            )
            results = _parse_llm_response(response_text, len(batch))
        except Exception as e:
            # On LLM failure, mark all as relevant (fail-open) to not lose news
            results = [{"index": i, "relevant": True, "title_zh": "", "summary_zh": "", "why_matters_zh": "", "category": ""} for i in range(len(batch))]
            import sys
            print(f"LLM batch failed: {e}", file=sys.stderr)

        # Merge results back into items
        result_map = {r.get("index", i): r for i, r in enumerate(results)}
        for i, item in enumerate(batch):
            r = result_map.get(i, {})
            item = dict(item)
            item["llm_relevant"] = bool(r.get("relevant", True))
            item["title_zh"] = r.get("title_zh", "") or ""
            item["summary_zh"] = r.get("summary_zh", "") or ""
            item["why_matters_zh"] = r.get("why_matters_zh", "") or ""
            item["llm_category"] = r.get("category", "") or ""
            enriched.append(item)

    return enriched


def is_llm_available() -> bool:
    """Check if LLM API key is configured."""
    return _load_api_key() is not None
