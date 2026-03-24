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
- 主流资产价格：仅限 BTC/ETH/BNB/SOL 任意 24h 波动 ≥5% 的事实性报道

【硬性排除】以下内容无论如何都排除，不得通过：
- 价格技术分析：含 "breakout"、"support"、"resistance"、"outlook"、"target"、"bullish"、"bearish"、"chart"、"Elliott wave"、"wedge"、"head and shoulders" 等技术分析术语的文章
- 价格预测/展望：预测某代币价格目标的文章（如 "XXX could reach $X.XX"、"eyes $X target"）
- 山寨币/小市值代币：ASTER、NEAR、XRP（非重大事件）、ADA、HYPE 等非主流资产的行情分析
- meme coin 和投机叙事
- 鲸鱼动向、链上数据分析（无结构性意义）
- 地缘政治（无明确加密传导链）
- 预测市场分析（Polymarket 等盘口数据）
- **周内/本周看点/事件日历类聚合文章**：标题含"week ahead"、"weekly preview"、"this week"、"周内重点"、"本周看点"等的内容，属于活动汇总而非独立事件，直接排除

【判断规则】
- 如果文章标题含有价格数字 + 方向词（"$X.XX"、"reach"、"target"、"could open"），直接排除
- 如果文章的核心内容是某代币的价格走势分析，直接排除，即使来源是加密媒体
- 只有"发生了什么"类的事实性报道才纳入，"可能会怎样"类的预测性内容排除
""".strip()

BATCH_PROMPT_TEMPLATE = """
以下是 {n} 条加密行业候选内容，请逐一判断是否符合纳入标准。

{items}

请严格按以下 JSON 格式输出，数组长度必须等于输入条目数：
[
  {{
    "index": 0,
    "relevant": true,
    "significance": "normal",
    "track": false,
    "track_reason": "",
    "title_zh": "中文标题（20字以内）",
    "summary_zh": "发生了什么（1-2句话）",
    "why_matters_zh": "为什么重要（1句话）",
    "category": "监管/安全/协议/钱包支付/机构/行业/宏观/价格"
  }},
  {{
    "index": 1,
    "relevant": false,
    "significance": "normal",
    "track": false,
    "track_reason": "",
    "title_zh": "",
    "summary_zh": "",
    "why_matters_zh": "",
    "category": ""
  }}
]

注意：
- relevant=false 时，其他字段留空字符串即可
- significance 取值：
  * "high"：行业级重大事件，需满足以下至少一条：
    - $1亿以上安全漏洞/被盗事件
    - 主要国家/地区重大监管立法或执法行动（非日常指引）
    - 主流资产（BTC/ETH/BNB/SOL）24h 波动 ≥10%
    - 顶级机构（贝莱德/高盛/监管机构）首次进入加密领域的重大动作
    - 以太坊/比特币协议级别的重大升级（非提案阶段）
  * "normal"：其他所有内容（绝大多数是这个）
  - high 标准要非常严格，每份日报预期 0-2 条，大多数时候 0 条
- track=true 表示该事件值得跨天持续追踪（只用于以下类型）：
  * 重大安全事件（>$100万被盗，且后续可能有资金追回/起诉/和解）
  * EIP/协议提案进入关键阶段（Final Review、最后征集意见）
  * 重大监管进展（法案投票、起诉/和解/判决）
  * 大型机构动作的后续（ETF 获批、破产清算重要节点）
- track_reason 简述为何追踪（10字以内），track=false 时留空
- title_zh 必须简洁，不超过 20 个汉字
- 只输出 JSON 数组，不要有其他文字
"""


def _test_api_key(token: str) -> bool:
    """Quick probe to check if an API key works (not rate-limited)."""
    payload = json.dumps({
        "model": "claude-haiku-4-5",
        "max_tokens": 5,
        "messages": [{"role": "user", "content": "hi"}],
    }).encode()
    req = urllib.request.Request(
        ANTHROPIC_API_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": token,
            "anthropic-version": ANTHROPIC_VERSION,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            r.read()
        return True
    except Exception:
        return False


def _load_api_key() -> str | None:
    """Load a working Anthropic API key from openclaw auth profiles.
    Tests each key and skips rate-limited ones."""
    import os
    # Try environment first
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    # Collect all candidate keys from auth profiles
    candidates = [
        Path.home() / ".openclaw" / "agents" / "mini" / "agent" / "auth-profiles.json",
        Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json",
    ]
    keys = []
    for path in candidates:
        if path.exists():
            try:
                data = json.loads(path.read_text())
                for profile in data.get("profiles", {}).values():
                    token = profile.get("token", "")
                    if token and token.startswith("sk-ant-") and token not in keys:
                        keys.append(token)
            except Exception:
                pass
    # Return first working key
    for token in keys:
        if _test_api_key(token):
            return token
    # Fall back to first available even if not tested successfully
    return keys[0] if keys else None


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
            r = result_map.get(i)
            item = dict(item)
            if r is None:
                item["llm_relevant"] = False
                item["llm_significance"] = "normal"
                item["llm_track"] = False
                item["llm_track_reason"] = ""
                item["title_zh"] = ""
                item["summary_zh"] = ""
                item["why_matters_zh"] = ""
                item["llm_category"] = ""
                item["llm_parse_error"] = "missing_result"
                enriched.append(item)
                continue
            item["llm_relevant"] = bool(r.get("relevant", False))
            item["llm_significance"] = r.get("significance", "normal") or "normal"
            item["llm_track"] = bool(r.get("track", False))
            item["llm_track_reason"] = r.get("track_reason", "") or ""
            item["title_zh"] = r.get("title_zh", "") or ""
            item["summary_zh"] = r.get("summary_zh", "") or ""
            item["why_matters_zh"] = r.get("why_matters_zh", "") or ""
            item["llm_category"] = r.get("category", "") or ""
            item["llm_parse_error"] = ""
            enriched.append(item)

    return enriched


def is_llm_available() -> bool:
    """Check if LLM API key is configured."""
    return _load_api_key() is not None


MATERIAL_UPDATE_PROMPT = """
判断新内容是否是对已追踪事件的 material update（实质性新进展）。

已追踪事件：
标题：{tracked_title}
追踪原因：{track_reason}

新内容：
标题：{new_title}
摘要：{new_content}

material update 的标准（必须满足至少一条）：
- 事件状态发生变化（如：调查→起诉，草案→Final，冻结→追回/损失确认）
- 出现重要新进展（新的官方声明、法庭判决、资金动向确认）
- 事件规模发生重大变化（损失金额更新、影响范围扩大）

不算 material update：
- 重复报道同一事实
- 分析/评论性文章
- 相同信息换个角度的报道

只回答 JSON，格式：
{{"is_material_update": true/false, "reason": "一句话说明"}}
"""


QUALITY_ASSESSMENT_PROMPT = """
今天的日报内容如下：

{report_content}

请做一次简短的覆盖分析，回答以下问题：
1. 今天覆盖了哪些话题？
2. 哪些重要话题今天没有内容？（钱包/AA/UX、协议/EIP、安全/风险、TRON/稳定币/支付、竞品情报）
3. 信息来源质量：X/Twitter 实时信号 vs RSS 滞后报道的比例如何？
4. 整体评价（1-2句话）

只输出 JSON 格式：
{{
  "covered": ["协议", "安全"],
  "missing": ["钱包/支付", "竞品情报"],
  "assessment": "今日协议和安全覆盖良好，但钱包和TRON相关内容空缺，建议关注相关X账号活跃度。"
}}
"""


def generate_quality_assessment(report_content: str, model: str = DEFAULT_MODEL) -> dict:
    """Generate a quality assessment for today's report."""
    if not report_content.strip() or "今天没有高价值" in report_content:
        return {"covered": [], "missing": ["全部"], "assessment": "今日无有效内容产出。"}
    prompt = QUALITY_ASSESSMENT_PROMPT.format(report_content=report_content[:2000])
    try:
        response = _call_anthropic(
            messages=[{"role": "user", "content": prompt}],
            system="你是加密日报质量评估助手，分析日报内容覆盖情况。",
            model=model,
        )
        match = re.search(r'\{.*\}', response, re.S)
        if match:
            return json.loads(match.group(0))
    except Exception:
        pass
    return {"covered": [], "missing": [], "assessment": "质量评估失败。"}


def check_material_update(tracked_event: dict, new_item: dict, model: str = DEFAULT_MODEL) -> tuple[bool, str]:
    """
    Check if new_item contains a material update for a tracked event.
    Returns (is_update, reason).
    """
    prompt = MATERIAL_UPDATE_PROMPT.format(
        tracked_title=tracked_event.get("canonical_title", ""),
        track_reason=tracked_event.get("track_reason", ""),
        new_title=new_item.get("title", ""),
        new_content=(new_item.get("content", "") or "")[:300],
    )
    try:
        response = _call_anthropic(
            messages=[{"role": "user", "content": prompt}],
            system="你是事件追踪助手，判断新内容是否对已追踪事件有实质性新进展。",
            model=model,
        )
        match = re.search(r'\{.*\}', response, re.S)
        if match:
            result = json.loads(match.group(0))
            return bool(result.get("is_material_update")), result.get("reason", "")
    except Exception:
        pass
    return False, ""
