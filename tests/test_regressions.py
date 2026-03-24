from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from crypto_daily_dose import db, llm, pipeline, prices, twitter  # noqa: E402


def test_llm_partial_json_marks_missing_items_explicitly(monkeypatch):
    items = [
        {"title": "a", "content": "a", "source": "s"},
        {"title": "b", "content": "b", "source": "s"},
        {"title": "c", "content": "c", "source": "s"},
    ]

    monkeypatch.setattr(
        llm,
        "_call_anthropic",
        lambda **kwargs: json.dumps([
            {
                "index": 0,
                "relevant": True,
                "title_zh": "命中",
                "summary_zh": "ok",
                "why_matters_zh": "ok",
                "category": "协议",
            }
        ]),
    )

    result = llm.llm_filter_and_summarize(items)

    assert len(result) == 3
    assert result[0]["llm_relevant"] is True
    # Missing items must not silently default to relevant=True.
    assert result[1]["llm_relevant"] is False
    assert result[2]["llm_relevant"] is False
    assert result[1]["llm_parse_error"] == "missing_result"
    assert result[2]["llm_parse_error"] == "missing_result"


def test_twitter_single_account_failure_is_reported_in_errors(monkeypatch):
    cutoff = datetime.now(timezone.utc)

    monkeypatch.setattr(twitter, "load_cookies", lambda: [{"name": "auth_token", "value": "ok"}])
    monkeypatch.setattr(twitter, "cookies_valid", lambda cookies: True)

    async def fake_fetch_account_tweets(page, username, max_tweets):
        raise RuntimeError("boom")

    class FakePage:
        def __init__(self):
            self.url = "https://x.com/home"

        async def goto(self, *args, **kwargs):
            return None

    class FakeContext:
        async def add_cookies(self, cookies):
            return None

        async def new_page(self):
            return FakePage()

    class FakeBrowser:
        async def new_context(self, **kwargs):
            return FakeContext()

        async def close(self):
            return None

    class FakePlaywright:
        chromium = None

        def __init__(self):
            class Chromium:
                async def launch(self, headless=True):
                    return FakeBrowser()

            self.chromium = Chromium()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeAsyncPlaywrightFactory:
        def __call__(self):
            return FakePlaywright()

    monkeypatch.setattr(twitter, "_fetch_account_tweets", fake_fetch_account_tweets)
    monkeypatch.setattr("playwright.async_api.async_playwright", FakeAsyncPlaywrightFactory())

    items, errors = twitter.fetch_tweets(cutoff, accounts=["ghost_account"])

    assert items == []
    assert any("ghost_account" in err and "boom" in err for err in errors)


def test_dedup_cross_source_currently_merges_distinct_events_with_three_shared_tokens():
    items = [
        {
            "title": "Ethereum security council approves bridge patch for Arbitrum",
            "content": "Patch approved after bridge outage review.",
            "url": "https://source-a.test/bridge-patch",
            "source": "Source A",
            "type": "news",
        },
        {
            "title": "Ethereum security council approves staking policy for Optimism",
            "content": "Policy approved after governance review.",
            "url": "https://source-b.test/staking-policy",
            "source": "Source B",
            "type": "news",
        },
    ]

    merged = pipeline.dedup_cross_source(items)

    # Documents current bug-prone behavior: 3 shared tokens are enough to merge
    # clearly different subjects/events.
    assert len(merged) == 1
    assert " · " in merged[0]["source"]


def test_start_tracking_upserts_missing_event(monkeypatch, tmp_path):
    event_db = tmp_path / "event_memory.db"
    monkeypatch.setattr(db, "EVENT_DB", event_db)

    event_id = "evt-123"
    db.start_tracking(event_id, "needs follow-up")

    with db.connect(event_db) as conn:
        row = conn.execute(
            "SELECT event_id, tracking_status, track_reason FROM events WHERE event_id = ?",
            (event_id,),
        ).fetchone()

    assert row is not None
    assert row["event_id"] == event_id
    assert row["tracking_status"] == "active"
    assert row["track_reason"] == "needs follow-up"


def test_fetch_price_changes_handles_missing_24h_change(monkeypatch):
    payload = {"bitcoin": {"usd": 80000}}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(payload).encode("utf-8")

    monkeypatch.setattr(prices.urllib.request, "urlopen", lambda req, timeout=15: FakeResponse())

    result = prices.fetch_price_changes()

    assert result["bitcoin"]["price_usd"] == 80000
    assert result["bitcoin"]["change_24h"] == 0
