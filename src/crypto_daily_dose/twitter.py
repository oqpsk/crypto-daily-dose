"""
X/Twitter account monitoring for Crypto Daily Dose V2.1

Uses Playwright with the user's existing X session cookies (from Edge browser).
Cookies are extracted automatically from Edge's SQLite DB using the macOS Keychain key.

Cookie refresh: Cookies expire every 30-90 days. When expired, the pipeline
will log a warning. Re-run scripts/refresh_x_cookies.py to update.
"""
import asyncio
import hashlib
import json
import shutil
import sqlite3
import subprocess
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COOKIES_FILE = ROOT / "state" / "x_cookies.json"
EDGE_COOKIES_DB = Path.home() / "Library/Application Support/Microsoft Edge/Default/Cookies"

# Accounts to monitor — ordered by priority
TRACKED_ACCOUNTS = [
    # Protocol / Core Devs
    "VitalikButerin",
    "TimBeiko",
    "ethereum",
    "justinsuntron",
    # Security
    "PeckShieldAlert",
    "SlowMist_Team",
    "CertiKAlert",
    "realScamSniffer",
    # Regulatory
    "SECGov",
    "CFTC",
    "circlepay",
    # Wallets / Products
    "MetaMask",
    "phantom",
    "safe",
    "TrustWallet",
    # Research
    "paradigm",
    "a16zcrypto",
    "MessariCrypto",
    # Chinese media
    "WuBlockchain",
    "BitpushNews",
    "CoinTelegraph_CN",
    # Regulatory / Institutional
    "HesterPeirce",
    "BrianQuintenz",
    # Ethereum Research
    "dannyryan",
    # Institutional / Market
    "saylor",
    # L2 / Competitor ecosystems
    "base",
    "arbitrum",
    "OptimismFND",
    "zksync",
    "RabbyWallet",
]

MAX_TWEETS_PER_ACCOUNT = 3
INTER_ACCOUNT_DELAY_S = 2  # Rate limit protection


def _extract_cookies_from_edge() -> list[dict]:
    """Extract and decrypt X.com cookies from Edge browser using macOS Keychain."""
    try:
        from Crypto.Cipher import AES
    except ImportError:
        raise RuntimeError("pycryptodome not installed: pip3 install pycryptodome --break-system-packages")

    if not EDGE_COOKIES_DB.exists():
        raise FileNotFoundError(f"Edge cookies DB not found: {EDGE_COOKIES_DB}")

    # Get decryption key from Keychain
    result = subprocess.run(
        ["security", "find-generic-password", "-w", "-s", "Microsoft Edge Safe Storage"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Keychain access failed: {result.stderr}")

    password = result.stdout.strip()
    key = hashlib.pbkdf2_hmac("sha1", password.encode("utf-8"), b"saltysalt", 1003, dklen=16)

    def decrypt(enc: bytes) -> str:
        if not enc or enc[:3] != b"v10":
            return enc.decode("utf-8", errors="replace") if enc else ""
        iv = b" " * 16
        cipher = AES.new(key, AES.MODE_CBC, iv)
        decrypted = cipher.decrypt(enc[3:])
        pad = decrypted[-1]
        result = decrypted[:-pad] if 1 <= pad <= 16 else decrypted
        # Skip 32-byte nonce prefix added by Chrome/Edge on macOS
        return result[32:].decode("utf-8", errors="replace") if len(result) > 32 else result.decode("utf-8", errors="replace")

    # Copy DB to avoid lock contention with running Edge
    tmp = Path(tempfile.mktemp(suffix=".db"))
    shutil.copy2(EDGE_COOKIES_DB, tmp)
    try:
        conn = sqlite3.connect(tmp)
        rows = conn.execute(
            "SELECT name, encrypted_value, host_key, path, is_secure, is_httponly, samesite "
            "FROM cookies WHERE host_key LIKE '%x.com%' OR host_key LIKE '%twitter.com%'"
        ).fetchall()
        conn.close()
    finally:
        tmp.unlink()

    cookies = []
    for name, enc, host, path, secure, httponly, samesite in rows:
        value = decrypt(enc)
        domain = host if host.startswith(".") else "." + host.lstrip(".")
        cookies.append({
            "name": name, "value": value, "domain": domain, "path": path,
            "secure": bool(secure), "httpOnly": bool(httponly),
            "sameSite": {0: "None", 1: "Lax", 2: "Strict"}.get(samesite, "Lax"),
        })
    return cookies


def refresh_cookies() -> int:
    """Extract fresh cookies from Edge and save to COOKIES_FILE. Returns cookie count."""
    cookies = _extract_cookies_from_edge()
    COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
    COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
    COOKIES_FILE.chmod(0o600)  # Restrict to owner only — cookies can hijack X session
    return len(cookies)


def load_cookies() -> list[dict]:
    """Load cookies from file, refreshing from Edge if missing or stale."""
    if not COOKIES_FILE.exists():
        refresh_cookies()
    return json.loads(COOKIES_FILE.read_text())


def cookies_valid(cookies: list[dict]) -> bool:
    """Check if auth_token cookie exists and looks valid."""
    for c in cookies:
        if c["name"] == "auth_token" and c["value"] and "\ufffd" not in c["value"]:
            return True
    return False


async def _fetch_account_tweets(page, username: str, max_tweets: int) -> list[dict]:
    """Fetch recent original tweets from a single X account."""
    tweets = []
    try:
        await page.goto(f"https://x.com/{username}", wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)

        url = page.url
        if "login" in url or "i/flow" in url:
            return []  # Session expired

        tweet_elements = await page.query_selector_all('[data-testid="tweet"]')
        now = datetime.now(timezone.utc)

        for el in tweet_elements[:max_tweets * 2]:  # Fetch extra to filter RTs
            try:
                # Skip retweets (they have a "Retweeted" indicator)
                rt_indicator = await el.query_selector('[data-testid="socialContext"]')
                if rt_indicator:
                    rt_text = await rt_indicator.inner_text()
                    if "retweeted" in rt_text.lower() or "转推" in rt_text:
                        continue

                # Get tweet text
                text_el = await el.query_selector('[data-testid="tweetText"]')
                if not text_el:
                    continue
                text = await text_el.inner_text()
                if not text.strip():
                    continue

                # Get timestamp
                time_el = await el.query_selector("time")
                ts_str = await time_el.get_attribute("datetime") if time_el else None
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")) if ts_str else now

                # Get tweet URL
                link_el = await el.query_selector('a[href*="/status/"]')
                tweet_url = ""
                if link_el:
                    href = await link_el.get_attribute("href")
                    tweet_url = f"https://x.com{href}" if href and href.startswith("/") else (href or "")

                tweets.append({
                    "title": f"@{username}: {text[:100]}",
                    "content": text[:500],
                    "url": tweet_url or f"https://x.com/{username}",
                    "source": f"X/@{username}",
                    "type": "tweet",
                    "timestamp": ts.isoformat(),
                    "author": username,
                })

                if len(tweets) >= max_tweets:
                    break
            except Exception:
                continue

    except Exception:
        pass

    return tweets


async def _fetch_all_tweets_async(accounts: list[str], cutoff: datetime) -> tuple[list[dict], list[str]]:
    """Fetch tweets from all accounts, filtering by cutoff time."""
    from playwright.async_api import async_playwright

    cookies = load_cookies()
    if not cookies_valid(cookies):
        # Try refreshing
        try:
            refresh_cookies()
            cookies = load_cookies()
        except Exception as e:
            return [], [f"X cookies refresh failed: {e}. Re-run scripts/refresh_x_cookies.py"]

    if not cookies_valid(cookies):
        return [], ["X cookies invalid or expired. Re-run scripts/refresh_x_cookies.py"]

    all_tweets = []
    errors = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        await ctx.add_cookies(cookies)
        page = await ctx.new_page()

        # Check session validity on first account
        await page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(2)
        if "login" in page.url or "i/flow" in page.url:
            await browser.close()
            return [], ["X session expired. Re-run scripts/refresh_x_cookies.py to refresh cookies."]

        for username in accounts:
            try:
                tweets = await _fetch_account_tweets(page, username, MAX_TWEETS_PER_ACCOUNT)
                # Filter by cutoff
                fresh = [t for t in tweets if datetime.fromisoformat(t["timestamp"]) >= cutoff]
                all_tweets.extend(fresh)
            except Exception as e:
                errors.append(f"X @{username}: {e}")
            await asyncio.sleep(INTER_ACCOUNT_DELAY_S)

        await browser.close()

    return all_tweets, errors


def fetch_tweets(cutoff: datetime, accounts: list[str] | None = None) -> tuple[list[dict], list[str]]:
    """
    Synchronous wrapper: fetch recent tweets from tracked accounts.
    Returns (items, errors).
    """
    if accounts is None:
        accounts = TRACKED_ACCOUNTS
    return asyncio.run(_fetch_all_tweets_async(accounts, cutoff))


def is_available() -> bool:
    """Check if X monitoring is configured and cookies are valid.
    Pure availability check — does not trigger side effects (no network calls)."""
    if not COOKIES_FILE.exists():
        try:
            refresh_cookies()
        except Exception:
            return False
    try:
        cookies = load_cookies()
        return cookies_valid(cookies)
    except Exception:
        return False


def check_cookie_expiry() -> int | None:
    """
    Check how many days until X cookies expire.
    Returns days remaining (0 = expired), or None if can't determine.
    Reads expiry from Edge Cookies DB (expires_utc is microseconds since 1601-01-01).
    """
    if not EDGE_COOKIES_DB.exists():
        return None
    try:
        tmp = Path(tempfile.mktemp(suffix=".db"))
        shutil.copy2(EDGE_COOKIES_DB, tmp)
        conn = sqlite3.connect(tmp)
        # Get the auth_token cookie's expiry
        row = conn.execute(
            "SELECT expires_utc FROM cookies WHERE name='auth_token' AND host_key LIKE '%x.com%' LIMIT 1"
        ).fetchone()
        conn.close()
        tmp.unlink()
        if not row or not row[0]:
            return None
        # Chromium epoch: microseconds since 1601-01-01
        chromium_epoch_offset = 11644473600  # seconds between 1601-01-01 and 1970-01-01
        expires_unix = row[0] / 1_000_000 - chromium_epoch_offset
        now_unix = datetime.now(timezone.utc).timestamp()
        days_left = int((expires_unix - now_unix) / 86400)
        return max(0, days_left)
    except Exception:
        return None
