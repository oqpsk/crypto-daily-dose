#!/usr/bin/env python3
"""
X/Twitter Cookie Export Tool

Usage:
    python3 scripts/export_x_cookies.py

This script opens a Chrome browser window for you to log in to X/Twitter.
Once you're logged in, press Enter to save the session cookies.
Cookies are saved to state/x_cookies.json (gitignored).

The cookies are valid for approximately 30-90 days.
When they expire, the pipeline will warn you and you can re-run this script.
"""
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COOKIES_FILE = ROOT / "state" / "x_cookies.json"


async def export_cookies():
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("Error: playwright not installed. Run: pip3 install playwright && python3 -m playwright install chromium")
        sys.exit(1)

    print("=" * 60)
    print("X/Twitter Cookie Export")
    print("=" * 60)
    print()
    print("Opening browser... Please log in to X/Twitter if not already.")
    print("After logging in, come back here and press Enter.")
    print()

    async with async_playwright() as p:
        # Use non-headless so user can see and interact
        browser = await p.chromium.launch(headless=False, args=["--window-size=1280,800"])
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto("https://x.com/home", wait_until="domcontentloaded")

        print("Browser opened. If you're not logged in, please log in now.")
        print("When ready (logged in and on home/timeline page), press Enter here...")
        input()

        # Check if actually logged in
        current_url = page.url
        if "login" in current_url or "i/flow" in current_url:
            print("Warning: doesn't look like you're logged in yet.")
            print("Please log in and press Enter again...")
            input()

        # Export cookies
        cookies = await context.cookies(["https://x.com", "https://twitter.com"])
        await browser.close()

    if not cookies:
        print("Error: No cookies found. Make sure you're logged in.")
        sys.exit(1)

    # Save cookies
    COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
    COOKIES_FILE.write_text(json.dumps(cookies, indent=2))

    # Show summary
    cookie_names = [c["name"] for c in cookies]
    important = [n for n in cookie_names if n in ("auth_token", "ct0", "twid")]
    print()
    print(f"✅ Saved {len(cookies)} cookies to {COOKIES_FILE}")
    print(f"   Key auth cookies found: {important}")

    if "auth_token" not in cookie_names:
        print()
        print("⚠️  Warning: 'auth_token' not found. You may not be fully logged in.")
        print("   Please try logging in again and re-run this script.")
        sys.exit(1)

    print()
    print("Done. Cookies are valid for approximately 30-90 days.")
    print("When they expire, the pipeline will warn you in Discord.")


if __name__ == "__main__":
    asyncio.run(export_cookies())
