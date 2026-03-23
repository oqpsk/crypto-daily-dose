#!/usr/bin/env python3
"""
Refresh X/Twitter cookies from Edge browser.
Run this when the pipeline warns that X cookies have expired.

Usage:
    python3 scripts/refresh_x_cookies.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from crypto_daily_dose.twitter import refresh_cookies, load_cookies, cookies_valid

if __name__ == "__main__":
    print("Refreshing X cookies from Edge browser...")
    count = refresh_cookies()
    cookies = load_cookies()
    valid = cookies_valid(cookies)
    if valid:
        print(f"✅ Success: saved {count} cookies, auth_token present and clean")
    else:
        print(f"⚠️  Saved {count} cookies but auth_token missing or invalid")
        print("   Make sure Edge is running and you're logged in to X")
        sys.exit(1)
