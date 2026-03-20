#!/usr/bin/env python3
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
LEGACY = Path('/Users/mini/.openclaw/workspace-mini/scripts/crypto_daily_dose.py')
TARGET = ROOT / 'state'
TARGET.mkdir(parents=True, exist_ok=True)


def main() -> int:
    args = [sys.executable, str(LEGACY)] + sys.argv[1:]
    proc = subprocess.run(args, cwd=ROOT, check=False)
    # sync latest outputs into project-local state for easier inspection
    for name in ['crypto_daily_dose.json', 'crypto_daily_dose_report.md']:
        src = Path('/Users/mini/.openclaw/workspace-mini/state') / name
        dst = TARGET / name
        if src.exists():
            shutil.copy2(src, dst)
    return proc.returncode


if __name__ == '__main__':
    raise SystemExit(main())
