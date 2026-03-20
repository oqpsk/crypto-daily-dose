#!/usr/bin/env python3
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    args = [sys.executable, '-m', 'crypto_daily_dose.pipeline'] + sys.argv[1:]
    env = None
    proc = subprocess.run(args, cwd=ROOT, check=False, env=env)
    return proc.returncode


if __name__ == '__main__':
    raise SystemExit(main())
