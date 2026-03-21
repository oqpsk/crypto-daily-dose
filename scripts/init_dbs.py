#!/usr/bin/env python3
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

if __name__ == "__main__":
    from crypto_daily_dose.db import init_all
    result = init_all()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    sys.exit(0)
