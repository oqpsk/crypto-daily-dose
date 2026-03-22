#!/usr/bin/env python3
"""
Wrapper script for the crypto-daily-dose pipeline.

Auto-reset behaviour:
  On the first run of each calendar day (Asia/Singapore), the repeat-suppression
  memory is automatically cleared so that today's news always appears fresh.
  Subsequent runs on the same day keep the dedup state intact to avoid re-sending.

  The "first run today" check is based on the `last_reset_date` key stored in
  state/run_daily_state.json.  Pass --force-reset to override, or --no-reset to
  skip the auto-reset unconditionally.
"""
from pathlib import Path
import datetime
import json
import os
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
RUN_STATE_FILE = STATE_DIR / "run_daily_state.json"
TZ_SGT = datetime.timezone(datetime.timedelta(hours=8))


def today_sgt() -> str:
    return datetime.datetime.now(TZ_SGT).strftime("%Y-%m-%d")


def load_run_state() -> dict:
    if RUN_STATE_FILE.exists():
        try:
            return json.loads(RUN_STATE_FILE.read_text())
        except Exception:
            pass
    return {}


def save_run_state(state: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    RUN_STATE_FILE.write_text(json.dumps(state, indent=2))


def should_auto_reset(force: bool, skip: bool) -> bool:
    if skip:
        return False
    if force:
        return True
    state = load_run_state()
    return state.get("last_reset_date") != today_sgt()


def main() -> int:
    force_reset = "--force-reset" in sys.argv
    no_reset = "--no-reset" in sys.argv
    # Strip our custom flags before passing to pipeline
    passthrough = [a for a in sys.argv[1:] if a not in ("--force-reset", "--no-reset")]
    # --use-llm passes through to pipeline directly

    do_reset = should_auto_reset(force=force_reset, skip=no_reset)

    if do_reset:
        print(f"[run_daily] First run today ({today_sgt()}) — auto-resetting repeat memory.", flush=True)
        passthrough = [a for a in passthrough if a != "--reset-repeat-memory"]
        passthrough.append("--reset-repeat-memory")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src") + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    args = [sys.executable, "-m", "crypto_daily_dose.pipeline"] + passthrough
    proc = subprocess.run(args, cwd=ROOT, check=False, env=env)

    if proc.returncode == 0 and do_reset:
        state = load_run_state()
        state["last_reset_date"] = today_sgt()
        save_run_state(state)
        print(f"[run_daily] Reset state saved for {today_sgt()}.", flush=True)

    return proc.returncode


if __name__ == '__main__':
    raise SystemExit(main())
