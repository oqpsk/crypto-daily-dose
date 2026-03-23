#!/usr/bin/env python3
"""
Wrapper script for the crypto-daily-dose pipeline.

Two-report mode:
  --window morning    02:28 SGT run, covers past 16h (previous afternoon/evening + overnight)
  --window afternoon  14:28 SGT run, covers past 12h (morning news)

  Each window has its own reset state, so they don't interfere with each other.
  The event_memory 48h dedup prevents the same story from appearing in both reports.

Auto-reset behaviour:
  On the first run of each calendar day per window, repeat-suppression memory is
  reset so content appears fresh. Subsequent runs in the same window keep dedup state.

Manual flags:
  --force-reset    Reset regardless of date/window
  --no-reset       Skip reset unconditionally
  --no-pushover    Skip Pushover notification
  --use-llm        Enable LLM filter + summarization
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

# Window definitions: (lookback_hours, label)
WINDOWS = {
    "morning": 16,    # 02:28 SGT: covers prev 16h
    "afternoon": 12,  # 14:28 SGT: covers prev 12h
}


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


def should_auto_reset(force: bool, skip: bool, window: str) -> bool:
    if skip:
        return False
    if force:
        return True
    state = load_run_state()
    key = f"last_reset_date_{window}"
    return state.get(key) != today_sgt()


def main() -> int:
    force_reset = "--force-reset" in sys.argv
    no_reset = "--no-reset" in sys.argv

    # Parse window flag
    window = "afternoon"  # default
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--window" and i + 1 < len(sys.argv) - 1:
            window = sys.argv[i + 2]
            break

    # Strip our custom flags before passing to pipeline
    custom_flags = {"--force-reset", "--no-reset"}
    passthrough = []
    skip_next = False
    for arg in sys.argv[1:]:
        if skip_next:
            skip_next = False
            continue
        if arg == "--window":
            skip_next = True
            continue
        if arg not in custom_flags:
            passthrough.append(arg)

    do_reset = should_auto_reset(force=force_reset, skip=no_reset, window=window)

    if do_reset:
        print(f"[run_daily] First {window} run today ({today_sgt()}) — auto-resetting repeat memory.", flush=True)
        passthrough = [a for a in passthrough if a != "--reset-repeat-memory"]
        passthrough.append("--reset-repeat-memory")

    # Override lookback hours for window-specific coverage
    if window in WINDOWS and "--lookback" not in " ".join(passthrough):
        passthrough += ["--lookback", str(WINDOWS[window])]

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src") + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    args = [sys.executable, "-m", "crypto_daily_dose.pipeline"] + passthrough
    proc = subprocess.run(args, cwd=ROOT, check=False, env=env)

    if proc.returncode == 0 and do_reset:
        state = load_run_state()
        state[f"last_reset_date_{window}"] = today_sgt()
        save_run_state(state)
        print(f"[run_daily] Reset state saved for {window} {today_sgt()}.", flush=True)

    return proc.returncode


if __name__ == '__main__':
    raise SystemExit(main())
