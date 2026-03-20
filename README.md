# Crypto Daily Dose

Daily crypto intelligence pipeline for Discord + Pushover.

## MVP goals
- Fetch high-signal crypto sources
- Normalize + deduplicate items
- Score and filter for useful intel
- Deliver:
  - full report to Discord
  - concise brief to Pushover

## Project layout
- `src/crypto_daily_dose/` core pipeline code
- `scripts/` runnable entrypoints
- `state/` local runtime output/state (gitignored)
- `docs/` notes and design docs
- `tests/` basic tests

## Run
```bash
python3 scripts/run_daily.py --no-pushover
```
