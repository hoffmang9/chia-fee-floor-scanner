# Agent Context

This file provides project-specific context for coding agents working in this repo.

## Project overview

- Purpose: measure an observed Chia transaction fee floor from Coinset block data.
- Main script: `min_tx_fee_scan.py`.
- Primary output: CSV with per-block minimum per-spend fee for qualifying blocks.

## Policy and behavior

- Candidate blocks are transaction blocks with non-zero total block fee.
- A candidate block is excluded if any spend has zero fee.
- Per-spend fee is estimated as:
  - `coin_spend.coin.amount - sum(CREATE_COIN amounts)`
- Spend-level data source is Coinset `block_spends_with_conditions`.
- Script is fail-soft on phase-2 per-block errors, but now reports skipped counts
  and optional skipped details via `--skipped-csv`.

## Key files

- `min_tx_fee_scan.py`: scanner implementation and CLI.
- `README.md`: user-facing docs and runtime guidance.
- `COINSET_DOCS_AND_API.md`: endpoint notes and API conventions.
- `tests/test_min_tx_fee_scan.py`: unit tests for parsing, fee logic, and skip accounting.

## Coinset reference guidance

- Use `COINSET_DOCS_AND_API.md` as the first-stop reference when working on
  API-related changes.
- It is especially useful for endpoint coverage, required request fields,
  response-shape quirks, and known runtime pitfalls.
- Before changing request/parse behavior in `min_tx_fee_scan.py`, confirm the
  endpoint assumptions against that document.

## Common commands

- Run scanner:
  - `python3 min_tx_fee_scan.py --days 1 --output-csv min_tx_fee_blocks_1day.csv`
- Run tests:
  - `python3 -m unittest discover -s tests -p "test_*.py"`

## Conventions

- Prefer `python3` in commands.
- Keep README language user-facing and consistent with implementation.
- Keep policy wording precise: "zero-fee spend excludes the block."
- Avoid introducing third-party dependencies unless explicitly requested.
