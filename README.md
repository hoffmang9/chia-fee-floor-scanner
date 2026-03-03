# Min Fee Scan (Coinset / Chia)

This project scans recent Chia transaction blocks via Coinset and measures a
practical per-spend fee floor from observed chain data.

The implemented policy is intentionally strict:
- Ignore non-transaction blocks.
- Ignore tx blocks with no transactions.
- Ignore tx blocks with zero total block fee.
- Inspect all spends in each remaining block.
- If any spend in a block has a zero fee, exclude the whole block.
- For accepted blocks, record the minimum spend fee in that block.

## Why this exists

Some pools/operators appear to enforce fee minimums before including
transactions. This scanner provides empirical evidence from observed on-chain
transaction blocks.

## Requirements

- Python `3.10+` (tested with Python `3.11`)
- No third-party packages (standard library only)
- Use `python3` to run the script (not `python`, unless your system maps it to
  Python 3).

## Setup

```bash
python3 --version
```

## Methodology (2-phase pipeline)

1. Estimate lookback height range from `--days` using current average block
   time.
2. Query `/get_blocks` in chunks and keep candidate tx blocks with non-zero
   total block fees.
3. For each candidate block, inspect all spends via
   `/get_block_spends_with_conditions`.
4. Exclude blocks containing any spend with a zero fee.
5. Write one CSV row per qualifying block.

Coinset endpoints used:
- `POST /get_blockchain_state` (peak height, average block time)
- `POST /get_blocks` (candidate filtering)
- `POST /get_block_spends_with_conditions` (spend-level inspection)
- `POST /get_block_record_by_height` (header hash fallback when needed)

## Run recipes

### 1 day (quick test)

```bash
python3 min_tx_fee_scan.py --days 1 --output-csv min_tx_fee_blocks_1day.csv
```

### 7 days (recommended first real sample)

```bash
python3 min_tx_fee_scan.py --days 7 --output-csv min_tx_fee_blocks_7day.csv
```

### Slower but gentler on API

```bash
python3 min_tx_fee_scan.py \
  --days 7 \
  --chunk-size 200 \
  --max-workers 4 \
  --sleep-between-chunks 0.1 \
  --output-csv min_tx_fee_blocks_7day_slow.csv
```

## Expected runtime (observed)

Observed on this repo on 2026-03-02 using default settings
(`--chunk-size 400`, `--max-workers 8`, `--sleep-between-chunks 0`):

- `--days 1`: about `73s` (`real 73.21`)
- `--days 7`: about `192s` (`real 191.50`)

Actual runtime varies with Coinset/API responsiveness, local network conditions,
and the number of candidate blocks in the selected window.

## Defaults

- `--base-url`: `https://api.coinset.org`
- `--days`: `1`
- `--chunk-size`: `400`
- `--timeout`: `30` seconds
- `--sleep-between-chunks`: `0`
- `--max-workers`: `8`
- `--output-csv`: auto-generated when omitted
- `--skipped-csv`: disabled by default
- `--summary-json`: disabled by default

If `--output-csv` is omitted, output defaults to
`min_tx_fee_blocks_<days>day.csv`, for example:
- `--days 1` -> `min_tx_fee_blocks_1day.csv`
- `--days 0.5` -> `min_tx_fee_blocks_0p5day.csv`

## Output format

CSV columns:
- `block_height`: transaction block height
- `min_spend_fee_mojo`: minimum per-spend fee seen in that block
- `spend_count`: number of spends inspected in that block

Example row:

```csv
8386889,17230,3
```

Interpretation:
- Each row is one qualifying block.
- Lower `min_spend_fee_mojo` values are stronger fee-floor candidates in the
  selected scan window.

Console output also prints:
- peak height
- scanned block count
- candidate tx block count
- qualifying tx block count
- skipped candidate tx block count
- phase-2 error breakdown (when present)
- lowest observed `min_spend_fee_mojo` in the run window

## Fee calculation detail

Per-spend fee is estimated as:

`coin_spend.coin.amount - sum(CREATE_COIN amounts)`

This is derived from `block_spends_with_conditions`. In this API, spend entries
are the practical transaction-like unit available for block-level fee analysis.

## Notes and caveats

- The script sends a fixed `User-Agent` because generic clients may be blocked.
- Some Coinset block payloads may omit `header_hash`; the script resolves it by
  `height` via `get_block_record_by_height`.
- Current behavior is fail-soft during deep inspection: if a block cannot be
  parsed/fetched in phase 2, it is skipped so long scans can complete.
- Because of strict exclusions (any zero-fee spend excludes the entire block),
  this
  method may report a higher floor than permissive methods.
- This is observational tooling, not a consensus rule or protocol guarantee.

## Troubleshooting

- HTTP/network errors: retry, reduce `--max-workers`, and/or add
  `--sleep-between-chunks`.
- Timeouts: increase `--timeout`.
- Very few/no qualifying rows: increase `--days` and rerun.

## Useful options

```bash
python3 min_tx_fee_scan.py --help
```

Key flags:
- `--days`
- `--chunk-size`
- `--max-workers`
- `--timeout`
- `--sleep-between-chunks`
- `--output-csv`
- `--skipped-csv`
- `--base-url`

If `--skipped-csv` is provided, a second CSV is written with:
- `block_height`
- `reason` (`zero_fee_spend`, `network_error`, `http_error`, etc.)

If `--summary-json` is provided, a JSON file is written with run metadata,
aggregate counters, skipped reason counts, and minimum fee summary.

## Tests

Run the test suite:

```bash
python3 -m unittest discover -s tests -p "test_*.py"
```

Fixture-based regression tests live under `tests/fixtures/` and validate parsing
and candidate filtering behavior against representative Coinset response shapes.

## See also

- `COINSET_DOCS_AND_API.md` for endpoint details and integration notes.

## License

Apache License 2.0. See `LICENSE`.
