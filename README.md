# Min Fee Scan (Coinset / Chia)

This project scans recent Chia transaction blocks via Coinset and measures a
naive per-transaction fee proxy from observed chain data.

Current approach:
- Ignore non-transaction blocks.
- Ignore tx blocks with no transactions.
- Ignore tx blocks with zero total block fee.
- Inspect all spends in each remaining block.
- Reject a block if calculated naive fee-per-tx is below a configured threshold
  (default `1000` mojos).
- Use spend count as a transaction-count proxy.
- Report `estimated_fee_per_tx_mojo = block_total_fee_mojo / spend_count`.

## Why this exists

Some pools/operators appear to enforce fee minimums before including
transactions. This scanner provides empirical evidence from observed on-chain
transaction blocks.

## Requirements

- Python `3.10+`
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
4. Count spends in the block and compute naive fee-per-tx proxy as
   `block_total_fee / spend_count`.
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
- `--min-estimated-fee-per-tx-mojo`: `1000`
- `--output-csv`: auto-generated when omitted
- `--skipped-csv`: disabled by default
- `--summary-json`: disabled by default
- `--negative-cache-path`: `.cache/non_qualifying_blocks.json`
- `--no-negative-cache`: disabled by default

If `--output-csv` is omitted, output defaults to
`min_tx_fee_blocks_<days>day.csv`, for example:
- `--days 1` -> `min_tx_fee_blocks_1day.csv`
- `--days 0.5` -> `min_tx_fee_blocks_0p5day.csv`

## Output format

CSV columns:
- `block_height`: transaction block height
- `min_spend_fee_mojo`: minimum positive spend-delta in that block
- `spend_count`: number of spends inspected in that block (tx-count proxy)
- `block_total_fee_mojo`: block-level total fee from `transactions_info.fees`
- `estimated_fee_per_tx_mojo`: naive fee proxy (`block_total_fee_mojo / spend_count`)

Example row:

```csv
8386889,17230,3,535299410,41176877.692307696
```

Interpretation:
- Each row is one qualifying block.
- Lower `estimated_fee_per_tx_mojo` values indicate lower observed naive
  fee-per-tx proxy in the selected scan window.

Console output also prints:
- peak height
- scanned block count
- candidate tx block count
- qualifying tx block count
- skipped candidate tx block count
- phase-2 error breakdown (when present)
- lowest observed `estimated_fee_per_tx_mojo` in the run window

## Fee calculation detail

Per-spend delta is estimated as:

`coin_spend.coin.amount - sum(CREATE_COIN amounts)`

This is derived from `block_spends_with_conditions`. In this API, spend entries
are coin spends, not exact spend-bundle transaction boundaries.

## Notes and caveats

- A fixed `User-Agent` is sent because generic clients may be blocked.
- Some Coinset block payloads omit `header_hash`; the script resolves it by
  `height` via `get_block_record_by_height`.
- Deep inspection is fail-soft: if a block cannot be parsed/fetched in phase 2,
  it is skipped so long scans can complete.
- Repeated negative spend deltas (for example `-875000000000`) are often paired
  with corresponding positive deltas in the same block. This reflects
  spend-level decomposition artifacts, not negative transaction fees.
- `estimated_fee_per_tx_mojo` is intentionally naive because `spend_count` is a
  proxy, not a true spend-bundle transaction count.
- Without historical mempool data, these results are directional rather than
  definitive. Once mempool history is available, this tool should be upgraded
  to use that stronger evidence source.
- This is observational tooling, not a consensus or protocol guarantee.

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
- `--min-estimated-fee-per-tx-mojo`
- `--timeout`
- `--sleep-between-chunks`
- `--output-csv`
- `--skipped-csv`
- `--summary-json`
- `--negative-cache-path`
- `--no-negative-cache`
- `--min-spends-per-block`
- `--max-spends-per-block`
- `--base-url`

If `--skipped-csv` is provided, a second CSV is written with:
- `block_height`
- `reason` (`no_usable_spend_fee`, `network_error`, `http_error`, etc.)

If `--summary-json` is provided, a JSON file is written with run metadata,
aggregate counters, skipped reason counts, and minimum fee summary.

Negative cache behavior:
- The scanner caches phase-1 classification by block height for this API base
  URL (candidate vs non-candidate, with block fee/header metadata).
- Future runs skip re-querying cached heights for candidate discovery and only
  fetch uncached heights in the selected window.
- Phase-2 errors are not cached as terminal outcomes, so they are retried on
  subsequent runs.

Threshold rationale:
- The default minimum is `1000` mojos for naive fee-per-tx filtering.
- Very low naive values can occur naturally (for example, 3 mojos over 3
  spends), so they are treated as weak evidence of an enforced fee floor and
  filtered out by default.

## Current 7-day benchmark (observed)

Date of run:
- Local: `Mon Mar 2 22:35:40 PST 2026`
- UTC: `Tue Mar 3 06:35:40 UTC 2026`

Host and network context:
- `MacBook Air (M4, 2025)` on `Wi-Fi 5`
- Internet service: `1 Gb fiber`
- API target: `https://api.coinset.org`

Quick Speedtest snapshot for this host/network:
- Ping / jitter: `7 ms` / `5 ms`
- Download: `356.83 Mbps`
- Upload: `265.37 Mbps`
- This is a point-in-time measurement and can vary with Wi-Fi conditions.

Command used (explicitly skipping the cache):

```bash
/usr/bin/time -p python3 min_tx_fee_scan.py \
  --days 7 \
  --no-negative-cache \
  --output-csv min_tx_fee_blocks_7day_nocache.csv \
  --skipped-csv min_tx_fee_blocks_7day_nocache_skipped.csv \
  --summary-json min_tx_fee_blocks_7day_nocache_summary.json
```

Observed wall-clock and CPU timing:
- `real`: `263.74s` (~4m24s)
- `user`: `89.28s`
- `sys`: `11.49s`

Observed run summary:
- Peak height: `8392263`
- Scanned blocks: `35578`
- Candidate tx blocks (non-zero block fee): `8862`
- Qualifying tx blocks: `8518`
- Skipped candidate blocks: `344`
- Skipped reasons: `network_error=13`, `no_usable_spend_fee=331`
- Lowest naive fee-per-tx proxy: `1000.0` mojo at height `8365490`

This benchmark is an observation, not a guarantee. Runtime and counts vary with
Coinset responsiveness, current chain activity, and local network conditions.

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
