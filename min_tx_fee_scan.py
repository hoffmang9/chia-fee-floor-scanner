#!/usr/bin/env python3
"""
Scan recent Chia blocks via Coinset API and report transaction fee floors.

This script performs a two-phase scan over a lookback window:
1) Candidate filtering with `/get_blocks` (transaction blocks with non-zero
   total block fee).
2) Spend-level inspection with `/get_block_spends_with_conditions`.

Current output metric:
- For each qualifying block, estimate a naive fee-per-transaction proxy as:
  block_total_fee / spend_count
- Here, spend_count is the number of `block_spends_with_conditions` entries in
  the block (a practical proxy, not exact spend-bundle count).

Per-spend fee estimator:
  coin_spend.coin.amount - sum(CREATE_COIN output amounts)

Coinset endpoints used:
- `/get_blockchain_state` (peak height and average block time)
- `/get_blocks` (candidate filtering)
- `/get_block_spends_with_conditions` (spend-level inspection)
- `/get_block_record_by_height` (header hash fallback)

Output CSV columns:
- block_height
- min_spend_fee_mojo
- spend_count
- block_total_fee_mojo
- estimated_fee_per_tx_mojo
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import math
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_BASE_URL = "https://api.coinset.org"
DEFAULT_DAYS = 1.0
DEFAULT_CHUNK_SIZE = 400
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_MAX_WORKERS = 8
DEFAULT_NEGATIVE_CACHE_PATH = ".cache/non_qualifying_blocks.json"
DEFAULT_MIN_ESTIMATED_FEE_PER_TX_MOJO = 1000
MOJO_PER_XCH = 1_000_000_000_000
PROGRESS_DOT_INTERVAL_SECONDS = 10.0


@dataclass
class CandidateBlock:
    height: int
    header_hash: str | None
    total_block_fee_mojo: int = 0


@dataclass
class BlockFeeRow:
    height: int
    min_spend_fee_mojo: int
    spend_count: int
    block_total_fee_mojo: int = 0
    estimated_fee_per_tx_mojo: float = 0.0


@dataclass
class SkippedBlockRow:
    height: int
    reason: str

class ProgressDotTicker:
    """Print a dot at start, then every interval seconds until stopped."""

    def __init__(self, interval_seconds: float = PROGRESS_DOT_INTERVAL_SECONDS) -> None:
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._printed_any = False

    def start(self) -> None:
        if self._thread is not None:
            return
        # First dot immediately at scan start.
        print(".", end="", flush=True)
        self._printed_any = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            print(".", end="", flush=True)
            self._printed_any = True

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop_event.set()
        self._thread.join()
        self._thread = None
        # Move summary output to a fresh line after progress dots.
        if self._printed_any:
            print()


def post_json(base_url: str, endpoint: str, payload: dict[str, Any], timeout: int) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    body = json.dumps(payload).encode("utf-8")
    # Cloudflare may reject generic clients. Keep explicit headers stable.
    req = urllib.request.Request(
        url=url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "min-fee-scanner/0.1 (+https://api.coinset.org)",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        message = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {message}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc}") from exc

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from {url}: {raw[:200]!r}") from exc

    if isinstance(data, dict) and data.get("success") is False:
        raise RuntimeError(f"API error from {url}: {data.get('error')}")
    return data


def iter_height_ranges(start: int, end: int, chunk_size: int) -> Iterable[tuple[int, int]]:
    """Yield half-open [start, end) ranges for chunked block queries."""
    current = start
    while current < end:
        chunk_end = min(current + chunk_size, end)
        yield current, chunk_end
        current = chunk_end


def iter_uncached_height_ranges(
    start: int,
    end: int,
    chunk_size: int,
    excluded_heights: set[int],
) -> Iterable[tuple[int, int]]:
    """Yield [start, end) ranges excluding explicitly cached heights."""
    if not excluded_heights:
        yield from iter_height_ranges(start, end, chunk_size)
        return

    excluded = sorted(h for h in excluded_heights if start <= h < end)
    current = start
    for height in excluded:
        if current < height:
            yield from iter_height_ranges(current, height, chunk_size)
        current = max(current, height + 1)
    if current < end:
        yield from iter_height_ranges(current, end, chunk_size)


def load_phase1_cache(cache_path: Path, base_url: str) -> dict[int, dict[str, Any]]:
    if not cache_path.exists():
        return {}
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    by_base_url = data.get("by_base_url", {}) if isinstance(data, dict) else {}
    base_entry = by_base_url.get(base_url, {}) if isinstance(by_base_url, dict) else {}
    by_height = base_entry.get("phase1_by_height", {})
    result: dict[int, dict[str, Any]] = {}
    if not isinstance(by_height, dict):
        return result
    for key, value in by_height.items():
        try:
            parsed = int(key)
        except (TypeError, ValueError):
            continue
        if parsed < 0 or not isinstance(value, dict):
            continue
        candidate = bool(value.get("candidate", False))
        header_hash = value.get("header_hash")
        if not isinstance(header_hash, str):
            header_hash = None
        try:
            total_fee = int(value.get("total_block_fee_mojo", 0))
        except (TypeError, ValueError):
            total_fee = 0
        result[parsed] = {
            "candidate": candidate,
            "header_hash": header_hash,
            "total_block_fee_mojo": total_fee,
        }
    return result


def save_phase1_cache(cache_path: Path, base_url: str, phase1_by_height: dict[int, dict[str, Any]]) -> None:
    data: dict[str, Any]
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                data = {}
        except (json.JSONDecodeError, OSError):
            data = {}
    else:
        data = {}

    by_base_url = data.get("by_base_url")
    if not isinstance(by_base_url, dict):
        by_base_url = {}
    data["by_base_url"] = by_base_url
    serialized: dict[str, Any] = {}
    for height, entry in phase1_by_height.items():
        if height < 0:
            continue
        serialized[str(int(height))] = {
            "candidate": bool(entry.get("candidate", False)),
            "header_hash": entry.get("header_hash") if isinstance(entry.get("header_hash"), str) else None,
            "total_block_fee_mojo": int(entry.get("total_block_fee_mojo", 0)),
        }
    by_base_url[base_url] = {
        "phase1_by_height": serialized,
    }

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def get_peak_and_avg_block_time(base_url: str, timeout: int) -> tuple[int, float]:
    data = post_json(base_url, "/get_blockchain_state", {}, timeout)
    state = data.get("blockchain_state", {})
    peak = state.get("peak", {})
    peak_height = int(peak.get("height", 0))
    avg = state.get("average_block_time")
    try:
        avg_block_time = float(avg)
    except (TypeError, ValueError):
        avg_block_time = 18.75  # Chia-ish fallback
    if avg_block_time <= 0:
        avg_block_time = 18.75
    return peak_height, avg_block_time


def block_is_transaction_block(block: dict[str, Any]) -> bool:
    if isinstance(block.get("is_transaction_block"), bool):
        return bool(block["is_transaction_block"])
    reward_chain = block.get("reward_chain_block", {})
    return bool(reward_chain.get("is_transaction_block", False))


def block_has_transactions(block: dict[str, Any]) -> bool:
    # Coinset's `get_blocks` returns `transactions_generator: null` when there
    # are no txs in that tx block.
    return block.get("transactions_generator") is not None


def block_total_fee(block: dict[str, Any]) -> int:
    tx_info = block.get("transactions_info", {}) or {}
    try:
        return int(tx_info.get("fees", 0))
    except (TypeError, ValueError):
        return 0


def block_height(block: dict[str, Any]) -> int:
    rcb = block.get("reward_chain_block", {}) or {}
    height = rcb.get("height")
    if height is None:
        height = block.get("height", 0)
    return int(height)


def block_header_hash(block: dict[str, Any]) -> str | None:
    header_hash = block.get("header_hash")
    if isinstance(header_hash, str) and header_hash:
        return header_hash
    return None


def resolve_header_hash_by_height(base_url: str, timeout: int, height: int) -> str:
    data = post_json(base_url, "/get_block_record_by_height", {"height": height}, timeout)
    block_record = data.get("block_record", {}) or {}
    header_hash = block_record.get("header_hash")
    if not isinstance(header_hash, str) or not header_hash:
        raise RuntimeError(f"Could not resolve header_hash for height {height}")
    return header_hash


def parse_mojo(value: Any) -> int:
    """Parse Coinset mojo values that may appear as int/decimal/hex strings."""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("", "0x"):
            return 0
        return int(text, 16) if text.startswith("0x") else int(text)
    raise ValueError(f"Unsupported mojo value type: {type(value)!r}")


def mojo_to_xch(mojo: int) -> float:
    return mojo / MOJO_PER_XCH


def format_xch(xch: float) -> str:
    return f"{xch:.12f}".rstrip("0").rstrip(".")


def format_days_label(days: float) -> str:
    if float(days).is_integer():
        return f"{int(days)}day"
    compact = f"{days:.6f}".rstrip("0").rstrip(".").replace(".", "p")
    return f"{compact}day"


def default_output_csv_for_days(days: float) -> str:
    return f"min_tx_fee_blocks_{format_days_label(days)}.csv"


def is_create_coin_opcode(opcode: Any) -> bool:
    if isinstance(opcode, int):
        return opcode == 51
    if isinstance(opcode, str):
        op = opcode.lower()
        return op == "0x33" or op == "51"
    return False


def spend_fee_mojo(spend_with_conditions: dict[str, Any]) -> int:
    """
    Estimate per-spend fee from CLVM conditions.

    Fee = input coin amount - sum(all CREATE_COIN amounts emitted by spend).
    """
    coin_spend = spend_with_conditions.get("coin_spend", {}) or {}
    coin = coin_spend.get("coin", {}) or {}
    input_amount = parse_mojo(coin.get("amount", 0))

    output_amount = 0
    for cond in spend_with_conditions.get("conditions", []):
        if not is_create_coin_opcode(cond.get("opcode")):
            continue
        vars_ = cond.get("vars", [])
        if not isinstance(vars_, list) or len(vars_) < 2:
            continue
        output_amount += parse_mojo(vars_[1])

    return input_amount - output_amount


def inspect_block_spends(
    base_url: str,
    timeout: int,
    candidate: CandidateBlock,
    min_spends_per_block: int | None = None,
    max_spends_per_block: int | None = None,
    min_estimated_fee_per_tx_mojo: int = DEFAULT_MIN_ESTIMATED_FEE_PER_TX_MOJO,
) -> BlockFeeRow | None:
    header_hash = candidate.header_hash or resolve_header_hash_by_height(
        base_url=base_url,
        timeout=timeout,
        height=candidate.height,
    )
    data = post_json(
        base_url,
        "/get_block_spends_with_conditions",
        {"header_hash": header_hash},
        timeout,
    )
    # "block_spends_with_conditions" is the transaction-like unit available
    # from Coinset for block-level deep inspection.
    spends = data.get("block_spends_with_conditions", [])
    if not isinstance(spends, list) or not spends:
        return None
    if min_spends_per_block is not None and len(spends) < min_spends_per_block:
        return None
    if max_spends_per_block is not None and len(spends) > max_spends_per_block:
        return None

    fees: list[int] = []
    positive_fees: list[int] = []
    for spend in spends:
        fee = spend_fee_mojo(spend)
        fees.append(fee)
        if fee > 0:
            positive_fees.append(fee)

    if not fees or not positive_fees:
        return None

    # Sanity check: aggregate spend deltas should reconstruct block-level fee.
    # If not, skip block rather than publishing inconsistent metrics.
    block_fee_from_spends = sum(fees)
    if block_fee_from_spends <= 0:
        return None
    if candidate.total_block_fee_mojo > 0 and block_fee_from_spends != candidate.total_block_fee_mojo:
        return None
    estimated_fee_per_tx_mojo = candidate.total_block_fee_mojo / len(spends)
    if estimated_fee_per_tx_mojo < min_estimated_fee_per_tx_mojo:
        return None

    return BlockFeeRow(
        height=candidate.height,
        min_spend_fee_mojo=min(positive_fees),
        spend_count=len(fees),
        block_total_fee_mojo=candidate.total_block_fee_mojo,
        estimated_fee_per_tx_mojo=estimated_fee_per_tx_mojo,
    )


def collect_candidate_blocks(
    base_url: str,
    days: float,
    chunk_size: int,
    timeout: int,
    sleep_between_chunks: float,
    phase1_cache_by_height: dict[int, dict[str, Any]] | None = None,
) -> tuple[list[CandidateBlock], int, int, dict[int, dict[str, Any]], int]:
    # Phase 1: broad scan by height, keep only likely fee-carrying tx blocks.
    peak_height, avg_block_time = get_peak_and_avg_block_time(base_url, timeout)
    lookback_blocks = max(1, math.ceil((days * 86400.0) / avg_block_time))
    start_height = max(0, peak_height - lookback_blocks)
    end_height_exclusive = peak_height + 1
    phase1_cache_by_height = dict(phase1_cache_by_height or {})
    cached_in_window = {
        h: v for h, v in phase1_cache_by_height.items() if start_height <= h < end_height_exclusive
    }
    cached_heights_in_window = set(cached_in_window.keys())

    candidates: list[CandidateBlock] = []
    for height, entry in sorted(cached_in_window.items()):
        if bool(entry.get("candidate", False)):
            candidates.append(
                CandidateBlock(
                    height=height,
                    header_hash=entry.get("header_hash") if isinstance(entry.get("header_hash"), str) else None,
                    total_block_fee_mojo=int(entry.get("total_block_fee_mojo", 0)),
                )
            )
    scanned = 0
    for start, end in iter_uncached_height_ranges(
        start_height,
        end_height_exclusive,
        chunk_size,
        cached_heights_in_window,
    ):
        payload = {
            "start": start,
            "end": end,
            "exclude_header_hash": False,
            "exclude_reorged": True,
        }
        data = post_json(base_url, "/get_blocks", payload, timeout)
        blocks = data.get("blocks", [])
        scanned += len(blocks)
        for block in blocks:
            height = block_height(block)
            if not block_is_transaction_block(block):
                phase1_cache_by_height[height] = {
                    "candidate": False,
                    "header_hash": None,
                    "total_block_fee_mojo": 0,
                }
                continue
            if not block_has_transactions(block):
                phase1_cache_by_height[height] = {
                    "candidate": False,
                    "header_hash": None,
                    "total_block_fee_mojo": 0,
                }
                continue
            fee = block_total_fee(block)
            if fee <= 0:
                phase1_cache_by_height[height] = {
                    "candidate": False,
                    "header_hash": None,
                    "total_block_fee_mojo": 0,
                }
                continue
            header_hash = block_header_hash(block)
            phase1_cache_by_height[height] = {
                "candidate": True,
                "header_hash": header_hash,
                "total_block_fee_mojo": fee,
            }
            candidates.append(
                CandidateBlock(
                    height=height,
                    header_hash=header_hash,
                    total_block_fee_mojo=fee,
                )
            )
        if sleep_between_chunks > 0:
            time.sleep(sleep_between_chunks)

    return (
        candidates,
        scanned,
        peak_height,
        phase1_cache_by_height,
        len(cached_heights_in_window),
    )


def classify_failure_reason(exc: Exception) -> str:
    text = str(exc).lower()
    if "network error" in text:
        return "network_error"
    if "http " in text:
        return "http_error"
    if "api error" in text:
        return "api_error"
    if "invalid json" in text:
        return "invalid_json"
    if "header_hash" in text:
        return "missing_header_hash"
    if isinstance(exc, ValueError):
        return "value_error"
    return "other_error"


def collect_block_fees(
    base_url: str,
    timeout: int,
    candidates: list[CandidateBlock],
    max_workers: int,
    min_spends_per_block: int | None = None,
    max_spends_per_block: int | None = None,
    min_estimated_fee_per_tx_mojo: int = DEFAULT_MIN_ESTIMATED_FEE_PER_TX_MOJO,
) -> tuple[list[BlockFeeRow], dict[str, int], list[SkippedBlockRow]]:
    # Phase 2: deep inspection for each candidate block in parallel.
    rows: list[BlockFeeRow] = []
    failure_counts: dict[str, int] = {}
    skipped_blocks: list[SkippedBlockRow] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures: dict[concurrent.futures.Future[BlockFeeRow | None], CandidateBlock] = {
            pool.submit(
                inspect_block_spends,
                base_url,
                timeout,
                candidate,
                min_spends_per_block,
                max_spends_per_block,
                min_estimated_fee_per_tx_mojo,
            ): candidate
            for candidate in candidates
        }
        for future in concurrent.futures.as_completed(futures):
            candidate = futures[future]
            try:
                row = future.result()
            except Exception as exc:
                # Keep long scans resilient to occasional malformed/failed blocks,
                # but account for what was skipped and why.
                reason = classify_failure_reason(exc)
                failure_counts[reason] = failure_counts.get(reason, 0) + 1
                skipped_blocks.append(SkippedBlockRow(height=candidate.height, reason=reason))
                continue
            if row is not None:
                rows.append(row)
            else:
                skipped_blocks.append(
                    SkippedBlockRow(height=candidate.height, reason="no_usable_spend_fee")
                )
    return rows, failure_counts, skipped_blocks


def write_csv(rows: list[BlockFeeRow], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "block_height",
                "min_spend_fee_mojo",
                "spend_count",
                "block_total_fee_mojo",
                "estimated_fee_per_tx_mojo",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.height,
                    row.min_spend_fee_mojo,
                    row.spend_count,
                    row.block_total_fee_mojo,
                    row.estimated_fee_per_tx_mojo,
                ]
            )


def write_skipped_csv(rows: list[SkippedBlockRow], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["block_height", "reason"])
        for row in rows:
            writer.writerow([row.height, row.reason])


def write_summary_json(summary: dict[str, Any], output_json: Path) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    with output_json.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
        f.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="min_tx_fee_scan.py",
        add_help=False,
        description=(
            "Scan recent Chia transaction blocks via Coinset and measure\n"
            "a naive fee-per-tx proxy among qualifying blocks.\n"
            "Excludes: non-tx blocks, tx blocks with no txs, and tx blocks with\n"
            "zero total block fee.\n"
            "Always writes a CSV file for the run.\n"
            "CSV columns: block_height, min_spend_fee_mojo, spend_count,\n"
            "block_total_fee_mojo, estimated_fee_per_tx_mojo."
        ),
        epilog=(
            "Examples:\n"
            "  python3 min_tx_fee_scan.py --days 1 --output-csv min_tx_fee_blocks_1day.csv\n"
            "  python3 min_tx_fee_scan.py --days 7 --chunk-size 200 --max-workers 4\n"
            "  python3 min_tx_fee_scan.py -help\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("-h", "--help", "-help", action="help", help="show this help message and exit")
    parser.add_argument(
        "--base-url",
        metavar="URL",
        default=DEFAULT_BASE_URL,
        help=f"Coinset API base URL. Default: {DEFAULT_BASE_URL}",
    )
    parser.add_argument(
        "--days",
        type=float,
        default=DEFAULT_DAYS,
        help=f"How many days back to scan. Default: {DEFAULT_DAYS}",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Block query chunk size for /get_blocks. Default: {DEFAULT_CHUNK_SIZE}",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"HTTP timeout in seconds. Default: {DEFAULT_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--sleep-between-chunks",
        type=float,
        default=0.0,
        help="Optional pause between chunk requests in seconds. Default: 0",
    )
    parser.add_argument(
        "--output-csv",
        metavar="PATH",
        default=None,
        help=(
            "Output CSV path.\n"
            "If omitted, defaults to min_tx_fee_blocks_<days>day.csv "
            "(e.g. min_tx_fee_blocks_1day.csv)."
        ),
    )
    parser.add_argument(
        "--skipped-csv",
        metavar="PATH",
        default=None,
        help=(
            "Optional CSV path for skipped block details.\n"
            "Columns: block_height, reason."
        ),
    )
    parser.add_argument(
        "--summary-json",
        metavar="PATH",
        default=None,
        help=(
            "Optional JSON path for run metadata and aggregate counters.\n"
            "Includes scan counts, skipped reasons, and minimum fee summary."
        ),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Parallel workers for per-block spend inspection. Default: {DEFAULT_MAX_WORKERS}",
    )
    parser.add_argument(
        "--min-estimated-fee-per-tx-mojo",
        type=int,
        default=DEFAULT_MIN_ESTIMATED_FEE_PER_TX_MOJO,
        help=(
            "Reject a block if calculated naive fee-per-tx is below this threshold.\n"
            "Default: 1000 mojos. Rationale: very low naive values can occur\n"
            "naturally and are weak evidence of an enforced fee floor."
        ),
    )
    parser.add_argument(
        "--min-spends-per-block",
        type=int,
        default=None,
        help=(
            "Optional lower bound for spends per block (used as temporary tx-count proxy).\n"
            "Default: none. Example: 1 spend"
        ),
    )
    parser.add_argument(
        "--max-spends-per-block",
        type=int,
        default=None,
        help=(
            "Optional upper bound for spends per block (used as temporary tx-count proxy).\n"
            "Default: none. Example: 3 spends"
        ),
    )
    parser.add_argument(
        "--negative-cache-path",
        metavar="PATH",
        default=DEFAULT_NEGATIVE_CACHE_PATH,
        help=(
            "Path to phase-1 cache by block height (candidate vs non-candidate).\n"
            "Default: "
            f"{DEFAULT_NEGATIVE_CACHE_PATH}"
        ),
    )
    parser.add_argument(
        "--no-negative-cache",
        action="store_true",
        help="Disable negative-cache reads/writes for this run.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.days <= 0:
        print("--days must be > 0", file=sys.stderr)
        return 2
    if args.chunk_size <= 0:
        print("--chunk-size must be > 0", file=sys.stderr)
        return 2
    if args.max_workers <= 0:
        print("--max-workers must be > 0", file=sys.stderr)
        return 2
    if args.min_estimated_fee_per_tx_mojo < 0:
        print("--min-estimated-fee-per-tx-mojo must be >= 0", file=sys.stderr)
        return 2
    if args.min_spends_per_block is not None and args.min_spends_per_block <= 0:
        print("--min-spends-per-block must be > 0", file=sys.stderr)
        return 2
    if args.max_spends_per_block is not None and args.max_spends_per_block <= 0:
        print("--max-spends-per-block must be > 0", file=sys.stderr)
        return 2
    if (
        args.min_spends_per_block is not None
        and args.max_spends_per_block is not None
        and args.min_spends_per_block > args.max_spends_per_block
    ):
        print("--min-spends-per-block cannot be greater than --max-spends-per-block", file=sys.stderr)
        return 2

    run_started_at = time.time()
    negative_cache_enabled = not args.no_negative_cache
    cache_path = Path(args.negative_cache_path)
    phase1_cache_by_height = (
        load_phase1_cache(cache_path, args.base_url) if negative_cache_enabled else {}
    )

    # Two-pass pipeline:
    # 1) Candidate filtering by block metadata.
    # 2) Spend-level inspection + strict exclusion rule.
    dot_ticker = ProgressDotTicker()
    dot_ticker.start()
    try:
        (
            candidates,
            scanned_count,
            peak_height,
            updated_phase1_cache_by_height,
            cached_heights_in_window_count,
        ) = collect_candidate_blocks(
            base_url=args.base_url,
            days=args.days,
            chunk_size=args.chunk_size,
            timeout=args.timeout,
            sleep_between_chunks=args.sleep_between_chunks,
            phase1_cache_by_height=phase1_cache_by_height if negative_cache_enabled else None,
        )
        newly_cached_phase1_count = 0
        if negative_cache_enabled:
            before_count = len(phase1_cache_by_height)
            phase1_cache_by_height = updated_phase1_cache_by_height
            newly_cached_phase1_count = max(0, len(phase1_cache_by_height) - before_count)
            save_phase1_cache(cache_path, args.base_url, phase1_cache_by_height)

        rows, failure_counts, skipped_blocks = collect_block_fees(
            base_url=args.base_url,
            timeout=args.timeout,
            candidates=candidates,
            max_workers=args.max_workers,
            min_spends_per_block=args.min_spends_per_block,
            max_spends_per_block=args.max_spends_per_block,
            min_estimated_fee_per_tx_mojo=args.min_estimated_fee_per_tx_mojo,
        )
    finally:
        dot_ticker.stop()

    rows.sort(key=lambda r: r.height)
    output_csv_value = args.output_csv or default_output_csv_for_days(args.days)
    output_csv = Path(output_csv_value)
    write_csv(rows, output_csv)
    if args.skipped_csv:
        write_skipped_csv(skipped_blocks, Path(args.skipped_csv))

    print("Run summary:")
    print(f"- Peak height: {peak_height}")
    print(f"- Scanned blocks: {scanned_count}")
    print(f"- Candidate tx blocks (non-zero block fee): {len(candidates)}")
    print(f"- Qualifying tx blocks: {len(rows)}")
    print(f"- Skipped candidate tx blocks: {len(skipped_blocks)}")
    if negative_cache_enabled:
        print(f"- Negative cache path: {cache_path}")
        print(f"- Cached heights in window (phase 1): {cached_heights_in_window_count}")
        print(f"- Newly cached heights (phase 1): {newly_cached_phase1_count}")
    if failure_counts:
        failure_parts = [f"{name}={count}" for name, count in sorted(failure_counts.items())]
        print(f"- Skipped due to phase-2 errors: {', '.join(failure_parts)}")
    print(f"- Wrote CSV: {output_csv}")
    if args.skipped_csv:
        print(f"- Wrote skipped CSV: {args.skipped_csv}")
    if rows:
        min_row = min(rows, key=lambda r: r.estimated_fee_per_tx_mojo)
        min_xch = min_row.estimated_fee_per_tx_mojo / MOJO_PER_XCH
        print(
            "- Lowest naive fee per tx proxy (block_total_fee/spend_count): "
            f"{min_row.estimated_fee_per_tx_mojo} mojo ({format_xch(min_xch)} XCH) "
            f"at height {min_row.height}"
        )
    else:
        min_row = None
        print("- No qualifying blocks found in the selected window.")

    if args.summary_json:
        skipped_reason_counts: dict[str, int] = {}
        for skipped in skipped_blocks:
            skipped_reason_counts[skipped.reason] = skipped_reason_counts.get(skipped.reason, 0) + 1
        summary = {
            "base_url": args.base_url,
            "days": args.days,
            "chunk_size": args.chunk_size,
            "timeout_seconds": args.timeout,
            "max_workers": args.max_workers,
            "min_estimated_fee_per_tx_mojo_threshold": args.min_estimated_fee_per_tx_mojo,
            "min_spends_per_block": args.min_spends_per_block,
            "max_spends_per_block": args.max_spends_per_block,
            "negative_cache_enabled": negative_cache_enabled,
            "negative_cache_path": str(cache_path),
            "cached_heights_in_window_phase1": (
                cached_heights_in_window_count if negative_cache_enabled else 0
            ),
            "newly_cached_heights_phase1": (
                newly_cached_phase1_count if negative_cache_enabled else 0
            ),
            "cached_heights_total_phase1": (
                len(phase1_cache_by_height) if negative_cache_enabled else 0
            ),
            "sleep_between_chunks_seconds": args.sleep_between_chunks,
            "peak_height": peak_height,
            "scanned_blocks": scanned_count,
            "candidate_tx_blocks_non_zero_fee": len(candidates),
            "qualifying_tx_blocks": len(rows),
            "skipped_candidate_blocks": len(skipped_blocks),
            "skipped_reasons": dict(sorted(skipped_reason_counts.items())),
            "output_csv": str(output_csv),
            "skipped_csv": args.skipped_csv,
            "elapsed_seconds": round(time.time() - run_started_at, 3),
        }
        if min_row is not None:
            summary["lowest_naive_fee_per_tx_proxy"] = {
                "block_height": min_row.height,
                "spend_count_proxy": min_row.spend_count,
                "block_total_fee_mojo": min_row.block_total_fee_mojo,
                "estimated_fee_per_tx_mojo": min_row.estimated_fee_per_tx_mojo,
                "estimated_fee_per_tx_xch": format_xch(
                    min_row.estimated_fee_per_tx_mojo / MOJO_PER_XCH
                ),
            }
        write_summary_json(summary, Path(args.summary_json))
        print(f"- Wrote summary JSON: {args.summary_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
