import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import min_tx_fee_scan as scanner

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict:
    with (FIXTURES_DIR / name).open("r", encoding="utf-8") as f:
        return json.load(f)


class ParseMojoTests(unittest.TestCase):
    def test_parse_mojo_handles_int_decimal_hex_and_zero_like(self) -> None:
        self.assertEqual(scanner.parse_mojo(123), 123)
        self.assertEqual(scanner.parse_mojo("456"), 456)
        self.assertEqual(scanner.parse_mojo("0x10"), 16)
        self.assertEqual(scanner.parse_mojo("0x"), 0)
        self.assertEqual(scanner.parse_mojo(""), 0)

    def test_parse_mojo_raises_on_unsupported_type(self) -> None:
        with self.assertRaises(ValueError):
            scanner.parse_mojo(1.5)


class OpcodeAndFeeTests(unittest.TestCase):
    def test_is_create_coin_opcode(self) -> None:
        self.assertTrue(scanner.is_create_coin_opcode(51))
        self.assertTrue(scanner.is_create_coin_opcode("51"))
        self.assertTrue(scanner.is_create_coin_opcode("0x33"))
        self.assertFalse(scanner.is_create_coin_opcode("0x34"))

    def test_spend_fee_mojo(self) -> None:
        spend = {
            "coin_spend": {"coin": {"amount": "0x64"}},  # 100
            "conditions": [
                {"opcode": 51, "vars": ["0xabc", 30]},
                {"opcode": "0x33", "vars": ["0xdef", "0x14"]},  # 20
            ],
        }
        self.assertEqual(scanner.spend_fee_mojo(spend), 50)


class InspectBlockTests(unittest.TestCase):
    @patch("min_tx_fee_scan.post_json")
    def test_inspect_block_spends_excludes_zero_fee_block(self, mock_post_json) -> None:
        mock_post_json.return_value = {
            "block_spends_with_conditions": [
                {
                    "coin_spend": {"coin": {"amount": 100}},
                    "conditions": [{"opcode": 51, "vars": ["0xabc", 100]}],
                }
            ]
        }
        candidate = scanner.CandidateBlock(height=123, header_hash="0xabc")
        self.assertIsNone(
            scanner.inspect_block_spends(
                base_url="https://api.coinset.org",
                timeout=30,
                candidate=candidate,
            )
        )

    @patch("min_tx_fee_scan.post_json")
    def test_inspect_block_spends_returns_min_fee_and_count(self, mock_post_json) -> None:
        mock_post_json.return_value = {
            "block_spends_with_conditions": [
                {
                    "coin_spend": {"coin": {"amount": 100}},
                    "conditions": [{"opcode": 51, "vars": ["0xabc", 80]}],  # fee 20
                },
                {
                    "coin_spend": {"coin": {"amount": 120}},
                    "conditions": [{"opcode": 51, "vars": ["0xdef", 90]}],  # fee 30
                },
            ]
        }
        candidate = scanner.CandidateBlock(height=124, header_hash="0xdef")
        row = scanner.inspect_block_spends(
            base_url="https://api.coinset.org",
            timeout=30,
            candidate=candidate,
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.height, 124)
        self.assertEqual(row.min_spend_fee_mojo, 20)
        self.assertEqual(row.spend_count, 2)


class CollectBlockFeesTests(unittest.TestCase):
    @patch("min_tx_fee_scan.inspect_block_spends")
    def test_collect_block_fees_tracks_failures_and_skips(self, mock_inspect) -> None:
        candidates = [
            scanner.CandidateBlock(height=1, header_hash="0x1"),
            scanner.CandidateBlock(height=2, header_hash="0x2"),
            scanner.CandidateBlock(height=3, header_hash="0x3"),
        ]

        mock_inspect.side_effect = [
            scanner.BlockFeeRow(height=1, min_spend_fee_mojo=10, spend_count=1),
            RuntimeError("Network error for url: timeout"),
            None,
        ]

        rows, failure_counts, skipped = scanner.collect_block_fees(
            base_url="https://api.coinset.org",
            timeout=30,
            candidates=candidates,
            max_workers=1,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].height, 1)
        self.assertEqual(failure_counts.get("network_error"), 1)
        self.assertEqual(len(skipped), 2)
        reasons = sorted(item.reason for item in skipped)
        self.assertEqual(reasons, ["network_error", "zero_fee_spend"])


class FixtureRegressionTests(unittest.TestCase):
    @patch("min_tx_fee_scan.get_peak_and_avg_block_time")
    @patch("min_tx_fee_scan.post_json")
    def test_collect_candidate_blocks_from_fixture(self, mock_post_json, mock_peak) -> None:
        mock_peak.return_value = (1003, 20.0)
        mock_post_json.return_value = load_fixture("get_blocks_response.json")

        candidates, scanned, peak_height = scanner.collect_candidate_blocks(
            base_url="https://api.coinset.org",
            days=0.0001,  # 1 block lookback with avg block time 20s
            chunk_size=100,
            timeout=30,
            sleep_between_chunks=0.0,
        )

        self.assertEqual(peak_height, 1003)
        self.assertEqual(scanned, 4)
        self.assertEqual([c.height for c in candidates], [1002, 1003])
        self.assertEqual([c.header_hash for c in candidates], ["0xheader1002", "0xheader1003"])

    @patch("min_tx_fee_scan.post_json")
    def test_inspect_block_spends_from_positive_fixture(self, mock_post_json) -> None:
        mock_post_json.return_value = load_fixture("block_spends_positive.json")
        candidate = scanner.CandidateBlock(height=1002, header_hash="0xheader1002")

        row = scanner.inspect_block_spends(
            base_url="https://api.coinset.org",
            timeout=30,
            candidate=candidate,
        )

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.min_spend_fee_mojo, 30)
        self.assertEqual(row.spend_count, 2)

    @patch("min_tx_fee_scan.post_json")
    def test_inspect_block_spends_from_zero_fee_fixture(self, mock_post_json) -> None:
        mock_post_json.return_value = load_fixture("block_spends_zero_fee.json")
        candidate = scanner.CandidateBlock(height=1001, header_hash="0xheader1001")

        row = scanner.inspect_block_spends(
            base_url="https://api.coinset.org",
            timeout=30,
            candidate=candidate,
        )
        self.assertIsNone(row)

    def test_write_summary_json(self) -> None:
        summary = {
            "qualifying_tx_blocks": 2,
            "skipped_reasons": {"zero_fee_spend": 1},
            "lowest_per_spend_fee": {"min_spend_fee_mojo": 10000},
        }
        with TemporaryDirectory() as tmp_dir:
            out = Path(tmp_dir) / "run_summary.json"
            scanner.write_summary_json(summary, out)
            loaded = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(loaded["qualifying_tx_blocks"], 2)
        self.assertEqual(loaded["skipped_reasons"]["zero_fee_spend"], 1)
        self.assertEqual(loaded["lowest_per_spend_fee"]["min_spend_fee_mojo"], 10000)


if __name__ == "__main__":
    unittest.main()
