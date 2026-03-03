import unittest
from unittest.mock import patch

import min_tx_fee_scan as scanner


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


if __name__ == "__main__":
    unittest.main()
