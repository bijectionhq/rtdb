import argparse
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from run_mixed_iid_native import OBSERVATIONS, PATCH_SQL, normalize_rows, observations, run, verify_tape


def tape():
    ticks = []
    for tick in range(70):
        instant = 1_577_836_800_000_000 + tick * 21_600_000_000
        ticks.append({
            "tick": tick, "tx_id": tick + 1, "system_time_micros": instant,
            "put": {"ids": list(range(30)), "value": tick, "valid_from_micros": instant},
            "delete": {"ids": [0, 1, 2], "valid_from_micros": instant + 3_600_000_000} if tick % 10 == 0 else None,
        })
    return {"first_ticks": ticks[:40], "ticks": ticks[40:],
            "patch": {"tx_id": 71, "system_time_micros": 1_609_459_200_000_000, "sql": PATCH_SQL}}


class MixedIidNativeTests(unittest.TestCase):
    def test_ticks_and_patch_preserve_exact_actions(self):
        record = tape()
        verify_tape(record)
        record["ticks"][0]["delete"]["valid_from_micros"] -= 3_600_000_000
        with self.assertRaisesRegex(ValueError, "at tick 40"):
            verify_tape(record)
        record = tape()
        record["patch"]["system_time_micros"] += 1
        with self.assertRaisesRegex(ValueError, "PATCH differs"):
            verify_tape(record)

    def test_typed_comparison_preserves_order_and_signed_iid_bytes(self):
        signed = {"kind": "fixed-size-binary", "signed_bytes": [-128] + [0] * 15}
        integer = {"kind": "i64", "value": 7}
        timestamp = {"kind": "timestamp", "epoch_second": 1_577_836_800, "nanosecond": 1200, "zone": "UTC"}
        native = {name: [{"n": integer}] for name in OBSERVATIONS}
        native["history"] = [{"xt/valid-from": timestamp, "xt/system-to": {"kind": "null"}}]
        native["sorted-iids"] = [{"xt/id": integer, "xt/iid": signed},
                                 {"xt/id": {"kind": "i64", "value": 8}, "xt/iid": signed}]
        reference = {name: [normalize_rows(rows), normalize_rows(rows)] for name, rows in native.items()}
        for side in reference["history"]:
            side[0]["xt/valid-from"]["zone"] = "Z"
        execution = {"control": {"observations": native}, "mixed": {"observations": copy.deepcopy(native)}}
        self.assertTrue(all(item["equal"] for item in observations(execution, reference)))
        execution["mixed"]["observations"]["sorted-iids"].reverse()
        report = [item for item in observations(execution, reference) if not item["equal"]]
        self.assertEqual([("mixed", "sorted-iids"), ("control-vs-mixed", "sorted-iids")],
                         [(item["layout"], item["id"]) for item in report])
        self.assertEqual(0, report[0]["first_different_row"])

    def test_phase_gate_precedes_reference_reads_and_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "native"
            args = argparse.Namespace(phase_two=False, output=output)
            with patch("run_mixed_iid_native.read_reference") as reference, patch("run_mixed_iid_native.run_process") as process:
                with self.assertRaisesRegex(ValueError, "requires --phase-two"):
                    run(args)
                reference.assert_not_called()
                process.assert_not_called()
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
