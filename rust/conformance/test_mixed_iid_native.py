import argparse
import copy
import json
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

from run_controls import REFERENCE
from run_history import verify_inputs
from run_mixed_iid_native import (
    FLIGHT_SQL, OBSERVATIONS, PATCH_SQL, PGWIRE_SQL, flight_values, normalize_rows, observations, pgwire_values,
    read_receipt_bytes, run, verify_tape, wire_observations,
)
from run_mixed_iid_reference import ASSERTIONS, SOURCE, UNIQUE_ID


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


def wire_results():
    iid = {"kind": "fixed-size-binary", "signed_bytes": [-128, -1] + [0] * 14}
    rows = [{"xt/id": {"kind": "i64", "value": 7}, "xt/iid": iid}]
    fields = [{"name": name, "type_oid": oid, "type_size": size, "format": 0}
              for name, oid, size in (("_id", 20, 8), ("_iid", 17, -1))]
    field = {"name": "_iid", "type": {"kind": "fixed-size-binary", "byte_width": 16},
             "nullable": False, "metadata": {}}
    schema = {"fields": [field], "metadata": {}}
    wire = {"pgwire": {"sql": PGWIRE_SQL, "rows": rows, "fields": fields, "command_tag": "SELECT 1"},
            "flight_sql": {"sql": FLIGHT_SQL, "declared": schema, "stream_schema": copy.deepcopy(schema),
                           "batches": [{"schema": copy.deepcopy(schema), "rows": 1} for _ in range(2)],
                           "iids": [iid, iid]}}
    reference = {"pgwire": [rows, rows], "pgwire-nonempty": [rows],
                 "flight-declared": [field["type"], field["type"]], "flight-batch-fields": [[field], [field], [field]],
                 "flight-iids": [[iid, iid], [iid, iid]],
                 "flight-multiple-batches": [{"kind": "i64", "value": 1}, {"kind": "i64", "value": 3}]}
    return copy.deepcopy(wire), copy.deepcopy(reference)


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

    def test_receipt_replacement_prevents_success(self):
        for changed, message in (
            ("reference", "history runner input changed"),
            ("comparison", "native mixed IID report changed after comparison"),
            ("native", "native mixed IID execution changed after comparison"),
        ):
            with self.subTest(changed=changed):
                self.assert_replaced_receipt_rejected(changed, message)

    def assert_replaced_receipt_rejected(self, changed, message):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            reference = root / "c84"
            reference_run = root / "reference-run"
            native = root / "native-binary"
            output = root / "output"
            source = reference / SOURCE
            source.parent.mkdir(parents=True)
            source.write_bytes(b"pinned fixture source")
            source_hash = sha256(source.read_bytes()).hexdigest()
            native.write_bytes(b"native executable")
            reference_run.mkdir()

            execution = {"assertions": [{"id": "history", "operands": [[{"kind": "i64", "value": 7}]]}]}
            receipts = {
                "execution.json": execution,
                "runtime-artifacts.json": {},
                "inputs.json": {"reference_revision": REFERENCE, "original_case": UNIQUE_ID,
                                "inputs": [{"path": str(source), "sha256": source_hash}]},
                "process.json": {"completed": True, "exit_code": 0},
                "stdout.log": "reference stdout",
                "stderr.log": "reference stderr",
            }
            hashes = {}
            for name, record in receipts.items():
                payload = json.dumps(record).encode()
                (reference_run / name).write_bytes(payload)
                hashes[name] = sha256(payload).hexdigest()
            result_path = reference_run / "result.json"
            result_payload = json.dumps({"reference_observed": True, "complete_parity": False, "case": UNIQUE_ID,
                                         "assertions": len(ASSERTIONS), "sha256": hashes}).encode()
            result_path.write_bytes(result_payload)
            result_hash = sha256(result_payload).hexdigest()

            def check_reference_artifacts(_):
                if changed == "reference":
                    (reference_run / "execution.json").write_bytes(b'{"assertions": []}')
                    result_path.write_bytes(b"{}")

            def verify_then_replace(inputs):
                verify_inputs(inputs)
                if changed == "comparison":
                    (output / "comparison.json").write_bytes(b'{"observations": []}')
                if changed == "native":
                    (output / "native" / "execution.json").write_bytes(b'{"changed": true}')

            def complete_native(command, _cwd, stdout, _stderr, _timeout):
                command[-1].mkdir()
                (command[-1] / "execution.json").write_bytes(b"{}")
                stdout.write_bytes(b"{}")
                return {"completed": True, "exit_code": 0}

            args = argparse.Namespace(phase_two=True, reference=reference, reference_run=reference_run,
                                      native=native, output=output, timeout=1)
            with (patch("run_mixed_iid_native.require_phase_two"),
                  patch("run_mixed_iid_native.git", return_value="revision"),
                  patch("run_mixed_iid_native.require_revision"),
                  patch("run_mixed_iid_native.SOURCE_SHA256", source_hash),
                  patch("run_mixed_iid_native.verify_runtime_artifacts", side_effect=check_reference_artifacts),
                  patch("run_mixed_iid_native.verify_execution") as verify_reference,
                  patch("run_mixed_iid_native.run_process", side_effect=complete_native),
                  patch("run_mixed_iid_native.verify_native"),
                  patch("run_mixed_iid_native.observations", return_value=[{"equal": True}]) as compare,
                  patch("run_mixed_iid_native.wire_observations", return_value=[{"equal": True}]),
                  patch("run_mixed_iid_native.verify_inputs", side_effect=verify_then_replace)):
                with self.assertRaisesRegex(ValueError, message):
                    run(args)

            verify_reference.assert_called_once_with(execution, reference_run / "workspace")
            compare.assert_called_once_with({}, {"history": execution["assertions"][0]["operands"]})
            inputs = json.loads((output / "inputs.json").read_bytes())["inputs"]
            captured = {item["path"]: item["sha256"] for item in inputs}
            self.assertEqual(result_hash, captured[str(result_path)])
            self.assertEqual(hashes["execution.json"], captured[str(reference_run / "execution.json")])
            if changed != "comparison":
                comparison = json.loads((output / "comparison.json").read_bytes())
                self.assertEqual(result_hash, comparison["reference_result_sha256"])
            self.assertFalse((output / "result.json").exists())

    def test_json_receipt_read_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "result.json"
            receipt.write_bytes(b"{}\n")
            with patch("run_mixed_iid_native.MAX_RECEIPT_BYTES", 2):
                with self.assertRaisesRegex(ValueError, "receipt file exceeds its size bound"):
                    read_receipt_bytes(receipt)

    def test_wire_checks_keep_original_values_and_multiple_mixed_batches(self):
        wire, reference = wire_results()
        execution = {"control": {"wire": wire}, "mixed": {"wire": copy.deepcopy(wire)}}
        report = wire_observations(execution, reference)
        self.assertEqual(10, len(report))
        self.assertTrue(all(item["equal"] for item in report))
        self.assertEqual(("flight-multiple-batches", 2, 3),
                         (report[-1]["id"], report[-1]["native_batches"], report[-1]["reference_batches"]))
        mixed = execution["mixed"]["wire"]["flight_sql"]
        mixed["batches"] = [{"schema": mixed["stream_schema"], "rows": 2}]
        self.assertFalse(wire_observations(execution, reference)[-1]["equal"])

    def test_pgwire_requires_iid_row_description_type_even_when_rows_agree(self):
        wire, _ = wire_results()
        pgwire = wire["pgwire"]
        pgwire_values(pgwire)
        pgwire["fields"][1]["type_oid"] = 25
        with self.assertRaisesRegex(ValueError, "RowDescription field"):
            pgwire_values(pgwire)

    def test_flight_rejects_changed_later_batch_field_and_equal_values_with_wrong_width(self):
        wire, _ = wire_results()
        flight = wire["flight_sql"]
        flight_values(flight)
        flight["batches"][1]["schema"]["fields"][0]["nullable"] = True
        with self.assertRaisesRegex(ValueError, "batch field differs"):
            flight_values(flight)
        wire, _ = wire_results()
        flight = wire["flight_sql"]
        for schema in [flight["declared"], flight["stream_schema"], *(batch["schema"] for batch in flight["batches"])]:
            schema["fields"][0]["type"]["byte_width"] = 15
        with self.assertRaisesRegex(ValueError, r"fixed-size binary\(16\)"):
            flight_values(flight)


if __name__ == "__main__":
    unittest.main()
