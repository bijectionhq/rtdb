import argparse
import copy
import io
import json
import struct
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from run_parser_wire import (
    FLIGHT_CALLS, FOLLOW_UP, SQL, compare, fields_exact, main, pg_error, read_receipt, run, validate_execution_header,
    verify_jvm_runtime,
)


def frame(tag, data):
    return {"tag": tag, "bytes": list(data)}


def sample_execution():
    pg_errors, flight_errors = {}, {}
    for identity, sql in SQL.items():
        failure = "mismatched input 'SELEC'" if identity == "statement_mismatch" else "extraneous input 'extra'"
        message = f"Errors parsing SQL statement:\n  - line 1:0 {failure} expecting SELECT"
        detail = json.dumps({"category": "incorrect", "code": "xtdb/sql-error", "message": message}).encode()
        pg_body = b"SERROR\0VERROR\0C08P01\0M" + message.encode() + b"\0D" + detail + b"\0\0"
        pg_errors[identity] = {"sql": sql, "frames": [frame("E", pg_body), frame("Z", b"I")]}
        flight_errors[identity] = {"sql": sql, "status": "INVALID_ARGUMENT", "message": message, "metadata_bytes": list(detail)}
    pg_follow_up = {"sql": FOLLOW_UP, "frames": [
        frame("T", b"\0\1answer\0" + struct.pack(">IhIhih", 0, 0, 20, 8, -1, 0)),
        frame("D", b"\0\1\0\0\0\x0242"), frame("C", b"SELECT 1\0"), frame("Z", b"I"),
    ]}
    schema = {"fields": [{"name": "answer", "type": "i64", "nullable": False, "metadata": {}}], "metadata": {}}
    flight_follow_up = {"sql": FOLLOW_UP, "declared_schema": copy.deepcopy(schema), "stream_schema": copy.deepcopy(schema),
                        "batches": [{"schema": copy.deepcopy(schema), "rows": [[{"kind": "i64", "value": 42}]]}]}
    return {"observations": {
        "pgwire": {"errors": pg_errors, "follow_up": pg_follow_up},
        "flight_sql": {"errors": flight_errors, "follow_up": flight_follow_up,
                       "session": {"options_applied": True, "cookie_established": True, "calls": list(FLIGHT_CALLS)}},
    }}


class ParserWireTests(unittest.TestCase):
    def test_matching_wire_fields_and_payloads_keep_separate_source_identities(self):
        reference = sample_execution()
        native = copy.deepcopy(reference)
        for error in native["observations"]["flight_sql"]["errors"].values():
            error["status"] = "InvalidArgument"
        report = compare(reference, native)
        self.assertTrue(report["diagnostic_exact_match"])
        self.assertTrue(report["flight_follow_up"]["exact_match"])
        self.assertEqual(4, len(report["comparisons"]))
        self.assertEqual([415, 44, 415, 44], [row["source"]["line"] for row in report["comparisons"]])

    def test_flight_message_and_metadata_differences_are_retained_as_a_completed_mismatch(self):
        reference = sample_execution()
        native = copy.deepcopy(reference)
        error = native["observations"]["flight_sql"]["errors"]["statement_mismatch"]
        detail = json.loads(bytes(error["metadata_bytes"]))
        detail.update(message="sql parser error: Expected: an SQL statement, found: SELEC at Line: 1, Column: 1",
                      data={"span": {"start": 0, "end": 5}})
        error.update(message=detail["message"], metadata_bytes=list(json.dumps(detail).encode()))
        report = compare(reference, native)
        self.assertFalse(report["diagnostic_exact_match"])
        difference, = [row for row in report["comparisons"] if not row["exact_match"]]
        self.assertEqual(("flight_sql", "statement_mismatch"), (difference["protocol"], difference["id"]))
        self.assertEqual(["message", "detail", "payload"], difference["different_fields"])
        self.assertEqual(detail, difference["native"]["detail"])

    def test_postgresql_requires_unique_fields_and_idle_ready_frame(self):
        sql = SQL["statement_mismatch"]
        error = sample_execution()["observations"]["pgwire"]["errors"]["statement_mismatch"]
        error["frames"][1]["bytes"] = [ord("T")]
        with self.assertRaisesRegex(ValueError, "return idle"):
            pg_error(error, sql)
        error["frames"][1]["bytes"] = [ord("I")]
        error["frames"][0]["bytes"] = list(b"C08P01\0") + error["frames"][0]["bytes"]
        with self.assertRaisesRegex(ValueError, "omitted required diagnostics"):
            pg_error(error, sql)

    def test_missing_flight_wire_metadata_and_wrong_follow_up_fail_closed(self):
        observed = sample_execution()
        observed["observations"]["flight_sql"]["follow_up"]["batches"][0]["rows"][0][0]["value"] = 43
        with self.assertRaisesRegex(ValueError, "follow-up"):
            compare(sample_execution(), observed)

        observed = sample_execution()
        observed["observations"]["flight_sql"]["errors"]["unwanted_token"]["metadata_bytes"] = []
        with self.assertRaises(ValueError):
            compare(sample_execution(), observed)

    def test_rejects_malformed_flight_session_and_follow_up_schema_shapes(self):
        for key, value in (("options_applied", 1), ("cookie_established", False), ("calls", FLIGHT_CALLS[:-1])):
            with self.subTest(key=key):
                observed = sample_execution()
                observed["observations"]["flight_sql"]["session"][key] = value
                with self.assertRaisesRegex(ValueError, "establish and retain one session"):
                    compare(sample_execution(), observed)

        observed = sample_execution()
        field = observed["observations"]["flight_sql"]["follow_up"]["batches"][0]["schema"]["fields"][0]
        field["nullable"] = True
        with self.assertRaisesRegex(ValueError, "schema differs"):
            compare(sample_execution(), observed)

    def test_valid_flight_schema_difference_is_retained_and_reported(self):
        reference, native = sample_execution(), sample_execution()
        follow_up = native["observations"]["flight_sql"]["follow_up"]
        for schema in (follow_up["declared_schema"], follow_up["stream_schema"], follow_up["batches"][0]["schema"]):
            schema["fields"][0]["nullable"] = True
        report = compare(reference, native)
        self.assertTrue(report["diagnostic_exact_match"])
        self.assertFalse(report["flight_follow_up"]["exact_match"])
        self.assertTrue(report["flight_follow_up"]["native"]["stream_schema"]["fields"][0]["nullable"])

    def test_extra_token_must_reach_the_parser_branch(self):
        reference, native = sample_execution(), sample_execution()
        error = native["observations"]["flight_sql"]["errors"]["unwanted_token"]
        detail = json.loads(bytes(error["metadata_bytes"]))
        detail["message"] = "Errors parsing SQL statement: an unrelated error"
        error.update(message=detail["message"], metadata_bytes=list(json.dumps(detail).encode()))
        with self.assertRaisesRegex(ValueError, "extra-token branch"):
            compare(reference, native)

    def test_receipts_reject_duplicate_keys_and_type_substitution(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution.json"
            path.write_bytes(b'{"observation":{"answer":42,"answer":43}}')
            with self.assertRaisesRegex(ValueError, "duplicate JSON field"):
                read_receipt(path)
        for raw in (True, 1.0):
            with self.subTest(schema_version=raw):
                with self.assertRaisesRegex(ValueError, "different fixture or engine"):
                    validate_execution_header({"schema_version": raw, "engine": "jvm", "fixture_sha256": "sha"}, "jvm", "sha")
                self.assertFalse(fields_exact({"value": raw}, {"value": 1}))
        observed = sample_execution()
        observed["observations"]["flight_sql"]["follow_up"]["batches"][0]["rows"][0][0]["value"] = 42.0
        with self.assertRaisesRegex(ValueError, "ordered bigint answer"):
            compare(sample_execution(), observed)

    def test_malformed_nested_runtime_receipts_retain_failure_without_calling_legacy_verifier(self):
        runtime = {
            "classpath": ["/fixture/classes"], "jdk": ["/fixture/bin/java", "/fixture/lib/modules"], "absent_classpath": [],
            "class_locations": {"fixture.Class": "/fixture/classes"}, "library": None,
            "artifacts": [{"kind": "directory", "path": "/fixture/classes", "files": []}],
            "java_home": "/fixture", "java_executable": "/fixture/bin/java", "java_modules": "/fixture/lib/modules",
        }
        for changes, expected in (
            ({"artifacts": [{"kind": "directory", "path": "/fixture/classes", "files": [1]}]}, "directory member"),
            ({"artifacts": [{"kind": "directory", "path": "/fixture/classes", "files": {"file": 1}}]}, "directory record"),
            ({"class_locations": ["fixture.Class"]}, "class locations"),
            ({"class_locations": {"fixture.Class": ["/fixture/classes"]}}, "class locations"),
        ):
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as directory:
                output = Path(directory).resolve() / "output"
                argv = ["run_parser_wire.py", "--reference", directory, "--native", directory, "--output", str(output)]

                def read_runtime(args):
                    args.output.mkdir()
                    args.created_output = True
                    verify_jvm_runtime({**runtime, **changes})

                with (patch("sys.argv", argv), patch("run_parser_wire.run", side_effect=read_runtime),
                      patch("run_parser_wire.verify_runtime") as legacy, redirect_stderr(io.StringIO())):
                    self.assertEqual(2, main())
                    legacy.assert_not_called()
                failure = json.loads((output / "failure.json").read_bytes())
                self.assertIs(False, failure["comparison_completed"])
                self.assertIn(expected, failure["error"])

    def test_phase_gate_precedes_any_worker_or_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "observation"
            with patch("run_parser_wire.run_process") as worker:
                with self.assertRaisesRegex(ValueError, "requires --phase-two"):
                    run(argparse.Namespace(phase_two=False, output=output))
                worker.assert_not_called()
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
