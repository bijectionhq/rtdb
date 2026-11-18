import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import run_embedded_migration as migration


class EmbeddedMigrationTest(unittest.TestCase):
    def setUp(self):
        self.configuration = json.loads(migration.CONFIGURATION.read_bytes())
        self.fixture_sha256 = migration.digest(migration.FIXTURE)
        self.mapping = migration.selection(self.configuration, self.fixture_sha256)

    def test_fixture_contains_operation_inputs_and_no_expectations(self):
        fixture = json.loads(migration.FIXTURE.read_bytes())
        self.assertEqual({"schema_version", "sql", "native"}, set(fixture))
        self.assertEqual({"statement_mismatch", "unwanted_token", "follow_up"}, set(fixture["sql"]))
        self.assertEqual({"yaml", "memory_bytes", "legacy_merge_sort"}, set(fixture["native"]))
        self.assertEqual("SELEC 1", fixture["sql"]["statement_mismatch"])
        self.assertEqual("extra SELECT 1", fixture["sql"]["unwanted_token"])
        self.assertEqual("SELECT 42 AS answer", fixture["sql"]["follow_up"])

    def test_selection_rejects_new_fixture_inventory_flags_and_duplicate_groups(self):
        with self.assertRaisesRegex(ValueError, "fixture differs"):
            migration.selection(self.configuration, "a" * 64)
        for flag in ("inventory_complete", "runtime_reconciled"):
            with self.subTest(flag=flag):
                configuration = copy.deepcopy(self.configuration)
                configuration["ledger"][flag] = True
                with self.assertRaisesRegex(ValueError, "unreconciled inventory"):
                    migration.selection(configuration, self.fixture_sha256)
        self.configuration["ledger"]["cases"].append(self.mapping)
        with self.assertRaisesRegex(ValueError, "one scoped case"):
            migration.selection(self.configuration, self.fixture_sha256)

    def test_observer_commands_get_only_selected_inputs_and_library(self):
        reference = Path("/reference")
        native = Path("/implementation")
        fixture = Path("/evidence/fixture.json")
        library = Path("/evidence/librtdb_ffi.so")
        reference_command = migration.gradle_command("reference", reference, fixture, Path("/evidence/reference"))
        native_command = migration.gradle_command("native", native, fixture, Path("/evidence/native"), library)
        self.assertEqual(reference / "gradlew", reference_command[0])
        self.assertIn(":rtdbConformanceEmbeddedParser", reference_command)
        self.assertEqual(native / "gradlew", native_command[0])
        self.assertIn(":bindings:xtdb-rust-bindings:rtdbConformanceEmbeddedParser", native_command)
        for command in (reference_command, native_command):
            self.assertIn(f"-Prtdb.fixture={fixture}", command)
            self.assertFalse(any("ledger.json" in str(value) or "configuration.json" in str(value) for value in command))
        self.assertNotIn(f"-Dxtdb.native.library={library}", reference_command)
        self.assertIn(f"-Dxtdb.native.library={library}", native_command)

    def test_closed_gate_precedes_checkout_and_process_access(self):
        with (patch.object(migration, "require_phase_two", side_effect=ValueError("Phase 2 closed")) as gate,
              patch.object(migration, "git") as git,
              patch.object(migration, "run_process") as process):
            with self.assertRaisesRegex(ValueError, "Phase 2 closed"):
                migration.run(SimpleNamespace(phase_two=True))
        gate.assert_called_once_with(migration.REPOSITORY, True)
        git.assert_not_called()
        process.assert_not_called()

    def test_native_library_requires_exact_packaged_bytes_and_platform(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "package.json"
            library = package.parent / migration.LIBRARY
            library.parent.mkdir()
            library.write_bytes(b"selected fixture library")
            record = {"path": migration.LIBRARY, "sha256": migration.digest(library), "bytes": library.stat().st_size}
            package.write_text(json.dumps({"image": {}, "platform": "linux/arm64", "artifacts": [record]}))
            with (patch.object(migration, "validate_package") as validate_package,
                  patch.object(migration, "validate_package_files") as validate_package_files,
                  patch.object(migration.platform, "system", return_value="Linux"),
                  patch.object(migration.platform, "machine", return_value="aarch64")):
                self.assertEqual((library, record), migration.native_library(package, "revision"))
                validate_package.assert_called_once()
                validate_package_files.assert_called_once()
                library.write_bytes(b"same path, different fixture bytes")
                with self.assertRaisesRegex(ValueError, "packaged artifact"):
                    migration.native_library(package, "revision")
            with (patch.object(migration, "validate_package"),
                  patch.object(migration, "validate_package_files"),
                  patch.object(migration.platform, "system", return_value="Darwin"),
                  patch.object(migration.platform, "machine", return_value="arm64")):
                with self.assertRaisesRegex(ValueError, "host matching"):
                    migration.native_library(package, "revision")

    def test_receipt_preserves_executed_outcome_and_stage_from_observer(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            (output / "inputs.json").write_text("{}")
            arguments = self.mapping["observations"][0]
            observed = {"id": arguments["id"], "executed": True, "outcome": {
                "kind": "expected_failure", "stage": "parse_statement_ast", "category": "incorrect",
                "code": "xtdb/sql-error", "signature": {"cause": None},
            }}

            def finished(command, repository, stdout, stderr, timeout):
                stdout.write_bytes(b"")
                stderr.write_bytes(b"")
                runtime = stdout.parent / "runtime-artifacts.json"
                runtime.write_text("{}")
                (stdout.parent / "execution.json").write_text(json.dumps({
                    "schema_version": 1, "side": "reference", "fixture_sha256": self.fixture_sha256,
                    "binary_sha256": migration.digest(runtime), "observations": [observed],
                    "supplemental": {"captured": "actual"},
                }))
                return {"completed": True, "exit_code": 0}

            with (patch.object(migration, "run_process", side_effect=finished),
                  patch.object(migration, "verify_runtime") as verify_runtime):
                receipt, supplemental = migration.observe(
                    "reference", output, migration.REFERENCE, output / "reference", migration.FIXTURE,
                    self.fixture_sha256, "a" * 64, self.configuration["context"], self.mapping, 10)
            verify_runtime.assert_called_once_with({}, "reference", None)
            self.assertEqual([observed], receipt["observations"])
            self.assertEqual({"captured": "actual"}, supplemental)
            self.assertEqual(migration.digest(output / "reference/trace.json"), receipt["trace_sha256"])
            self.assertEqual(migration.REFERENCE, receipt["context"]["artifact_revision"])

    def test_scoped_report_never_qualifies_as_strict_parity(self):
        report = {
            "required_groups": 1, "passing_both": 0, "matching_expected_failure": 0,
            "matched_declared_migration": 1, "missing_evidence": 0, "mismatches": 0,
            "cases": [{"identity": self.mapping["identity"], "disposition": "matched_declared_migration",
                       "comparisons": 1, "differences": []}],
            "manifest_errors": ["source inventory is incomplete", "runtime discovery has not been reconciled"],
            "strict_agreement": None, "complete_parity": False,
        }
        migration.require_migration_report(report, self.mapping)
        for key, value in (("passing_both", 1), ("matched_declared_migration", 0), ("missing_evidence", 1),
                           ("mismatches", 1), ("manifest_errors", []), ("strict_agreement", 1.0), ("complete_parity", True)):
            with self.subTest(key=key):
                with self.assertRaisesRegex(ValueError, "did not confirm"):
                    migration.require_migration_report({**report, key: value}, self.mapping)
        for case_update in ({"disposition": "matching_expected_failure"}, {"comparisons": 0},
                            {"differences": [{"reason": "unexpected cause"}]}, {"identity": {}}):
            with self.subTest(case_update=case_update):
                cases = [{**report["cases"][0], **case_update}]
                with self.assertRaisesRegex(ValueError, "did not confirm"):
                    migration.require_migration_report({**report, "cases": cases}, self.mapping)
