import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from external_inventory import Evidence, canonical, joined, reconcile_external


def fixture(directory):
    path = Path(directory) / "original.test"
    path.write_text("statement error\nSELECT invalid;\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    reference = {"path": path.name, "sha256": digest}
    source_name = "sqlite-test/case.test"
    runtime = {"reference": "reference", "source": source_name, "source_sha256": digest,
        "selection": "slt-test", "options": {"direct_sql": False}, "ordinal": 1, "assertion_ordinal": 1, "line": 1}
    configuration = canonical({"adapter": "sql_logic", "selection": runtime["selection"], "options": runtime["options"]})
    identity = {"reference_revision": "reference", "source": source_name, "qualified_name": "record:1",
        "parameter_fixture": "original-line:1", "configuration": configuration}
    build_selection = {"variant": "local", "features": []}
    native_configuration = canonical({"build": "local-production", "selection": build_selection, "execution": json.loads(configuration)})
    binding = {"runtime_identity": runtime, "identity": identity, "counterparts": ["sql-logic/record:1"], "native_configuration": native_configuration}
    source = {"reference": "reference", "files": [{"path": source_name, "sha256": digest, "lane": "sql_logic", "record_count": 1}], "candidates": []}
    ledger = {"cases": [{"identity": identity, "counterparts": binding["counterparts"]}]}
    run = {"id": "selected", "kind": "sql_logic", "root": ".", "recorded_root": str(directory), "artifacts": [reference],
        "sources": [{"source": source_name, "artifact": reference}], "bindings": [binding]}
    document = {"schema_version": 1, "reference_revision": "reference", "implementation_revision": "native", "runs": [run],
        "files": [{"source": source_name, "sha256": digest, "required_invocations": [{"run": "selected", "identity": runtime}]}]}
    observed = [{"runtime_identity": runtime, "source": source_name, "passed": True, "outcome": "expected_error_passed", "observations": [], "errors": [],
                 "native_build_configuration": "local-production", "native_build_selection": build_selection}]
    return document, source, ledger, observed


class ExternalEvidenceIdentity(unittest.TestCase):
    def test_expected_error_pass_is_retained_distinctly_with_exact_original_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            document, source, ledger, observed = fixture(directory)
            with patch("external_inventory.sql_cases", return_value=observed):
                result = reconcile_external(document, Path(directory), source, ledger)
            self.assertTrue(result["all_passed"])
            self.assertEqual("expected_error_passed", result["cases"][0]["outcome"])
            self.assertEqual({"sqlite-test/case.test"}, result["resolved_files"])

    def test_binding_cannot_change_options_or_omit_an_actual_invocation(self):
        for change in ("configuration", "invocation"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                document, source, ledger, observed = fixture(directory)
                if change == "configuration":
                    document["runs"][0]["bindings"][0]["native_configuration"] = "different"
                else:
                    additional = copy.deepcopy(observed[0])
                    additional["runtime_identity"]["ordinal"] = 2
                    observed.append(additional)
                with patch("external_inventory.sql_cases", return_value=observed), self.assertRaises(ValueError):
                    reconcile_external(document, Path(directory), source, ledger)

    def test_matching_reference_failure_does_not_resolve_source(self):
        with tempfile.TemporaryDirectory() as directory:
            document, source, ledger, observed = fixture(directory)
            observed[0].update(passed=False, outcome="unverified")
            with patch("external_inventory.sql_cases", return_value=observed):
                result = reconcile_external(document, Path(directory), source, ledger)
            self.assertFalse(result["all_passed"])
            self.assertEqual(set(), result["resolved_files"])

    def test_mutated_or_unretained_artifact_cannot_establish_external_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            document, _, _, _ = fixture(directory)
            evidence = Evidence(Path(directory), document["runs"][0])
            with self.assertRaises(ValueError):
                evidence.path("unretained.json")
            (Path(directory) / "original.test").write_text("different original")
            with self.assertRaises(ValueError):
                Evidence(Path(directory), document["runs"][0])

    def test_exit_success_without_join_never_establishes_execution(self):
        for process in ({"exit_code": 0}, {"completed": True, "exit_code": 0, "unexpected_live_descendants": [12]}):
            with self.assertRaises(ValueError):
                joined(process)

    def test_unknown_candidate_free_source_cannot_self_declare_support(self):
        with tempfile.TemporaryDirectory() as directory:
            document, source, ledger, observed = fixture(directory)
            original = source["files"][0]
            unknown = "lang/python/undetected_tests.py"
            source["files"].append({**original, "path": unknown, "lane": "language_client", "record_count": 0})
            document["files"].append({"source": unknown, "sha256": original["sha256"], "classification": "support_input",
                                      "required_invocations": [], "artifact": document["runs"][0]["artifacts"][0]})
            with patch("external_inventory.sql_cases", return_value=observed):
                result = reconcile_external(document, Path(directory), source, ledger)
            self.assertNotIn(unknown, result["resolved_files"])


if __name__ == "__main__":
    unittest.main()
