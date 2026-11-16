import json
import copy
import tempfile
import unittest
from pathlib import Path

from runtime_inventory import read_plan, reconcile
from list_native import parse_listing


def descriptor(identity, parent=None, test=True):
    return {"unique_id": identity, "parent_id": parent, "is_test": test,
            "is_container": not test, "display_name": identity, "legacy_name": identity,
            "source": {"class_name": "Example", "method_name": identity}, "tags": []}


def plan_file(directory, events):
    path = Path(directory) / "plan.jsonl"
    path.write_text("".join(json.dumps({"sequence": index, **event}) + "\n" for index, event in enumerate(events)))
    return path


def inventory_fixture(directory, invocation_ids=("test",)):
    directory = Path(directory)
    events = [{"event": "plan_started", "project": ":", "task": ":test"}]
    descriptors = []
    for unique_id in invocation_ids:
        value = descriptor(unique_id)
        value["source"]["method_name"] = "test"
        descriptors.append(value)
        events.extend([{"event": "dynamic_registered", "descriptor": value},
                       {"event": "execution_started", "descriptor": value},
                       {"event": "execution_finished", "descriptor": value, "status": "SUCCESSFUL"}])
    events.append({"event": "plan_finished"})
    plan_file(directory, events)
    (directory / "selection.json").write_text(json.dumps({"project": ":", "task": ":test"}))
    (directory / "requested-lanes.json").write_text(json.dumps({"lanes": [{"project": ":", "task": ":test"}]}))
    (directory / "root-discovery.json").write_text(json.dumps({"kind": "discovery_only", "project": ":", "descriptors": descriptors}))
    identity = {"reference_revision": "revision", "source": "Example.kt", "qualified_name": "Example.test",
                "parameter_fixture": "fixture:1", "configuration": "jvm-default"}
    source = {"reference": "revision", "files": [], "candidates": [{"id": "candidate", "kind": "junit_template_candidate",
              "identity": {"source": "Example.kt", "name": "Example/test"}}]}
    ledger = {"inventory_revision": "revision", "cases": [{"identity": identity, "counterparts": ["native::test"]}]}
    native = {"complete": True, "tests": [{"counterpart": "native::test", "configuration": "native-default"}]}
    bindings = [{"project": ":", "task": ":test", "unique_id": invocation_ids[0], "identity": identity,
                 "native_configuration": "native-default"}]
    return source, ledger, directory, native, {}, bindings


class RuntimeEvidenceTests(unittest.TestCase):
    def test_skipped_container_keeps_its_unexecuted_descendant_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            path = plan_file(directory, [
                {"event": "plan_started", "project": ":", "task": ":test"},
                {"event": "discovered", "descriptor": descriptor("container", test=False)},
                {"event": "discovered", "descriptor": descriptor("test", "container")},
                {"event": "execution_skipped", "descriptor": descriptor("container", test=False), "reason": "disabled"},
                {"event": "plan_finished"}])
            result = read_plan(path)
            self.assertEqual(result["errors"], [])
            self.assertEqual(result["cases"][0]["outcome"]["event"], "ancestor_skipped")
            self.assertEqual(result["cases"][0]["outcome"]["reason"], "disabled")

    def test_aborted_and_duplicate_event_streams_do_not_establish_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            path = plan_file(directory, [
                {"event": "plan_started", "project": ":", "task": ":test"},
                {"event": "discovered", "descriptor": descriptor("test")},
                {"event": "execution_started", "descriptor": descriptor("test")},
                {"event": "execution_started", "descriptor": descriptor("test")}])
            result = read_plan(path)
            self.assertTrue(any("completion" in error for error in result["errors"]))
            self.assertTrue(any("twice" in error for error in result["errors"]))
            self.assertEqual(result["cases"][0]["outcome"]["event"], "missing_outcome")

    def test_dynamic_invocations_have_distinct_runtime_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            events = [{"event": "plan_started", "project": ":", "task": ":property-test"}]
            for identity in ["seed:1", "seed:2"]:
                events.extend([{"event": "dynamic_registered", "descriptor": descriptor(identity)},
                               {"event": "execution_started", "descriptor": descriptor(identity)},
                               {"event": "execution_finished", "descriptor": descriptor(identity), "status": "SUCCESSFUL"}])
            events.append({"event": "plan_finished"})
            result = read_plan(plan_file(directory, events))
            self.assertEqual(result["errors"], [])
            self.assertEqual(len(result["cases"]), 2)

    def test_empty_runtime_cannot_be_reconciled_even_with_an_empty_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            source = {"reference": "revision", "candidates": [], "files": []}
            ledger = {"inventory_revision": "revision", "cases": []}
            report = reconcile(source, ledger, Path(directory), {"complete": True, "tests": []}, {})
            self.assertFalse(report["complete"])
            self.assertIsNone(report["assertion_denominator"])
            self.assertTrue(report["errors"])

    def test_native_integration_and_library_counterparts_keep_distinct_owners(self):
        self.assertEqual(parse_listing("test_name: test\n", "rtdb-format", "term_sequence", ["test"])[0]["counterpart"],
                         "rtdb-format::term_sequence::test_name")
        self.assertEqual(parse_listing("codec::test_name: test\n", "rtdb-format", "rtdb_format", ["lib"])[0]["counterpart"],
                         "rtdb-format::codec::test_name")
        with self.assertRaises(ValueError):
            parse_listing("runner crashed\n", "package", "target", ["test"])

    def test_explicit_invocation_fixture_and_native_configuration_establish_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            report = reconcile(*inventory_fixture(directory))
            self.assertEqual(report["errors"], [])
            self.assertTrue(report["complete"])
            self.assertEqual(report["assertion_denominator"], 1)

    def test_unfiltered_discovery_keeps_unknown_excluded_tests_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            inputs = inventory_fixture(directory)
            path = Path(directory) / "root-discovery.json"
            discovery = json.loads(path.read_bytes())
            discovery["descriptors"].append(descriptor("filtered"))
            path.write_text(json.dumps(discovery))
            report = reconcile(*inputs)
            self.assertFalse(report["complete"])
            self.assertEqual([case["descriptor"]["unique_id"] for case in report["unmapped_discovery"]], ["filtered"])

    def test_parameter_invocations_do_not_inherit_a_method_level_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            inputs = inventory_fixture(directory, ("seed:1", "seed:2"))
            report = reconcile(*inputs)
            self.assertFalse(report["complete"])
            self.assertTrue(any("no explicit fixture/configuration binding" in error and "seed:2" in error for error in report["errors"]))
            second_mapping = copy.deepcopy(inputs[1]["cases"][0])
            second_mapping["identity"]["parameter_fixture"] = "fixture:2"
            inputs[1]["cases"].append(second_mapping)
            inputs[5].append({**inputs[5][0], "unique_id": "seed:2", "identity": second_mapping["identity"]})
            report = reconcile(*inputs)
            self.assertTrue(report["complete"], report["errors"])
            self.assertEqual(report["assertion_denominator"], 2)

    def test_a_counterpart_in_another_native_configuration_does_not_cover_the_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            inputs = inventory_fixture(directory)
            inputs[5][0]["native_configuration"] = "different-features"
            report = reconcile(*inputs)
            self.assertFalse(report["complete"])
            self.assertEqual(report["candidates"][0]["missing_counterparts"], ["native::test"])
            self.assertTrue(any("different-features" in error for error in report["errors"]))

    def test_duplicate_or_orphan_invocation_bindings_cannot_complete_inventory(self):
        for unique_id in ["test", "not-recorded"]:
            with self.subTest(unique_id=unique_id), tempfile.TemporaryDirectory() as directory:
                inputs = inventory_fixture(directory)
                inputs[5].append({**inputs[5][0], "unique_id": unique_id})
                report = reconcile(*inputs)
                self.assertFalse(report["complete"])
                self.assertTrue(any("duplicate invocation binding" in error or "no recorded runtime case" in error for error in report["errors"]))

    def test_invocation_binding_must_match_the_entire_ledger_identity(self):
        for field in ["parameter_fixture", "configuration", "source", "reference_revision"]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                inputs = inventory_fixture(directory)
                inputs[5][0]["identity"] = {**inputs[5][0]["identity"], field: "different"}
                report = reconcile(*inputs)
                self.assertFalse(report["complete"])
                self.assertTrue(any("no exact ledger case" in error for error in report["errors"]))


if __name__ == "__main__":
    unittest.main()
