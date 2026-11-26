import json
import tempfile
import unittest
from pathlib import Path

from corpus_ledger import build, canonical_var
from corpus_summary import Exclusions
from runtime_inventory import read_plan, reconcile

CONFIGURATION = "corpus:native-release"


def clojure_descriptor(var):
    namespace, name = var.split("/", 1)
    return {"unique_id": f"[namespace:{namespace}]/[name:{name}]", "parent_id": None, "is_test": True, "is_container": False,
            "display_name": name, "legacy_name": name, "clojure_var": var, "tags": [],
            "source": {"class_name": namespace.replace("-", "_") + "__init"}}


def junit_descriptor(owner, method):
    return {"unique_id": f"[class:{owner}]/[method:{method}]", "parent_id": None, "is_test": True, "is_container": False,
            "display_name": method, "legacy_name": method, "tags": [],
            "source": {"class_name": owner, "method_name": method}}


def candidate(kind, source, name):
    return {"id": f"{source}#{name}", "kind": kind, "identity": {"reference": "revision", "source": source, "name": name}}


SOURCE = {"reference": "revision", "files": [], "candidates": [
    candidate("clojure_var_candidate", "src/test/clojure/xtdb/a_test.clj", "xtdb.a-test/passes"),
    candidate("clojure_var_candidate", "src/test/clojure/xtdb/a_test.clj", "xtdb.a-test/fails"),
    candidate("clojure_var_candidate", "src/test/clojure/xtdb/a_test.clj", "xtdb.a-test/reaches-the-jvm"),
    candidate("clojure_var_candidate", "src/test/clojure/xtdb/kafka_test.clj", "^:kafka/streams"),
    candidate("junit_template_candidate", "core/src/test/kotlin/xtdb/StorageTest.kt", "xtdb.StorageTest/lists"),
]}
CORPUS = [{"var": "xtdb.a-test/passes", "ns": "xtdb.a-test", "result": "pass"},
          {"var": "xtdb.a-test/fails", "ns": "xtdb.a-test", "result": "fail", "detail": "expected 1"},
          {"var": "xtdb.a-test/reaches-the-jvm", "ns": "xtdb.a-test", "result": "error",
           "detail": "cannot be cast to class xtdb.api.Xtdb"},
          {"var": "xtdb.kafka-test/streams", "ns": "xtdb.kafka-test", "result": "pass"}]


EXCLUSIONS = Exclusions({
    "decisions": {"not-a-jvm-node": {"outcome": "jvm-api-boundary", "decision": "the binding's node is not an xtdb.api.Xtdb",
                                     "match": [r"cannot be cast to class xtdb\.api\.Xtdb\b"]}},
    "vars": [{"var": "xtdb.a-test/reaches-the-jvm", "decision": "not-a-jvm-node"}]})


def runtime_fixture(directory):
    directory = Path(directory)
    descriptors = [clojure_descriptor("xtdb.a-test/passes"), clojure_descriptor("xtdb.a-test/fails"),
                   clojure_descriptor("xtdb.a-test/reaches-the-jvm"), clojure_descriptor("xtdb.kafka-test/streams"),
                   junit_descriptor("xtdb.LocalStorageTest", "lists")]
    events = [{"event": "plan_started", "project": ":", "task": ":test"}]
    for value in descriptors:
        events.extend([{"event": "discovered", "descriptor": value}, {"event": "execution_started", "descriptor": value},
                       {"event": "execution_finished", "descriptor": value, "status": "SUCCESSFUL"}])
    events.append({"event": "plan_finished"})
    (directory / "plan.jsonl").write_text("".join(json.dumps({"sequence": index, **event}) + "\n" for index, event in enumerate(events)))
    (directory / "selection.json").write_text(json.dumps({"project": ":", "task": ":test"}))
    (directory / "requested-lanes.json").write_text(json.dumps({"lanes": [{"project": ":", "task": ":test"}]}))
    (directory / "root-discovery.json").write_text(json.dumps({"kind": "discovery_only", "project": ":", "descriptors": descriptors}))
    reference = directory / "reference"
    (reference / "core/src/test/kotlin/xtdb").mkdir(parents=True)
    (reference / "core/src/test/kotlin/xtdb/StorageTest.kt").write_text(
        "abstract class StorageTest {\n @Test fun lists() {}\n}\nclass LocalStorageTest : StorageTest() {}\n")
    return [case for case in read_plan(directory / "plan.jsonl")["cases"]], reference


class CorpusLedgerTests(unittest.TestCase):
    def generate(self, directory):
        cases, reference = runtime_fixture(directory)
        return build(SOURCE, cases, CORPUS, CONFIGURATION, reference, "corpus-revision", EXCLUSIONS)

    def test_namespace_metadata_does_not_hide_a_var(self):
        self.assertEqual(canonical_var(SOURCE["candidates"][3]), "xtdb.kafka-test/streams")

    def test_counterparts_classifications_and_bindings(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger, invocations, aliases, native, counts = self.generate(directory)
        by_name = {case["identity"]["qualified_name"]: case for case in ledger["cases"]}
        self.assertEqual(by_name["xtdb.a-test/passes"]["counterparts"], ["corpus:xtdb.a-test/passes"])
        self.assertEqual(by_name["xtdb.a-test/fails"]["counterparts"], ["corpus:xtdb.a-test/fails"])
        self.assertEqual(by_name["xtdb.a-test/reaches-the-jvm"]["classification"],
                         {"kind": "jvm-api-boundary", "reason": "the binding's node is not an xtdb.api.Xtdb"})
        self.assertEqual(by_name["xtdb.StorageTest.lists"]["classification"]["kind"], "engine-internal")
        self.assertEqual(by_name["^:kafka/streams"]["counterparts"], ["corpus:xtdb.kafka-test/streams"])
        self.assertEqual(len(invocations), 5)
        self.assertEqual(aliases["[class:xtdb.LocalStorageTest]/[method:lists]"], "xtdb.StorageTest/lists")
        self.assertEqual(aliases["[namespace:xtdb.kafka-test]/[name:streams]"], "^:kafka/streams")
        self.assertEqual({test["counterpart"]: test["outcome"] for test in native["tests"]}["corpus:xtdb.a-test/fails"], "fail")
        self.assertEqual(counts, {})

    def test_reconciliation_counts_classified_cases_apart_and_reports_failing_counterparts(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger, invocations, aliases, native, _ = self.generate(directory)
            report = reconcile(SOURCE, ledger, Path(directory), native, aliases, invocations)
        self.assertFalse(report["complete"])
        self.assertEqual(report["classified_case_count"], 2)
        self.assertEqual(report["classified_by_kind"], {"engine-internal": 1, "jvm-api-boundary": 1})
        self.assertEqual(report["native_counterparts_passing"], 2)
        self.assertEqual(report["native_counterparts_failing"], ["corpus:xtdb.a-test/fails (fail)"])
        self.assertEqual([error for error in report["errors"] if "did not pass" not in error], [])

    def test_a_redefined_var_is_one_case_and_one_candidate(self):
        redefined = dict(SOURCE, candidates=SOURCE["candidates"] + [dict(candidate("clojure_var_candidate", "src/test/clojure/xtdb/a_test.clj", "xtdb.a-test/passes"), id="again")])
        with tempfile.TemporaryDirectory() as directory:
            cases, reference = runtime_fixture(directory)
            ledger, invocations, aliases, native, counts = build(redefined, cases, CORPUS, CONFIGURATION, reference, "corpus-revision", EXCLUSIONS)
            for test in native["tests"]:
                test["outcome"] = "pass"
            report = reconcile(redefined, ledger, Path(directory), native, aliases, invocations)
        self.assertEqual(counts, {"redefined_vars": 1})
        self.assertEqual(report["errors"], [])

    def test_a_case_with_neither_counterparts_nor_classification_is_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger, invocations, aliases, native, _ = self.generate(directory)
            ledger["cases"][0]["counterparts"] = []
            report = reconcile(SOURCE, ledger, Path(directory), native, aliases, invocations)
        self.assertTrue(any("neither native counterparts nor a classification" in error for error in report["errors"]))

    def test_passing_counterparts_and_classifications_reconcile_completely(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger, invocations, aliases, native, _ = self.generate(directory)
            for test in native["tests"]:
                test["outcome"] = "pass"
            report = reconcile(SOURCE, ledger, Path(directory), native, aliases, invocations)
        self.assertEqual(report["errors"], [])
        self.assertTrue(report["complete"])
        self.assertEqual(report["assertion_denominator"], 3)


if __name__ == "__main__":
    unittest.main()
