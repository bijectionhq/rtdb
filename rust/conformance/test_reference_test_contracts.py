import json
import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from run_reference_test_contracts import CASES, compare, junit_outcome, native_observations


class ReferenceTestContractsTest(unittest.TestCase):
    def test_junit_requires_the_selected_method_and_an_executed_outcome(self):
        selection = CASES["offloaded-retention"]
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            report = directory / "TEST-selected.xml"
            suite = ET.Element("testsuite", tests="1", skipped="0", failures="0")
            case = ET.SubElement(suite, "testcase", classname=selection["class"],
                                 name=selection["method"] + "(Path, BufferAllocator)")

            def outcome():
                ET.ElementTree(suite).write(report)
                return junit_outcome(directory, selection)["status"]

            self.assertEqual(outcome(), "passed")
            case.set("name", "garbageCollectBlocks preserves offloaded payloads beyond the metadata horizon")
            with self.assertRaisesRegex(ValueError, "different case"):
                outcome()
            case.set("name", selection["method"])
            ET.SubElement(case, "skipped")
            with self.assertRaisesRegex(ValueError, "skipped"):
                outcome()
            case.remove(case.find("skipped"))
            ET.SubElement(case, "failure")
            suite.set("failures", "1")
            self.assertEqual(outcome(), "failed")

    def test_comparison_reports_mismatch_and_agreement_independently(self):
        reference = {case: {"status": "passed"} for case in CASES}
        native = {
            "offloaded-retention": {
                "published_payloads": {"6": "aged", "9": "young"},
                "remaining_payloads": ["aged", "young"],
            },
            "disk-cache": {"retained": ["b", "c"], "evicted": ["a"]},
            "memory-cache": {"admitted_three_while_pinned": False},
        }
        comparisons = compare(reference, native)
        self.assertEqual(comparisons["offloaded-retention"]["aged_payload_absent"],
                         {"upstream": True, "native": False, "matches": False})
        self.assertFalse(comparisons["offloaded-retention"]["only_young_payload_remains"]["matches"])
        self.assertTrue(comparisons["disk-cache-final-keys"]["retained"]["matches"])
        self.assertTrue(comparisons["disk-cache-final-keys"]["evicted"]["matches"])
        self.assertFalse(comparisons["memory-cache-admission"]["admitted_three_while_pinned"]["matches"])

        reference["pinning-cache"]["status"] = "failed"
        self.assertEqual(set(compare(reference, native)), {"offloaded-retention"})

    def test_native_receipts_require_the_clean_revision_and_unique_case_identity(self):
        record = {"case": "memory-cache", "revision": "expected", "source_dirty": False, "observation": {}}
        with tempfile.TemporaryDirectory() as directory:
            stdout = Path(directory) / "native.stdout.log"
            line = "VE-11 " + json.dumps(record) + "\n"
            stdout.write_text(line)
            self.assertEqual(native_observations(stdout, "expected"), {"memory-cache": {}})
            with self.assertRaisesRegex(ValueError, "another revision"):
                native_observations(stdout, "other")
            stdout.write_text(line * 2)
            with self.assertRaisesRegex(ValueError, "repeated native case"):
                native_observations(stdout, "expected")
