import copy
import json
import unittest

from client_results import compare_clients, parse_exunit, parse_junit, parse_libtest, parse_trx


class ClientReportTests(unittest.TestCase):
    def test_junit_keeps_skips_errors_parameters_and_case_source(self):
        raw = b'''<testsuites tests="4" failures="1" errors="1" skipped="1">
          <testsuite name="driver" tests="4" failures="1" errors="1" skipped="1">
            <testcase classname="insert" name="integer[42]" file="test_driver.py" line="7"/>
            <testcase classname="insert" name="integer[42]"><failure type="AssertionError">wrong row</failure></testcase>
            <testcase classname="insert" name="date"><skipped message="driver bug"/></testcase>
            <testcase classname="insert" name="uuid"><error>setup failed</error></testcase>
          </testsuite></testsuites>'''
        cases = parse_junit(raw)
        self.assertEqual([case["outcome"] for case in cases], ["passed", "failed", "skipped", "error"])
        self.assertEqual(cases[0]["source"], {"file": "test_driver.py", "line": "7"})
        self.assertIn("wrong row", cases[1]["details"][0]["text"])

    def test_junit_keeps_suite_files_and_rejects_ambiguous_duplicate_names(self):
        raw = b'''<testsuites tests="2"><testsuite file="first.js"><testcase classname="driver" name="read"/></testsuite>
                 <testsuite file="second.js"><testcase classname="driver" name="read"/></testsuite></testsuites>'''
        cases = parse_junit(raw)
        self.assertEqual([case["source"]["file"] for case in cases], ["first.js", "second.js"])
        with self.assertRaises(ValueError):
            parse_junit(raw.replace(b"second.js", b"first.js"))

    def test_empty_incomplete_conflicting_and_entity_reports_cannot_pass(self):
        for raw in [b'<testsuite tests="0"/>', b'<testsuite tests="2"><testcase name="one"/></testsuite>',
                    b'<testsuite tests="1"><testcase name="one"><failure/><skipped/></testcase></testsuite>',
                    b'<!DOCTYPE testsuite><testsuite tests="1"><testcase name="one"/></testsuite>',
                    b'<testsuite tests="1"><testcase name="one">']:
            with self.subTest(raw=raw), self.assertRaises(Exception):
                parse_junit(raw)

    def test_trx_preserves_display_parameters_and_requires_test_definition(self):
        raw = b'''<TestRun xmlns="http://microsoft.com/schemas/VisualStudio/TeamTest/2010">
          <TestDefinitions><UnitTest id="t1"><TestMethod className="Driver" name="RoundTrip" codeBase="driver.dll"/></UnitTest></TestDefinitions>
          <Results><UnitTestResult testId="t1" executionId="e1" testName="RoundTrip(42)" outcome="Passed"/></Results>
          <ResultSummary outcome="Completed"><Counters total="1" passed="1" failed="0" notExecuted="0"/></ResultSummary>
          </TestRun>'''
        case = parse_trx(raw)[0]
        self.assertEqual((case["suite"], case["name"], case["outcome"]), ("Driver", "RoundTrip(42)", "passed"))
        self.assertEqual(case["source"]["name"], "RoundTrip")
        for bad in [raw.replace(b'testId="t1"', b'testId="unknown"'), raw.replace(b'total="1"', b'total="2"'),
                    raw.replace(b'outcome="Completed"', b'outcome="Aborted"')]:
            with self.assertRaises(ValueError):
                parse_trx(bad)

    def exunit_events(self):
        return [{"sequence": 0, "kind": "suite_started"},
                {"sequence": 1, "kind": "test_started", "suite": "Driver", "name": "test round trip"},
                {"sequence": 2, "kind": "test_finished", "suite": "Driver", "name": "test round trip", "outcome": "passed", "source": {"file": "driver.exs", "line": 12}, "details": "nil"},
                {"sequence": 3, "kind": "test_finished", "suite": "Driver", "name": "test skipped", "outcome": "skipped", "source": {"file": "driver.exs", "line": 15}, "details": "reason"},
                {"sequence": 4, "kind": "suite_finished", "test_count": 2}]

    def parse_events(self, events):
        return parse_exunit("\n".join(json.dumps(event) for event in events).encode())

    def test_exunit_retains_unstarted_skips_and_rejects_unfinished_or_missing_events(self):
        events = self.exunit_events()
        self.assertEqual([case["outcome"] for case in self.parse_events(events)], ["passed", "skipped"])
        for bad in [events[:-1], events[:2] + events[3:], events[:1] + events[2:]]:
            with self.assertRaises(ValueError):
                self.parse_events(bad)
        events[2]["kind"] = "module_failed"
        with self.assertRaises(ValueError):
            self.parse_events(events)

    def test_libtest_requires_single_target_interval_and_matching_totals(self):
        raw = b'''running 3 tests
test module::read ... ok
test module::write ... FAILED
test module::cancel ... ignored, requires external service

failures:
  module::write
test result: FAILED. 1 passed; 1 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.01s
'''
        cases = parse_libtest(raw, "integration_test")
        self.assertEqual([case["outcome"] for case in cases], ["passed", "failed", "skipped"])
        self.assertEqual(cases[0]["source"]["file"], "lang/rust/tests/integration_test.rs")
        for bad in [raw + raw, raw.replace(b"1 passed", b"2 passed"), raw.replace(b"0 filtered", b"1 filtered"),
                    b"running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.0s\n"]:
            with self.assertRaises(ValueError):
                parse_libtest(bad, "integration_test")

    def lane(self, outcome="passed", **process):
        return {"lane": "driver", "cases": [{"framework": "junit", "suite": "roundtrip", "name": "integer", "source": {"file": "driver.py"}, "outcome": outcome}],
                "process": {"completed": True, "exit_code": 0, **process}}

    def test_matching_failures_and_skips_are_not_successful_assertions(self):
        for outcome in ["failed", "error", "skipped", "excluded", "invalid"]:
            report = compare_clients([self.lane(outcome)], [self.lane(outcome)], ["driver"])
            self.assertFalse(report["client_selection_passed"])
            self.assertEqual(len(report["unverified"]), 1)

    def test_missing_or_unselected_lanes_and_failed_processes_do_not_pass(self):
        good = [self.lane()]
        self.assertTrue(compare_clients(good, good, ["driver"])["client_selection_passed"])
        self.assertFalse(compare_clients(good, good, ["driver", "flight"])["client_selection_passed"])
        self.assertFalse(compare_clients(good, [self.lane(exit_code=1)], ["driver"])["client_selection_passed"])
        self.assertFalse(compare_clients(good, [self.lane(completed=False)], ["driver"])["client_selection_passed"])
        different = copy.deepcopy(good)
        different[0]["cases"][0]["name"] = "text"
        self.assertEqual(len(compare_clients(good, different, ["driver"])["missing"]), 2)
        report = compare_clients(good, good, ["driver"])
        self.assertFalse(report["assertion_ledger_reconciled"])
        self.assertFalse(report["complete_parity"])

    def test_same_display_name_from_different_sources_does_not_match(self):
        good = [self.lane()]
        changed = copy.deepcopy(good)
        changed[0]["cases"][0]["source"]["file"] = "substitute.py"
        self.assertEqual(len(compare_clients(good, changed, ["driver"])["missing"]), 2)

    def test_empty_required_lane_cannot_be_hidden_by_another_passing_lane(self):
        lanes = [self.lane(), {"lane": "empty", "cases": [], "process": {"completed": True, "exit_code": 0}}]
        self.assertFalse(compare_clients(lanes, lanes, ["driver", "empty"])["client_selection_passed"])


if __name__ == "__main__":
    unittest.main()
