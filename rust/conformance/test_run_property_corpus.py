import json
import unittest

from run_property_corpus import MANIFEST, plan_cases, summarize


class PropertyCorpusReceiptsTest(unittest.TestCase):
    def setUp(self):
        self.manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        self.case_name = "xtdb.block-boundary-test/delete-and-re-add-record-across-flush-boundaries"
        selected, families = plan_cases(self.manifest, "document-history", self.case_name, 42, 3, 16)
        self.plan = {"mode": "sweep", "selected": selected, "families": families,
                     "upstream_commit": self.manifest["upstream_commit"], "manifest_sha256": "manifest",
                     "native_library_sha256": "native"}
        self.case = selected[0]["case"]
        self.start = {"event": "start",
                      "identity": {"upstream_commit": self.manifest["upstream_commit"], "manifest_sha256": "manifest"},
                      "runtime": {"native_library_sha256": "native"}}

    def trial(self, number, outcome="passed", observations=("sql_query",)):
        return {"event": "trial", "case": self.case, "trial": number,
                "evidence": {"outcome": outcome, "observations": [{"kind": kind} for kind in observations]}}

    def terminal(self, passed, failed, unobserved):
        return {"event": "case_complete", "case": self.case, "planned_trials": 3,
                "passed_trials": passed, "failed_trials": failed, "unobserved_trials": unobserved}

    def test_denominators_are_pinned_by_family_and_exclude_unported_cases_from_sweep(self):
        selected, families = plan_cases(self.manifest, None, None, 42, None, 32)
        self.assertEqual(self.case, selected[0]["case"])
        self.assertEqual([(19, 15), (1, 1), (22, 22), (2, 0), (16, 16), (7, 0)],
                         [(row["pinned_vars"], row["implemented_vars"]) for row in families])
        self.assertTrue(all("unported" not in item["case"] for item in selected))
        with self.assertRaisesRegex(ValueError, "no implemented properties"):
            plan_cases(self.manifest, "vector-reader", None, 42, None, 32)

    def test_first_failure_retains_the_unobserved_remainder_of_its_own_case(self):
        rows = [self.start, self.trial(1), self.trial(2, "invariant_failed"),
                self.terminal(1, 1, 1), {"event": "complete"}]
        result = summarize(self.plan, rows, completed=True)
        self.assertTrue(result["execution_complete"])
        self.assertFalse(result["all_selected_passed"])
        self.assertEqual(1, result["families"][0]["unobserved_trials"])

    def test_interrupted_stream_retains_observed_trials_without_a_pass_receipt(self):
        result = summarize(self.plan, [self.start, self.trial(1)], completed=False)
        self.assertFalse(result["execution_complete"])
        self.assertFalse(result["all_selected_passed"])
        self.assertEqual(1, result["families"][0]["passed_trials"])
        self.assertEqual(2, result["families"][0]["unobserved_trials"])

    def test_wrong_artifact_and_claims_without_native_observations_are_rejected(self):
        for start, first in [(dict(self.start, runtime={"native_library_sha256": "other"}), self.trial(1)),
                             (self.start, self.trial(1, observations=("flush",)))]:
            with self.subTest(start=start, first=first):
                rows = [start, first, self.trial(2), self.trial(3), self.terminal(3, 0, 0), {"event": "complete"}]
                result = summarize(self.plan, rows, completed=True)
                self.assertFalse(result["execution_complete"])
                self.assertFalse(result["all_selected_passed"])
                self.assertEqual(3, result["families"][0]["unobserved_trials"])

    def test_replay_completion_has_no_generated_trial_pass_counts(self):
        plan = dict(self.plan, mode="replay")
        replay = {"event": "replay_complete", "case": self.case, "reproduced": True}
        result = summarize(plan, [self.start, replay, {"event": "complete"}], completed=True)
        self.assertTrue(result["execution_complete"])
        self.assertEqual(replay, result["replay"])
        self.assertNotIn("families", result)
        self.assertNotIn("all_selected_passed", result)
