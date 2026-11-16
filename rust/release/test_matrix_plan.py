import copy
import json
import unittest
from pathlib import Path

from matrix_plan import expand, summarize, validate_bindings


class ReleaseSelections(unittest.TestCase):
    def setUp(self):
        self.matrix = json.loads((Path(__file__).parent / "matrix.json").read_text())

    def test_all_platform_package_variants_and_original_scheduled_directories_remain_required(self):
        lanes = expand(self.matrix)
        packages = [lane for lane in lanes if lane["runner"] == "package"]
        self.assertEqual(24, len(packages))
        self.assertEqual({"linux/amd64", "linux/arm64"}, {lane["platform"] for lane in lanes})
        directories = [lane for lane in lanes if lane["runner"] == "sql-logic" and "directory" in lane["selection"]]
        self.assertEqual(2 * 8 * 9, len(directories))
        self.assertFalse(any(lane["selection"].get("directory") == "random/expr/" for lane in directories))
        properties = [lane for lane in lanes if lane["runner"] == "gradle" and lane["selection"]["task"] == "property-test"]
        self.assertTrue(all(lane["selection"]["iterations"] == 500 and lane["selection"]["simulation_iterations"] == 1000 for lane in properties))

    def test_features_and_missing_lanes_cannot_be_inferred_from_supplied_receipts(self):
        lanes = expand(self.matrix)
        bindings = [{"id": lane["id"], "runner": lane["runner"], "selection": lane["selection"], "inputs": {}} for lane in lanes]
        self.assertEqual(len(lanes), len(validate_bindings(lanes, bindings)))
        with self.assertRaises(ValueError):
            validate_bindings(lanes, bindings[:-1])
        changed = copy.deepcopy(bindings)
        changed[0]["selection"]["task"] = "different-task"
        with self.assertRaises(ValueError):
            validate_bindings(lanes, changed)
        self.assertFalse(summarize(lanes, [])["release_matrix_verified"])

    def test_success_requires_joined_process_and_retained_artifacts(self):
        lanes = expand(self.matrix)[:1]
        receipt = {**lanes[0], "verified": True, "process_joined": True, "artifacts": [{"path": "result.json", "sha256": "a" * 64}]}
        self.assertTrue(summarize(lanes, [receipt])["release_matrix_verified"])
        for field, value in (("verified", False), ("process_joined", False), ("artifacts", [])):
            self.assertFalse(summarize(lanes, [{**receipt, field: value}])["release_matrix_verified"])


if __name__ == "__main__":
    unittest.main()
