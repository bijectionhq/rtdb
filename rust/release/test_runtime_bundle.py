import json
import tempfile
import unittest
from pathlib import Path

from runtime_bundle import merge_runtime


def source(root, name, task):
    directory = root / name
    directory.mkdir()
    (directory / "requested-lanes.json").write_text(json.dumps({"lanes": [{"project": ":", "task": task, "enabled": True}]}))
    (directory / "_-discovery.json").write_text('{"descriptors": []}')
    selected = directory / task.replace(":", "_")
    selected.mkdir()
    (selected / "selection.json").write_text(json.dumps({"project": ":", "task": task}))
    return directory


class RuntimeSelectionBundle(unittest.TestCase):
    def test_all_original_task_identities_survive_aggregation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inputs = [source(root, "unit", ":test"), source(root, "property", ":property-test")]
            output = merge_runtime(root / "bundle", inputs)
            requested = json.loads((output / "requested-lanes.json").read_text())
            self.assertEqual({":test", ":property-test"}, {lane["task"] for lane in requested["lanes"]})
            self.assertEqual(2, len(list(output.rglob("selection.json"))))

    def test_duplicate_task_artifacts_cannot_be_credited_twice(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inputs = [source(root, "first", ":test"), source(root, "second", ":test")]
            with self.assertRaises(ValueError):
                merge_runtime(root / "bundle", inputs)


if __name__ == "__main__":
    unittest.main()
