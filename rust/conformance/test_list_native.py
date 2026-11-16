import copy
import json
import unittest
from pathlib import Path

from list_native import artifacts, parse_listing, validate_selection


def artifact(name="example", features=()):
    return {"reason": "compiler-artifact", "package_id": "path+file:///workspace/example#example@0.1.0",
            "target": {"name": name, "kind": ["test"]}, "features": list(features),
            "profile": {"test": True}, "executable": "/workspace/target/" + name}


def messages(records):
    return "\n".join(json.dumps(record) for record in records)


def selection(targets):
    return {"schema_version": 1, "workspace": str(Path("/workspace").resolve()),
            "implementation_revision": "revision", "configuration": "local",
            "command": ["cargo", "test", "--workspace", "--no-run", "--message-format=json"],
            "targets": [{key: value[key] for key in ("package_id", "target", "features")} for value in targets]}


class NativeInventoryTests(unittest.TestCase):
    def test_only_a_terminal_successful_build_can_supply_artifacts(self):
        value = artifact()
        success = {"reason": "build-finished", "success": True}
        self.assertEqual(artifacts(messages([value, success])), [value])
        for records in [[], [value], [value, {"reason": "build-finished", "success": False}],
                        [value, success, value], [value, success, success], [success]]:
            with self.subTest(records=records), self.assertRaises(ValueError):
                artifacts(messages(records))

    def test_all_declared_targets_and_features_must_have_compiler_artifacts(self):
        observed = [artifact("one"), artifact("two", ["remote", "tls"])]
        expected = selection(observed)
        validate_selection(expected, observed, Path("/workspace"), "revision", "local")
        for actual in [observed[:1], observed + [artifact("extra")], [observed[0], artifact("two", ["tls"])]]:
            with self.subTest(actual=actual), self.assertRaises(ValueError):
                validate_selection(expected, actual, Path("/workspace"), "revision", "local")

    def test_duplicate_artifact_identities_and_changed_selections_are_rejected(self):
        value = artifact()
        for changed in [{**value, "features": ["tls"]}, {**value, "target": {"name": "example", "kind": ["bin"]}}]:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                artifacts(messages([value, changed, {"reason": "build-finished", "success": True}]))
        with self.assertRaises(ValueError):
            validate_selection(selection([value, value]), [value], Path("/workspace"), "revision", "local")

    def test_workspace_revision_configuration_and_command_provenance_are_required(self):
        observed = [artifact()]
        for field, value in [("workspace", "/different"), ("implementation_revision", "old"),
                             ("configuration", "remote"), ("command", []), ("targets", []), ("schema_version", 2)]:
            changed = copy.deepcopy(selection(observed))
            changed[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_selection(changed, observed, Path("/workspace"), "revision", "local")

    def test_binary_and_library_tests_with_the_same_name_have_different_counterparts(self):
        binary = parse_listing("lifecycle: test\n", "rtdb-server", "rtdb", ["bin"])
        library = parse_listing("lifecycle: test\n", "rtdb-server", "rtdb_server", ["lib"])
        self.assertEqual(binary[0]["counterpart"], "rtdb-server::rtdb::lifecycle")
        self.assertEqual(library[0]["counterpart"], "rtdb-server::lifecycle")


if __name__ == "__main__":
    unittest.main()
