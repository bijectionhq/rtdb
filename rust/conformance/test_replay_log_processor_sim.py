from copy import deepcopy
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import replay_log_processor_sim as replay
from test_run_log_processor_sim import tape


class ProjectedSimulationTest(unittest.TestCase):
    def test_projects_exact_boot_and_authored_edge_sequences(self):
        original = tape()
        replay.reference.check_tape(original, original["unique_id"], -123)
        projected = replay.project(original, "a" * 64, "b" * 64)
        self.assertEqual(len(projected["boot"]), 11)
        self.assertEqual(projected["seed"], -123)
        self.assertEqual(projected["rows_per_block"], 16)
        source = next(frame for frame in original["events"] if frame["kind"] == "enqueue" and frame["fields"][0] == "src")
        self.assertEqual(projected["inputs"][0], {"sequence": source["sequence"],
                                                  "event": {"kind": "flush", "offset": 0, "time": [100, 0]}})
        first, applied = projected["inputs"][1:3]
        self.assertEqual(first["event"], {"kind": "external_start", "action": 0, "token": "00000000",
                                           "tx_id": 0, "time": [200, 0],
                                           "documents": ["01234567-89ab-cdef-0123-456789abcdef"]})
        self.assertTrue(applied["event"]["committed"])
        self.assertFalse(projected["inputs"][-1]["event"]["committed"])
        for section in ("boot", "inputs"):
            self.assertEqual([frame["sequence"] for frame in projected[section]],
                             sorted(frame["sequence"] for frame in projected[section]))

    def test_early_source_append_remains_before_boot_in_the_projection(self):
        original = tape()
        frames = original["events"]
        source = next(frame for frame in frames if frame["kind"] == "enqueue" and frame["fields"][0] == "src")
        frames.remove(source)
        frames.insert(2, source)
        for index, frame in enumerate(frames):
            frame["sequence"] = index
        replay.reference.check_tape(original, original["unique_id"], -123)
        result = replay.project(original, "a" * 64, "b" * 64)
        self.assertLess(result["inputs"][0]["sequence"], result["boot"][0]["sequence"])

    def test_unrecorded_assigned_key_fails_projection(self):
        original = tape()
        observer = next(frame for frame in original["events"] if frame["kind"] == "external_writer")
        original["events"].remove(observer)
        with self.assertRaisesRegex(ValueError, "external receipt differs from writer"):
            replay.project(original, "a" * 64, "b" * 64)

    def test_archived_members_cannot_escape_or_misstate_retained_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            for invalid in ("../worker.jsonl", "/worker.jsonl", "inputs/./worker.jsonl"):
                with self.subTest(path=invalid), self.assertRaisesRegex(ValueError, "invalid archived path"):
                    replay.owned_file(root, invalid)
            worker = root / "worker.jsonl"
            worker.write_bytes(b"evidence")
            receipt = {"path": worker.name, "bytes": 8, "sha256": replay.digest(worker)}
            self.assertEqual(replay.verify_file(root, receipt, "path"), worker)
            worker.write_bytes(b"different")
            with self.assertRaisesRegex(ValueError, "differs from its receipt"):
                replay.verify_file(root, receipt, "path")

    def test_unreadable_or_nonregular_runtime_members_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            nested = root / "nested"
            nested.mkdir()
            iterdir = Path.iterdir

            def deny_nested(path):
                if path == nested:
                    raise PermissionError("cannot enumerate nested runtime")
                return iterdir(path)

            with patch.object(Path, "iterdir", deny_nested), self.assertRaisesRegex(PermissionError, "nested runtime"):
                list(replay.reference.runtime_tree(root))
            (root / "redirect").symlink_to(nested, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "non-regular runtime member"):
                list(replay.reference.runtime_tree(root))

    def test_runtime_origin_uses_one_posix_root(self):
        self.assertEqual(replay.canonical_path("/tmp/runtime"), Path("/tmp/runtime"))
        with self.assertRaisesRegex(ValueError, "invalid canonical JVM path"):
            replay.canonical_path("//tmp/runtime")

    def test_relocated_capture_replays_aliases_and_external_directory_members(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory).resolve()
            run = parent / "capture"
            outside = parent / "jdk-and-dependencies"
            tree = run / "reference"
            recorder = tree / "build/rtdb-conformance/recorder"
            test = tree / "core/build/classes/kotlin/test"
            processor = tree / "core/build/classes/kotlin/main"
            service = run / "adapter/jvm-services"
            java = outside / "worker-java"
            java_home = outside / "reached-jdk"
            modules = outside / "reached-modules"
            agent = outside / "reached-agent.jar"
            dependency = outside / "expanded-dependency"
            outside_member = dependency / "nested/archive.dat"
            classes = {name: test for name in replay.reference.CLOSURE_CLASSES}
            classes["xtdb.indexer.LogProcessor"] = processor
            classes["rtdb.conformance.JunitRecorder"] = recorder
            paths = [java, modules, agent, outside_member, service / "META-INF/services/listener"]
            paths.extend(location / (name.replace(".", "/") + ".class") for name, location in classes.items())
            for index, file in enumerate(paths):
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_bytes(f"owned fixture {index}".encode())
            (run / "artifacts").mkdir()
            bin_directory = java_home / "bin"
            bin_directory.mkdir()
            (bin_directory / "java").symlink_to(java)
            lib_directory = java_home / "lib"
            lib_directory.mkdir()
            (lib_directory / "modules").symlink_to(modules)
            jdk_alias = outside / "jdk-alias"
            jdk_alias.symlink_to(java_home, target_is_directory=True)
            agent_alias = outside / "agent-alias.jar"
            agent_alias.symlink_to(agent)
            dependency_alias = outside / "dependency-alias"
            dependency_alias.symlink_to(dependency, target_is_directory=True)
            second_alias = outside / "second-dependency-alias"
            second_alias.symlink_to(dependency, target_is_directory=True)
            test_alias = outside / "test-alias"
            test_alias.symlink_to(test, target_is_directory=True)
            selection = {"classpath": [str(path) for path in (recorder, test_alias, processor, service, dependency_alias, second_alias)],
                         "test_class_directories": [str(test_alias)]}
            worker = {"classes": {name: str(location) for name, location in classes.items()},
                      "java_home": str(java_home), "java_home_request": str(jdk_alias),
                      "java_executable": str(java), "java_executable_request": str(jdk_alias / "bin/java"),
                      "java_modules": str(modules),
                      "jvm_args": [f"-javaagent:{agent_alias}=fixture"]}
            for overlap in (parent, run / "artifacts"):
                disputed = dict(selection, classpath=selection["classpath"] + [str(overlap)])
                with self.subTest(overlap=overlap), self.assertRaisesRegex(ValueError, "overlaps its own artifact directory"):
                    replay.reference.retain_runtime(run, tree, disputed, worker)
            self.assertEqual(list((run / "artifacts").iterdir()), [])

            runtime = replay.reference.retain_runtime(run, tree, selection, worker)
            self.assertEqual(runtime["requested_roots"]["classpath"][-2:], [
                {"requested": str(alias), "canonical": str(dependency)} for alias in (dependency_alias, second_alias)
            ])
            root = next(entry for entry in runtime["roots"] if entry["path"] == str(dependency))
            self.assertEqual(root, {"path": str(dependency), "kind": "directory", "members": ["nested/archive.dat"]})

            archive = parent / "relocated"
            run.rename(archive)
            shutil.rmtree(outside)
            capture_paths = replay.reference.bounded_json(archive / "runtime-paths.json")
            replay.verify_runtime(archive, run, selection, worker, runtime, capture_paths)

            wrong_alias = deepcopy(runtime)
            wrong_alias["requested_roots"]["classpath"][-1]["canonical"] = None
            with self.assertRaisesRegex(ValueError, "aliases differ from their capture-time receipt"):
                replay.verify_runtime(archive, run, selection, worker, wrong_alias, capture_paths)

            inconsistent_alias = deepcopy(runtime)
            inconsistent_alias["requested_roots"]["test_class_directories"][0]["canonical"] = str(processor)
            with self.assertRaisesRegex(ValueError, "one JVM runtime request has inconsistent resolutions"):
                replay.verify_runtime(archive, run, selection, worker, inconsistent_alias, inconsistent_alias["requested_roots"])

            wrong_modules = deepcopy(runtime)
            wrong_modules["requested_roots"]["modules"][0]["canonical"] = str(agent)
            wrong_modules["roots"] = [entry for entry in wrong_modules["roots"] if entry["path"] != str(modules)]
            wrong_modules["artifacts"] = [item for item in wrong_modules["artifacts"] if item["path"] != str(modules)]
            with self.assertRaisesRegex(ValueError, "JDK modules differ from the archived worker observation"):
                replay.verify_runtime(archive, run, selection, worker, wrong_modules, wrong_modules["requested_roots"])

            wrong_member = deepcopy(runtime)
            entry = next(root for root in wrong_member["roots"] if root["path"] == str(dependency))
            entry["members"] = []
            wrong_member["artifacts"] = [item for item in wrong_member["artifacts"] if item["path"] != str(outside_member)]
            with self.assertRaisesRegex(ValueError, "runtime directory closure differs"):
                replay.verify_runtime(archive, run, selection, worker, wrong_member, capture_paths)

            orphan_root = deepcopy(wrong_member)
            for alias in orphan_root["requested_roots"]["classpath"][-2:]:
                alias["canonical"] = None
            orphan_root["roots"] = [entry for entry in orphan_root["roots"] if entry["path"] != str(dependency)]
            with self.assertRaisesRegex(ValueError, "physical JVM runtime archive contains missing or orphaned entries"):
                replay.verify_runtime(archive, run, selection, worker, orphan_root, orphan_root["requested_roots"])

            published = next(item for item in runtime["artifacts"] if item["path"] == str(outside_member))
            (archive / published["retained"]).write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "differs from its receipt"):
                replay.verify_runtime(archive, run, selection, worker, runtime, capture_paths)

    def test_validated_jvm_tape_and_marker_are_retained_with_the_same_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory).resolve()
            original_tape = run / "tape.json"
            original_marker = run / "marker.json"
            value = tape()
            replay.reference.write_json(original_tape, value)
            tape_file = replay.reference.RecordedFile.read(original_tape)
            report = replay.reference.check_tape(json.loads(tape_file.data), value["unique_id"], -123)
            replay.reference.write_json(original_marker, {"observation": report, "tape_sha256": tape_file.sha256})
            marker_file = replay.reference.RecordedFile.read(original_marker)
            for recorded in (tape_file, marker_file):
                with self.subTest(file=recorded.path.name):
                    recorded.path.write_bytes(b"{}")
                    wrong = replay.reference.RecordedFile.read(recorded.path).receipt(run)
                    with self.assertRaisesRegex(ValueError, "receipt differs from the validated bytes"):
                        replay.validate_recorded_file(run, wrong, recorded)
                    output = run / ("retained-" + recorded.path.name)
                    replay.retain_validated(recorded, output)
                    self.assertEqual(output.read_bytes(), recorded.data)
                    self.assertEqual(replay.digest(output), recorded.sha256)
