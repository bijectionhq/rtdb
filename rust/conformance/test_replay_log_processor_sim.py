import argparse
from copy import deepcopy
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import replay_log_processor_sim as replay
from test_run_log_processor_sim import tape


def joined_execution():
    return {"summary": {"captured_replica_tip": 1, "native_replica": [
        {"offset": 0, "term_admitted": True, "message": {"kind": "noop"}},
        {"offset": 1, "term_admitted": True, "message": {"kind": "transaction", "tx_id": 29}},
    ], "joined_live_index": {"replica_tip": 2, "last_applied_tx_id": 29,
        "actors": [{"actor": actor, "tx_id": 29} for actor in range(3)],
        "replica_suffix": [{"offset": 2, "term_admitted": False, "message": {"kind": "transaction", "tx_id": 99}}]}}}


class ProjectedSimulationTest(unittest.TestCase):
    def test_legacy_capture_keeps_its_original_replay_scope(self):
        original = tape()
        projected = replay.project(original, "a" * 64, "b" * 64, replay.reference.C84_REFERENCE)
        self.assertEqual(projected["schema_version"], 1)
        self.assertEqual(replay.match_live_index_assertion(original, {}), [])

    def test_projection_retains_the_reference_and_rejects_other_commits(self):
        original = tape(schema_version=replay.reference.LIVE_INDEX_TAPE_SCHEMA)
        revision = replay.reference.UPSTREAM_COMMIT
        projected = replay.project(original, "a" * 64, "b" * 64, revision)
        self.assertEqual(projected["reference_revision"], revision)
        with self.assertRaisesRegex(ValueError, "unsupported upstream simulation reference"):
            replay.project(original, "a" * 64, "b" * 64, "f" * 40)

    def test_matches_original_live_index_operands_to_the_joined_native_replica(self):
        original = tape(schema_version=replay.reference.LIVE_INDEX_TAPE_SCHEMA)
        replay.reference.check_tape(original, original["unique_id"], -123)
        projected = replay.project(original, "a" * 64, "b" * 64, replay.reference.C84_REFERENCE)
        legacy = replay.project(tape(), "a" * 64, "b" * 64, replay.reference.C84_REFERENCE)
        self.assertEqual(projected["schema_version"], 2)
        self.assertEqual(projected["boot"], legacy["boot"])
        self.assertEqual(projected["inputs"], legacy["inputs"])
        execution = joined_execution()
        match, = replay.match_live_index_assertion(original, execution)
        self.assertEqual((match["source"], match["line"], match["method"], match["unique_id"]), (
            replay.reference.FIXTURE, 461, "assertLiveIndexesCaughtUp", original["unique_id"],
        ))
        self.assertEqual(match["native"], [{"actor": actor, "expected_tx_id": 29, "actual_tx_id": 29} for actor in range(3)])
        self.assertEqual([row["sequence"] for row in match["reference"]],
                         [frame["sequence"] for frame in original["events"][-4:-1]])
        for defect, failure in (
            ("missing", "lacks the three actor operands"),
            ("duplicate actor", "native joined live index differs"),
            ("stale index", "native joined live index differs"),
            ("fenced expectation", "expectation differs from the admitted replica"),
            ("short replica", "does not cover its complete prefix"),
            ("different cohort", "differs from the original JVM assertion"),
        ):
            with self.subTest(defect=defect):
                bad = joined_execution()
                summary = bad["summary"]
                joined = summary["joined_live_index"]
                if defect == "missing":
                    joined["actors"].pop()
                elif defect == "duplicate actor":
                    joined["actors"][2]["actor"] = 1
                elif defect == "stale index":
                    joined["actors"][2]["tx_id"] = 28
                elif defect == "fenced expectation":
                    joined["last_applied_tx_id"] = 99
                elif defect == "short replica":
                    joined["replica_suffix"].clear()
                else:
                    summary["native_replica"][1]["message"]["tx_id"] = 28
                    joined["last_applied_tx_id"] = 28
                    for actor in joined["actors"]:
                        actor["tx_id"] = 28
                with self.assertRaisesRegex(ValueError, failure):
                    replay.match_live_index_assertion(original, bad)

    def test_native_receipts_require_identical_bytes_and_the_projected_version(self):
        for defect, failure in (
            (None, None),
            ("late replacement", "evidence changed before publication"),
            ("bool as int", "execution receipt bytes differ from stdout"),
            ("int as float", "execution receipt bytes differ from stdout"),
            ("legacy receipt", "receipt differs from its observed inputs"),
            ("wrong reference", "receipt differs from its observed inputs"),
        ):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                capture = root / "capture"
                capture.mkdir()
                original = tape(schema_version=replay.reference.LIVE_INDEX_TAPE_SCHEMA)
                replay.reference.check_tape(original, original["unique_id"], -123)
                tape_path = capture / "tape.json"
                marker_path = capture / "marker.json"
                replay.write_json(tape_path, original)
                revision = replay.reference.UPSTREAM_COMMIT
                replay.write_json(marker_path, {"original": original["unique_id"], "reference_revision": revision})
                tape_file = replay.reference.RecordedFile.read(tape_path)
                marker_file = replay.reference.RecordedFile.read(marker_path)
                binary = root / "binary"
                binary.write_bytes(b"native")
                output = root / "output"
                execution_path = output / "native" / "execution.json"

                def worker_bytes(value):
                    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

                def completed(command, _directory, stdout, stderr, _timeout):
                    execution_path.parent.mkdir()
                    execution = joined_execution()
                    execution.update({"schema_version": 1 if defect == "legacy receipt" else 2,
                        "scope": replay.SCOPE, "execution_complete": True,
                        "reference_revision": replay.reference.C84_REFERENCE if defect == "wrong reference" else revision,
                        "binary_sha256": replay.digest(binary), "input_sha256": replay.digest(command[2]),
                        "reference_capture_sha256": marker_file.sha256, "tape_sha256": tape_file.sha256,
                        "unique_id": original["unique_id"], "seed": original["seed"],
                        "artifact_revision": "clean", "source_dirty": False})
                    stdout_value = dict(execution)
                    if defect == "bool as int":
                        stdout_value["execution_complete"] = 1
                    elif defect == "int as float":
                        stdout_value["seed"] = float(execution["seed"])
                    execution_bytes, stdout_bytes = worker_bytes(execution), worker_bytes(stdout_value)
                    if defect in ("bool as int", "int as float"):
                        self.assertEqual(json.loads(execution_bytes), json.loads(stdout_bytes))
                        self.assertNotEqual(execution_bytes, stdout_bytes)
                    execution_path.write_bytes(execution_bytes)
                    stdout.write_bytes(stdout_bytes)
                    stderr.write_bytes(b"")
                    return {"completed": True, "exit_code": 0}

                original_match = replay.match_live_index_assertion

                def after_match(tape, execution):
                    matched = original_match(tape, execution)
                    if defect == "late replacement":
                        execution_path.write_bytes(execution_path.read_bytes() + b"\n")
                    return matched

                args = argparse.Namespace(capture=capture, output=output, native=binary, phase_two=True, timeout=1)
                with (patch.object(replay, "require_phase_two"),
                      patch.object(replay.reference, "clean_checkout", return_value="clean"),
                      patch.object(replay, "read_capture", return_value=(original, tape_file, marker_file)),
                      patch.object(replay, "run_process", side_effect=completed),
                      patch.object(replay, "match_live_index_assertion", side_effect=after_match)):
                    if failure is not None:
                        with self.assertRaisesRegex(ValueError, failure):
                            replay.replay(args)
                        self.assertFalse((output / "result.json").exists())
                    else:
                        result = json.loads(replay.replay(args).read_bytes())
                        self.assertEqual(result["reference_revision"], revision)
                        self.assertEqual(result["matched_assertions"][0]["line"], 461)
                        record = next(item for item in result["artifacts"] if item["path"] == "native/execution.json")
                        self.assertEqual(record["sha256"], replay.digest(execution_path))

    def test_projects_exact_boot_and_authored_edge_sequences(self):
        original = tape()
        replay.reference.check_tape(original, original["unique_id"], -123)
        projected = replay.project(original, "a" * 64, "b" * 64, replay.reference.C84_REFERENCE)
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
        result = replay.project(original, "a" * 64, "b" * 64, replay.reference.C84_REFERENCE)
        self.assertLess(result["inputs"][0]["sequence"], result["boot"][0]["sequence"])

    def test_unrecorded_assigned_key_fails_projection(self):
        original = tape()
        observer = next(frame for frame in original["events"] if frame["kind"] == "external_writer")
        original["events"].remove(observer)
        with self.assertRaisesRegex(ValueError, "external receipt differs from writer"):
            replay.project(original, "a" * 64, "b" * 64, replay.reference.C84_REFERENCE)

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
