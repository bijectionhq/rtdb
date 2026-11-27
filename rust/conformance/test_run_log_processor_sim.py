import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import run_log_processor_sim as sim


def tape(unique="[engine:junit-jupiter]/[test-template-invocation:#1]", *, schema_version=1):
    events = []

    def emit(kind, *fields):
        events.append({"sequence": len(events), "kind": kind, "fields": list(fields)})

    def append(stream, offset, message):
        actor = 0 if stream == "replica" else None
        emit("enqueue", stream, actor, 0, offset, [100, offset], message)
        emit("durable", stream, offset)
        emit("receipt", stream, offset)
        if actor is not None:
            emit("replica_ack", actor, offset)

    emit("case_started")
    docs = [["01234567-89ab-cdef-0123-456789abcdef"]] + [None] * 29
    emit("scenario", 16, 30, 20, docs)
    emit("pick", 2, 1)
    for actor in range(3):
        emit("tail_open", "replica", actor, actor, 0)
        if actor != 0:
            emit("claim_refused", actor, 1, 0, None)
    append("replica", 0, ["noop", 1, 0, None])
    for actor in range(3):
        emit("delivery", "replica", actor, 0, 1)
    emit("tail_open", "src", 3, None, 0)
    emit("assignment", None, 0)
    for step in range(20):
        emit("step", step)
        emit("role_sample", 1, False)
        emit("role_sample", 2, False)
        emit("choice", step, "flush" if step == 0 else "tip")
        if step == 0:
            append("src", 0, ["flush", None])
            emit("delivery", "src", 3, 0, 1)
        else:
            for actor in range(3):
                emit("tip_requested", "replica", actor)
            emit("tip_round_finished", step)
            for actor in range(3):
                emit("tip", "replica", actor, 1)
    for index in range(30):
        token = index.to_bytes(4, "big").hex()
        time = [200, index]
        emit("external_start", index, token)
        emit("external_writer", index, index, time)
        append("replica", index + 1, ["transaction", 1, index + 1, index, time, docs[index] is not None, token, 0])
        for actor in range(3):
            emit("delivery", "replica", actor, index + 1, index + 2)
        emit("external_applied", index, index, time, docs[index] is not None)
    emit("external_settled")
    emit("replica_delivered", 31, 31, [[actor, 31] for actor in range(3)])
    emit("await_tx", 29)
    emit("nodes_caught_up")
    emit("idle", [[actor, False, 19] for actor in range(3)])
    for actor in (1, 2, 0, 1, 2):
        emit("role_sample", actor, actor == 0)
    emit("quiescence", False, 1)
    for actor in range(3):
        emit("tail_closed", "replica", actor, 31, False, 19)
    emit("tail_closed", "src", 3, 1, False, 0)
    emit("joined")
    if schema_version == sim.LIVE_INDEX_TAPE_SCHEMA:
        for actor in range(3):
            emit(sim.LIVE_INDEX_ASSERTION, actor, 29, 29)
    emit("assertions_finished")
    return {"schema_version": schema_version, "unique_id": unique, "seed": -123, "overflow": False,
            "assertions_finished": True, "events": events}


class LogProcessorReferenceTest(unittest.TestCase):
    def test_exact_template_task_keeps_method_with_spaces_in_one_argument(self):
        command = sim.gradle_command(Path("/reference"), Path("/run"))
        self.assertEqual(command[-3:], [":xtdb-core:property-test", "--tests", f"{sim.TEST_CLASS}.{sim.METHOD}"])
        self.assertIn("-PsimulationIterations=1", command)
        self.assertEqual(command[command.index("--init-script") + 1], Path("/run/adapter/inventory.init.gradle"))
        self.assertIn("-Prtdb.adapterRoot=/run/adapter", command)

    def test_runtime_inventory_requires_the_owned_adapter_service_root(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory).resolve()
            tree = run / "reference"
            compiled = tree / "build/rtdb-conformance/recorder"
            compiled.mkdir(parents=True)
            external = run / "external-services"
            external.mkdir()
            java = external / "bin/java"
            agent = external / "watchdog.jar"
            modules = external / "lib/modules"
            for path in (java, agent, modules):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"fixture")
            with self.assertRaisesRegex(ValueError, "owned adapter services"):
                sim.retain_runtime(run, tree, {
                    "classpath": [str(compiled), str(external)], "test_class_directories": [],
                }, {
                    "java_home_request": str(external), "java_executable_request": str(java),
                    "jvm_args": [f"-javaagent:{agent}"],
                })

    def test_overlay_requires_the_pinned_input_and_a_unique_anchor(self):
        self.assertEqual(sim.replace_once("original anchor", "anchor", "anchor + observer"), "original anchor + observer")
        for source in ("absent", "anchor twice anchor"):
            with self.subTest(source=source), self.assertRaisesRegex(ValueError, "anchor is missing or repeated"):
                sim.replace_once(source, "anchor", "observer")
        with self.assertRaisesRegex(ValueError, "differs from the pinned overlay input"):
            sim.derived_source(sim.FIXTURE, b"modified", None)

    def test_original_phase_order_checks_durable_source_and_applied_links(self):
        observed = tape()
        verified = sim.check_tape(observed, observed["unique_id"], -123)
        self.assertEqual(verified, {"seed": -123, "events": len(observed["events"]), "actions": 30, "steps": 20})
        for tag, index, update, failure in (
            ("claim_refused", 3, 0, "invalid pre-enqueue"),
            ("durable", 1, 2, "durable is out of order"),
            ("delivery", 3, 2, "outside its durable prefix"),
            ("external_applied", 1, 11, "differs from its original writer or durable record"),
        ):
            with self.subTest(tag=tag):
                invalid = tape()
                frame = next(frame for frame in invalid["events"] if frame["kind"] == tag)
                frame["fields"][index] = update
                with self.assertRaisesRegex(ValueError, failure):
                    sim.check_tape(invalid, invalid["unique_id"], -123)

    def test_live_index_assertion_requires_each_original_operand_after_join(self):
        observed = tape(schema_version=sim.LIVE_INDEX_TAPE_SCHEMA)
        expected = [[actor, 29, 29] for actor in range(3)]
        self.assertEqual(sim.check_tape(observed, observed["unique_id"], -123)[sim.LIVE_INDEX_ASSERTION], expected)
        for defect in ("missing", "early", "repeated actor", "wrong expectation", "stale actual", "legacy injection"):
            with self.subTest(defect=defect):
                attempt = tape(schema_version=sim.LIVE_INDEX_TAPE_SCHEMA)
                frames = attempt["events"]
                if defect == "missing":
                    del frames[-2]
                elif defect == "early":
                    frames[-5], frames[-4] = frames[-4], frames[-5]
                elif defect == "repeated actor":
                    frames[-2]["fields"][0] = 1
                elif defect == "wrong expectation":
                    frames[-2]["fields"][1:] = [28, 28]
                elif defect == "stale actual":
                    frames[-2]["fields"][2] = 28
                else:
                    attempt["schema_version"] = 1
                for sequence, frame in enumerate(frames):
                    frame["sequence"] = sequence
                with self.assertRaisesRegex(ValueError, "live-index assertion"):
                    sim.check_tape(attempt, attempt["unique_id"], -123)

    def test_replica_drain_rejects_missing_or_late_delivery(self):
        for absent in (True, False):
            with self.subTest(absent=absent):
                invalid = tape()
                frames = invalid["events"]
                if absent:
                    frames[:] = [frame for frame in frames if frame["kind"] != "tip"
                                 and not (frame["kind"] == "delivery" and frame["fields"][0] == "replica")]
                    for frame in frames:
                        if frame["kind"] == "tail_closed" and frame["fields"][0] == "replica":
                            frame["fields"][2] = 0
                else:
                    last_delivery = next(frame for frame in reversed(frames)
                                         if frame["kind"] == "delivery" and frame["fields"][0] == "replica")
                    frames.remove(last_delivery)
                    drain = next(index for index, frame in enumerate(frames) if frame["kind"] == "replica_delivered")
                    frames.insert(drain + 1, last_delivery)
                for index, frame in enumerate(frames):
                    frame["sequence"] = index
                with self.assertRaisesRegex(ValueError, "replica drain checkpoint differs"):
                    sim.check_tape(invalid, invalid["unique_id"], -123)

    def test_tip_choices_and_idle_reject_omitted_notifications_or_observations(self):
        for event, failure in (("tip_requested", "tip choice omitted"), ("tip", "idle checkpoint differs")):
            with self.subTest(event=event):
                invalid = tape()
                invalid["events"] = [frame for frame in invalid["events"] if frame["kind"] != event]
                for index, frame in enumerate(invalid["events"]):
                    frame["sequence"] = index
                with self.assertRaisesRegex(ValueError, failure):
                    sim.check_tape(invalid, invalid["unique_id"], -123)

    def test_one_missing_early_tip_cannot_hide_behind_a_later_request(self):
        invalid = tape()
        frames = invalid["events"]
        first = next(frame for frame in frames if frame["kind"] == "tip")
        frames.remove(first)
        for index, frame in enumerate(frames):
            frame["sequence"] = index
        with self.assertRaisesRegex(ValueError, "idle checkpoint differs.*empty-tip counts"):
            sim.check_tape(invalid, invalid["unique_id"], -123)

    def test_tail_closure_requires_the_observed_empty_tip_count(self):
        invalid = tape()
        close = next(frame for frame in invalid["events"] if frame["kind"] == "tail_closed")
        close["fields"][-1] -= 1
        with self.assertRaisesRegex(ValueError, "tail closed.*empty-tip count"):
            sim.check_tape(invalid, invalid["unique_id"], -123)

    def test_quiescence_role_result_must_match_the_incumbent_and_refused_samples(self):
        for actor in (0, 1, 2):
            with self.subTest(actor=actor):
                invalid = tape()
                sample = next(frame for frame in reversed(invalid["events"])
                              if frame["kind"] == "role_sample" and frame["fields"][0] == actor)
                sample["fields"][1] = not sample["fields"][1]
                with self.assertRaisesRegex(ValueError, "quiescence role aggregate contradicts"):
                    sim.check_tape(invalid, invalid["unique_id"], -123)

    def test_swapping_incumbent_and_refused_count_samples_cannot_preserve_quiescence(self):
        for refused in (1, 2):
            with self.subTest(refused=refused):
                invalid = tape()
                for actor in (0, refused):
                    sample = next(frame for frame in reversed(invalid["events"])
                                  if frame["kind"] == "role_sample" and frame["fields"][0] == actor)
                    sample["fields"][1] = not sample["fields"][1]
                with self.assertRaisesRegex(ValueError, "refused peer role changed"):
                    sim.check_tape(invalid, invalid["unique_id"], -123)

    def test_trace_requires_first_last_and_matching_invocation(self):
        for mark, failure in (
            ("first", "missing its original entry"),
            ("last", "missing its original entry"),
            ("overflow", "observer is incomplete"),
            ("invocation", "different invocation"),
        ):
            with self.subTest(mark=mark):
                invalid = tape()
                unique = invalid["unique_id"]
                if mark == "first":
                    invalid["events"].pop(0)
                elif mark == "last":
                    invalid["events"].pop()
                elif mark == "overflow":
                    invalid["overflow"] = True
                else:
                    invalid["unique_id"] = "other invocation"
                with self.assertRaisesRegex(ValueError, failure):
                    sim.check_tape(invalid, unique, None)

    def test_worker_binds_the_tape_to_the_original_invocation_and_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory).resolve()
            lane = run / "inventory" / sim.LANE
            lane.mkdir(parents=True)
            sim.write_json(lane.parent / "requested-lanes.json", {
                "schema_version": 1, "lanes": [{"project": sim.PROJECT, "task": sim.TASK, "enabled": True}],
            })
            sim.write_json(lane / "selection.json", {
                "schema_version": 1, "project": sim.PROJECT, "task": sim.TASK,
                "include_tags": ["property"], "simulation_iterations": "1",
            })
            original = tape()
            case = {"unique_id": original["unique_id"], "is_test": True, "parent_id": "template",
                    "display_name": f"{sim.METHOD} [iteration 1]"}
            template = {"unique_id": "template", "is_test": False, "parent_id": None,
                        "source": {"class_name": sim.TEST_CLASS, "method_name": sim.METHOD}}
            target = run / "causal" / (hashlib.sha256(case["unique_id"].encode()).hexdigest() + ".json")
            target.parent.mkdir()
            sim.write_json(target, original)
            frames = [
                {"event": "plan_started", "project": sim.PROJECT, "task": sim.TASK},
                {"event": "discovered", "descriptor": template},
                {"event": "dynamic_registered", "descriptor": case},
                {"event": "execution_started", "descriptor": case},
                {"event": "report_entry", "descriptor": case, "entries": {
                    "rtdb.simulation.trace": target.relative_to(run).as_posix(),
                    "rtdb.simulation.sha256": sim.digest(target),
                }},
                {"event": "execution_finished", "descriptor": case, "status": "SUCCESSFUL"},
                {"event": "plan_finished"},
            ]
            journal = lane / "worker.jsonl"

            def write_frames():
                journal.write_text("\n".join(json.dumps({"sequence": n, **frame}) for n, frame in enumerate(frames)) + "\n")

            write_frames()
            self.assertEqual(sim.selected_invocation(run)[0], case["unique_id"])
            frames[4]["descriptor"] = template
            write_frames()
            with self.assertRaisesRegex(ValueError, "different invocation"):
                sim.selected_invocation(run)
            frames[4]["descriptor"] = case
            write_frames()
            target.write_text("{}")
            with self.assertRaisesRegex(ValueError, "differs from the retained bytes"):
                sim.selected_invocation(run)
