#!/usr/bin/env python3
"""Run immutable controls and retain paired observations, traces, and negative-control reports."""

import argparse
import copy
import ctypes
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from source_gate import require_phase_two


REFERENCE = "c84f9334eb060ceb92d28ecdd4a06e6b625932fd"
HERE = Path(__file__).resolve().parent
REPOSITORY = HERE.parents[1]


class ProcessCancelled(RuntimeError):
    pass


class _CancellationScope:
    def __init__(self, deferred):
        self.armed = not deferred
        self.pending = None
        self.cleaning = False
        self.previous = {}

    def __enter__(self):
        self.previous = {value: signal.getsignal(value) for value in (signal.SIGTERM, signal.SIGINT)}
        parent = getattr(self.previous[signal.SIGTERM], "__self__", None)
        if isinstance(parent, _CancellationScope) and parent.cleaning:
            self.cleaning = True
            self.armed = False
        signal.signal(signal.SIGTERM, self.receive)
        return self

    def receive(self, number, frame):
        if self.cleaning:
            return
        self.pending = number
        if self.armed:
            raise ProcessCancelled(f"conformance owner received signal {number}")

    def arm(self):
        self.armed = True
        if self.pending is not None and not self.cleaning:
            raise ProcessCancelled(f"conformance owner received signal {self.pending}")

    def begin_cleanup(self):
        self.cleaning = True
        self.armed = False
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def __exit__(self, exception_type, exception, traceback):
        for number, handler in reversed(self.previous.items()):
            signal.signal(number, handler)


def owned_cancellation(deferred=False):
    """Raise ProcessCancelled on SIGTERM on the main thread, restoring prior handlers on exit.
    Call begin_cleanup() before draining resources; nested owners then ignore repeated cancellation.
    With deferred=True, call arm() after acquiring a resource whose constructor cannot be interrupted safely.
    """
    return _CancellationScope(deferred)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def git(repository, *args):
    return subprocess.check_output(["git", "-C", str(repository), *args], text=True).strip()


def enable_child_reaping():
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        pr_set_child_subreaper = 36
        if libc.prctl(pr_set_child_subreaper, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "cannot enable child-process reaping")


def group_alive(process):
    process.poll()
    if process.returncode is not None:
        while True:
            try:
                pid, _ = os.waitpid(-process.pid, os.WNOHANG)
                if pid == 0:
                    break
            except ChildProcessError:
                break
    try:
        os.killpg(process.pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _signal_group(process, sig):
    try:
        os.killpg(process.pid, sig)
        return True
    except ProcessLookupError:
        return True
    except PermissionError:
        return False


def drain_group(process, signal_number, timeout_seconds):
    """Deliver a signal to an owned POSIX process group and wait under a monotonic deadline.
    - Denied sends are retried until accepted; accepted signals are not repeated.
    - True requires a reaped leader and a group probe reporting absence.
    - False leaves the unresolved group with the caller for its next cleanup phase.
    """
    deadline = time.monotonic() + timeout_seconds
    while True:
        if signal_number is not None and _signal_group(process, signal_number):
            signal_number = None
        if not group_alive(process):
            process.wait()
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.02)


def terminate(process, grace=10):
    if not drain_group(process, signal.SIGTERM, grace):
        if not drain_group(process, signal.SIGKILL, 10):
            raise RuntimeError(f"owned process group {process.pid} did not drain after SIGKILL")


def run_process(command, cwd, output, log, timeout):
    if os.name != "posix":
        raise RuntimeError("the conformance process owner requires POSIX process groups")
    enable_child_reaping()
    started = time.monotonic()
    status = {"command": [str(value) for value in command], "cwd": str(cwd), "completed": False}
    with owned_cancellation(deferred=True) as cancellation, Path(output).open("xb") as stdout, Path(log).open("xb") as stderr:
        process = subprocess.Popen(status["command"], cwd=cwd, stdout=stdout, stderr=stderr,
                                   start_new_session=os.name == "posix", env={**os.environ, "TZ": "UTC"})
        try:
            cancellation.arm()
            status["exit_code"] = process.wait(timeout=timeout)
            if group_alive(process):
                status["unexpected_live_descendants"] = True
                cancellation.begin_cleanup()
                terminate(process)
            else:
                status["completed"] = True
        except subprocess.TimeoutExpired:
            status["timeout_seconds"] = timeout
            cancellation.begin_cleanup()
            terminate(process)
            status["exit_code"] = process.returncode
        except BaseException:
            cancellation.begin_cleanup()
            terminate(process)
            raise
        finally:
            cancellation.begin_cleanup()
            status["elapsed_seconds"] = time.monotonic() - started
    return status


def validate_execution(execution, fixture_digest, mapping):
    if execution.get("schema_version") != 1 or execution.get("fixture_sha256") != fixture_digest:
        raise ValueError("runner did not observe the selected fixture bytes")
    binary = execution.get("binary_sha256", "")
    if len(binary) != 64 or any(char not in "0123456789abcdef" for char in binary):
        raise ValueError("runner omitted its actual artifact digest")
    observations = execution.get("observations", [])
    ids = [item["id"] for item in observations]
    required = [item["id"] for item in mapping["observations"]]
    if len(ids) != len(set(ids)) or set(ids) != set(required):
        raise ValueError("runner observation IDs differ from the assertion mapping")
    if any(item.get("executed") is not True for item in observations):
        raise ValueError("control contains an unexecuted observation")


def validate_assertions(execution, fixture):
    if fixture["kind"] == "temporal_ceilings":
        expected = fixture["expected_observations"]
    elif fixture["kind"] == "replica_term_sequence":
        expected = {value["name"] + "/decoded": {"term": value["expected_term"],
                    "term_sequence": value["expected_term_sequence"]} for value in fixture["envelopes"]}
    elif fixture["kind"] == "table_column_assertions":
        expected = {case["id"]: {case["column"]: case["expected"]} for case in fixture["cases"]}
    else:
        raise ValueError("control fixture has no immutable assertion adapter")
    actual = {}
    for observation in execution["observations"]:
        outcome = observation["outcome"]
        if outcome["kind"] != "returned":
            raise ValueError(f"control assertion did not return: {observation['id']}")
        actual[observation["id"]] = outcome["typed_value"]
    canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if canonical(actual) != canonical(expected):
        raise ValueError("control observation differs from the immutable upstream assertion; inspect execution.json")


def validate_native_revision(execution, revision):
    if execution.get("artifact_revision") != revision or execution.get("source_dirty") is not False:
        raise ValueError("native executable was not built from this clean implementation revision")


def execute_case(mapping, side, args, directory, config, revision):
    fixture = REPOSITORY / mapping["identity"]["parameter_fixture"]
    fixture_digest = digest(fixture)
    directory.mkdir()
    output = directory / "stdout.log"
    log = directory / "stderr.log"
    result = directory / "execution.json"
    if side == "native":
        command = [args.native, "observe", fixture]
        cwd = REPOSITORY
    else:
        command = [args.reference / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk",
                   "--init-script", HERE / "reference.init.gradle", "rtdbConformanceObserve",
                   f"-Prtdb.adapterRoot={HERE / 'jvm'}", f"-Prtdb.fixture={fixture}", f"-Prtdb.output={result}"]
        cwd = args.reference
    process = run_process(command, cwd, output, log, args.timeout)
    write_json(directory / "process.json", process)
    if not process["completed"] or process["exit_code"] != 0:
        raise RuntimeError(f"{side} control process failed; inspect {directory}")
    if side == "native":
        execution = json.loads(output.read_bytes())
        write_json(result, execution)
        validate_native_revision(execution, revision)
        if execution["binary_sha256"] != digest(args.native):
            raise ValueError("native executable changed during the observation")
    else:
        execution = json.loads(result.read_bytes())
    validate_execution(execution, fixture_digest, mapping)
    validate_assertions(execution, json.loads(fixture.read_bytes()))
    if digest(fixture) != fixture_digest:
        raise ValueError("fixture changed while its runner was executing")
    trace = {"process": process, "stdout_sha256": digest(output), "stderr_sha256": digest(log),
             "execution_sha256": digest(result), "runner_sha256": digest(__file__),
             "reference_adapter_sha256": digest(HERE / "jvm/rtdb/conformance/controls.clj"),
             "gradle_init_sha256": digest(HERE / "reference.init.gradle")}
    write_json(directory / "trace.json", trace)
    receipt = {"identity": mapping["identity"], "runner": f"rtdb-controls/{side}/1",
               "trace_sha256": digest(directory / "trace.json"),
               "context": {key: config[key] for key in ["applied_basis", "valid_time", "system_time", "clock", "timezone", "storage_version", "features"]},
               "observations": execution["observations"]}
    receipt["context"].update(binary_sha256=execution["binary_sha256"], fixture_sha256=fixture_digest,
                              configuration_sha256=digest(args.configuration), artifact_revision=revision)
    write_json(directory / "receipt.json", receipt)
    return receipt


def compare_files(args, selection, reference, native, directory):
    ledger = args.output / "ledger.json"
    if digest(ledger) != selection.ledger_sha256:
        raise ValueError("captured global ledger changed before comparison")
    directory.mkdir()
    output = directory / "report.json"
    status = run_process([args.native, "compare", ledger, reference, native], REPOSITORY, output,
                         directory / "stderr.log", args.timeout)
    write_json(directory / "process.json", status)
    if not status["completed"] or status["exit_code"] not in (0, 1):
        raise RuntimeError(f"comparator process failed; inspect {directory}")
    report = json.loads(output.read_bytes())
    if digest(ledger) != selection.ledger_sha256:
        raise ValueError("captured global ledger changed during comparison")
    return report


IDENTITY_FIELDS = ("reference_revision", "source", "qualified_name", "parameter_fixture", "configuration")


def case_key(identity):
    if not isinstance(identity, dict) or set(identity) != set(IDENTITY_FIELDS):
        raise ValueError("control selection requires a complete case identity")
    values = tuple(identity[field] for field in IDENTITY_FIELDS)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ValueError("control case identity fields must be nonempty strings")
    return values


def _select_cases(ledger, selection):
    if not isinstance(ledger, dict) or type(ledger.get("schema_version")) is not int or ledger["schema_version"] != 1 or ledger.get("inventory_revision") != REFERENCE:
        raise ValueError("control ledger does not identify the pinned inventory")
    if not isinstance(selection, dict) or type(selection.get("schema_version")) is not int or selection["schema_version"] != 1 or selection.get("inventory_revision") != REFERENCE:
        raise ValueError("control selection does not identify the pinned inventory")
    mappings = {}
    cases = ledger.get("cases")
    if not isinstance(cases, list):
        raise ValueError("control ledger has no case array")
    for mapping in cases:
        key = case_key(mapping.get("identity") if isinstance(mapping, dict) else None)
        if key[0] != REFERENCE or key in mappings:
            raise ValueError("control ledger has an unpinned or duplicate case identity")
        mappings[key] = mapping
    identities = selection.get("cases")
    if not isinstance(identities, list) or not identities:
        raise ValueError("control selection has no cases")
    keys = [case_key(identity) for identity in identities]
    if len(keys) != len(set(keys)) or any(key not in mappings for key in keys):
        raise ValueError("control selection contains a duplicate or unmapped case")
    return [mappings[key] for key in keys]


@dataclass(frozen=True)
class ControlSelection:
    ledger_bytes: bytes
    selection_bytes: bytes
    _global_identities: frozenset = field(init=False, repr=False)
    _selected_identities: tuple = field(init=False, repr=False)
    _case_json: tuple = field(init=False, repr=False)
    _manifest_errors: frozenset = field(init=False, repr=False)

    def __post_init__(self):
        if not isinstance(self.ledger_bytes, bytes) or not isinstance(self.selection_bytes, bytes):
            raise ValueError("control inputs require immutable bytes")
        ledger, selection = json.loads(self.ledger_bytes), json.loads(self.selection_bytes)
        mappings = _select_cases(ledger, selection)
        if any(type(ledger.get(flag)) is not bool for flag in ("inventory_complete", "runtime_reconciled")):
            raise ValueError("control ledger lacks explicit inventory completeness flags")
        errors = set()
        if not ledger["inventory_complete"]:
            errors.add("source inventory is incomplete")
        if not ledger["runtime_reconciled"]:
            errors.add("runtime discovery has not been reconciled")
        object.__setattr__(self, "_global_identities", frozenset(case_key(mapping["identity"]) for mapping in ledger["cases"]))
        object.__setattr__(self, "_selected_identities", tuple(case_key(mapping["identity"]) for mapping in mappings))
        object.__setattr__(self, "_case_json", tuple(json.dumps(mapping) for mapping in mappings))
        object.__setattr__(self, "_manifest_errors", frozenset(errors))

    @property
    def ledger_sha256(self):
        return hashlib.sha256(self.ledger_bytes).hexdigest()

    @property
    def selection_sha256(self):
        return hashlib.sha256(self.selection_bytes).hexdigest()

    def mappings(self):
        return [json.loads(mapping) for mapping in self._case_json]

    def reports(self, report):
        errors = report.get("manifest_errors", [])
        if len(errors) != len(self._manifest_errors) or set(errors) != self._manifest_errors:
            raise ValueError("comparison manifest differs from the complete captured inventory")
        cases = {}
        for case in report["cases"]:
            key = case_key(case["identity"])
            if key in cases:
                raise ValueError("comparison contains duplicate case reports")
            cases[key] = case
        if set(cases) != self._global_identities or type(report.get("required_groups")) is not int or report["required_groups"] != len(self._global_identities):
            raise ValueError("comparison omitted or added global assertion groups")
        fields = {"passing_both": "passing_both", "matching_expected_failure": "matching_expected_failure",
                  "missing_evidence": "missing_evidence", "mismatches": "mismatch"}
        counts = {field: sum(case["disposition"] == disposition for case in cases.values()) for field, disposition in fields.items()}
        if sum(counts.values()) != len(cases) or any(type(report.get(field)) is not int or report[field] != count for field, count in counts.items()):
            raise ValueError("comparison summary differs from its global case dispositions")
        agreement = None if errors else (counts["passing_both"] + counts["matching_expected_failure"]) / len(cases)
        parity = not errors and counts["missing_evidence"] == 0 and counts["mismatches"] == 0
        agreement_valid = report.get("strict_agreement") is None if agreement is None else type(report.get("strict_agreement")) is float and report["strict_agreement"] == agreement
        if "strict_agreement" not in report or not agreement_valid or report.get("complete_parity") is not parity:
            raise ValueError("comparison completeness differs from the captured global inventory")
        if any(case["disposition"] != "missing_evidence" for key, case in cases.items() if key not in self._selected_identities):
            raise ValueError("comparison contains unexpected evidence outside the control selection")
        return [cases[key] for key in self._selected_identities]

    def require_agreement(self, report):
        if any(case["disposition"] != "passing_both" for case in self.reports(report)):
            raise ValueError("mandatory control observations did not execute successfully on both engines")


def replace_pointer(value, path, replacement):
    if not path.startswith("/"):
        raise ValueError("negative control requires a non-root JSON pointer")
    parts = [part.replace("~1", "/").replace("~0", "~") for part in path[1:].split("/")]
    for part in parts[:-1]:
        value = value[int(part)] if isinstance(value, list) else value[part]
    key = int(parts[-1]) if isinstance(value, list) else parts[-1]
    if value[key] == replacement:
        raise ValueError("negative control must change the observed value")
    value[key] = replacement


def negative_control(args, selection, reference, native_path, mapping, directory):
    mutated = copy.deepcopy(reference)
    fixture = json.loads((REPOSITORY / mapping["identity"]["parameter_fixture"]).read_bytes())
    control = fixture["negative_control"]
    case = next(case for case in mutated if case["identity"] == mapping["identity"])
    observation = next(value for value in case["observations"] if value["id"] == control["observation"])
    replace_pointer(observation["outcome"]["typed_value"], control["path"], control["replacement"])
    directory.mkdir()
    mutation = {"kind": "isolated-comparator-negative-control", "identity": mapping["identity"],
                "original_trace_sha256": case["trace_sha256"], "control": control}
    write_json(directory / "mutation.json", mutation)
    case["runner"] += "/negative-control"
    case["trace_sha256"] = digest(directory / "mutation.json")
    reference_path = directory / "mutated-reference.json"
    write_json(reference_path, mutated)
    report = compare_files(args, selection, reference_path, native_path, directory / "comparison")
    selected = selection.reports(report)
    differences = [(case["identity"], difference) for case in selected for difference in case["differences"]]
    if report["mismatches"] != 1 or any(case["disposition"] != ("mismatch" if case["identity"] == mapping["identity"] else "passing_both") for case in selected) or len(differences) != 1:
        raise ValueError("negative control did not produce exactly its designated mismatch")
    identity, difference = differences[0]
    if identity != mapping["identity"] or difference["observation"] != control["observation"] or difference["path"] != control["path"]:
        raise ValueError("negative control reported the wrong case, observation, or JSON pointer")
    write_json(directory / "control-result.json", {"detected": True, "difference": difference,
               "comparison_sha256": digest(directory / "comparison/report.json")})


def run(args):
    require_phase_two(REPOSITORY, args.phase_two)
    if git(args.reference, "rev-parse", "HEAD") != REFERENCE or git(args.reference, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("the reference checkout must be clean and pinned to the required revision")
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    if git(REPOSITORY, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("commit the implementation and fixture sources before collecting reproducible receipts")
    selection = ControlSelection(args.ledger.read_bytes(), args.selection.read_bytes())
    configuration_bytes = args.configuration.read_bytes()
    config = json.loads(configuration_bytes)
    mappings = selection.mappings()
    args.output.mkdir(parents=True, exist_ok=False)
    args.created_output = True
    write_json(args.output / "run.json", {"reference_revision": REFERENCE, "implementation_revision": revision,
               "ledger_sha256": selection.ledger_sha256, "selection_sha256": selection.selection_sha256, "configuration_sha256": hashlib.sha256(configuration_bytes).hexdigest(),
               "native_binary_sha256": digest(args.native), "status": "running"})
    for name, payload in [("ledger.json", selection.ledger_bytes), ("selection.json", selection.selection_bytes), ("configuration.json", configuration_bytes)]:
        with (args.output / name).open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    execution_args = copy.copy(args)
    execution_args.configuration = args.output / "configuration.json"
    reference, repeat, native = [], [], []
    for index, mapping in enumerate(mappings):
        reference.append(execute_case(mapping, "reference", execution_args, args.output / f"{index:04d}-reference", config, REFERENCE))
        repeat.append(execute_case(mapping, "reference", execution_args, args.output / f"{index:04d}-reference-repeat", config, REFERENCE))
        native.append(execute_case(mapping, "native", execution_args, args.output / f"{index:04d}-native", config, revision))
        write_json(args.output / "reference.json", reference)
        write_json(args.output / "reference-repeat.json", repeat)
        write_json(args.output / "native.json", native)
    repeat_report = compare_files(args, selection, args.output / "reference.json", args.output / "reference-repeat.json", args.output / "reference-repeat-comparison")
    selection.require_agreement(repeat_report)
    report = compare_files(args, selection, args.output / "reference.json", args.output / "native.json", args.output / "comparison")
    selection.require_agreement(report)
    for index, mapping in enumerate(mappings):
        negative_control(args, selection, reference, args.output / "native.json", mapping, args.output / f"{index:04d}-negative-control")
    write_json(args.output / "result.json", {"controls_verified": True, "full_inventory_parity": report["complete_parity"],
               "paired_groups": len(mappings), "inventory_groups": report["required_groups"], "report_sha256": digest(args.output / "comparison/report.json")})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, default=HERE / "assertion-ledger.json")
    parser.add_argument("--selection", type=Path, default=HERE / "configurations/control-selection.json")
    parser.add_argument("--configuration", type=Path, default=HERE / "configurations/controls.json")
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    args.created_output = False
    for field in ("reference", "native", "output", "ledger", "selection", "configuration"):
        setattr(args, field, getattr(args, field).resolve())
    try:
        run(args)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        if args.created_output:
            write_json(args.output / "failure.json", {"error": str(error), "verified": False})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
