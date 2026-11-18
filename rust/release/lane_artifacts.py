from pathlib import Path

from evidence import artifact, closure, index, integer, read_json, retained, sha256, text


ASSERTION_COMPARISON_INPUTS = (
    ("ledger", "ledger.json"),
    ("reference_receipts", "reference-receipts.json"),
    ("native_receipts", "native-receipts.json"),
)
CASE_IDENTITY_FIELDS = ("reference_revision", "source", "qualified_name", "parameter_fixture", "configuration")
PARITY_DISPOSITIONS = ("passing_both", "matching_expected_failure")


def package_matches(package, lane):
    selected = lane["selection"]
    if any(package.get(field) != expected for field, expected in (
        ("engine", selected["engine"]), ("variant", selected["variant"]), ("platform", lane["platform"]))):
        raise ValueError("package differs from the selected engine, variant or platform")
    field = "features" if selected["engine"] == "rust" else "capabilities"
    if package.get(field) != selected[field]:
        raise ValueError("package differs from the selected feature/capability set")


def expected_artifacts(lane, dependency):
    prefix = lane["platform"].replace("/", "-")
    native = prefix + "/rust/" + lane["selection"].get("variant", "")
    runner = lane["runner"]
    roles = {}

    def add(role, identity, select):
        if identity not in lane["dependencies"]:
            raise ValueError("artifact producer is not a declared selected dependency")
        producer, directory = dependency(identity)
        if producer["platform"] != lane["platform"]:
            raise ValueError("dependency artifact uses another platform")
        roles[role] = (identity, select(producer, directory))

    def executable(producer, directory):
        build = read_json(directory / "production-artifacts.json")
        if build["selection"] != producer["selection"] or build["configuration"] != producer["id"]:
            raise ValueError("producer build selection differs")
        binaries = [value for value in build["binaries"] if value["compiler_artifact"]["target"]["name"] == "rtdb-conformance"
                    and not value["compiler_artifact"]["profile"]["test"]]
        if len(binaries) != 1:
            raise ValueError("selected build must retain one production conformance executable")
        return artifact(directory, binaries[0]["retained_artifact"])

    def package(producer, directory):
        path = directory / "run/package.json"
        package_matches(read_json(path), producer)
        return path

    def library(producer, directory):
        path = package(producer, directory)
        references = [value for value in read_json(path)["artifacts"] if value["path"] == "artifacts/librtdb_ffi.so"]
        if len(references) != 1:
            raise ValueError("selected package must retain its embedded shared library")
        return artifact(path.parent, references[0])

    if runner in {"controls", "history", "sql-logic", "recovery", "packaged-recovery", "assertion-comparison"}:
        add("native", native + "/production", executable)
    if runner == "embedded":
        add("native_library", native + "/package", library)
    if runner in {"clients", "performance", "packaged-recovery"}:
        add("native_package", native + "/package", package)
        add("jvm_package", prefix + "/jvm/" + lane["selection"]["jvm_variant"] + "/package", package)
    if runner == "recovery":
        add("history", native + "/history", lambda _, directory: directory / "run/result.json")
    if runner == "inventory":
        add("native_inventory", native + "/tests", lambda _, directory: directory / "run/native-inventory.json")
    if runner == "assertion-comparison":
        add("inventory", native + "/inventory", lambda _, directory: directory / "inventory.json")
        add("ledger", native + "/inventory", lambda _, directory: directory / "ledger.json")
    return roles


def bind_inputs(lane, directory, root, source, dependency):
    expected = expected_artifacts(lane, dependency)
    plan_role = {"performance": "performance_plan", "packaged-recovery": "recovery_plan"}.get(lane["runner"])
    plan_path = source(plan_role) if plan_role else None
    plan = read_json(plan_path) if plan_path else None
    if plan is not None and lane["runner"] == "performance":
        if plan["resources"]["architecture"] != lane["platform"] or plan.get("package_capabilities") != lane["selection"]["capabilities"]:
            raise ValueError("performance plan differs from selected platform or package capabilities")
        (directory / "selected-performance-plan.json").write_bytes(plan_path.read_bytes())
    if plan is not None and lane["runner"] == "packaged-recovery":
        if plan.get("platform") != lane["platform"] or plan.get("capability") != lane["selection"]["capability"]:
            raise ValueError("packaged recovery plan differs from its selected platform or capability")
        (directory / "selected-recovery-plan.json").write_bytes(plan_path.read_bytes())
    result = []
    for role, (identity, produced) in expected.items():
        if plan is not None:
            reference = plan["native"] if role == "native" else plan["engines"]["rust" if role == "native_package" else "jvm"]["package"]
            consumed = artifact(plan_path.parent, reference)
        else:
            consumed = source(role)
            if role == "history":
                consumed = consumed / "result.json"
        if sha256(consumed) != sha256(produced):
            raise ValueError("consumed " + role + " differs from selected dependency " + identity)
        value = {"role": role, "dependency": identity, "produced": retained(root, produced),
                 "consumed_path": str(consumed), "consumed_sha256": sha256(consumed)}
        if role == "history":
            value["files"] = closure(consumed.parent)
            if value["files"] != closure(produced.parent):
                raise ValueError("recovery input differs from selected history artifact closure")
        if role == "native_library":
            value["executed_path"] = str(directory / consumed.name)
        result.append(value)
    if plan_path:
        result.append({"role": plan_role, "consumed_path": str(plan_path), "consumed_sha256": sha256(plan_path)})
    return result


def selected_input_snapshot(directory, role, filename):
    selected = [item for item in read_json(directory / "input-artifacts.json") if item.get("input") == role]
    if len(selected) != 1:
        raise ValueError("expected one selected input file for " + role)
    return artifact(directory, {**selected[0], "path": filename})


def verify_inventory_ledger(directory):
    ledger = selected_input_snapshot(directory, "ledger", "ledger.json")
    process = read_json(directory / "inventory.process.json")
    command = process["command"]
    original_directory = Path(process["cwd"])
    if (len(command) < 2 or Path(command[1]).name != "runtime_inventory.py" or not original_directory.is_absolute()
            or process.get("completed") is not True or process.get("unexpected_live_descendants") or process.get("exit_code") != 0):
        raise ValueError("inventory lacks a successful reconciliation process")
    for flag, filename in (("--ledger", ledger.name), ("--output", "inventory.json")):
        if command.count(flag) != 1:
            raise ValueError("inventory command requires one " + flag + " operand")
        operand = command.index(flag) + 1
        if command[operand:operand + 1] != [str(original_directory / filename)]:
            raise ValueError("inventory invocation did not bind its retained assertion ledger and report")
    return ledger


def assertion_comparison_command(native, directory):
    return [str(native), "compare", *(str(directory / filename) for _, filename in ASSERTION_COMPARISON_INPUTS)]


def assertion_case_key(case):
    identity = case.get("identity")
    if not isinstance(identity, dict) or set(identity) != set(CASE_IDENTITY_FIELDS):
        raise ValueError("assertion comparison requires a complete case identity")
    return tuple(text(identity[field], field) for field in CASE_IDENTITY_FIELDS)


def verify_assertion_comparison(directory, expected, bindings, revisions):
    inventory_path = expected["inventory"][1]
    ledger = verify_inventory_ledger(inventory_path.parent)
    snapshots = {role: selected_input_snapshot(directory, role, filename)
                 for role, filename in ASSERTION_COMPARISON_INPUTS}
    if sha256(snapshots["ledger"]) != sha256(ledger):
        raise ValueError("assertion comparison did not retain the reconciled ledger")

    process = read_json(directory / "assertion-comparison.process.json")
    original_directory = Path(process["cwd"])
    if (not original_directory.is_absolute() or not Path(bindings["native"]["consumed_path"]).is_absolute()
            or process["command"] != assertion_comparison_command(bindings["native"]["consumed_path"], original_directory)
            or process.get("completed") is not True or process.get("unexpected_live_descendants")):
        raise ValueError("assertion comparison invocation differs from the selected executable and operands")

    report = read_json(directory / "assertion-comparison.stdout.log")
    if process.get("exit_code") != 0 or report.get("complete_parity") is not True:
        raise ValueError("full assertion comparison did not establish strict parity; inspect comparator stdout")

    ledger = read_json(ledger)
    inventory = read_json(inventory_path)
    if (integer(ledger.get("schema_version"), "assertion ledger schema", 1) != 1
            or integer(inventory.get("schema_version"), "inventory schema", 1) != 1
            or ledger.get("inventory_revision") != revisions["reference_revision"]
            or inventory.get("reference_revision") != revisions["reference_revision"]
            or ledger.get("inventory_complete") is not True or ledger.get("runtime_reconciled") is not True
            or inventory.get("complete") is not True or inventory.get("all_assertions_passed") is not True):
        raise ValueError("assertion comparison does not use a complete pinned inventory and ledger")
    cases = index(ledger.get("cases"), assertion_case_key, "ledger case")
    group_count = integer(inventory.get("assertion_denominator"), "assertion denominator", 1)
    if group_count != len(cases) or any(key[0] != revisions["reference_revision"] for key in cases):
        raise ValueError("assertion ledger identities or denominator differ from the reconciled inventory")

    def observation_key(observation):
        return text(observation.get("id"), "observation identity")

    required = {key: index(case.get("observations"), observation_key, "required observation") for key, case in cases.items()}
    for role, revision in (("reference_receipts", "reference_revision"), ("native_receipts", "implementation_revision")):
        receipts = index(read_json(snapshots[role]), assertion_case_key, role)
        if receipts.keys() != cases.keys() or any(
                receipt["context"]["artifact_revision"] != revisions[revision] for receipt in receipts.values()):
            raise ValueError("assertion receipt identities or artifact revision differ for " + role)
        for key, receipt in receipts.items():
            if (role == "native_receipts" and receipt.get("runner") == "rtdb-controls/native/1"
                    and receipt["context"].get("binary_sha256") != bindings["native"]["consumed_sha256"]):
                raise ValueError("native control receipt uses another production executable")
            observations = index(receipt.get("observations"), observation_key, "observation receipt")
            if observations.keys() != required[key].keys() or any(
                    observation.get("executed") is not True for observation in observations.values()):
                raise ValueError("assertion receipt lacks required executed observations for " + role)

    observed = index(report.get("cases"), assertion_case_key, "comparison case")
    if observed.keys() != cases.keys() or any(
            case.get("disposition") not in PARITY_DISPOSITIONS or case.get("differences") != []
            or integer(case.get("comparisons"), "compared observations", 1) != len(required[key])
            for key, case in observed.items()):
        raise ValueError("assertion comparison cases differ from the required ledger observations")
    counts = {"required_groups": group_count, "missing_evidence": 0, "mismatches": 0,
              **{kind: sum(case["disposition"] == kind for case in observed.values()) for kind in PARITY_DISPOSITIONS}}
    text(report.get("comparator_version"), "assertion comparator version")
    if (any(integer(report.get(field), field) != count for field, count in counts.items())
            or integer(report.get("matched_declared_migration", 0), "matched declared migrations") != 0
            or report.get("manifest_errors") != [] or type(report.get("strict_agreement")) not in (int, float)
            or report["strict_agreement"] != 1):
        raise ValueError("assertion comparison summary differs from its complete case results")


def verify_bindings(lane, directory, root, dependency):
    expected = expected_artifacts(lane, dependency)
    records = read_json(directory / "artifact-bindings.json")
    bindings = {value["role"]: value for value in records}
    plan_role = {"performance": "performance_plan", "packaged-recovery": "recovery_plan"}.get(lane["runner"])
    roles = set(expected) | ({plan_role} if plan_role else set())
    if len(bindings) != len(records) or set(bindings) != roles:
        raise ValueError("consumed artifact binding denominator differs from selected lane")
    for role, (identity, produced) in expected.items():
        value = bindings[role]
        if value.get("dependency") != identity or artifact(root, value["produced"]) != produced or value.get("consumed_sha256") != sha256(produced):
            raise ValueError("consumed artifact binding differs from selected producer")
        if role == "history" and value.get("files") != closure(produced.parent):
            raise ValueError("consumed history closure differs from selected producer")
    if lane["runner"] == "assertion-comparison":
        revisions = read_json(root / "inputs.json")
        _, production = dependency(bindings["native"]["dependency"])
        if read_json(production / "production-artifacts.json")["implementation_revision"] != revisions["implementation_revision"]:
            raise ValueError("assertion comparator was built from another implementation revision")
        verify_assertion_comparison(directory, expected, bindings, revisions)
        return
    if lane["runner"] == "performance":
        from performance_results import compare
        plan_path = directory / "selected-performance-plan.json"
        plan = read_json(plan_path)
        if sha256(plan_path) != bindings["performance_plan"]["consumed_sha256"] or plan["resources"]["architecture"] != lane["platform"] or plan.get("package_capabilities") != lane["selection"]["capabilities"]:
            raise ValueError("retained performance plan selection differs")
        runs = [read_json(path) for path in (directory / "run").glob("*/execution.json")]
        for role, engine in (("native_package", "rust"), ("jvm_package", "jvm")):
            if plan["engines"][engine]["package"]["sha256"] != bindings[role]["consumed_sha256"]:
                raise ValueError("performance plan uses another server artifact")
            package = read_json(expected[role][1])
            selected_runs = [value for value in runs if value.get("engine") == engine]
            if not selected_runs or any(value.get("server_image") != package["image_id"] for value in selected_runs):
                raise ValueError("performance execution uses another packaged server image")
        comparison = read_json(directory / "run/comparison.json")
        actual = compare(plan, runs)
        if comparison.get("plan_sha256") != sha256(plan_path) or actual.get("packaged_performance_qualified") is not True or any(comparison.get(key) != value for key, value in actual.items()):
            raise ValueError("performance summary differs from retained selected execution")
    if lane["runner"] == "clients":
        for role, side in (("native_package", "native"), ("jvm_package", "reference")):
            captured = read_json(directory / "run" / (side + "-package.json"))
            if captured["manifest_sha256"] != bindings[role]["consumed_sha256"] or captured["manifest"] != read_json(expected[role][1]):
                raise ValueError("client execution uses another package manifest")
    if lane["runner"] == "packaged-recovery":
        from packaged_recovery_plan import validate_plan
        from packaged_recovery_results import comparison as compare_recovery
        plan_path = directory / "selected-recovery-plan.json"
        plan = validate_plan(read_json(plan_path))
        if sha256(plan_path) != bindings["recovery_plan"]["consumed_sha256"] or plan["platform"] != lane["platform"] or plan["capability"] != lane["selection"]["capability"]:
            raise ValueError("retained recovery plan selection differs")
        if plan["native"]["sha256"] != bindings["native"]["consumed_sha256"]:
            raise ValueError("recovery helper differs from the selected production build")
        for role, engine in (("native_package", "rust"), ("jvm_package", "jvm")):
            captured = read_json(directory / "run" / (engine + "-package.json"))
            if captured["manifest_sha256"] != bindings[role]["consumed_sha256"] or captured["manifest"] != read_json(expected[role][1]) or plan["engines"][engine]["package"]["sha256"] != captured["manifest_sha256"]:
                raise ValueError("recovery execution uses another selected package")
        runs = [read_json(path) for path in sorted((directory / "run").glob("*/receipt.json"))]
        actual = compare_recovery(plan, runs, directory / "run")
        recorded = read_json(directory / "run/comparison.json")
        if recorded.get("plan_sha256") != sha256(plan_path) or actual["packaged_recovery_qualified"] is not True or any(recorded.get(key) != value for key, value in actual.items()):
            raise ValueError("recovery summary differs from retained selected observations")
    flags = {"native": "--native", "native_package": "--native-provenance", "jvm_package": "--reference-provenance",
             "performance_plan": "--plan", "recovery_plan": "--plan", "history": "--history", "native_inventory": "--native"}
    if bindings:
        runner = lane["runner"]
        process = read_json(directory / ("embedded-binding.process.json" if runner == "embedded" else runner + ".process.json"))
        command = process["command"]
        for role, value in bindings.items():
            if plan_role is not None and role != plan_role:
                continue
            if role == "native_library":
                result = read_json(directory / "embedded-results.json")
                if result["library"]["sha256"] != value["consumed_sha256"] or command.count("-Dxtdb.native.library=" + value["executed_path"]) != 1 or Path(value["executed_path"]).name != result["library"]["path"]:
                    raise ValueError("embedded invocation differs from retained selected library")
                continue
            flag = flags[role]
            consumed = str(Path(value["consumed_path"]).parent) if role == "history" else value["consumed_path"]
            if command.count(flag) != 1 or command[command.index(flag) + 1] != consumed:
                raise ValueError("invocation does not consume its bound " + role)
