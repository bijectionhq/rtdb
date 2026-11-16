from pathlib import Path

from evidence import artifact, closure, read_json, retained, sha256


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

    if runner in {"controls", "history", "sql-logic", "recovery"}:
        add("native", native + "/production", executable)
    if runner == "embedded":
        add("native_library", native + "/package", library)
    if runner in {"clients", "performance"}:
        add("native_package", native + "/package", package)
        add("jvm_package", prefix + "/jvm/" + lane["selection"]["jvm_variant"] + "/package", package)
    if runner == "recovery":
        add("history", native + "/history", lambda _, directory: directory / "run/result.json")
    if runner == "inventory":
        add("native_inventory", native + "/tests", lambda _, directory: directory / "run/native-inventory.json")
    return roles


def bind_inputs(lane, directory, root, source, dependency):
    expected = expected_artifacts(lane, dependency)
    plan_path = source("performance_plan") if lane["runner"] == "performance" else None
    plan = read_json(plan_path) if plan_path else None
    if plan is not None:
        if plan["resources"]["architecture"] != lane["platform"] or plan.get("package_capabilities") != lane["selection"]["capabilities"]:
            raise ValueError("performance plan differs from selected platform or package capabilities")
        (directory / "selected-performance-plan.json").write_bytes(plan_path.read_bytes())
    result = []
    for role, (identity, produced) in expected.items():
        if plan is not None:
            consumed = artifact(plan_path.parent, plan["engines"]["rust" if role == "native_package" else "jvm"]["package"])
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
        result.append({"role": "performance_plan", "consumed_path": str(plan_path), "consumed_sha256": sha256(plan_path)})
    return result


def verify_bindings(lane, directory, root, dependency):
    expected = expected_artifacts(lane, dependency)
    records = read_json(directory / "artifact-bindings.json")
    bindings = {value["role"]: value for value in records}
    roles = set(expected) | ({"performance_plan"} if lane["runner"] == "performance" else set())
    if len(bindings) != len(records) or set(bindings) != roles:
        raise ValueError("consumed artifact binding denominator differs from selected lane")
    for role, (identity, produced) in expected.items():
        value = bindings[role]
        if value.get("dependency") != identity or artifact(root, value["produced"]) != produced or value.get("consumed_sha256") != sha256(produced):
            raise ValueError("consumed artifact binding differs from selected producer")
        if role == "history" and value.get("files") != closure(produced.parent):
            raise ValueError("consumed history closure differs from selected producer")
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
    flags = {"native": "--native", "native_package": "--native-provenance", "jvm_package": "--reference-provenance",
             "performance_plan": "--plan", "history": "--history", "native_inventory": "--native"}
    if bindings:
        runner = lane["runner"]
        process = read_json(directory / ("embedded-binding.process.json" if runner == "embedded" else runner + ".process.json"))
        command = process["command"]
        for role, value in bindings.items():
            if runner == "performance" and role != "performance_plan":
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
