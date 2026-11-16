import re

from evidence import canonical, index, text


SLT_DIRECTORIES = ["random/aggregates/", "random/groupby/", "random/select/", "index/between/", "index/commute/",
                   "index/orderby/", "index/orderby_nosort/", "index/in/", "index/random/"]
CAPABILITIES = {"local": {"local"}, "kafka": {"local", "kafka"}, "kafka-connect": {"local", "kafka", "kafka-connect"},
                "s3": {"local", "s3"}, "azure": {"local", "azure"}, "gcs": {"local", "gcs"}, "pg-source": {"local", "pg-source"},
                "all-backends": {"local", "kafka", "kafka-connect", "s3", "azure", "gcs", "pg-source"}}
PAIRS = {"local": ["standalone"], "kafka": ["aws"], "kafka-connect": ["aws"], "s3": ["aws"], "azure": ["azure"],
         "gcs": ["google-cloud"], "pg-source": ["standalone"], "all-backends": ["aws", "azure", "google-cloud"]}


def lane(identity, runner, platform, dependencies=(), **selection):
    return {"id": identity, "runner": runner, "platform": platform, "dependencies": list(dependencies), "selection": selection}


def expand(matrix):
    if matrix.get("schema_version") != 1 or set(matrix.get("variants", {})) != set(CAPABILITIES) or set(matrix.get("jvm_variants", {})) != {"standalone", "aws", "azure", "google-cloud"}:
        raise ValueError("release matrix capabilities require an explicit versioned selection update")
    if set(matrix.get("platforms", [])) != {"linux/amd64", "linux/arm64"} or len(matrix["platforms"]) != 2:
        raise ValueError("release matrix platform denominator differs from its supported selections")
    result = []
    for platform in matrix["platforms"]:
        prefix = platform.replace("/", "-")
        reference = prefix + "/reference"
        for task in ("test", "integration-test", "property-test", "nightly-test", "rtdbUnselectedTags"):
            selection = {"task": task}
            if task == "property-test":
                selection.update(iterations=500, simulation_iterations=1000)
            if task == "rtdbUnselectedTags":
                selection["tags"] = ["jdbc", "timescale", "docker"]
            if task == "nightly-test":
                selection.update(tags=["s3", "google-cloud", "azure"], prerequisites=["aws", "azure", "google-cloud", "kafka-7.8.0"])
            result.append(lane(reference + "/" + task, "gradle", platform, **selection))
        for variant, definition in matrix["jvm_variants"].items():
            result.append(lane(prefix + "/jvm/" + variant + "/package", "package", platform, engine="jvm", variant=variant,
                               capabilities=definition["capabilities"]))
        for variant, features in matrix["variants"].items():
            native = prefix + "/rust/" + variant
            result.extend([lane(native + "/production", "cargo-production", platform, variant=variant, features=features),
                           lane(native + "/tests", "native-tests", platform, variant=variant, features=features),
                           lane(native + "/package", "package", platform, (native + "/production",), engine="rust", variant=variant, features=features)])
            for jvm in PAIRS[variant]:
                paired = native + "/with-" + jvm
                package_dependencies = [native + "/package", prefix + "/jvm/" + jvm + "/package"]
                capabilities = sorted(CAPABILITIES[variant].intersection(matrix["jvm_variants"][jvm]["capabilities"]))
                for runner in ("clients", "performance"):
                    result.append(lane(paired + "/" + runner, runner, platform, package_dependencies,
                                       variant=variant, jvm_variant=jvm, capabilities=capabilities))
            for runner in ("controls", "history", "embedded"):
                dependency = native + ("/package" if runner == "embedded" else "/production")
                result.append(lane(native + "/" + runner, runner, platform, (native + "/tests", dependency), variant=variant, features=features))
            result.append(lane(native + "/recovery", "recovery", platform, (native + "/history", native + "/production"), variant=variant,
                               capabilities=["local"]))
            for capability in sorted(CAPABILITIES[variant] - {"local"}):
                result.append(lane(native + "/recovery-" + capability, "required-recovery-adapter", platform,
                                   (native + "/package",), variant=variant, capability=capability,
                                   directions=["jvm-to-rust", "rust-to-jvm"],
                                   checkpoints=["acknowledged-write", "partial-compaction", "external-resume-token"]))
            for selection in ("slt-test", "slt-test-2", "direct-sql-test"):
                result.append(lane(native + "/" + selection, "sql-logic", platform, (native + "/tests", native + "/production"), variant=variant, selection=selection))
            for directory in SLT_DIRECTORIES:
                result.append(lane(native + "/slt-dir/" + directory.rstrip("/"), "sql-logic", platform, (native + "/tests", native + "/production"),
                                   variant=variant, selection="slt-test-dir", directory=directory))
            prerequisites = [entry["id"] for entry in result if entry["id"].startswith(native + "/")
                             and entry["runner"] in {"native-tests", "sql-logic", "clients"}]
            prerequisites.extend(entry["id"] for entry in result if entry["id"].startswith(reference + "/"))
            result.append(lane(native + "/inventory", "inventory", platform, prerequisites, variant=variant,
                               capabilities=sorted(CAPABILITIES[variant])))
    index(result, lambda value: value["id"], "release lane")
    return result


def validate_workflows(reference):
    slt = (reference / ".github/workflows/slt.yml").read_text()
    active = "\n".join(line for line in slt.splitlines() if not line.lstrip().startswith("#"))
    if re.findall(r'\{dir:\s*"([^"]+)"', active) != SLT_DIRECTORIES:
        raise ValueError("scheduled SLT directory selection changed from the retained baseline")
    nightly = (reference / ".github/workflows/nightly-property-test.yml").read_text()
    if 'iteration_count: "500"' not in nightly or 'simulation_iteration_count: "1000"' not in nightly:
        raise ValueError("original property/simulation iteration requirements changed")
    cloud = (reference / ".github/workflows/nightly-test.yml").read_text()
    for required in ("confluentinc/cp-kafka:7.8.0", "arguments: nightly-test", "GOOGLE_CLOUD_CREDENTIALS", "AWS_ACCESS_KEY_ID", "AZURE_CLIENT_ID"):
        if required not in cloud:
            raise ValueError("original cloud lane prerequisites changed")


def validate_bindings(lanes, bindings):
    expected = {item["id"]: item for item in lanes}
    selected = index(bindings, lambda item: text(item.get("id"), "lane binding"), "lane binding")
    if set(selected) != set(expected):
        raise ValueError("execution bindings must name every required lane exactly once")
    for identity, binding in selected.items():
        if canonical(binding.get("selection")) != canonical(expected[identity]["selection"]) or binding.get("runner") != expected[identity]["runner"]:
            raise ValueError("execution binding changes an original release selection")
        if not isinstance(binding.get("inputs"), dict):
            raise ValueError("execution binding must name its explicit artifact inputs")
    return selected


def summarize(lanes, receipts):
    observed = index(receipts, lambda item: text(item.get("id"), "receipt ID"), "lane receipt") if receipts else {}
    if set(observed) - {lane["id"] for lane in lanes}:
        raise ValueError("release evidence includes unselected lanes")
    missing, passed = [], []
    for selected in lanes:
        receipt = observed.get(selected["id"])
        if receipt is None:
            missing.append({"id": selected["id"], "reason": "no execution receipt"})
        elif receipt.get("selection") != selected["selection"] or receipt.get("runner") != selected["runner"] or receipt.get("platform") != selected["platform"]:
            raise ValueError("release receipt differs from immutable selection")
        elif receipt.get("verified") is not True or receipt.get("process_joined") is not True or not receipt.get("artifacts"):
            missing.append({"id": selected["id"], "reason": receipt.get("reason", "missing successful execution and artifact evidence")})
        else:
            passed.append(selected["id"])
    return {"schema_version": 1, "required_lanes": len(lanes), "verified_lanes": passed, "unmet_lanes": missing,
            "release_matrix_verified": bool(lanes) and not missing, "production_cutover_authorized": False,
            "deployment_cutover_prerequisites": "require separate operator-evidence assessment"}
