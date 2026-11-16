import json
import re
from urllib.parse import urlsplit


CAPABILITIES = {"local", "kafka", "kafka-connect", "s3", "azure", "gcs", "pg-source"}
DIRECTIONS = (("jvm", "rust"), ("rust", "jvm"))
PREFIXES = ("june", "july", "august", "september")


def scenarios(capability):
    if capability not in CAPABILITIES:
        raise ValueError("unrecognized packaged recovery capability")
    result = []
    for origin, target in DIRECTIONS:
        for prefix in PREFIXES:
            result.append({"id": origin + "-to-" + target + "-" + prefix, "kind": "history", "origin": origin, "target": target, "prefix": prefix})
        result.append({"id": origin + "-to-" + target + "-partial", "kind": "partial_compaction", "origin": origin, "target": target, "prefix": "july"})
        if capability in {"kafka-connect", "pg-source"}:
            for mode in ("retained-log", "storage-only"):
                result.append({"id": origin + "-to-" + target + "-" + mode, "kind": "external", "origin": origin, "target": target, "mode": mode})
    return result


def reference(value):
    if not isinstance(value, dict) or not isinstance(value.get("path"), str) or not value["path"] or not re.fullmatch(r"[0-9a-f]{64}", value.get("sha256", "")):
        raise ValueError("recovery input requires a path and lowercase SHA-256")


def environment_name(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z_][A-Z0-9_]*", value):
        raise ValueError("invalid explicit credential environment name")


def credentials(value):
    if not isinstance(value, dict) or set(value) - {"environment", "files"}:
        raise ValueError("credential binding has unknown fields")
    for target, source in value.get("environment", {}).items():
        environment_name(target)
        environment_name(source)
        if target in {"JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "CLASSPATH", "LD_PRELOAD", "LD_LIBRARY_PATH", "PATH", "PYTHONPATH", "RUSTFLAGS"}:
            raise ValueError("credential bindings cannot replace packaged runtime code or startup options")
    targets = set()
    for item in value.get("files", []):
        reference(item)
        target = item.get("target", "")
        if not target.startswith("/secrets/") or ".." in target.split("/") or target in targets:
            raise ValueError("credential files need distinct /secrets paths")
        targets.add(target)


def validate_plan(plan):
    """Validate a version-1 packaged recovery selection without opening any resource.

    - Input: the JSON object documented in `packaged-recovery-inputs.adoc`.
    - Return: the same object, with its original identities and references unchanged.
    - Errors: ValueError for invalid selections; malformed JSON shapes can raise access or type errors.
    - Ownership: callers resolve artifact references and provide credentials only at execution.
    """
    allowed = {"schema_version", "namespace", "platform", "capability", "engines", "native", "fixture", "storage", "kafka", "postgres",
               "credentials", "client_password", "faults", "memory_bytes", "cpu_count", "timeout_seconds", "maximum_capture_bytes"}
    if set(plan) - allowed or plan.get("schema_version") != 1 or plan.get("capability") not in CAPABILITIES:
        raise ValueError("unsupported packaged recovery plan")
    if not re.fullmatch(r"rtdb-recovery-[a-z0-9]{12,24}", plan.get("namespace", "")):
        raise ValueError("recovery needs a new operator-reserved namespace")
    if plan.get("platform") not in {"linux/amd64", "linux/arm64"} or set(plan.get("engines", {})) != {"jvm", "rust"}:
        raise ValueError("recovery requires both packaged engines on one explicit platform")
    for engine in plan["engines"].values():
        if set(engine) != {"package"}:
            raise ValueError("packaged engine selection contains execution overrides")
        reference(engine["package"])
    for field in ("native", "fixture", "client_password"):
        reference(plan[field])
    for field, minimum, maximum in (("memory_bytes", 256 * 1024 * 1024, 64 * 1024 * 1024 * 1024), ("cpu_count", 1, 64),
                                   ("timeout_seconds", 1, 600), ("maximum_capture_bytes", 1, 8 * 1024 * 1024 * 1024)):
        if type(plan[field]) is not int or not minimum <= plan[field] <= maximum:
            raise ValueError("invalid recovery resource bound: " + field)
    storage = plan["storage"]
    expected_storage = plan["capability"] if plan["capability"] in {"s3", "azure", "gcs"} else "local"
    if storage.get("kind") != expected_storage:
        raise ValueError("recovery storage does not exercise its selected capability")
    keys = {"local": {"kind"}, "s3": {"kind", "bucket", "region", "prefix", "endpoint", "path_style"},
            "azure": {"kind", "account", "container", "prefix", "endpoint"}, "gcs": {"kind", "project", "bucket", "prefix"}}[expected_storage]
    if set(storage) != keys:
        raise ValueError("storage backend fields differ from the versioned contract")
    if expected_storage != "local":
        prefix = storage["prefix"]
        if not isinstance(prefix, str) or not prefix or any(part in {"", ".", ".."} for part in prefix.split("/")) or any(char in prefix for char in "\\%?#\x00"):
            raise ValueError("remote prefix is not a canonical provider directory")
        if storage.get("endpoint") is not None:
            endpoint = urlsplit(storage["endpoint"])
            if endpoint.scheme not in {"https", "http"} or not endpoint.hostname or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
                raise ValueError("provider endpoint must not contain credential material or query overrides")
    if plan["capability"] in {"kafka", "kafka-connect"}:
        kafka = plan["kafka"]
        if set(kafka) != {"bootstrap_servers", "properties_environment"} or not kafka["bootstrap_servers"]:
            raise ValueError("Kafka input must identify a real cluster and explicit property environment")
        for value in kafka["properties_environment"].values():
            environment_name(value)
    elif "kafka" in plan:
        raise ValueError("Kafka configuration is not part of this selected scenario")
    if plan["capability"] == "pg-source":
        postgres = plan["postgres"]
        if set(postgres) != {"hostname", "port", "database", "username", "password_environment"} or type(postgres["port"]) is not int or not 1 <= postgres["port"] <= 65535:
            raise ValueError("PostgreSQL input must identify the upstream database")
        environment_name(postgres["password_environment"])
    elif "postgres" in plan:
        raise ValueError("PostgreSQL configuration is not part of this selected scenario")
    credentials(plan["credentials"])
    if "RTDB_RECOVERY_PASSWORD" not in plan["credentials"].get("environment", {}):
        raise ValueError("server password needs an explicit environment binding")
    faults = plan.get("faults", {})
    fault_ids = {item["id"] for item in scenarios(plan["capability"]) if item["kind"] == "partial_compaction"}
    if set(faults) - fault_ids:
        raise ValueError("fault configuration names an unselected scenario")
    for identity, fault in faults.items():
        fields = {"policy", "metadata_key", "credentials", "apparmor_profile"}
        if expected_storage == "gcs" and identity.startswith("jvm-"):
            fields.add("gcs_audit")
        if expected_storage == "s3" and identity.startswith("jvm-") and "cloudtrail" in fault:
            fields.add("cloudtrail")
        if set(fault) != fields or not re.fullmatch(r"tables/[^/]+/meta/l[0-9a-f]+-[a-zA-Z0-9_-]+\.arrow", fault["metadata_key"]):
            raise ValueError("fault requires one exact compaction metadata key")
        reference(fault["policy"])
        credentials(fault["credentials"])
        profile = fault["apparmor_profile"]
        if expected_storage == "local" and profile != plan["namespace"] + "-" + identity:
            raise ValueError("local fault requires its exact invocation-scoped AppArmor profile")
        if expected_storage != "local" and profile is not None:
            raise ValueError("remote storage faults use provider policy credentials")
        if "gcs_audit" in fault:
            audit = fault["gcs_audit"]
            if set(audit) != {"principal_email"} or not re.fullmatch(r"[A-Za-z0-9_.+%-]+@[A-Za-z0-9.-]+", audit["principal_email"]):
                raise ValueError("GCS JVM fault requires its explicit audited writer principal")
        if "cloudtrail" in fault:
            audit = fault["cloudtrail"]
            if set(audit) != {"bucket", "prefix", "region", "principal"} or not all(isinstance(audit[field], str) and audit[field] for field in ("bucket", "prefix", "region")):
                raise ValueError("CloudTrail input requires its actual S3 log location")
            if any(part in {"", ".", ".."} for part in audit["prefix"].split("/")) or any(char in audit["prefix"] for char in "\\%?#\x00"):
                raise ValueError("CloudTrail prefix must be a canonical region directory")
            principal = audit["principal"]
            if set(principal) not in ({"arn"}, {"principal_id"}) or not isinstance(next(iter(principal.values())), str):
                raise ValueError("CloudTrail writer identity requires one exact ARN or principal ID")
            if "arn" in principal and not re.fullmatch(r"arn:aws(?:-[a-z]+)*:(?:iam|sts)::[0-9]{12}:(?:user/.+|assumed-role/.+/.+|root)", principal["arn"]):
                raise ValueError("CloudTrail ARN must identify the actual writer, including its role session")
            if "principal_id" in principal and not re.fullmatch(r"[A-Z0-9]+(?::[A-Za-z0-9+=,.@_-]+)?", principal["principal_id"]):
                raise ValueError("CloudTrail principal ID must be exact")
    return plan


def quoted(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False)


def backend_configuration(plan, namespace, epoch=0):
    if epoch not in {0, 1}:
        raise ValueError("recovery log epochs are explicit zero or one")
    root = "/var/lib/rtdb/" + namespace
    if plan["capability"] in {"kafka", "kafka-connect"}:
        log = "!Kafka\n  cluster: recovery\n  topic: " + quoted(namespace + "-source-" + str(epoch)) + "\n  epoch: " + str(epoch)
    else:
        log = "!Local\n  path: " + quoted(root + "/log-" + str(epoch)) + "\n  epoch: " + str(epoch)
    storage = plan["storage"]
    kind = storage["kind"]
    if kind == "local":
        store = "!Local\n  path: " + quoted(root + "/storage")
    else:
        fields = {"s3": [("bucket", "bucket"), ("region", "region"), ("endpoint", "endpoint"), ("pathStyleAccessEnabled", "path_style")],
                  "azure": [("storageAccount", "account"), ("container", "container"), ("storageAccountEndpoint", "endpoint")],
                  "gcs": [("projectId", "project"), ("bucket", "bucket")]}[kind]
        store = "!Remote\n  objectStore: !" + {"s3": "S3", "azure": "Azure", "gcs": "GoogleCloud"}[kind]
        store += "\n    prefix: " + quoted(storage["prefix"] + "/" + namespace)
        for output, source in fields:
            if storage[source] is not None:
                store += "\n    " + output + ": " + quoted(storage[source])
    return "log: " + log + "\nstorage: " + store + "\n"


def remotes_configuration(plan):
    values = []
    if "kafka" in plan:
        kafka = plan["kafka"]
        value = "  recovery: !Kafka\n    bootstrapServers: " + quoted(kafka["bootstrap_servers"])
        if kafka["properties_environment"]:
            value += "\n    propertiesMap:"
            for name, environment in sorted(kafka["properties_environment"].items()):
                value += "\n      " + quoted(name) + ": !Env " + environment
        values.append(value)
    if "postgres" in plan:
        postgres = plan["postgres"]
        values.append("  recovery: !Postgres\n" + "\n".join("    " + field + ": " + quoted(postgres[field]) for field in ("hostname", "port", "database", "username")) + "\n    password: !Env " + postgres["password_environment"])
    return "remotes:\n" + "\n".join(values) + "\n" if values else ""


def node_configuration(plan, namespace, role, epoch=0, external=False, readonly=False, compactor=False):
    backend = backend_configuration(plan, namespace, epoch)
    if external:
        backend = "log: !Local\n  path: " + quoted("/var/lib/rtdb/" + namespace + "/primary-" + role + "/log") + "\nstorage: !Local\n  path: " + quoted("/var/lib/rtdb/" + namespace + "/primary-" + role + "/storage") + "\n"
    return (backend + remotes_configuration(plan) + "diskCache:\n  path: " + quoted("/var/lib/rtdb/" + namespace + "/cache-" + role) + "\n  maxSizeBytes: 268435456\n"
        "server: {host: '0.0.0.0', port: 5432}\nflightSql: {host: '0.0.0.0', port: 3000}\nhealthz: {host: '0.0.0.0', port: 8080}\n"
        "authn: !SingleRootUser\n  password: !Env RTDB_RECOVERY_PASSWORD\n"
        "garbageCollector: {enabled: false}\nindexer: {flushDuration: PT24H, rowsPerBlock: 100000000}\n"
        "compactor: {threads: " + ("1" if compactor else "0") + "}\n"
        "readOnlyDatabases: " + ("true" if readonly else "false") + "\n")


def attached_configuration(plan, namespace, epoch):
    source = ("!Postgres\n  remote: recovery\n  slotName: " + quoted(namespace.replace("-", "_")) + "\n  publicationName: " + quoted(namespace.replace("-", "_")) + "\n  indexer: !DirectMirror {}\n"
              if plan["capability"] == "pg-source" else
              "!KafkaConnect\n  remote: recovery\n  topic: " + quoted(namespace + "-upstream") + "\n  connectConfig:\n    key.converter: org.apache.kafka.connect.storage.StringConverter\n"
              "    value.converter: org.apache.kafka.connect.json.JsonConverter\n    value.converter.schemas.enable: 'false'\n  indexer: !Docs\n    table: events\n")
    return backend_configuration(plan, namespace, epoch) + "externalSource: " + source
