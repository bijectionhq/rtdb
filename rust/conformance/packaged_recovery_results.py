import hashlib
import gzip
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

from packaged_recovery_plan import node_configuration, scenarios
from packaged_lifecycle import validate_stopped


def sha(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def artifact_path(root, reference):
    relative = Path(reference["path"])
    path = root / relative
    if relative.is_absolute() or ".." in relative.parts or any(part.is_symlink() for part in (path, *path.parents)) or not path.resolve().is_relative_to(root.resolve()) or sha(path) != reference["sha256"]:
        raise ValueError("recovery receipt artifact escapes or differs from retained evidence")
    return path


def load(root, reference):
    return json.loads(artifact_path(root, reference).read_bytes())


def validate_server_configuration(plan, run, root, event, inspected):
    role = event["role"]
    case = run["selection"]
    external = case["kind"] == "external"
    epoch = int(case.get("mode") == "storage-only" and role in {"target", "restarted", "fallback"})
    readonly = not external and role in {"reader", "second", "restarted", "fallback"}
    expected = node_configuration(plan, run["namespace"], role, epoch, external, readonly, role in {"fault", "retry"})
    configuration = artifact_path(root, event["configuration"])
    if configuration.read_text() != expected or inspected["Config"]["Cmd"] != ["node", "--file", "/evidence/" + role + ".yaml"]:
        raise ValueError("actual server command or configuration differs from its selected recovery role")


def manifest(root, reference, helper_sha):
    value = load(root, reference)
    if value.get("execution_complete") is not True or value.get("resources_closed") is not True or value.get("binary_sha256") != helper_sha or value.get("source_dirty") is not False:
        raise ValueError("backend capture lacks a clean selected helper and closed resource region")
    directory = (root / reference["path"]).parent
    result = value["result"]
    for item in result["objects"]:
        file = (directory / item["file"]).resolve()
        if not file.is_relative_to(directory.resolve()) or file.stat().st_size != item["bytes"] or sha(file) != item["sha256"]:
            raise ValueError("captured provider bytes differ from their manifest")
    for name in ("source", "replica"):
        records = result[name]["records"]
        offsets = [record["offset"] for record in records]
        if offsets != sorted(set(offsets)) or (offsets[-1] if offsets else -1) != result[name]["tip"]:
            raise ValueError("captured log records do not reach their exact durable tip")
        for record in records:
            file = (directory / name / record["file"]).resolve()
            if not file.is_relative_to(directory.resolve()) or file.stat().st_size != record["bytes"] or sha(file) != record["sha256"]:
                raise ValueError("captured log payload differs from its manifest")
    return value


def protected(before, after, preserve_logs=True):
    if before["identity"]["storage"] != after["identity"]["storage"] or before["namespace"] != after["namespace"]:
        raise ValueError("recovery changed its protected storage identity")
    objects = {item["key"]: item for item in after["result"]["objects"]}
    for item in before["result"]["objects"]:
        if item["key"] not in objects or any(item[field] != objects[item["key"]][field] for field in ("bytes", "sha256")):
            raise ValueError("recovery changed protected immutable object bytes")
    if preserve_logs:
        if before["identity"]["log"] != after["identity"]["log"]:
            raise ValueError("retained-log recovery changed its log identity")
        for name in ("source", "replica"):
            records = {item["offset"]: item for item in after["result"][name]["records"]}
            for item in before["result"][name]["records"]:
                if item["offset"] not in records or any(item[field] != records[item["offset"]][field] for field in ("bytes", "sha256", "timestamp_micros")):
                    raise ValueError("recovery changed protected log records")


def worker_failure(engine, text, job, gcs_audit=False, s3_audit=False):
    if engine == "rust":
        events = []
        for line in text.splitlines():
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if isinstance(value, dict) and value.get("event") == "compaction_failed" and value.get("output_trie_key") == job["output"]:
                events.append(value)
        for value in events:
            if (value.get("table") == job["table"]
                and value.get("object_key") == job["metadata_key"] and value.get("operation") == "put_file"
                and value.get("permission_denied") is True and value.get("anomaly_code") == "rtdb.storage/permission-denied"
                and value.get("anomaly_category") == "forbidden" and re.fullmatch(r"[0-9a-f]{64}", value.get("message_sha256", ""))):
                return value
    else:
        marker = "error running compaction job: " + job["table"] + "/" + job["output"]
        for match in re.finditer(re.escape(marker) + r"(?=[,\s]|$)", text):
            chunk = text[match.end():].split("error running compaction job:", 1)[0]
            exact_key = bool(re.search(re.escape(job["metadata_key"]) + r"(?![A-Za-z0-9._/-])", chunk))
            if not exact_key:
                provider = text[:match.start()].rsplit("Exception thrown when putting object ", 1)
                if len(provider) == 2 and provider[1].splitlines()[0].strip() == job["metadata_key"]:
                    headers = [line.strip() for line in provider[1].splitlines()[1:] if "BlobStorageException:" in line and re.search(r"RequestId|requestId|request-id", line)]
                    exact_key = any(header in chunk for header in headers)
            request_id = re.search(r"(?<!Extended )Request ID: ([A-Za-z0-9_-]+)", chunk)
            if (exact_key or (s3_audit and request_id)) and (gcs_audit or re.search(r"AccessDenied(?:Exception)?|AuthorizationPermissionMismatch|PermissionDenied|\b403\b", chunk)):
                return {"event": "compaction_failed", "table": job["table"], "output_trie_key": job["output"],
                        "object_key": job["metadata_key"] if exact_key else None, "permission_denied": not gcs_audit,
                        "request_id": request_id[1] if request_id else None, "trace_sha256": hashlib.sha256((marker + chunk).encode()).hexdigest()}
    raise ValueError("packaged compaction worker did not report the exact metadata permission denial")


def utc_nanos(value):
    matched = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(?:Z|\+00:00)", value)
    if not matched:
        raise ValueError("provider audit time must be a precise UTC instant")
    whole = datetime.fromisoformat(matched[1]) - datetime(1970, 1, 1)
    return (whole.days * 86400 + whole.seconds) * 1000000000 + int((matched[2] or "0").ljust(9, "0"))


def gcs_denial(entries, request):
    """Find exact provider denial evidence for the admitted worker interval.

    - Inputs: raw Cloud Logging entries and the retained exact-object audit request.
    - Return: the matching entry, without modifying or redacting its provider fields.
    - Errors: ValueError when permission, principal, object or time evidence is absent.
    - Ownership: pure validation; the caller retains the raw response pages.
    """
    start, end = utc_nanos(request["started_at"]), utc_nanos(request["ended_at"])
    if start > end:
        raise ValueError("invalid worker audit interval")
    resource = "projects/_/buckets/" + request["bucket"] + "/objects/" + request["object"]
    for entry in entries:
        payload = entry.get("protoPayload", {})
        if (entry.get("resource", {}).get("type") != "gcs_bucket"
            or entry.get("resource", {}).get("labels", {}).get("bucket_name") != request["bucket"]
            or not entry.get("logName", "").startswith("projects/" + request["project"] + "/logs/cloudaudit.googleapis.com%2F")
            or payload.get("@type") != "type.googleapis.com/google.cloud.audit.AuditLog"
            or payload.get("serviceName") != "storage.googleapis.com" or payload.get("methodName") != "storage.objects.create"
            or payload.get("status", {}).get("code") != 7 or payload.get("resourceName") != resource
            or payload.get("authenticationInfo", {}).get("principalEmail") != request["principal_email"]):
            continue
        observed = utc_nanos(entry["timestamp"])
        if start <= observed <= end:
            return entry
    raise ValueError("no exact GCS worker permission denial; audit logging or viewer evidence is missing")


def cloudtrail_denial(entries, request):
    """Match a real failed worker request to its CloudTrail destination and principal.

    - Inputs: decoded CloudTrail Records and the retained exact worker request.
    - Return: the matching original event for PutObject or final CopyObject publication.
    - Errors: ValueError for missing evidence; malformed document shapes can raise access or type errors.
    - Time: whole-second provider timestamps denote that second; request ID must match exactly.
    """
    start, end = utc_nanos(request["started_at"]), utc_nanos(request["ended_at"])
    principal = request["principal"]
    identity_field = "arn" if "arn" in principal else "principalId"
    identity = principal.get("arn", principal.get("principal_id"))
    for entry in entries:
        if (entry.get("eventSource") != "s3.amazonaws.com" or entry.get("eventName") not in {"PutObject", "CopyObject"}
            or entry.get("eventType") != "AwsApiCall" or entry.get("errorCode") not in {"AccessDenied", "AccessDeniedException"}
            or entry.get("requestID") != request["request_id"] or entry.get("userIdentity", {}).get(identity_field) != identity
            or entry.get("requestParameters", {}).get("bucketName") != request["target_bucket"]
            or entry.get("requestParameters", {}).get("key") != request["target_object"]):
            continue
        observed = utc_nanos(entry["eventTime"])
        precision = 1000000000 if "." not in entry["eventTime"] else 1
        if start <= end and observed <= end and observed + precision > start:
            return entry
    raise ValueError("no exact CloudTrail worker denial; data-event logging or viewer evidence is missing")


def cloudtrail_entries(root, event):
    receipt = load(root, event["receipt"])
    request = load(root, event["request"])
    directory = (root / event["receipt"]["path"]).parent
    result = receipt["result"]
    start = datetime.fromisoformat(request["started_at"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(request["ended_at"].replace("Z", "+00:00"))
    prefixes = list(dict.fromkeys(request["prefix"] + "/" + value.strftime("%Y/%m/%d") + "/" for value in (start, end, end + timedelta(seconds=600))))
    if result["prefixes"] != prefixes:
        raise ValueError("CloudTrail object selection differs from the exact requested day directories")
    objects = result["objects"]
    if len(objects) > 1000 or sum(item["bytes"] for item in objects) != result["compressed_bytes"] or result["compressed_bytes"] > 67108864:
        raise ValueError("CloudTrail evidence exceeds its retained byte/object bounds")
    entries, expanded = [], 0
    for item in objects:
        file = directory / item["file"]
        if (file.is_symlink() or ".." in Path(item["file"]).parts or not file.resolve().is_relative_to(directory.resolve())
            or file.stat().st_size != item["bytes"] or item["bytes"] > 8388608 or sha(file) != item["sha256"]
            or item["bucket"] != request["bucket"] or not any(item["key"].startswith(prefix) for prefix in result["prefixes"])):
            raise ValueError("CloudTrail raw object differs from its actual provider capture")
        with gzip.open(file, "rb") as stream:
            raw = stream.read(min(8388608, 67108864-expanded) + 1)
        if len(raw) > 8388608 or expanded + len(raw) > 67108864:
            raise ValueError("CloudTrail expanded evidence exceeds its byte bound")
        expanded += len(raw)
        entries.extend(json.loads(raw)["Records"])
    return entries


def token_position(value):
    if not isinstance(value, dict) or not re.fullmatch(r"[0-9a-f]{64}", value.get("sha256", "")):
        raise ValueError("external token is absent or has no captured-byte digest")
    decoded = value["decoded"]
    field = {"postgres": "lsn", "kafka_connect": "offset"}[decoded["kind"]]
    position = decoded[field]
    if type(position) is not int or position < 0:
        raise ValueError("external token has an invalid upstream position")
    return position


def lsn(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", value):
        raise ValueError("PostgreSQL slot position is not an LSN")
    left, right = value.split("/")
    return int(left, 16) * (1 << 32) + int(right, 16)


def validate_invocations(plan, run, root, lifecycle):
    packages = {engine: json.loads((root / (engine + "-package.json")).read_bytes())["manifest"] for engine in ("rust", "jvm")}
    fixture = load(root, {"path": "fixture.json", "sha256": plan["fixture"]["sha256"]})
    compose = load(root, run["compose"])
    client_image = json.loads((root / "client-image.json").read_bytes())["Id"]
    for service, expected in (("helper", packages["rust"]["image_id"]), ("kafka", packages["jvm"]["image_id"]), ("client", client_image)):
        value = compose["services"][service]
        if value["image"] != expected or value["platform"] != plan["platform"] or value["pull_policy"] != "never":
            raise ValueError("recovery helper service differs from its retained selected image")
    starts = {}
    for event in lifecycle["events"]:
        role = event["role"]
        if event["event"] == "started":
            if role in starts:
                raise ValueError("server role has two overlapping owners")
            engine = run["selection"]["origin"] if role in {"origin", "fault", "fallback"} else run["selection"]["target"]
            inspected = load(root, event["inspection"])
            if event["engine"] != engine or event["image_id"] != packages[engine]["image_id"] or len(inspected) != 1 or inspected[0]["Image"] != event["image_id"] or inspected[0]["Id"] != event["container"]:
                raise ValueError("actual recovery container differs from its selected packaged engine")
            if inspected[0]["Config"]["Labels"].get("com.docker.compose.project") != run["namespace"]:
                raise ValueError("server container belongs to another execution owner")
            validate_server_configuration(plan, run, root, event, inspected[0])
            starts[role] = event
        else:
            started = starts.pop(role, None)
            if not started or started["container"] != event["container"] or started["engine"] != event["engine"]:
                raise ValueError("server termination does not join its admitted owner")
            state = load(root, event["state"])
            log = (root / event["logs"]["path"]).resolve()
            wait = (root / event["wait"]["path"]).resolve()
            if not log.is_relative_to(root.resolve()) or sha(log) != event["logs"]["sha256"] or not wait.is_relative_to(root.resolve()) or sha(wait) != event["wait"]["sha256"] or wait.read_text().strip() != str(state["ExitCode"]):
                raise ValueError("joined server state differs from retained wait and logs")
            if event["event"] == "killed":
                if role != "fault" or state["Running"] is not False or state["OOMKilled"] is not False or state["ExitCode"] != 137:
                    raise ValueError("fault owner did not join the selected crash boundary")
            elif event["event"] == "stopped":
                validate_stopped(event["engine"], state, log.read_text())
            else:
                raise ValueError("unknown packaged lifecycle event")
    if starts:
        raise ValueError("server lifetime has no observed owner join")
    referenced = {value["sha256"] for value in run["calls"].values()} | {value["sha256"] for value in run["captures"].values()}
    if "probe" in run:
        referenced.add(run["probe"]["sha256"])
    actual = set()
    observations = {}
    for event in run["events"]:
        request = load(root, event["request"])
        receipt = load(root, event["receipt"])
        process = load(root, event["process"])
        if process.get("completed") is not True or process.get("exit_code") != 0 or receipt.get("request_sha256") != event["request"]["sha256"] or receipt.get("execution_complete") is not True:
            raise ValueError("recovery observation lacks its actual completed invocation")
        argv = process["command"]
        if "--project-name" not in argv or argv[argv.index("--project-name") + 1] != run["namespace"] or "--rm" not in argv:
            raise ValueError("recovery invocation is outside its owned process/container region")
        request_name = "/evidence/" + Path(event["request"]["path"]).name
        if argv.count(request_name) != 1 or request["namespace"] != run["namespace"]:
            raise ValueError("recovery process did not consume its recorded namespace request")
        if "--file" not in argv or Path(argv[argv.index("--file") + 1]).name != Path(run["compose"]["path"]).name:
            raise ValueError("recovery invocation does not consume its retained Compose definition")
        if event["kind"] == "backend":
            if receipt != load(root, event["stdout"]) or receipt.get("binary_sha256") != run["helper_sha256"] or receipt.get("artifact_revision") != packages["rust"]["revision"] or receipt.get("resources_closed") is not True or "backend" not in argv:
                raise ValueError("backend summary differs from raw selected helper execution")
        elif event["kind"] == "client":
            if "/adapters/recovery.py" not in argv or receipt.get("client_closed") is not True or receipt["action"] != request["action"]:
                raise ValueError("protocol receipt has another client or action")
            if "fixture" in request and request["fixture"] != fixture:
                raise ValueError("protocol client did not consume the selected historical fixture")
        elif event["kind"] == "kafka":
            if "/adapters/kafka_recovery.clj" not in argv or event["image_id"] != packages["jvm"]["image_id"] or receipt.get("clients_closed") is not True:
                raise ValueError("upstream receipt has another Kafka producer artifact")
        elif event["kind"] == "gcs_audit":
            if "/adapters/gcs_recovery_audit.clj" not in argv or event["image_id"] != packages["jvm"]["image_id"] or receipt.get("client_closed") is not True:
                raise ValueError("audit receipt has another provider client artifact")
        elif event["kind"] == "cloudtrail":
            if "/adapters/s3_recovery_audit.clj" not in argv or event["image_id"] != packages["jvm"]["image_id"] or receipt.get("clients_closed") is not True:
                raise ValueError("CloudTrail receipt has another provider client artifact")
        else:
            raise ValueError("unknown recovery invocation kind")
        actual.add(event["receipt"]["sha256"])
        observations[event["receipt"]["sha256"]] = (event, request)
    if not referenced <= actual:
        raise ValueError("recovery summary contains an observation without an actual invocation")
    for name, expected in required_requests(run, fixture, plan["capability"]).items():
        event, request = observations[run["calls"][name]["sha256"]]
        if event["kind"] != "client" or any(request.get(key) != value for key, value in expected.items()):
            raise ValueError("recovery assertion is bound to another actual request: " + name)
    for reference in run["captures"].values():
        event, request = observations[reference["sha256"]]
        if event["kind"] != "backend" or request["action"] != {"kind": "capture"}:
            raise ValueError("provider capture is bound to another helper action")
    if "probe" in run:
        event, request = observations[run["probe"]["sha256"]]
        if event["kind"] != "backend" or request["action"] != {"kind":"denial_probe", "key":run["job"]["metadata_key"], "policy_sha256":run["policy"]["sha256"]}:
            raise ValueError("fault probe is bound to another provider operation")


def external_expectations(capability):
    postgres = capability == "pg-source"
    if not postgres and capability != "kafka-connect":
        raise ValueError("external recovery needs its selected PostgreSQL or Kafka Connect capability")
    key = lambda value: value if postgres else str(value)
    rows = [[key(1), "snapshot-row"], [key(2), "durable-row"], [key(3), "unflushed-row"], [key(4), "after-stop"]]
    resumed = rows if postgres else [rows[0], rows[2], rows[3]]
    return {"snapshot":rows[:1], "durable-rows":rows[:2], "unflushed-rows":rows[:3],
            "resumed-rows":resumed, "restarted":resumed, "fallback":resumed}


def validate_external_observation(value, expected):
    rows = [[{"kind":"integer", "value":str(key)} if type(key) is int else {"kind":"text", "value":key},
             {"kind":"text", "value":name}] for key, name in expected]
    current = value["current"]
    if [field["name"] for field in current["schema"]] != ["_id", "name"] or current["rows"] != rows:
        raise ValueError("external recovery current rows differ from the fixed upstream transactions")


def required_requests(run, fixture, capability):
    case = run["selection"]
    if case["kind"] == "external":
        expected = external_expectations(capability)
        kind = "postgres" if capability == "pg-source" else "kafka_connect"
        return {name: {"action": "external_rows", "node": role, "external_kind":kind, "expected":expected[name]} for name, role in
                (("snapshot", "origin"), ("durable-rows", "origin"), ("unflushed-rows", "origin"), ("resumed-rows", "target"), ("restarted", "restarted"), ("fallback", "fallback"))}
    count = next(value["transactions"] for value in fixture["prefixes"] if value["id"] == case["prefix"])
    requests = {"seed-" + str(index): {"action":"history_step", "node":"origin", "step":index, "fixture":fixture} for index in range(count)}
    partial = case["kind"] == "partial_compaction"
    requests["baseline"] = {"action":"observe", "node":"origin", "prefix":case["prefix"], "fixture":fixture, "canary":partial}
    for role in (("retry", "fallback") if partial else ("reader", "second", "restarted", "fallback")):
        requests[role] = {"action":"observe", "node":role, "prefix":case["prefix"], "fixture":fixture, "canary":role!="reader"}
    requests["canary-before-fault" if partial else "canary-acknowledgement"] = {"action":"canary", "node":"origin" if partial else "canary"}
    if partial:
        requests["adopted-retry"] = {"action":"compaction_adopted", "node":"retry", "trie_key":run["job"]["output"]}
    return requests


def table_row(value):
    names = [field["name"] for field in value["schema"]]
    if len(value["rows"]) != 1 or len(set(names)) != len(names):
        raise ValueError("transaction evidence is not one unambiguous row")
    return dict(zip(names, value["rows"][0]))


def history_rows(value, fixture, prefix):
    expected = {(query["id"] + "/" + product): (query, product) for query in fixture["queries"] if query["prefix"] == prefix for product in ("A", "B")}
    observed = {item["id"]: item for item in value}
    if len(observed) != len(value) or set(observed) != set(expected):
        raise ValueError("historical observation denominator differs from the fixed fixture")
    for identity, (query, product) in expected.items():
        item = observed[identity]
        if [field["name"] for field in item["schema"]] != ["price", "valid_from", "valid_to", "system_from", "system_to"]:
            raise ValueError("historical observation omits its projected temporal bounds")
        price = query["expected_prices"][product]
        if [row[0] for row in item["rows"]] != ([] if price is None else [{"kind": "integer", "value": str(price)}]):
            raise ValueError("historical price differs from the fixed expected result")
        for row in item["rows"]:
            if len(row) != 5:
                raise ValueError("historical row lacks temporal fields")
            for lower, upper, time in ((row[1], row[2], query["valid_time"]), (row[3], row[4], query["system_time"])):
                point = datetime.fromisoformat(time.replace("Z", "+00:00"))
                if lower.get("kind") != "timestamp" or datetime.fromisoformat(lower["value"]) > point:
                    raise ValueError("historical lower bound excludes the selected time")
                if upper.get("kind") != "null" and (upper.get("kind") != "timestamp" or datetime.fromisoformat(upper["value"]) <= point):
                    raise ValueError("historical exclusive upper bound excludes the selected time")


def comparison(plan, runs, root):
    """Recompute required recovery outcomes from retained invocation and provider artifacts.

    - Inputs: the validated plan, scenario receipts, and their common artifact directory.
    - Return: a qualification report; absent or failed scenarios remain explicit unmet entries.
    - Errors: ValueError or malformed-document access errors reject contradictory or altered evidence.
    - Ownership: reads existing files only; it neither contacts providers nor authorizes cutover.
    """
    selected = scenarios(plan["capability"])
    by_id = {run["id"]: run for run in runs}
    if len(by_id) != len(runs) or set(by_id) - {case["id"] for case in selected}:
        raise ValueError("recovery evidence repeats or invents a selected scenario")
    unmet, verified = [], []
    for case in selected:
        run = by_id.get(case["id"])
        if not run or run.get("execution_complete") is not True:
            unmet.append({"id": case["id"], "reason": run.get("reason", "no completed execution") if run else "no execution"})
            continue
        if run["selection"] != case or run["namespace"] != plan["namespace"] + "-" + case["id"] or run["helper_sha256"] != plan["native"]["sha256"]:
            raise ValueError("scenario selection or helper identity differs")
        lifecycle = load(root, run["lifecycle"])
        if lifecycle.get("drained") is not True or lifecycle.get("project") != run["namespace"]:
            raise ValueError("recovery owner did not drain its exact invocation resources")
        validate_invocations(plan, run, root, lifecycle)
        captures = {name: manifest(root, value, run["helper_sha256"]) for name, value in run["captures"].items()}
        if any(value["identity"]["capability"] != plan["capability"] or value["namespace"] != run["namespace"] for value in captures.values()):
            raise ValueError("captured provider selection differs")
        before = captures["handoff"]
        for name, after in captures.items():
            if name not in {"initial", "handoff", "durable", "live"}:
                protected(before, after, preserve_logs=case.get("mode") != "storage-only")
        calls = {name: load(root, value) for name, value in run["calls"].items()}
        if any(value.get("execution_complete") is not True or value.get("client_closed") is not True for value in calls.values()):
            raise ValueError("recovery client invocation did not finish and close")
        if case["kind"] != "external":
            fixture = load(root, {"path": "fixture.json", "sha256": plan["fixture"]["sha256"]})
            history_rows(calls["baseline"]["result"]["history"], fixture, case["prefix"])
            count = next(value["transactions"] for value in fixture["prefixes"] if value["id"] == case["prefix"])
            for index in range(count):
                if table_row(calls["seed-" + str(index)]["result"]).get("committed") != {"kind":"bool","value":True}:
                    raise ValueError("historical seed transaction was not acknowledged committed")
        if case["kind"] == "history":
            completed = [item["decoded"]["body"]["tx_id"] for item in before["result"]["replica"]["records"]
                         if item.get("decoded") and item["decoded"].get("body") and item["decoded"]["body"].get("kind") == "resolved_tx"]
            if not completed or not before["result"]["catalog"] or max(completed) <= before["result"]["catalog"]["tx_key"]["tx_id"]:
                raise ValueError("historical handoff did not retain an unflushed transaction above the adopted block")
            acknowledgement = table_row(calls["canary-acknowledgement"]["result"])
            if acknowledgement.get("committed") != {"kind":"bool","value":True} or acknowledgement.get("tx_id",{}).get("kind") != "integer" or int(acknowledgement["tx_id"]["value"]) < 0:
                raise ValueError("canary has no actual committed transaction identity")
            if captures["acknowledged"]["result"]["source"]["tip"] <= before["result"]["source"]["tip"] or captures["acknowledged"]["result"]["replica"]["tip"] <= before["result"]["replica"]["tip"]:
                raise ValueError("acknowledged canary did not advance both actual logs")
            for name in ("reader", "second", "restarted", "fallback"):
                if calls[name]["result"]["history"] != calls["baseline"]["result"]["history"]:
                    raise ValueError("cross-engine historical rows or temporal bounds differ")
            if calls["reader"]["result"]["canary"]["rows"] != []:
                raise ValueError("canary already existed before its acknowledged write")
            for name in ("second", "restarted", "fallback"):
                if calls[name]["result"]["canary"]["rows"] != [[{"kind": "integer", "value": "991"}]]:
                    raise ValueError("acknowledged canary did not survive read-back, restart and reverse recovery")
        elif case["kind"] == "partial_compaction":
            job = run["job"]
            if job not in before["result"]["catalog"]["available_compactions"] or job["metadata_key"] != plan["faults"][case["id"]]["metadata_key"]:
                raise ValueError("fault key differs from an actual deterministic compaction job")
            probe = load(root, run["probe"])
            policy = root / run["policy"]["path"]
            if not policy.resolve().is_relative_to(root.resolve()) or sha(policy) != run["policy"]["sha256"] or run["policy"]["sha256"] != plan["faults"][case["id"]]["policy"]["sha256"]:
                raise ValueError("fault policy differs from the selected retained artifact")
            if probe.get("execution_complete") is not True or probe.get("resources_closed") is not True or probe["result"].get("permission_denied") is not True or probe["result"]["key"] != job["metadata_key"] or probe["result"]["policy_sha256"] != plan["faults"][case["id"]]["policy"]["sha256"]:
                raise ValueError("fault probe does not bind the exact policy and metadata key")
            log_path = (root / run["worker_log"]["path"]).resolve()
            killed = next(value for value in lifecycle["events"] if value["event"] == "killed" and value["role"] == "fault")
            if run["worker_log"] != killed["logs"]:
                raise ValueError("worker trace does not belong to the joined fault container")
            if not log_path.is_relative_to(root.resolve()) or sha(log_path) != run["worker_log"]["sha256"]:
                raise ValueError("worker failure log changed")
            requires_audit = plan["capability"] == "gcs" and case["origin"] == "jvm"
            worker = worker_failure(case["origin"], log_path.read_text(), job, requires_audit, "cloudtrail" in run)
            if worker["object_key"] is None:
                event = run["cloudtrail"]
                if event not in run["events"] or event["kind"] != "cloudtrail" or case["origin"] != "jvm" or plan["capability"] != "s3":
                    raise ValueError("keyless denial lacks its actual selected CloudTrail invocation")
                selected_audit = plan["faults"][case["id"]]["cloudtrail"]
                request = load(root, event["request"])
                started = next(value for value in lifecycle["events"] if value["event"] == "started" and value["role"] == "fault")
                inspected = load(root, started["inspection"])[0]
                expected = {"schema_version":1, "namespace":run["namespace"], **selected_audit, "request_id":worker["request_id"],
                    "target_bucket":plan["storage"]["bucket"], "target_object":plan["storage"]["prefix"] + "/" + run["namespace"] + "/v06/" + job["metadata_key"],
                    "started_at":inspected["State"]["StartedAt"], "ended_at":request["ended_at"]}
                if request != expected or utc_nanos(request["ended_at"]) > utc_nanos(load(root, killed["state"])["FinishedAt"]):
                    raise ValueError("CloudTrail search differs from the selected actual worker lifetime or request")
                cloudtrail_denial(cloudtrail_entries(root, event), request)
            if requires_audit:
                event = run["gcs_audit"]
                if event not in run["events"] or event["kind"] != "gcs_audit":
                    raise ValueError("GCS denial has no actual provider audit invocation")
                request = load(root, event["request"])
                started = next(value for value in lifecycle["events"] if value["event"] == "started" and value["role"] == "fault")
                inspected = load(root, started["inspection"])[0]
                expected = {"schema_version": 1, "namespace": run["namespace"], "project": plan["storage"]["project"], "bucket": plan["storage"]["bucket"],
                    "object": plan["storage"]["prefix"] + "/" + run["namespace"] + "/v06/" + job["metadata_key"],
                    "principal_email": plan["faults"][case["id"]]["gcs_audit"]["principal_email"], "started_at": inspected["State"]["StartedAt"], "ended_at": request["ended_at"]}
                if request != expected:
                    raise ValueError("GCS audit does not name the actual worker principal, object and lifetime")
                stopped = next(value for value in lifecycle["events"] if value["event"] == "killed" and value["role"] == "fault")
                end = utc_nanos(request["ended_at"])
                finish = utc_nanos(load(root, stopped["state"])["FinishedAt"])
                if end > finish:
                    raise ValueError("GCS audit includes provider operations after the observed worker lifetime")
                receipt = load(root, event["receipt"])
                directory = (root / event["receipt"]["path"]).parent
                pages = receipt["result"]["pages"]
                if not 1 <= len(pages) <= 10:
                    raise ValueError("GCS audit lacks bounded raw response evidence")
                entries = []
                for index, page in enumerate(pages):
                    raw = load(directory, {"path": page["file"], "sha256": page["sha256"]})
                    if page["status"] != 200 or (directory / page["file"]).stat().st_size != page["bytes"] or bool(raw.get("nextPageToken")) != (index < len(pages)-1):
                        raise ValueError("GCS audit response evidence is incomplete")
                    entries.extend(raw.get("entries", []))
                gcs_denial(entries, request)
            partial = {item["key"]: item for item in captures["partial"]["result"]["objects"]}
            witnessed = {item["key"]: item for item in captures["before-crash"]["result"]["objects"]}
            retried = {item["key"]: item for item in captures["retry"]["result"]["objects"]}
            if job["data_key"] not in witnessed or job["metadata_key"] in witnessed or job["data_key"] not in partial or job["metadata_key"] in partial or job["metadata_key"] not in retried or partial[job["data_key"]]["sha256"] != retried[job["data_key"]]["sha256"] or witnessed[job["data_key"]]["sha256"] != partial[job["data_key"]]["sha256"]:
                raise ValueError("fault was not data-only or retry changed existing output bytes")
            if table_row(calls["canary-before-fault"]["result"]).get("committed") != {"kind":"bool","value":True}:
                raise ValueError("partial compaction had no preceding acknowledged canary")
            if calls["adopted-retry"]["result"]["rows"] != [[{"kind":"integer","value":"1"}]]:
                raise ValueError("retry did not observe live adoption of its actual output")
            for name in ("retry", "fallback"):
                if calls[name]["result"]["history"] != calls["baseline"]["result"]["history"] or calls[name]["result"]["canary"]["rows"] != [[{"kind":"integer","value":"991"}]]:
                    raise ValueError("partial-compaction retry changed historical results")
        else:
            for name, expected in external_expectations(plan["capability"]).items():
                validate_external_observation(calls[name]["result"], expected)
            durable = token_position(before["result"]["catalog"]["external_token"])
            applied = token_position(run["applied_token"])
            captured_tokens = [item["decoded"]["body"]["external_token"] for item in captures["live"]["result"]["replica"]["records"]
                               if item.get("decoded") and item["decoded"].get("body") and item["decoded"]["body"].get("kind")=="resolved_tx" and item["decoded"]["body"].get("external_token")]
            if not captured_tokens or captured_tokens[-1] != run["applied_token"]:
                raise ValueError("held external token differs from the actual replica payload")
            if applied <= durable:
                raise ValueError("external recovery did not retain an unflushed transaction")
            if case["mode"] == "storage-only":
                if captures["recovered"]["result"]["source_epoch"] != before["result"]["source_epoch"] + 1:
                    raise ValueError("storage-only recovery did not start a fresh log epoch")
            if token_position(captures["recovered"]["result"]["catalog"]["external_token"]) < applied:
                raise ValueError("external recovery did not make the resumed transaction durable")
            identifier = {"kind":"integer","value":"1"} if plan["capability"]=="pg-source" else {"kind":"text","value":"1"}
            original = [row for row in calls["durable-rows"]["result"]["history"]["rows"] if row[0]==identifier]
            if not original:
                raise ValueError("external durable history is empty")
            for name in ("resumed-rows","restarted","fallback"):
                value = calls[name]["result"]
                if [row for row in value["history"]["rows"] if row[0]==identifier] != original or value["current"] != calls["resumed-rows"]["result"]["current"]:
                    raise ValueError("external durable row history or resumed current rows changed across recovery")
            if plan["capability"] == "pg-source":
                samples = [load(root, value)["result"] for value in run["confirmation_samples"]]
                if not samples or any(lsn(value["confirmed_flush_lsn"]) >= applied for value in samples):
                    raise ValueError("PostgreSQL confirmed an unflushed held transaction")
                gap = load(root, run["gap_confirmation"])["result"]
                if not durable < lsn(gap["confirmed_flush_lsn"]) < applied:
                    raise ValueError("PostgreSQL unrelated-WAL gap was not observed before the held transaction")
                confirmed = load(root, run["durable_confirmation"])["result"]
                if lsn(confirmed["confirmed_flush_lsn"]) < applied:
                    raise ValueError("PostgreSQL slot did not advance after durable block adoption")
            else:
                produced = []
                for event in run["events"]:
                    if event["kind"] == "kafka":
                        request = load(root, event["request"])
                        if request["action"] == "produce" and request["records"] == [{"key": "3", "value": '{"name":"unflushed-row"}'}]:
                            produced.extend(load(root, event["receipt"])["result"]["records"])
                if len(produced) != 1 or produced[0]["topic"] != run["namespace"] + "-upstream" or produced[0]["partition"] != 0 or produced[0]["offset"] != applied:
                    raise ValueError("held Kafka token differs from the actual selected upstream acknowledgement")
        verified.append(case["id"])
    return {"schema_version": 1, "verified_scenarios": verified, "unmet_scenarios": unmet,
            "packaged_recovery_qualified": bool(selected) and not unmet, "production_cutover_authorized": False}
