import datetime
import hashlib
import json
from pathlib import Path


LAYOUTS = ("live", "flushed", "compacted", "restarted")
FIELDS = ("price", "valid_from", "valid_to", "system_from", "system_to")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def micros(value):
    date = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if date.utcoffset() is None:
        raise ValueError("history instant has no UTC offset")
    delta = date - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def integer(value):
    return type(value) is int and -(1 << 63) <= value < (1 << 63)


def required(fixture):
    queries = {query["id"]: query for query in fixture["queries"]}
    if len(queries) != len(fixture["queries"]) or not queries:
        raise ValueError("history fixture query identities must be unique and nonempty")
    prefixes = {prefix["id"]: prefix for prefix in fixture["prefixes"]}
    if len(prefixes) != len(fixture["prefixes"]) or not prefixes:
        raise ValueError("history fixture prefixes must be unique and nonempty")
    identities = {}
    for query in queries.values():
        if query["prefix"] not in prefixes:
            raise ValueError("history query references an unknown prefix")
        for layout in LAYOUTS:
            for pruning in ("normal", "bypassed"):
                for product in ("A", "B"):
                    identity = f"{query['prefix']}/{layout}/{pruning}/{query['id']}/{product}"
                    identities[identity] = (query, layout, pruning, product)
    return identities


def inspect_execution(fixture, execution, side, fixture_sha256=None):
    failures, observations = [], {}

    def fail(identity, field, detail):
        failures.append({"side": side, "id": identity, "field": field, "detail": detail})

    expected = required(fixture)
    if execution.get("schema_version") != 1 or execution.get("execution_complete") is not True:
        fail("execution", "status", "history execution is incomplete")
    if fixture_sha256 is not None and execution.get("fixture_sha256") != fixture_sha256:
        fail("execution", "fixture_sha256", "runner observed different fixture bytes")
    layouts = execution.get("layouts", [])
    if not isinstance(layouts, list):
        fail("execution", "layouts", "layouts is not an array")
        layouts = []
    seen = set()
    prefixes = {prefix["id"]: prefix for prefix in fixture["prefixes"]}
    expected_layouts = {(prefix, layout) for prefix in prefixes for layout in LAYOUTS}
    for receipt in layouts:
        try:
            key = (receipt["prefix"], receipt["layout"])
            identity = "/".join(key)
            if key not in expected_layouts or key in seen:
                fail(identity, "layout", "unknown or repeated physical layout")
                continue
            seen.add(key)
            if receipt.get("status") != "observed":
                fail(identity, "status", str(receipt.get("error", "layout did not execute")))
            checkpoint = receipt["checkpoint"]
            prefix = prefixes[key[0]]
            system_time = micros(fixture["transactions"][prefix["transactions"] - 1]["system_time"])
            if checkpoint["tx_key"]["system_time_micros"] != system_time:
                fail(identity, "checkpoint", "physical layout changed the user history prefix")
            for position in ("source_position", "replica_position", "open_block"):
                if not integer(checkpoint[position]):
                    fail(identity, position, "missing signed 64-bit checkpoint position")
            tries = checkpoint["live_product_tries"]
            if key[1] == "live":
                if not integer(checkpoint["live_product_segments"]) or checkpoint["live_product_segments"] <= 0 or tries:
                    fail(identity, "layout", "live observation did not retain unflushed products")
            else:
                if checkpoint["adopted_block"] is None or not tries:
                    fail(identity, "layout", "durable observation has no adopted block or live products trie")
                if checkpoint["live_product_segments"] != 0:
                    fail(identity, "layout", "durable observation retains unflushed products")
                if key[1] == "flushed" and any(trie["level"] != 0 for trie in tries):
                    fail(identity, "layout", "flush observation ran after compaction")
                if key[1] in ("compacted", "restarted") and not any(trie["level"] > 0 for trie in tries):
                    fail(identity, "layout", "compaction has no live products trie above level zero")
            for observation in receipt["observations"]:
                identity = observation["id"]
                if identity not in expected or identity in observations:
                    fail(identity, "identity", "unknown or repeated history observation")
                    continue
                query, layout, pruning, product = expected[identity]
                observations[identity] = observation
                if (query["prefix"], layout) != key:
                    fail(identity, "identity", "observation belongs to another physical layout")
                for field, value in (("executed", True), ("query", query["id"]), ("product", product),
                                     ("valid_time", query["valid_time"]), ("system_time", query["system_time"]), ("pruning", pruning)):
                    if canonical(observation.get(field)) != canonical(value):
                        fail(identity, field, "observation does not match its selected query")
                for field in ("read_bytes", "read_calls"):
                    if not integer(observation.get(field)) or observation[field] < 0:
                        fail(identity, field, "missing nonnegative object-read measurement")
                if observation.get("read_boundary") not in ("object_store_payload", "buffer_pool_logical_payload"):
                    fail(identity, "read_boundary", "read measurement does not identify its accounting boundary")
                schema = observation["schema"]
                if [field["name"] for field in schema] != list(FIELDS):
                    fail(identity, "schema", "history projection is incomplete or out of order")
                for index, field in enumerate(schema):
                    tags = field["types"]
                    wanted = {"kind": "i64"} if index == 0 else {"kind": "timestamp", "unit": "microsecond", "timezone": "UTC"}
                    if not isinstance(tags, list) or not tags or len(tags) != len({canonical(tag) for tag in tags}) or wanted not in tags or any(tag not in (wanted, {"kind": "null"}) for tag in tags):
                        fail(identity, "schema", "history field lacks its required logical type")
                rows = observation["rows"]
                price = query["expected_prices"][product]
                if not isinstance(rows, list) or len(rows) != (0 if price is None else 1):
                    fail(identity, "rows", "row count differs from the independent price assertion")
                    continue
                if price is None:
                    continue
                if len(rows[0]) != 5 or canonical(rows[0][0]) != canonical({"kind": "i64", "value": price}):
                    fail(identity, "price", "typed price differs from the independent fixture assertion")
                    continue
                bounds = rows[0][1:]
                if any(not (index in (1, 3) and bound == {"kind": "null"}) and
                       (bound.get("kind") != "timestamp" or bound.get("unit") != "microsecond" or
                        bound.get("timezone") != "UTC" or not integer(bound.get("value"))) for index, bound in enumerate(bounds)):
                    fail(identity, "temporal_bounds", "projection omitted typed temporal bounds")
                    continue
                upper = lambda bound: (1 << 63) - 1 if bound == {"kind": "null"} else bound["value"]
                if not bounds[0]["value"] <= micros(query["valid_time"]) < upper(bounds[1]):
                    fail(identity, "valid_bounds", "projected valid interval does not contain the query instant")
                if not bounds[2]["value"] <= micros(query["system_time"]) < upper(bounds[3]):
                    fail(identity, "system_bounds", "projected system interval does not contain the query instant")
        except (KeyError, TypeError, ValueError, IndexError) as error:
            fail("layout", "receipt", f"invalid history receipt: {error}")
    for prefix, layout in sorted(expected_layouts - seen):
        fail(f"{prefix}/{layout}", "layout", "required layout is missing")
    for identity in sorted(set(expected) - set(observations)):
        fail(identity, "observation", "required observation is missing")
    baseline = {}
    for identity, observation in observations.items():
        query, _, _, product = expected[identity]
        key = (query["id"], product)
        value = {field: observation.get(field) for field in ("schema", "rows")}
        if key in baseline and canonical(value) != canonical(baseline[key]):
            fail(identity, "layout_agreement", "schema or temporal rows differ between layouts or pruning modes")
        else:
            baseline[key] = value
    return observations, failures


def compare_history(fixture, reference, native, fixture_sha256=None):
    left, failures = inspect_execution(fixture, reference, "reference", fixture_sha256)
    right, native_failures = inspect_execution(fixture, native, "rust", fixture_sha256)
    failures.extend(native_failures)
    for identity in sorted(left.keys() & right.keys()):
        for field in ("schema", "rows"):
            if canonical(left[identity].get(field)) != canonical(right[identity].get(field)):
                failures.append({"side": "comparison", "id": identity, "field": field,
                                 "reference": left[identity].get(field), "rust": right[identity].get(field)})
    return {"history_selection_passed": not failures, "required_observations": len(required(fixture)),
            "reference_observations": len(left), "rust_observations": len(right), "failures": failures,
            "assertion_ledger_reconciled": False, "complete_parity": False}


def verify_artifacts(root, execution):
    root = Path(root).resolve()
    for layout in execution["layouts"]:
        directory = root / f"{layout['prefix']}-{layout['layout']}"
        if not directory.is_dir() or directory.is_symlink() or directory.parent != root:
            raise ValueError("history checkpoint directory is missing or redirected")
        files = {}
        for path in directory.rglob("*"):
            if path.is_symlink() or not (path.is_dir() or path.is_file()):
                raise ValueError("history checkpoint contains a redirected or non-regular artifact")
            if path.is_file():
                files[path.relative_to(directory).as_posix()] = path
        retained = layout["artifacts"]
        names = [entry["path"] for entry in retained]
        if not files or len(names) != len(set(names)) or set(names) != set(files):
            raise ValueError("history checkpoint inventory differs from retained artifacts")
        for entry in retained:
            path = files[entry["path"]]
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if path.stat().st_size != entry["bytes"] or digest != entry["sha256"]:
                raise ValueError("history checkpoint bytes changed after the observation")


def verify_runtime_artifacts(artifacts):
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("JVM runtime closure is empty")
    for artifact in artifacts:
        path = Path(artifact["path"])
        if not path.is_absolute() or path.is_symlink():
            raise ValueError("JVM artifact path must identify its canonical runtime location")
        if artifact["kind"] == "file":
            records = [(path, artifact)]
        elif artifact["kind"] == "directory" and path.is_dir():
            actual = {}
            for child in path.rglob("*"):
                if child.is_symlink() or not (child.is_dir() or child.is_file()):
                    raise ValueError("JVM artifact contains a redirected or non-regular file")
                if child.is_file():
                    actual[child.relative_to(path).as_posix()] = child
            expected = artifact["files"]
            names = [item["path"] for item in expected]
            if len(names) != len(set(names)) or set(names) != set(actual):
                raise ValueError("JVM runtime directory differs from its captured file set")
            records = [(actual[item["path"]], item) for item in expected]
        else:
            raise ValueError("JVM runtime artifact has an unknown or missing kind")
        for file, record in records:
            if not file.is_file() or file.stat().st_size != record["bytes"]:
                raise ValueError("JVM runtime artifact size changed")
            with file.open("rb") as stream:
                sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
            if sha256 != record["sha256"]:
                raise ValueError("JVM runtime artifact bytes changed")
