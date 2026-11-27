import copy
import hashlib
import os
import re
import shutil
import struct
from pathlib import Path

from history_results import FIELDS, canonical, integer, micros


def file_manifest(directory):
    directory = Path(directory)
    if not directory.is_absolute() or directory.is_symlink() or not directory.is_dir():
        raise ValueError("transfer root must be an absolute ordinary directory")
    result = []
    for path in sorted(directory.rglob("*")):
        if path.is_symlink() or not (path.is_dir() or path.is_file()):
            raise ValueError("transfer contains a redirected or non-regular artifact")
        if path.is_file():
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            result.append({"path": path.relative_to(directory).as_posix(), "bytes": path.stat().st_size, "sha256": digest})
    if not result:
        raise ValueError("transfer has no retained files")
    return result


def verify_files(directory, expected):
    if canonical(file_manifest(directory)) != canonical(expected):
        raise ValueError("protected transfer files changed")


def clone_closed(source, destination, expected):
    source, destination = Path(source).absolute(), Path(destination).absolute()
    if source.is_symlink():
        raise ValueError("transfer source is redirected")
    source = source.resolve()
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError("transfer copies must use disjoint regions")
    verify_files(source, expected)
    destination.mkdir(parents=False, exist_ok=False)
    for entry in expected:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise ValueError("invalid protected artifact path")
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        with (source / relative).open("rb") as input_file, target.open("xb") as output:
            shutil.copyfileobj(input_file, output, 65536)
            output.flush()
            os.fsync(output.fileno())
    verify_files(destination, expected)
    verify_files(source, expected)
    return destination


def local_log_head(path):
    path = Path(path)
    latest = -1
    length = path.stat().st_size
    if length >= 1 << 48:
        raise ValueError("local log exceeds its offset space")
    with path.open("rb") as stream:
        while stream.tell() < length:
            offset = stream.tell()
            header = stream.read(13)
            if len(header) != 13 or header[0] != 0x1e:
                raise ValueError("local log has an incomplete or invalid record header")
            size = struct.unpack(">i", header[1:5])[0]
            if size < 0 or offset + 21 + size > length:
                raise ValueError("local log has an incomplete payload")
            stream.seek(size, os.SEEK_CUR)
            trailer = stream.read(8)
            if len(trailer) != 8 or struct.unpack(">q", trailer)[0] != offset:
                raise ValueError("local log trailer does not identify its record")
            latest = offset
    return latest


def replica_boundary(path, position):
    if position < 0:
        return None
    with Path(path).open("rb") as stream:
        stream.seek(position)
        header = stream.read(13)
        size = struct.unpack(">i", header[1:5])[0]
        if size < 1 or size > 64 * 1024 * 1024:
            raise ValueError("replica envelope exceeds the bounded local fixture format")
        payload = stream.read(size)
    if len(payload) != size or payload[0] != 3:
        raise ValueError("replica envelope has an unsupported or truncated header")
    index, values = 1, {7: 0, 9: None}
    def varint():
        nonlocal index
        value = 0
        for shift in range(0, 70, 7):
            if index >= len(payload):
                raise ValueError("truncated replica varint")
            byte = payload[index]
            index += 1
            if shift == 63 and byte > 1:
                raise ValueError("replica varint exceeds 64 bits")
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ValueError("unterminated replica varint")
    while index < len(payload):
        tag = varint()
        field, wire = tag >> 3, tag & 7
        if field == 0:
            raise ValueError("invalid replica field zero")
        if wire == 0:
            value = varint()
            if field in values:
                values[field] = value if value < 1 << 63 else value - (1 << 64)
        elif wire == 1:
            index += 8
        elif wire == 2:
            length = varint()
            index += length
        elif wire == 5:
            index += 4
        else:
            raise ValueError("unsupported replica wire type")
        if index > len(payload):
            raise ValueError("truncated replica field")
    return {"position": position, "term": values[7], "term_sequence": values[9], "payload_sha256": hashlib.sha256(payload).hexdigest()}


def manifest(directory, fixture_sha256, prefix, checkpoint, origin):
    files = file_manifest(directory)
    roots = {Path(item["path"]).parts[1] for item in files if item["path"].startswith("storage/") and len(Path(item["path"]).parts) > 1}
    if roots and roots != {"v06"}:
        raise ValueError("recovery harness only qualifies storage version 6 at epoch zero")
    heads = {"source": local_log_head(Path(directory) / "log/LOG"), "replica": local_log_head(Path(directory) / "log/REPLICA_LOG")}
    return {"schema_version": 1, "closed": True, "fixture_sha256": fixture_sha256, "prefix": prefix,
            "storage_version": 6, "storage_epoch": 0, "partitions": 1, "external_source_token": None,
            "checkpoint": copy.deepcopy(checkpoint), "origin": origin, "files": files, "log_heads": heads,
            "last_replica_record": replica_boundary(Path(directory) / "log/REPLICA_LOG", heads["replica"]),
            "writer_lease": {"owner": "recovery_process_owner", "previous_writer": "joined", "concurrent_writers": False}}


def observation_key(value):
    return value["query"], value["product"], value["pruning"]


def expected_current(fixture, prefix, product, at, canary):
    if product == "C":
        return None if canary is None else canary["price"]
    count = next(value["transactions"] for value in fixture["prefixes"] if value["id"] == prefix)
    value = None
    for transaction in fixture["transactions"][:count]:
        operation = transaction["operation"]
        if operation == "erase" and transaction["id"] == product:
            value = None
        elif operation == "delete" and transaction["id"] == product and micros(transaction["valid_from"]) <= at:
            value = None
        elif operation == "put" and product in transaction["prices"] and micros(transaction["valid_from"]) <= at < (micros(transaction["valid_to"]) if transaction.get("valid_to") else (1 << 63) - 1):
            value = transaction["prices"][product]
    return value


def compare_recovery(fixture, baseline, execution, protected, canary=None):
    failures = []
    if execution.get("execution_complete") is not True or execution.get("closure") != "graceful":
        failures.append({"field": "execution", "detail": "recovery did not close successfully"})
    result = execution.get("result", {})
    actual = result.get("checkpoint", {})
    for field in ("tx_key", "source_position", "open_block", "adopted_block"):
        if canonical(actual.get(field)) != canonical(protected["checkpoint"].get(field)):
            failures.append({"field": "checkpoint/" + field, "expected": protected["checkpoint"].get(field), "actual": actual.get(field)})
    if actual.get("replica_position") != protected["log_heads"]["replica"]:
        failures.append({"field": "checkpoint/replica_position", "detail": "recovery did not reach exact retained log head"})
    expected = {observation_key(row): row for row in baseline["observations"]}
    if len(expected) != len(baseline["observations"]):
        failures.append({"field": "baseline", "detail": "duplicate protected observations"})
    required = set(expected) | {("current", product, "normal") for product in ("A", "B", "C")}
    for stage, observations in [("recovered", result.get("observations", []))] + ([("after_retry", result["after_retry"].get("observations", []))] if "after_retry" in result else []):
        seen = set()
        for observation in observations:
            try:
                key = observation_key(observation)
                if key not in required or key in seen or observation.get("executed") is not True:
                    raise ValueError("unknown, duplicate or unexecuted observation")
                if observation.get("id") != f"{protected['prefix']}/recovery/{key[2]}/{key[0]}/{key[1]}":
                    raise ValueError("observation identity differs from its selected recovery query")
                seen.add(key)
                if key[0] != "current":
                    original = expected[key]
                    for field in ("schema", "rows", "valid_time", "system_time"):
                        if canonical(observation.get(field)) != canonical(original[field]):
                            failures.append({"stage": stage, "query": list(key), "field": field, "expected": original[field], "actual": observation.get(field)})
                    continue
                time = protected["checkpoint"]["tx_key"]["system_time_micros"]
                if micros(observation["valid_time"]) != time or micros(observation["system_time"]) != time:
                    raise ValueError("current query used another transaction basis")
                price = expected_current(fixture, protected["prefix"], key[1], time, canary)
                schema, rows = observation["schema"], observation["rows"]
                if [field["name"] for field in schema] != list(FIELDS):
                    raise ValueError("current query omitted projected temporal columns")
                for index, field in enumerate(schema):
                    wanted = {"kind": "i64"} if index == 0 else {"kind": "timestamp", "unit": "microsecond", "timezone": "UTC"}
                    tags = field["types"]
                    if wanted not in tags or len(tags) != len({canonical(tag) for tag in tags}) or any(tag not in (wanted, {"kind": "null"}) for tag in tags):
                        raise ValueError("current query has an incorrect logical schema")
                if len(rows) != (0 if price is None else 1):
                    raise ValueError("current row count differs from independent fixture assertion")
                if price is not None:
                    row = rows[0]
                    if len(row) != 5 or row[0] != {"kind": "i64", "value": price}:
                        raise ValueError("current/canary value differs from its acknowledged identity")
                    for index, bound in enumerate(row[1:]):
                        if index in (1, 3) and bound == {"kind": "null"}:
                            continue
                        if bound.get("kind") != "timestamp" or bound.get("unit") != "microsecond" or bound.get("timezone") != "UTC" or not integer(bound.get("value")):
                            raise ValueError("current query has untyped temporal bounds")
                    if not row[1]["value"] <= time < (row[2].get("value", (1 << 63) - 1)) or not row[3]["value"] <= time < row[4].get("value", (1 << 63) - 1):
                        raise ValueError("current query lies outside returned temporal bounds")
                    if key[1] == "C" and (row[1]["value"] != micros(canary["valid_time"]) or row[3]["value"] != micros(canary["system_time"])):
                        raise ValueError("fallback did not recover canary valid/system identity")
            except (KeyError, TypeError, ValueError, IndexError) as error:
                failures.append({"stage": stage, "field": "observation", "detail": str(error)})
        for key in sorted(required - seen):
            failures.append({"stage": stage, "query": list(key), "field": "missing"})
    return {"recovery_selection_passed": not failures, "complete_parity": False, "failures": failures}


PRODUCTS = "storage/v06/tables/public$products"


def compaction_paths(target):
    if not re.fullmatch(r"l01-rc-b[0-9a-f]{2,17}", target):
        raise ValueError(f"invalid current L1 products key: {target}")
    block = target.removeprefix("l01-rc-b")
    if int(block[0], 16) != len(block) - 2 or (len(block) > 2 and block[1] == "0"):
        raise ValueError(f"noncanonical block in products key: {target}")
    return tuple(f"{PRODUCTS}/{kind}/{target}.arrow" for kind in ("data", "meta"))


def compaction_target(checkpoint):
    index = checkpoint["adopted_block"]["index"]
    if not integer(index) or index < 0:
        raise ValueError("compaction input has no adopted block")
    body = format(index, "x")
    block = f"b{len(body) - 1:x}{body}"
    l0 = f"l00-rc-{block}"
    tries = checkpoint["live_product_tries"]
    if checkpoint["live_product_segments"] != 0 or len(tries) != 1 or tries[0]["level"] != 0 or tries[0]["key"] != l0:
        raise ValueError("compaction input must have exactly one products trie in the adopted L0 block")
    return f"l01-rc-{block}", f"{PRODUCTS}/data/{l0}.arrow"


def verify_partial(directory, target, boundary):
    data, metadata = compaction_paths(target)
    files = {file["path"]: file for file in file_manifest(directory)}
    if (boundary.get("data_key"), boundary.get("metadata_key")) != tuple(path.removeprefix("storage/v06/") for path in (data, metadata)):
        raise ValueError(f"crash did not intercept the selected products key: {target}")
    if data not in files or (files[data]["bytes"], files[data]["sha256"]) != (boundary.get("data_bytes"), boundary.get("data_sha256")):
        raise ValueError(f"crash data differs from the durable boundary receipt: {data}")
    if metadata in files:
        raise ValueError(f"crashed writer already published metadata: {metadata}")


def verify_preserved_storage(before, after):
    current = {entry["path"]: entry for entry in after}
    for entry in before:
        if entry["path"].startswith("storage/") and current.get(entry["path"]) != entry:
            raise ValueError(f"compaction changed a protected object: {entry['path']}")


def products_compaction_inventory(files):
    return [entry for entry in files if any(entry["path"].startswith(f"{PRODUCTS}/{kind}/l")
            and not entry["path"].startswith(f"{PRODUCTS}/{kind}/l00-") for kind in ("data", "meta"))]


def catalogued(checkpoint, target):
    return [entry for entry in checkpoint["live_product_tries"] if entry["key"] == target]


def classify_compaction_retry(target, predecessor, clean, after, retry, same_inode):
    data_key = compaction_paths(target)[0].removeprefix("storage/v06/")
    if not same_inode or after["data"] != predecessor["data"]:
        raise ValueError(f"retry replaced the immutable predecessor: {data_key}")
    if predecessor["metadata"] is not None:
        raise ValueError(f"retry input already had metadata: {target}")
    trie = catalogued(retry["checkpoint"], target)
    error = retry.get("compaction_error")
    if predecessor["data"] != clean["data"]:
        if (not error or error.get("code") != "rtdb.storage/immutable-object-conflict"
                or error.get("context", {}).get("key") != data_key or error["context"].get("operation") != "put_file"
                or after["metadata"] is not None or trie):
            raise ValueError(f"conflicting predecessor was not refused at the selected data publication: {data_key}")
        return "immutable_conflict"
    if error or after != clean or trie != [{"key": target, "level": 1, "data_file_size": clean["data"]["bytes"]}]:
        raise ValueError(f"retry did not publish the clean native output at the selected key: {target}")
    return "reused"
