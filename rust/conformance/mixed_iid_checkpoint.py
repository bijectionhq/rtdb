import hashlib
import os
import stat
from contextlib import ExitStack
from pathlib import PurePosixPath

from history_results import integer
from recovery_results import local_log_head
from run_mixed_iid_reference import CASE_DIRECTORY, CHECKPOINT_SIDES, SOURCE_SHA256, UNIQUE_ID, checkpoint_files, require, verify_checkpoint


LAST_PREFIX_TIME = 1_578_679_200_000_000
DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK
FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK


def native_path(path):
    side, kind, *suffix = path.parts
    directory = "storage" if kind == "objects" else kind
    return PurePosixPath(f"mixed-iid-{side}", directory, *suffix)


def copy_file(root, source, target, size, checksum):
    target.parent.mkdir(parents=True, exist_ok=True)
    with ExitStack() as owners:
        directory = os.open(root, DIRECTORY_FLAGS)
        owners.callback(os.close, directory)
        for name in source.parts[:-1]:
            directory = os.open(name, DIRECTORY_FLAGS, dir_fd=directory)
            owners.callback(os.close, directory)
        path_fd = os.open(source.name, FILE_FLAGS, dir_fd=directory)
        origin = owners.enter_context(os.fdopen(path_fd, "rb"))
        info = os.fstat(origin.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size == size, f"checkpoint source size or kind changed: {source}")
        destination = owners.enter_context(target.open("xb"))
        digest = hashlib.sha256()
        remaining = size
        while remaining:
            block = origin.read(min(remaining, 65536))
            require(block, f"checkpoint source shrank: {source}")
            remaining -= len(block)
            digest.update(block)
            destination.write(block)
        require(not origin.read(1) and digest.hexdigest() == checksum, f"checkpoint source bytes changed: {source}")
        destination.flush()
        os.fsync(destination.fileno())


def verify_archive(root, checkpoint, rewrites):
    rewritten = {PurePosixPath(rewrite["after"]["path"]): rewrite["after"] for rewrite in rewrites}
    require(len(rewritten) == len(rewrites) and rewritten, "checkpoint has missing or repeated JVM rewrites")
    verify_checkpoint(root, checkpoint, rewritten)


def stage(root, destination, execution, reference_result_sha256, reference_execution_sha256):
    require(execution["schema_version"] == 2, "portable mixed IID requires the JVM checkpoint receipt")
    checkpoint, rewrites = execution["checkpoint"], execution["rewritten_files"]
    verify_archive(root, checkpoint, rewrites)
    inventory = checkpoint_files(checkpoint)
    require(root == root.resolve() and destination == destination.resolve()
            and not (root.is_relative_to(destination) or destination.is_relative_to(root)),
            "checkpoint archive and copies must use disjoint canonical paths")
    destination.mkdir()
    copies = []
    for source, (size, checksum) in sorted(inventory.items()):
        copied = native_path(source)
        copy_file(root, source, destination / copied, size, checksum)
        copies.append({"source": source.as_posix(), "destination": copied.as_posix(), "bytes": size, "sha256": checksum})
    heads = {}
    for side in CHECKPOINT_SIDES:
        log = destination / f"mixed-iid-{side}" / "log"
        heads[side] = {"source": local_log_head(log / "LOG"), "replica": local_log_head(log / "REPLICA_LOG")}
        require(min(heads[side].values()) >= 0, f"checkpoint logs are empty: {side}")
    verify_archive(root, checkpoint, rewrites)
    return {"schema_version": 1, "case": UNIQUE_ID, "source_sha256": SOURCE_SHA256,
            "reference_result_sha256": reference_result_sha256, "reference_execution_sha256": reference_execution_sha256,
            "checkpoint": checkpoint, "copies": copies, "heads": heads, "reference_rewrites": rewrites}


def native_rewrites(request):
    result = []
    for rewrite in request["reference_rewrites"]:
        before, after = rewrite["before"], rewrite["after"]
        source = PurePosixPath(after["path"]).relative_to(CASE_DIRECTORY)
        path = PurePosixPath(*native_path(source).parts[1:])
        result.append({"path": path.as_posix(), "trie_key": path.stem,
                       "before": {key: value for key, value in before.items() if key != "path"},
                       "after": {key: value for key, value in after.items() if key != "path"}})
    return result


def verify_recovered(recovered, heads):
    require(set(recovered) == {"archive_source_position", "archive_replica_position", "source_position", "replica_position",
                               "tx_id", "system_time_micros", "adopted_block", "open_block"}
            and all(integer(value) for value in recovered.values())
            and recovered["archive_source_position"] == heads["source"] == recovered["source_position"]
            and recovered["archive_replica_position"] == heads["replica"]
            and recovered["replica_position"] >= heads["replica"]
            and 0 <= recovered["tx_id"] <= heads["source"]
            and recovered["system_time_micros"] == LAST_PREFIX_TIME
            and recovered["adopted_block"] >= 0
            and recovered["open_block"] == recovered["adopted_block"] + 1,
            "native did not recover the protected JVM tick-39 basis")
