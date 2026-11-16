import shutil
from pathlib import Path

from evidence import canonical, closure, read_json, sha256


def merge_runtime(destination, sources):
    """Copy Gradle inventory roots into destination and return its Path.

    - Inputs: destination must not exist and its parent must exist.
      Each source contains requested-lanes.json and the discovery/event files emitted by inventory.init.gradle.
      Only byte-identical discovery files may share a relative path.
    - Result: return the destination Path containing the copied evidence and merged requested-lanes.json.
    - Failure: evidence validation raises ValueError; malformed schema objects propagate decoding, type or lookup errors, including KeyError and TypeError.
      Filesystem failures propagate as OSError.
      Failure can leave a partial destination for inspection.
    See rust/release/input-contracts.adoc, Runtime inventory inputs.
    """
    destination = Path(destination)
    destination.mkdir()
    requested = {}
    provenance = []
    for source in sources:
        source = Path(source)
        files = closure(source)
        if not files:
            raise ValueError("runtime evidence source is empty")
        for lane in read_json(source / "requested-lanes.json")["lanes"]:
            key = lane["project"], lane["task"]
            if key in requested and requested[key] != lane:
                raise ValueError("recorded requested task identity changed across original selections")
            requested[key] = lane
        for record in files:
            relative = Path(record["path"])
            if relative == Path("requested-lanes.json"):
                continue
            target = destination / relative
            if target.exists():
                if not relative.name.endswith("-discovery.json") or sha256(target) != record["sha256"]:
                    raise ValueError("runtime evidence records duplicate or conflicting task artifacts")
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source / relative, target)
        provenance.append({"root": str(source), "files": files})
    if not requested:
        raise ValueError("no original Gradle runtime selections were provided")
    (destination / "requested-lanes.json").write_text(canonical({"schema_version": 1, "lanes": list(requested.values())}) + "\n")
    (destination / "bundle-inputs.json").write_text(canonical(provenance) + "\n")
    return destination
