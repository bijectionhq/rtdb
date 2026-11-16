import hashlib
import json
import math
from pathlib import Path


MAX_JSON_BYTES = 64 * 1024 * 1024


def read_json(path, maximum=MAX_JSON_BYTES):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"evidence is not a regular, unredirected file: {path}")
    with path.open("rb") as stream:
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise ValueError("evidence exceeds its byte limit")
    return json.loads(data, parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite JSON number")))


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be nonempty text")
    return value


def integer(value, name, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def number(value, name, minimum=0):
    if type(value) not in (int, float) or not math.isfinite(value) or value < minimum:
        raise ValueError(f"{name} must be a finite number >= {minimum}")
    return value


def digest_value(value, name):
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{name} must be a SHA256 digest")
    return value


def index(values, key, name):
    if not isinstance(values, list) or not values:
        raise ValueError(f"{name} must be a nonempty array")
    result = {}
    for value in values:
        identity = key(value)
        if identity in result:
            raise ValueError(f"duplicate {name} identity: {identity}")
        result[identity] = value
    return result


def artifact(root, reference):
    relative = Path(text(reference.get("path"), "artifact path"))
    root = Path(root).resolve()
    if relative.is_absolute() or any(part in (".", "..") for part in relative.parts):
        raise ValueError("artifact path must stay beneath its evidence root")
    path = root / relative
    cursor = root
    for part in relative.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            raise ValueError("artifact path traverses a symbolic link")
    if not path.is_file() or sha256(path) != digest_value(reference.get("sha256"), "artifact digest"):
        raise ValueError("artifact is missing or differs from its retained digest")
    if "bytes" in reference and path.stat().st_size != integer(reference["bytes"], "artifact size"):
        raise ValueError("artifact size differs from its receipt")
    return path


def retained(root, path):
    path = Path(path)
    return {"path": str(path.relative_to(root)), "sha256": sha256(path), "bytes": path.stat().st_size}


def closure(root):
    root = Path(root)
    result = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("retained artifact closure contains a symbolic link")
        if path.is_file():
            result.append(retained(root, path))
    return result
