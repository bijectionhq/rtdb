import hashlib
import itertools
import json
import re
from pathlib import Path


MAX_RECORD_BYTES = 16 * 1024 * 1024
COMMENT = re.compile(r"[ \t\n\x0b\f\r]*#.+$")
WORDS = re.compile(r"[ \t\n\x0b\f\r]+")
HASH = re.compile(r"^([0-9]+) values hashing to ([0-9a-fA-F]{32})$")


def words(text):
    values = WORDS.split(text)
    while len(values) > 1 and values[-1] == "":
        values.pop()
    return values


def signed_long(text):
    if not re.fullmatch(r"[+-]?\d+", text):
        raise ValueError("invalid SQL logic signed integer")
    value = int(text)
    if not -(1 << 63) <= value < (1 << 63):
        raise ValueError("SQL logic integer exceeds pinned signed-long range")
    return value


def blank(text):
    return all(char in " \t\n\x0b\f\r\x1c\x1d\x1e\x1f" or
               char.isspace() and char not in "\u0085\u00a0\u2007\u202f" for char in text)


def parse_record(lines):
    if not lines:
        raise ValueError("empty SQL logic record")
    header = words(lines[0])
    kind = header[0]
    if kind in ("skipif", "onlyif"):
        if len(header) < 2:
            raise ValueError("missing engine condition")
        record = parse_record(lines[1:])
        if kind == "skipif":
            record.setdefault("skipif", []).append(header[1])
        else:
            record["onlyif"] = header[1]
        return record
    record = {"type": kind}
    if kind == "statement":
        if len(header) < 2 or header[1] not in ("ok", "error"):
            raise ValueError("invalid statement expectation")
        record.update(mode=header[1], sql="\n".join(lines[1:]))
    elif kind == "query":
        if len(header) < 2 or not re.fullmatch("[TIR]+", header[1]):
            raise ValueError("invalid query type string")
        sort = header[2] if len(header) > 2 else "nosort"
        if sort not in ("nosort", "rowsort", "valuesort"):
            raise ValueError("invalid query sort mode")
        groups = [list(group) for _, group in itertools.groupby(lines[1:], lambda line: line == "----")]
        result = groups[2] if len(groups) > 2 else []
        record.update(sql="\n".join(groups[0] if groups else []), type_string=header[1], sort=sort,
                      label=header[3] if len(header) > 3 else None)
        hashed = HASH.fullmatch(result[0]) if len(result) == 1 else None
        record["expected"] = {"count": signed_long(hashed[1]), "md5": hashed[2]} if hashed else {
            "count": len(result), "text": "\n".join(result) + "\n"}
    elif kind == "halt":
        if len(lines) != 1:
            raise ValueError("halt record has a body")
    elif kind == "hash-threshold":
        if len(lines) != 1 or len(header) < 2:
            raise ValueError("invalid hash threshold")
        record["threshold"] = signed_long(header[1])
    elif kind == "variable":
        record.update(variable=header[2] if len(header) > 2 else None,
                      value=header[4] if len(header) > 4 else None)
    else:
        raise ValueError("unknown SQL logic record: " + kind)
    return record


def records(stream):
    """Yield original one-based source lines and parsed blocks without retaining the script.

    Comment stripping follows the pinned Clojure runner, including SQL string contents.
    Malformed records remain evidence and do not erase subsequent source identities.
    """
    block, start, size, ordinal, assertion = [], None, 0, 0, 0

    def finish():
        nonlocal block, start, size, ordinal, assertion
        ordinal += 1
        try:
            parsed = parse_record(block)
            error = None
        except (ValueError, IndexError) as cause:
            parsed, error = None, str(cause)
        header = next((words(line)[0] for line in block if words(line)[0] not in ("skipif", "onlyif")), "")
        is_assertion = header in ("statement", "query")
        if is_assertion:
            assertion += 1
        result = {"ordinal": ordinal, "assertion_ordinal": assertion if is_assertion else None,
                  "line": start, "lines": block, "record": parsed, "parse_error": error}
        block, start, size = [], None, 0
        return result

    for number, raw in enumerate(iter(lambda: stream.readline(MAX_RECORD_BYTES + 1), ""), 1):
        line = raw[:-1].removesuffix("\r") if raw.endswith("\n") else raw
        line = COMMENT.sub("", line)
        if blank(line):
            if block:
                yield finish()
            continue
        start = number if start is None else start
        size += len(raw.encode("utf-8"))
        if size > MAX_RECORD_BYTES:
            raise ValueError(f"SQL logic record at line {start} exceeds the bounded reader")
        block.append(line)
    if block:
        yield finish()


def source_path(root, relative):
    path = Path(relative)
    if path.is_absolute() or not path.parts or any(part in (".", "..") for part in path.parts):
        raise ValueError("SQL logic source must be a normalized repository-relative path")
    result = root / path
    if result.is_symlink() or not result.is_file() or root.resolve() not in result.resolve().parents:
        raise ValueError("SQL logic source is not a regular file inside the reference checkout")
    return result


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def write_records(root, source, reference, selection_id, options, destination):
    path = source_path(root, source)
    source_sha = digest(path)
    count = assertions = parse_errors = 0
    with path.open(encoding="utf-8", newline="\n") as stream, destination.open("x", encoding="utf-8", newline="\n") as output:
        for entry in records(stream):
            count += 1
            assertions += entry["assertion_ordinal"] is not None
            parse_errors += entry["parse_error"] is not None
            entry["identity"] = {"reference": reference, "source": source, "source_sha256": source_sha,
                                 "selection": selection_id, "options": options, "ordinal": entry["ordinal"],
                                 "assertion_ordinal": entry["assertion_ordinal"], "line": entry["line"]}
            output.write(json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n")
    if digest(path) != source_sha:
        raise ValueError("SQL logic source changed while collecting records")
    return {"source": source, "source_sha256": source_sha, "records": count, "assertions": assertions, "parse_errors": parse_errors,
            "records_file": destination.name, "records_sha256": digest(destination), "selection": selection_id, "options": options}
