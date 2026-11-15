#!/usr/bin/env python3
"""Inventory pinned source without loading a test framework or executing a build."""

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path


REFERENCE = "c84f9334eb060ceb92d28ecdd4a06e6b625932fd"


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])


def lane(path):
    if path.startswith("sqlite-test/") and path.endswith(".test"):
        return "sql_logic"
    if path.startswith("lang/"):
        return "language_client"
    if "/src/test/" in "/" + path and path.endswith((".clj", ".kt", ".java")):
        return "gradle"
    if "/src/testFixtures/" in "/" + path or "/src/test/resources/" in "/" + path:
        return "fixture"
    if path.startswith(".github/workflows/") or path.endswith("build.gradle.kts"):
        return "selection"
    if path == ".gitmodules":
        return "submodules"
    return None


def declarations(path, text):
    lines = text.splitlines()
    if path.startswith("sqlite-test/") and path.endswith(".test"):
        ordinal = 0
        for number, line in enumerate(lines, 1):
            if re.match(r"^(?:statement|query)\s", line):
                ordinal += 1
                yield number, f"record:{ordinal}", "slt_record", line
        return
    if path.endswith(".clj"):
        namespace = re.search(r"\(ns\s+([^\s()]+)", text)
        namespace = namespace.group(1) if namespace else path
        pattern = re.compile(r"\((?:[\w.-]+/)?(?:deftest|defspec)\s+(?:\^:[\w-]+\s+)*([^\s()]+)")
        for match in pattern.finditer(text):
            yield text.count("\n", 0, match.start()) + 1, namespace + "/" + match.group(1), "clojure_var_candidate", match.group(0)
        return
    if path.endswith((".kt", ".java")):
        package = re.search(r"^package\s+([^;\s]+)", text, re.MULTILINE)
        package = package.group(1) if package else ""
        annotations = []
        for number, line in enumerate(lines, 1):
            if re.search(r"@(Test|ParameterizedTest|RepeatedTest|TestFactory|TestTemplate|RepeatableSimulationTest)\b", line):
                annotations.append(line.strip())
            match = re.search(r"\bfun\s+(?:`([^`]+)`|([\w]+))\s*\(", line)
            if not match and path.endswith(".java"):
                match = re.search(r"\b(?:public\s+)?(?:void|Stream<[^>]+>)\s+(\w+)\s*\(", line)
            if match and annotations:
                name = next(group for group in match.groups() if group)
                yield number, f"{package}.{Path(path).stem}/{name}", "junit_template_candidate", " ".join(annotations)
                annotations.clear()
        return
    patterns = {
        ".py": r"^\s*(?:async\s+)?def\s+(test_\w+)\s*\(",
        ".rs": r"^\s*(?:async\s+)?fn\s+(\w+)\s*\(",
        ".js": r"\b(?:it|test)\s*\(\s*['\"]([^'\"]+)",
        ".ts": r"\b(?:it|test)\s*\(\s*['\"]([^'\"]+)",
        ".exs": r'\btest\s+"([^"]+)"',
        ".cs": r"\bpublic\s+(?:async\s+)?(?:void|Task)\s+(\w+)\s*\(",
    }
    pattern = patterns.get(Path(path).suffix)
    if pattern:
        for number, line in enumerate(lines, 1):
            match = re.search(pattern, line)
            if match:
                yield number, match.group(1), "client_source_candidate", line.strip()


def collect(root, revision):
    actual = git(root, "rev-parse", revision + "^{commit}").decode().strip()
    tracked = git(root, "ls-tree", "-r", "--full-tree", "-z", actual)
    entries = []
    submodules = []
    for entry in tracked.split(b"\0"):
        if not entry:
            continue
        metadata, path = entry.split(b"\t", 1)
        mode, kind, digest = metadata.decode().split()
        path = path.decode()
        if kind == "commit":
            submodules.append({"path": path, "revision": digest, "inventory": "pending"})
        elif lane(path):
            entries.append((path, digest))
    files = []
    candidates = []
    process = subprocess.Popen(["git", "-C", str(root), "cat-file", "--batch"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        for path, digest in entries:
            process.stdin.write((digest + "\n").encode())
            process.stdin.flush()
            header = process.stdout.readline().decode().split()
            if len(header) != 3 or header[1] != "blob":
                raise RuntimeError(f"Cannot read source blob {digest}: {header}")
            content = process.stdout.read(int(header[2]))
            if process.stdout.read(1) != b"\n":
                raise RuntimeError(f"Truncated source blob {digest}")
            file_digest = hashlib.sha256(content).hexdigest()
            file_entry = {"path": path, "git_blob": digest, "sha256": file_digest, "lane": lane(path)}
            files.append(file_entry)
            if lane(path) not in ("gradle", "sql_logic", "language_client"):
                continue
            text = content.decode("utf-8", errors="replace")
            if lane(path) == "sql_logic":
                file_entry["record_count"] = sum(1 for _ in declarations(path, text))
                file_entry["identity_rule"] = ["reference", "source", "record_ordinal", "source_sha256"]
                file_entry["runtime_selection"] = "pending"
                continue
            file_entry["declarations"] = 0
            for number, name, kind, declaration in declarations(path, text):
                identity = {"reference": actual, "source": path, "name": name, "line": number, "parameter": "runtime_pending"}
                canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
                candidates.append({
                    "id": hashlib.sha256(canonical.encode()).hexdigest(), "identity": identity,
                    "kind": kind, "declaration": declaration, "source_sha256": file_digest,
                    "mapping": "unmapped", "runtime_discovery": "pending", "observations": "pending",
                })
                file_entry["declarations"] += 1
    finally:
        process.stdin.close()
        process.stdout.close()
        if process.wait() != 0:
            raise RuntimeError("git cat-file failed")
    return {
        "schema_version": 1, "reference": actual, "collection": "source_only",
        "runtime_discovery": "pending", "assertion_denominator": None, "parity_claim": "none",
        "declaration_limitations": [
            "Declarations are candidates; disabled source, metadata, inheritance and dynamic templates require runtime reconciliation.",
            "Client functions may be helpers; test frameworks have not been loaded.",
            "SLT selection, onlyif/skipif and per-record expectations require reconciliation against original task selections.",
            "Source locations distinguish duplicate declarations; runtime invocations must add parameter and fixture identities.",
        ],
        "files": files, "candidates": candidates, "submodules": submodules,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--revision", default=REFERENCE)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = collect(args.repository, args.revision)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"collection": manifest["collection"], "files": len(manifest["files"]),
                      "source_candidates": len(manifest["candidates"]),
                      "slt_corpus_records": sum(file.get("record_count", 0) for file in manifest["files"]),
                      "runtime_discovery": "pending"}))


if __name__ == "__main__":
    main()
