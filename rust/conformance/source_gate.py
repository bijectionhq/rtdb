#!/usr/bin/env python3
"""Report the source review and validate the recorded Phase 2 boundary without running project code."""

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path, PurePosixPath


REFERENCE = "c84f9334eb060ceb92d28ecdd4a06e6b625932fd"
DESIGN = "c995e6b35b6812aa423c8a158c4e201e27532320"
MANIFEST = "rust/conformance/source-completeness.json"
AREAS = (
    "types_columnar_durable_formats",
    "storage_indexing_historical_visibility",
    "sql_xtql_execution",
    "writes_replication_recovery",
    "compaction_gc_adapters",
    "client_operational_surfaces",
    "conformance_machinery",
)
TRACES = (
    "client_read", "data_dependent_write", "replica_recovery",
    "compaction_retry", "cdc_restart", "database_shutdown",
)


def _path(value):
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError("source evidence needs a repository-relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in (".", "..") for part in value.split("/")) or str(path) != value:
        raise ValueError(f"invalid source evidence path: {value}")
    return value


def _text(value, label):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be nonempty text")
    return value


def _revision(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError(f"{label} must be a full Git commit ID")
    return value


def assess(review, read_source):
    """Check a review against read_source(path), returning bytes for a repository-relative path.

    Return pending reviews and implementation blockers separately from runtime qualifications.
    Raise ValueError for malformed review data, unavailable source, or stale reviewed evidence.
    """
    if not isinstance(review, dict) or type(review.get("schema_version")) is not int or review["schema_version"] != 1:
        raise ValueError("unsupported source-completeness schema")
    if review.get("reference_revision") != REFERENCE or review.get("design_revision") != DESIGN:
        raise ValueError("source review does not identify the pinned reference and design")
    if review.get("implementation_revision") is not None:
        _revision(review["implementation_revision"], "implementation_revision")
    pending = []
    reviewed = []
    known = set(AREAS + TRACES)
    for section, required in (("areas", AREAS), ("traces", TRACES)):
        entries = review.get(section)
        if not isinstance(entries, dict) or set(entries) != set(required):
            raise ValueError(f"{section} must contain exactly the required source review entries")
        for identity in required:
            entry = entries[identity]
            if not isinstance(entry, dict) or entry.get("status") not in ("pending", "reviewed"):
                raise ValueError(f"invalid source review status: {identity}")
            _text(entry.get("conclusion"), f"{identity} conclusion")
            paths = entry.get("implementation_paths")
            if not isinstance(paths, list) or not paths or any(not isinstance(path, str) for path in paths) or len(paths) != len(set(paths)):
                raise ValueError(f"{identity} needs distinct implementation paths")
            for path in paths:
                _path(path)
                try:
                    read_source(path)
                except (OSError, KeyError, ValueError) as error:
                    raise ValueError(f"{identity} implementation path is unavailable: {path}") from error
            if entry["status"] == "pending":
                pending.append({"id": identity, "conclusion": entry["conclusion"], "implementation_paths": paths})
                continue
            evidence = entry.get("evidence")
            if not isinstance(evidence, dict) or not set(paths).issubset(evidence):
                raise ValueError(f"{identity} lacks reviewed evidence for its implementation paths")
            for path, expected in evidence.items():
                _path(path)
                if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
                    raise ValueError(f"{identity} has an invalid source digest: {path}")
                try:
                    actual = hashlib.sha256(read_source(path)).hexdigest()
                except (OSError, KeyError, ValueError) as error:
                    raise ValueError(f"{identity} reviewed source is unavailable: {path}") from error
                if actual != expected:
                    raise ValueError(f"{identity} source review is stale: {path}")
            reviewed.append(identity)
    findings = review.get("findings")
    if not isinstance(findings, list):
        raise ValueError("source review findings must be an array")
    seen = set()
    blockers, qualifications = [], []
    for finding in findings:
        if not isinstance(finding, dict):
            raise ValueError("source review finding must be an object")
        identity = _text(finding.get("id"), "finding identity")
        if identity in seen:
            raise ValueError(f"duplicate source review finding: {identity}")
        seen.add(identity)
        if finding.get("kind") not in ("implementation", "review", "qualification") or finding.get("status") not in ("open", "resolved"):
            raise ValueError(f"invalid finding disposition: {identity}")
        targets = finding.get("targets")
        if not isinstance(targets, list) or not targets or any(not isinstance(target, str) for target in targets) or not set(targets).issubset(known):
            raise ValueError(f"{identity} must identify required source review entries")
        _text(finding.get("requirement"), f"{identity} requirement")
        if finding["status"] == "resolved":
            _text(finding.get("resolution"), f"{identity} resolution")
        elif finding["kind"] == "qualification":
            qualifications.append(finding)
        else:
            blockers.append(finding)
    return {
        "schema_version": 1,
        "implementation_revision": review.get("implementation_revision"),
        "source_complete": not pending and not blockers,
        "implementation_revision_recorded": review.get("implementation_revision") is not None,
        "reviewed": reviewed,
        "pending_reviews": pending,
        "implementation_blockers": blockers,
        "phase_two_qualifications": qualifications,
        "compatibility_claim": "none",
    }


def _working_source(repository, path):
    root = repository.resolve()
    source = (root / _path(path)).resolve(strict=True)
    if not source.is_relative_to(root):
        raise ValueError(f"source evidence escapes the repository: {path}")
    return source.read_bytes()


def _git(repository, *args):
    result = subprocess.run(["git", "-C", str(repository), *args], capture_output=True, check=False)
    if result.returncode != 0:
        raise ValueError(f"cannot validate source review Git provenance: {' '.join(args)}")
    return result.stdout


def require_phase_two(repository, requested):
    """Validate the immutable Phase 1 review before a caller starts compilation, discovery, or execution.

    Later Phase 2 repair commits may descend from the reviewed implementation revision.
    Return the complete review report, or raise ValueError when the gate or its evidence is invalid.
    This function reads JSON and Git objects and never opens the gate or starts project code.
    """
    if requested is not True:
        raise ValueError("execution requires --phase-two after the source completeness gate has passed")
    repository = Path(repository)
    state = json.loads((repository / "rust/implementation-status.json").read_bytes())
    if not isinstance(state, dict) or state.get("phase_2_authorized_by_completeness_gate") is not True or state.get("source_completeness_review") != "complete":
        raise ValueError("implementation-status.json has not completed the Phase 1 source review")
    revision = _revision(state.get("implementation_revision"), "implementation_revision")
    review = json.loads((repository / MANIFEST).read_bytes())
    if not isinstance(review, dict) or review.get("implementation_revision") != revision:
        raise ValueError("source review and implementation status identify different implementation revisions")
    if state.get("reference_revision") != REFERENCE or state.get("design_revision") != DESIGN:
        raise ValueError("implementation status does not identify the pinned reference and design")
    if _git(repository, "rev-parse", revision + "^{commit}").decode().strip() != revision:
        raise ValueError("source review revision is not the recorded commit")
    _git(repository, "merge-base", "--is-ancestor", revision, "HEAD")
    report = assess(review, lambda path: _git(repository, "show", revision + ":" + _path(path)))
    if not report["source_complete"]:
        pending = [entry["id"] for entry in report["pending_reviews"] + report["implementation_blockers"]]
        raise ValueError("Phase 1 source review is incomplete: " + ", ".join(pending))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--review", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    review = json.loads((args.review or args.repository / MANIFEST).read_bytes())
    report = assess(review, lambda path: _working_source(args.repository, path))
    value = json.dumps(report, indent=2) + "\n"
    if args.output:
        with args.output.open("x") as output:
            output.write(value)
    else:
        print(value, end="")
    return 0 if report["source_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
