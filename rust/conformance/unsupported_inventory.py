"""Inventory production `unsupported` anomaly sites and classify each code against the pinned JVM reference.

reference-parity: the reference source raises the same code, so the native branch reproduces a reference rejection.
native-gap: the code exists only in RTDB, so the branch is a native divergence that Phase 2 must close or justify.
A native-gap code that has an entry in the classification file carries that entry's category, rationale and reference.
"""

import argparse
import json
import pathlib
import re
import subprocess
import sys

SITE = re.compile(r'(?:Anomaly::unsupported|\bunsupported)\(\s*"(?P<code>[A-Za-z0-9._-]+/[A-Za-z0-9._-]+)"')
TEST_PATH = re.compile(r"(^|/)(tests?|benches|examples)(/|\.rs$)|_tests?\.rs$")


def production_sites(crates: pathlib.Path):
    for path in sorted(crates.glob("*/src/**/*.rs")):
        relative = path.relative_to(crates.parent.parent).as_posix()
        if TEST_PATH.search(relative):
            continue
        text = path.read_text(encoding="utf-8")
        test_module = text.find("#[cfg(test)]")
        for match in SITE.finditer(text):
            if 0 <= test_module < match.start():
                continue
            line = text.count("\n", 0, match.start()) + 1
            yield match["code"], f"{relative}:{line}"


def reference_codes(repo: pathlib.Path, revision: str, codes):
    """Return the codes whose keyword appears in the pinned reference source."""
    found = set()
    for code in codes:
        namespace, name = code.split("/", 1)
        if not namespace.startswith("xtdb"):
            continue
        keyword = rf"(::|[A-Za-z0-9_.-]/){re.escape(name)}([^A-Za-z0-9_?!*+-]|$)"
        result = subprocess.run(
            ["git", "-C", str(repo), "grep", "-q", "-E", keyword, revision, "--",
             "*.clj", "*.kt", "*.java", ":!rust", ":!bindings"],
            capture_output=True, text=True)
        if result.returncode == 0:
            found.add(code)
    return found


CATEGORIES = {"reference-failure", "resource-or-format-guard", "harness", "internal-guard", "jvm-host-dependent", "deferred-parity"}


def classify(codes, classification):
    """Merge each native-gap code's classification entry into its inventory entry.

    An entry must name a known category, and must name a code that is still a native gap.
    """
    gaps = {entry["code"] for entry in codes if entry["classification"] == "native-gap"}
    stale = sorted(set(classification) - gaps)
    if stale:
        raise ValueError(f"classified codes are not native gaps: {', '.join(stale)}")
    for code, entry in classification.items():
        if entry.get("category") not in CATEGORIES:
            raise ValueError(f"{code}: unknown category {entry.get('category')!r}")
    merged = []
    for entry in codes:
        entry = dict(entry)
        if entry["code"] in classification:
            entry.update({key: classification[entry["code"]][key] for key in ("category", "rationale", "reference")})
        merged.append(entry)
    return merged


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=pathlib.Path(__file__).resolve().parents[2], type=pathlib.Path)
    parser.add_argument("--reference", default="c84f9334eb060ceb92d28ecdd4a06e6b625932fd")
    parser.add_argument("--classification", type=pathlib.Path,
                        default=pathlib.Path(__file__).resolve().parent / "unsupported-classification.json")
    parser.add_argument("--out", type=pathlib.Path)
    args = parser.parse_args(argv)
    sites = {}
    for code, location in production_sites(args.repo / "rust" / "crates"):
        sites.setdefault(code, []).append(location)
    parity = reference_codes(args.repo, args.reference, sites)
    codes = [{"code": code, "classification": "reference-parity" if code in parity else "native-gap", "sites": locations}
             for code, locations in sorted(sites.items())]
    classification = json.loads(args.classification.read_text(encoding="utf-8")) if args.classification.exists() else {}
    inventory = {"reference_revision": args.reference, "codes": classify(codes, classification)}
    text = json.dumps(inventory, indent=1, ensure_ascii=False) + "\n"
    if args.out:
        args.out.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    gaps = [entry for entry in inventory["codes"] if entry["classification"] == "native-gap"]
    unclassified = sum(1 for entry in gaps if "category" not in entry)
    print(f"{len(inventory['codes'])} codes, {len(gaps)} native-gap, {unclassified} unclassified", file=sys.stderr)


if __name__ == "__main__":
    main()
