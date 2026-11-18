"""Summarises native corpus results: totals per outcome and the largest failure clusters.

Errors raised because a test reaches a JVM-only API (a cast to xtdb.api.Xtdb, NodeBase or the database catalog)
are reported as jvm-api-boundary: the binding documents those types as a migration boundary, so such Vars
cannot run natively without changing the original test. Vars that need cloud CLIs or real cloud endpoints report
environment-unavailable.
"""

import argparse
import collections
import json
import pathlib
import re

BOUNDARY = re.compile(
    r"cannot be cast to class xtdb\.api\.Xtdb\b|xtdb\.NodeBase|xtdb\.database\.Database\$Catalog|"
    r"cannot be cast to class xtdb\.api\.Xtdb\$Connection|No implementation of method: :\S+ of protocol: #'xtdb\.(db|node)|"
    r"NativeDatabase cannot be cast to class clojure\.lang\.(Associative|ILookup|IPersistentMap)|"
    r"Unsupported native start-node options|No serializer found for class")
ENVIRONMENT = re.compile(r'Cannot run program "(az|gcloud)"|xtdb-object-store-iam-test|addressed using the specified endpoint|Moved Permanently \(Service: S3')
NOISE = re.compile(r"\d+|0x[0-9a-f]+")


def outcome(row):
    detail = row.get("detail") or ""
    if row["result"] in ("error", "fail") and BOUNDARY.search(detail):
        return "jvm-api-boundary"
    if row["result"] in ("error", "fail") and ENVIRONMENT.search(detail):
        return "environment-unavailable"
    return row["result"]


def load(directory: pathlib.Path):
    for path in sorted(directory.glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                yield json.loads(line)


def summarise(rows):
    totals = collections.Counter()
    clusters = collections.Counter()
    examples = {}
    for row in rows:
        kind = outcome(row)
        totals[kind] += 1
        if kind in ("error", "fail", "timeout", "process-crash", "load-error"):
            signature = NOISE.sub("#", (row.get("detail") or "")[:160])
            clusters[(kind, signature)] += 1
            examples.setdefault((kind, signature), row["var"])
    return totals, clusters, examples


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=pathlib.Path)
    parser.add_argument("--clusters", type=int, default=40)
    args = parser.parse_args(argv)
    totals, clusters, examples = summarise(load(args.results))
    print(json.dumps(dict(totals.most_common()), indent=1))
    for (kind, signature), count in clusters.most_common(args.clusters):
        print(f"{count:4d} {kind:13s} {examples[(kind, signature)]}\n     {signature}")


if __name__ == "__main__":
    main()
