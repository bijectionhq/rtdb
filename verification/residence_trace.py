"""Validate RTDB residence observations or project a bounded composed replay."""

import argparse
import json
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory

from recovery_trace import digest


def require(condition, message):
    if not condition:
        raise ValueError(message)


def scope(event, fields):
    return tuple(event[field] for field in fields)


def scoped_catalog(event):
    return scope(event, ("lineage", "installation", "residence", "namespace", "root", "epoch"))


def trim_scope(event):
    require(digest(event["anchor"]), "trim has no exact journal anchor")
    return (*scope(event, ("installation", "incarnation", "epoch", "stream", "revision", "through")), tuple(event["anchor"]))


def verify_native(events):
    require(events, "the native trace is empty")
    deletes, trims, catalogs, captures, holds, attempts, reads = {}, set(), {}, {}, {}, {}, set()
    for index, event in enumerate(events):
        try:
            kind = event["event"]
            if kind in {"authorize-version", "remove-version", "authorize-tombstones", "remove-tombstones",
                        "authorize-replay-pack", "remove-replay-pack"}:
                action, effect = kind.split("-", 1)
                require(event["key"].startswith(event["residence"] + "/"), "deletion targets another residence")
                key = (effect, *scope(event, ("lineage", "residence", "retirement", "key")))
                identity = {field: value for field, value in event.items() if field not in {"event", "revision"}}
                if action == "authorize":
                    require(deletes.setdefault(key, identity) == identity, "retirement changed its publication")
                else:
                    require(deletes.get(key) == identity, "delete lacks a matching exact authorization")
            elif kind == "authorize-trim":
                checkpoint = event["checkpoint"]
                require(digest(event["manifest"]) and digest(checkpoint["root_sha256"])
                        and event["through"] < checkpoint["boundary"], "trim lacks a covering checkpoint and manifest")
                require(all(first > event["through"] for first in event["first_required"]), "trim intersects a held suffix")
                trims.add(trim_scope(event))
            elif kind == "apply-trim":
                require(trim_scope(event) in trims, "physical trim lacks its exact authorization")
            elif kind in {"advance-floor", "catalog-hold", "catalog-release"}:
                key = scoped_catalog(event)
                catalogs.setdefault(key, []).append((index, event))
                if kind == "catalog-hold":
                    require(holds.setdefault(event["hold"], key) == key, "reader identity changed catalog scope")
                if kind == "catalog-release" and event["hold"] in holds:
                    require(holds[event["hold"]] == key, "release names another reader scope")
            elif kind in {"erasure-obligation", "key-revoked"}:
                key = scope(event, ("installation", "incarnation", "epoch", "capture"))
                obligation = scope(event, ("manifest", "due_at_ms", "authorized_at_ms"))
                require(captures.setdefault(key, obligation) == obligation, "capture changed its erasure obligation")
                if kind == "key-revoked":
                    require(event["revoked_at_ms"] >= max(event["due_at_ms"], event["authorized_at_ms"]),
                            "key revocation evidence precedes the authorized deadline")
            elif kind == "read-index":
                require(digest(event["ensemble"]) and event["epoch"] > 0 and event["through"] >= -1,
                        "read index lacks its active loglet identity")
            elif kind == "ordered-read-index":
                invocation = scope(event, ("deployment", "incarnation", "group", "generation", "epoch", "invocation"))
                require(invocation not in reads and event["epoch"] > 0 and event["through"] >= -1,
                        "ordered read index reused its invocation or has an invalid cut")
                reads.add(invocation)
            elif kind == "publish-checkpoint":
                require(digest(event["root"]) and digest(event["manifest"]), "checkpoint lacks its root or manifest digest")
            elif kind in {"admit-recovery", "finish-recovery"}:
                key = scope(event, ("installation", "stream", "epoch"))
                if kind == "admit-recovery":
                    require(event["first_required"] >= 0 and holds.setdefault(event["hold"], key) == key,
                            "invalid replay hold")
                elif event["hold"] in holds:
                    require(holds[event["hold"]] == key, "release names another replay scope")
            elif kind == "attempt-outcome":
                require(digest(event["history"]), "attempt lacks its original history")
                key = (tuple(event["history"]), event["attempt"])
                outcome = scope(event, ("epoch", "through", "system_time", "committed"))
                require(attempts.setdefault(key, outcome) == outcome, "attempt changed its original durable outcome")
            else:
                raise ValueError(f"unknown event {kind!r}")
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"native observation {index + 1}: {error}") from error

    # Directory revisions order observations that concurrent returns can deliver out of order.
    for records in catalogs.values():
        floor = None
        for index, event in sorted(records, key=lambda item: (item[1]["revision"], item[1]["event"] != "advance-floor")):
            if event["event"] == "advance-floor":
                require(floor is None or event["through"] >= floor, f"native observation {index + 1}: floor rewound")
                floor = event["through"]
            elif event["event"] == "catalog-hold":
                require(floor is None or (event["through"] is not None and event["through"] >= floor),
                        f"native observation {index + 1}: catalog hold precedes the admission floor")


def project(trace):
    """Validate observation identities before reducing them to this model's finite domains."""
    history, residence, fork = scope(trace, ("history", "residence", "fork"))
    require(all(isinstance(item, str) and item for item in (history, residence, fork)) and residence != fork,
            "the trace needs a history and distinct residences")
    publications = trace["publications"]
    require(set(publications) == {"old", "new", "shared"}, "expected the three model publications")
    identities = {}
    for role, publication in publications.items():
        key, version, epoch = scope(publication, ("key", "version", "epoch"))
        identity = (key, version, epoch)
        require(key.startswith(residence + "/") and version and identity not in identities, "invalid or aliased publication")
        require(type(epoch) is int and epoch == (1 if role == "new" else 0), "publication belongs to another naming epoch")
        identities[identity] = role
    require(trace["loglets"][0] and trace["loglets"][1] and len(set(trace["loglets"])) == len(trace["loglets"]) == 2,
            "expected two exact loglet identities")
    require(trace["attempt"] and trace["key_slot"], "the trace needs an attempt and a key slot")
    result = []
    for index, event in enumerate(trace["events"]):
        try:
            require(event["history"] == history and event["residence"] == residence, "observation crosses a residence")
            kind = event["event"]
            cut = event.get("cut", -1)
            epoch = event.get("epoch", -1)
            publication = event.get("body", "")
            outcome = event.get("outcome", "")
            observed_time = event.get("system_time", -1)
            require(all(type(number) is int for number in (cut, epoch, observed_time)), "expected integer coordinates")
            if kind in {"install", "authorize-version", "remove-version", "settle-version", "copy-fork", "erase-residence",
                        "prepare-erasure", "arm-erasure", "revoke-key", "read-old-cache"}:
                publication = identities[scope(event["publication"], ("key", "version", "epoch"))]
            if kind in {"prepare-fork", "publish-fork", "copy-fork"}:
                require(event["destination"] == fork, "fork observation names a different residence")
            if kind in {"change-loglet", "read-index", "capture-fresh"}:
                require(epoch in (0, 1) and event["loglet"] == trace["loglets"][epoch], "loglet identity does not match its epoch")
            if kind in {"attempt-outcome", "receipt-clock", "redeem-receipt", "collect-receipt"}:
                require(event["attempt"] == trace["attempt"], "receipt observation names another attempt")
            if kind in {"prepare-erasure", "arm-erasure", "revoke-key", "read-old-cache"}:
                require(event["key_slot"] == trace["key_slot"], "erasure observation names another key slot")
            require(all(isinstance(item, str) for item in (kind, publication, outcome)), "expected textual event fields")
            result.append([kind, cut, publication, epoch, outcome, observed_time])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"composed observation {index + 1}: {error}") from error
    require(result, "the composed trace is empty")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--tlc", type=Path, help="check a bounded JSON projection with this TLA+ tools jar")
    parser.add_argument("--java", default="java")
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    try:
        if args.tlc:
            if not args.evidence:
                parser.error("--tlc requires --evidence")
            from run import check_residence_trace

            args.evidence.mkdir(parents=True, exist_ok=True)
            tools = SimpleNamespace(java=args.java, tlc=str(args.tlc.resolve()), evidence=args.evidence.resolve())
            events = project(json.loads(args.trace.read_text()))
            with TemporaryDirectory(prefix="rtdb-residence-replay-") as scratch:
                check_residence_trace(tools, Path(scratch), "tlc-residence-trace-capture", events, extended=True)
        else:
            with args.trace.open() as stream:
                verify_native([json.loads(line) for line in stream])
            print("Native residence observations accepted")
    except (KeyError, TypeError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
