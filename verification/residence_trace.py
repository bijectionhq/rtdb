"""Validate RTDB residence observations or project a bounded composed replay."""

import argparse
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from urllib.parse import unquote

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


def verify_catalogs(catalogs):
    for records in catalogs.values():
        floor, holds = None, {}
        # Apply reader changes from a CAS before checking the paths selected at that revision.
        ordered = sorted(records, key=lambda event: (event["revision"], event["event"] not in {"catalog-hold", "catalog-release"}))
        for event in ordered:
            kind = event["event"]
            if kind == "catalog-release":
                holds.pop(event["hold"], None)
            elif kind == "catalog-hold":
                require(floor is None or (event["through"] is not None and event["through"] >= floor),
                        "catalog hold precedes the admission floor")
                holds[event["hold"]] = event["primary_paths"]
            else:
                require(floor is None or event["through"] >= floor, "catalog admission floor rewound")
                floor = event["through"]
                if kind == "close-catalog-admission":
                    for path in event["eligible_paths"]:
                        require(all(paths is not None and path not in paths for paths in holds.values()),
                                "exact retirement path is reachable by an observed catalog hold")


def physical_retirement(event):
    return (*scope(event, ("lineage", "residence", "retirement", "key")),
            json.dumps(event["publication"], sort_keys=True))


def ordered_object_fence(event):
    require(digest(event["name"]) and digest(event["version"]), "object fence lacks exact publication fingerprints")
    placement = scope(event, ("deployment", "incarnation", "group", "generation"))
    require(placement[3] >= 0 and all(placement[:3]), "object fence lacks its installed placement")
    return (*placement, json.dumps(event["position"], sort_keys=True), tuple(event["name"]), tuple(event["version"]))


def installed_publication(event):
    require(bytes(event["name"]) == hashlib.sha256(bytes(event["address_bytes"])).digest()
            and bytes(event["version"]) == hashlib.sha256(bytes(event["publication_bytes"])).digest(),
            "physical publication differs from the ordered fence")
    object = json.loads(bytes(event["publication_bytes"]))
    address = json.loads(bytes(event["address_bytes"]))
    record = object[object["publication"]]
    require(address == [record["namespace"], record["key"]], "the publication differs from the fenced provider path")
    physical_path = unquote(record["key"])
    require(physical_path == event["key"] or physical_path.endswith("/" + event["key"]),
            "the provider effect names another object path")
    return (*ordered_object_fence(event), event["key"])


def decision_identity(event, through="through"):
    require(digest(event["ensemble"]) and digest(event["decision"]) and event["epoch"] > 0 and event[through] >= 0,
            "certified decision lacks its exact authority")
    return (event["stream"], event["epoch"], tuple(event["ensemble"]), event[through], tuple(event["decision"]))


def certified_invocation(event):
    nonce = event["invocation"]
    require(len(nonce) == 16 and all(type(byte) is int and 0 <= byte < 256 for byte in nonce) and any(nonce),
            "certified read lacks its probe invocation")
    return tuple(nonce), decision_identity(event)


def verify_certificate(event):
    identity = decision_identity(event)
    statement = bytes(event["statement"])
    fingerprint = hashlib.sha256(b"rtdb.logkeeper/decision/v1\0" + statement).digest()
    require(fingerprint == bytes(event["decision"]) and digest(event["prefix_digest"])
            and digest(event["predecessor_digest"]) and -1 <= event["predecessor"] < event["through"],
            "the portable statement belongs to another prefix")
    roster = event["roster"]
    require(roster == sorted(set(roster)) and len(roster) // 2 < event["quorum"] <= len(roster),
            "the decision has no intersecting authority roster")
    for witnesses in (event["voters"], event["installers"]):
        require(len(witnesses) == len(set(witnesses)) == event["quorum"] and set(witnesses).issubset(roster),
                "the decision lacks its distinct physical-journal quorum")
    return identity


def verify_certified_reads(events):
    decisions, probes, captured = {}, {}, {}
    for event in events:
        kind = event["event"]
        if kind == "certified-decision":
            identity = verify_certificate(event)
            require(decisions.setdefault(identity[-1], identity) == identity,
                    "decision fingerprint changed its certified scope")
        elif kind == "certified-read-index":
            invocation, identity = certified_invocation(event)
            require(probes.setdefault(invocation, identity) == identity, "a read probe changed its certified decision")
        elif kind == "certified-catalog-capture":
            invocation, identity = certified_invocation(event)
            require(captured.setdefault(invocation, identity) == identity, "a catalog capture changed its certified decision")
    for invocation, identity in probes.items():
        require(decisions.get(identity[-1]) == identity, "read probe lacks its exact installed certificate")
    for invocation, identity in captured.items():
        require(probes.get(invocation) == identity, "catalog capture lacks its exact certified read probe")
    answers = 0
    for event in events:
        if event["event"] == "publish-checkpoint" and event.get("decision") is not None:
            require(digest(event["decision"]), "checkpoint names an invalid decision fingerprint")
            decision = decisions.get(tuple(event["decision"]))
            require(decision is not None and decision[0] == event["stream"] and decision[1] == event["writer_epoch"]
                    and decision[3] >= event["through"], "checkpoint lacks its covering certified decision")
        elif event["event"] == "certified-answer":
            invocation, identity = certified_invocation(event)
            require(captured.get(invocation) == identity, "answer lacks its exact certified catalog capture")
            answers += 1
    return answers


def verify_native(events, *, verify_catalog_history=True, verify_certificate_history=True):
    require(events, "the native trace is empty")
    deletes, trims, catalogs, captures, holds, attempts, reads = {}, set(), {}, {}, {}, {}, set()
    published_decisions, installed_grants, certificates = set(), set(), set()
    certified_stages = {}
    preceding_stage = {
        "certified-catalog-capture": "certified-read-index",
        "certified-answer": "certified-catalog-capture",
    }
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
            elif kind == "ordered-object-fence":
                ordered_object_fence(event)
            elif kind == "authorize-installed-publication":
                installed_grants.add(installed_publication(event))
            elif kind == "remove-installed-publication":
                require(installed_publication(event) in installed_grants, "installed deletion lacks its exact grant")
            elif kind == "certified-decision":
                certificates.add(verify_certificate(event))
            elif kind in {"certified-read-index", "certified-catalog-capture", "certified-answer"}:
                identity = certified_invocation(event)
                certified_stages.setdefault((kind, identity), index)
            elif kind == "quorum-publication":
                peers = event["installed"]
                require(digest(event["ensemble"]) and event["epoch"] > 0 and event["through"] >= 0
                        and event["quorum"] > 0 and len(peers) == len(set(peers)) and len(peers) >= event["quorum"],
                        "publication lacks its durable quorum")
                fingerprint = event.get("decision")
                if fingerprint is not None:
                    require(decision_identity(event) in certificates, "publication precedes its observed installation certificate")
                published_decisions.add((event["stream"], event["epoch"], tuple(event["ensemble"]), event["through"],
                                         tuple(fingerprint) if fingerprint is not None else None))
            elif kind == "quorum-receipt":
                fingerprint = event.get("decision")
                decision = (event["stream"], event["epoch"], tuple(event["ensemble"]), event["decision_through"],
                            tuple(fingerprint) if fingerprint is not None else None)
                require(decision in published_decisions and 0 <= event["through"] <= event["decision_through"],
                        "append receipt precedes its observed quorum publication")
            elif kind == "retirement-recovery-dependency":
                require(digest(event["anchor"]) and digest(event["frame"]) and 0 <= event["position"] <= event["trim_through"],
                        "retirement has no matching recovery exclusion")
                require(event["journals"] and len(set(event["journals"])) == len(event["journals"]),
                        "retirement has no complete set of physical journals")
                require(event["key"].startswith(event["residence"] + "/") and event["publication"]["version"],
                        "retirement has no exact physical publication")
            elif kind == "authorize-trim":
                checkpoint = event["checkpoint"]
                require(digest(event["manifest"]) and digest(checkpoint["root_sha256"])
                        and event["through"] < checkpoint["boundary"], "trim lacks a covering checkpoint and manifest")
                require(all(first > event["through"] for first in event["first_required"]), "trim intersects a held suffix")
                trims.add(trim_scope(event))
            elif kind == "apply-trim":
                require(trim_scope(event) in trims, "physical trim lacks its exact authorization")
                if event["settled_revision"] is not None:
                    require(event["revision"] <= event["settled_revision"] and event["journals"]
                            and len(set(event["journals"])) == len(event["journals"]),
                            "settled trim lacks its applied directory revision and physical journals")
            elif kind in {"advance-floor", "close-catalog-admission", "catalog-hold", "catalog-release"}:
                key = scoped_catalog(event)
                catalogs.setdefault(key, []).append(event)
                if kind == "catalog-hold":
                    require(holds.setdefault(event["hold"], key) == key, "reader identity changed catalog scope")
                    paths = event["primary_paths"]
                    require(paths is None or paths == sorted(set(paths)), "catalog claim has a noncanonical reachability census")
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
            elif kind == "hosted-recovery-closure":
                require(digest(event["root"]) and digest(event["manifest"]) and event["through"] >= 0,
                        "hosted confirmation has no exact recovery closure")
            elif kind == "ordered-recovery-closure":
                require(digest(event["root"]) and event["revision"] > 0 and event["through"] >= 0,
                        "ordered confirmation has no selected recovery closure")
            elif kind == "publish-checkpoint":
                require(digest(event["root"]) and digest(event["manifest"]), "checkpoint lacks its root or manifest digest")
            elif kind in {"admit-recovery", "finish-recovery", "read-retained-payload"}:
                key = scope(event, ("installation", "stream", "epoch"))
                if kind == "admit-recovery":
                    require(0 <= event["first_required"] <= event["through"] and holds.setdefault(event["hold"], key) == key,
                            "invalid replay hold")
                elif kind == "read-retained-payload":
                    require(event["position"] >= 0 and event["key"].startswith(event["residence"] + "/"),
                            "retained payload crosses its read residence")
                    if event["hold"] in holds:
                        require(holds[event["hold"]] == key, "payload read names another replay scope")
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

    for (kind, identity), index in certified_stages.items():
        prior = certified_stages.get((preceding_stage.get(kind), identity))
        # An independently captured owner may supply the earlier stage in its own file.
        require(prior is None or prior < index, f"{kind} precedes its exact prerequisite in the same capture")
    if verify_catalog_history:
        verify_catalogs(catalogs)
    if verify_certificate_history:
        verify_certified_reads(events)


def replica_scope(event):
    return scope(event, ("installation", "incarnation", "epoch", "stream"))


def exact_checkpoint(event, checkpoint, manifest):
    return (*replica_scope(event), checkpoint["block"], checkpoint["through"],
            tuple(checkpoint["root"]), tuple(manifest))


def verify_joined(captures):
    """Join independently captured owner events by immutable identity and authority revision."""
    for events in captures:
        verify_native(events, verify_catalog_history=False, verify_certificate_history=False)
    events = [event for capture in captures for event in capture]
    published, catalogs, installed_trims, dependencies, replay_holds, exclusions = {}, {}, {}, {}, {}, {}
    for event in events:
        kind = event["event"]
        if kind == "publish-checkpoint":
            identity = exact_checkpoint(event, event, event["manifest"])
            published[identity] = min(event["revision"], published.get(identity, event["revision"]))
        elif kind in {"advance-floor", "close-catalog-admission", "catalog-hold", "catalog-release"}:
            catalogs.setdefault(scoped_catalog(event), []).append(event)
        elif kind == "apply-trim" and event["settled_revision"] is not None:
            key = (*replica_scope(event), event["through"], tuple(event["anchor"]), frozenset(event["journals"]))
            revision = event["settled_revision"]
            installed_trims[key] = min(revision, installed_trims.get(key, revision))
        elif kind == "admit-recovery":
            key = (*replica_scope(event), event["residence"], event["hold"])
            hold = scope(event, ("first_required", "through", "revision"))
            require(replay_holds.setdefault(key, hold) == hold, "replay hold changed its admitted range")
        elif kind == "retirement-recovery-dependency":
            key = physical_retirement(event)
            dependency = (*replica_scope(event), event["position"], tuple(event["frame"]))
            require(dependencies.setdefault(key, dependency) == dependency,
                    "retirement changed its certified replica dependency")
            publication = (*replica_scope(event), event["key"], key[-1])
            exclusions[publication] = min(event["revision"], exclusions.get(publication, event["revision"]))
    verify_catalogs(catalogs)
    ordered_fences = {ordered_object_fence(event) for event in events if event["event"] == "ordered-object-fence"}
    joined = verify_certified_reads(events)
    for event in events:
        kind = event["event"]
        if kind == "authorize-trim":
            checkpoint = event["checkpoint"]
            identity = exact_checkpoint(event, {
                "block": checkpoint["block"], "through": checkpoint["boundary"], "root": checkpoint["root_sha256"],
            }, event["manifest"])
        elif kind == "hosted-recovery-closure":
            identity = exact_checkpoint(event, event, event["manifest"])
        elif kind == "retirement-recovery-dependency":
            trim = (*replica_scope(event), event["trim_through"], tuple(event["anchor"]), frozenset(event["journals"]))
            require(trim in installed_trims and installed_trims[trim] <= event["revision"],
                    "dependent retirement lacks the exact installed recovery exclusion")
            joined += 1
            continue
        elif kind == "read-retained-payload":
            hold = (*replica_scope(event), event["residence"], event["hold"])
            require(hold in replay_holds, "payload read lacks its exact replay hold")
            first, through, revision = replay_holds[hold]
            require(first <= event["position"] <= through and revision <= event["revision"],
                    "payload read is outside its admitted replay interval")
            source = event.get("source_hold")
            if source is not None:
                require(digest(event["frame"]), "broker read lacks its certified sealed frame")
                source_hold = (*replica_scope(event), source["residence"], source["hold"])
                require(source_hold != hold and source_hold in replay_holds, "broker read lacks its originating replay hold")
                source_first, source_through, source_revision = replay_holds[source_hold]
                require(source_first <= event["position"] <= source_through and source_revision < revision,
                        "broker read exceeds its originating replay hold")
            if event["publication"] is not None:
                publication = (*replica_scope(event), event["key"], json.dumps(event["publication"], sort_keys=True))
                require(publication not in exclusions or event["revision"] < exclusions[publication],
                        "payload read follows its installed recovery exclusion")
            joined += 1
            continue
        elif kind == "remove-installed-publication":
            require(ordered_object_fence(event) in ordered_fences,
                    "installed deletion lacks the matching ordered recovery exclusion")
            joined += 1
            continue
        elif kind == "remove-version" and "/oversized/" in event["key"]:
            require(physical_retirement(event) in dependencies, "payload removal lacks a matching recovery dependency")
            joined += 1
            continue
        else:
            continue
        require(identity in published and published[identity] <= event["revision"],
                f"{kind} lacks the exact published recovery closure")
        joined += 1
    require(joined > 0, "the captures have no cross-owner recovery observations")


def read_native(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream]


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
            transport = kind in {"admit-transport-answer", "handoff-transport-answer", "advance-transport-time", "refuse-transport-answer"}
            if transport or kind in {"install", "authorize-version", "remove-version", "settle-version", "copy-fork", "erase-residence",
                        "prepare-erasure", "arm-erasure", "revoke-key", "read-old-cache"}:
                publication = identities[scope(event["publication"], ("key", "version", "epoch"))]
            if kind in {"prepare-fork", "publish-fork", "copy-fork"}:
                require(event["destination"] == fork, "fork observation names a different residence")
            if kind in {"change-loglet", "read-index", "capture-fresh"}:
                require(epoch in (0, 1) and event["loglet"] == trace["loglets"][epoch], "loglet identity does not match its epoch")
            if kind in {"certify-decision", "publish-decision", "ack-commit"} or (kind in {"read-index", "capture-fresh"} and cut > 0):
                require(event["decision"] == trace["decision"], "decision belongs to another committed prefix")
            if kind in {"install-recovery-fence", "authorize-version", "trim"}:
                require(event["recovery_root"] == trace["recovery_root"], "recovery fence names another closure")
            if kind in {"attempt-outcome", "receipt-clock", "redeem-receipt", "collect-receipt"}:
                require(event["attempt"] == trace["attempt"], "receipt observation names another attempt")
            if transport or kind in {"prepare-erasure", "arm-erasure", "revoke-key", "read-old-cache"}:
                require(event["key_slot"] == trace["key_slot"], "observation names another key slot")
            if transport:
                require(event["answer"] == trace["answer"], "transport observation names another answer")
            if kind == "handoff-transport-answer":
                require(outcome in {"partial", "complete"}, "transport handoff has no delivery state")
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
    parser.add_argument("--join", action="append", type=Path, help="also verify identity joins with this native JSONL capture")
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
        elif args.join:
            verify_joined([read_native(path) for path in [args.trace, *args.join]])
            print("Joined native residence observations accepted")
        else:
            verify_native(read_native(args.trace))
            print("Native residence observations accepted")
    except (KeyError, TypeError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
