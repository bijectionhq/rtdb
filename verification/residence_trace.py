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
                holds[event["hold"]] = event["paths"]
            else:
                require(floor is None or event["through"] >= floor, "catalog admission floor rewound")
                floor = event["through"]
                if kind == "close-catalog-admission":
                    for path in event["eligible_paths"]:
                        require(all(paths is not None and path not in paths for paths in holds.values()),
                                "exact retirement path is reachable by an observed catalog hold")


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


def detached_effect(event):
    return event["residence"].startswith("replay-bodies/")


def detached_candidate(event, residence):
    version = event["publication"]
    provider_key = unquote(version["key"])
    require(event["key"].startswith(residence + "/oversized/") and version["namespace"]["bucket"]
            and version["version"] and (provider_key == event["key"] or provider_key.endswith("/" + event["key"]))
            and type(event["position"]) is int and event["position"] >= 0 and digest(event["frame"]),
            "detached effect lacks its exact provider publication and sealed frame")
    return (event["key"], json.dumps(version, sort_keys=True), event["position"], tuple(event["frame"]))


def detached_fence(event):
    start, through = event["interval"]
    checkpoint = event["checkpoint"]
    require(0 <= start <= through < checkpoint["boundary"] and event["revision"] > 0
            and digest(event["anchor"]) and digest(event["manifest"]) and digest(checkpoint["root_sha256"]),
            "detached recovery fence has no covering checkpoint")
    return (*replica_scope(event), *scope(event, ("lineage", "residence", "generation")),
            start, through, tuple(event["anchor"]))


def detached_census(event):
    first, through = event["interval"]
    start, end = event["batch"]
    previous = event["previous"]
    require(first <= start <= end == through and digest(event["batch_digest"]) and digest(event["decision"])
            and event["active_journals"], "detached census lacks its certified batch")
    if previous is None:
        require(start == first, "detached census skips the start of its recovery history")
    else:
        require(previous["interval"] == [first, start - 1] and digest(previous["digest"])
                and previous["root_incarnation"] == event["root_incarnation"],
                "detached census does not extend its preceding anchor")
    require(len(event["frames"]) == end - start + 1 and all(
        unit["position"] == position and digest(unit["frame"])
        for position, unit in enumerate(event["frames"], start)), "detached census skips a certified frame")
    discovered = {item["position"]: detached_candidate(item, event["residence"]) for item in event["discovered"]}
    require(len(discovered) == len(event["discovered"]) and all(
        start <= position <= end and tuple(event["frames"][position - start]["frame"]) == candidate[-1]
        for position, candidate in discovered.items()), "detached discovery differs from its certified frame")
    holds = {item["hold"]: item["interval"] for item in event["holds"]}
    require(len(holds) == len(event["holds"]) and all(first <= last for first, last in holds.values()),
            "detached census has an invalid originating hold")
    return holds


def detached_attempt(event):
    require(event["retirement"] and event["revision"] > 0, "detached effect lacks its durable grant")
    return (*replica_scope(event), *scope(event, ("lineage", "residence", "generation", "retirement")),
            detached_candidate(event, event["residence"]), event["revision"])


def detached_checkpoint(event):
    checkpoint = event["checkpoint"]
    return exact_checkpoint(event, {"block": checkpoint["block"], "through": checkpoint["boundary"],
                                   "root": checkpoint["root_sha256"]}, event["manifest"])


FORK_INVENTORY = {"fork-source-inventory", "fork-inventory-verified", "fork-directory-published"}
FORK_EDGE = {"fork-edge-selected", "fork-replacement-verified", "fork-edge-withdrawn"}
REPAIR_INVENTORY = {"repair-inventory-enrolled", "repair-inventory-verified", "repair-replayed"}


def fork_inventory(event):
    checkpoint = event["checkpoint"]
    identity = scope(event, ("lineage", "parent", "child", "source_incarnation", "child_incarnation",
                             "source_epoch", "child_epoch", "source_residence", "child_residence"))
    require(all(identity[index] for index in (0, 1, 2, 7, 8)) and identity[1] != identity[2]
            and identity[7] != identity[8] and checkpoint["block"] >= 0 and digest(checkpoint["root_sha256"]),
            "fork inventory lacks its exact history and checkpoint")
    return (*identity, json.dumps(checkpoint, sort_keys=True))


def fork_publication(publication, path):
    require(len(publication) == 1, "fork edge has no exact provider publication")
    kind, record = next(iter(publication.items()))
    key = unquote(record["key"])
    require(record["namespace"]["bucket"] and (key == path or key.endswith("/" + path))
            and ((kind == "version" and record["version"]) or
                 (kind == "terminal" and record["allocation"] and record["etag"])),
            "fork edge names another provider publication")
    return json.dumps(publication, sort_keys=True)


def repair_anchor(event):
    require(event["residence"] and event["anchor_block"] >= 0 and event["anchor_boundary"] >= 0
            and digest(event["anchor_root"]) and digest(event["inventory"])
            and event["inventory"] == event["observed_inventory"], "repair inventory differs from its enrolled primary")
    return (*scope(event, ("residence", "anchor_block", "anchor_boundary")),
            tuple(event["anchor_root"]), tuple(event["inventory"]))


def verify_inventory_shape(event):
    kind = event["event"]
    if kind in FORK_INVENTORY:
        fork_inventory(event)
        if kind == "fork-directory-published":
            require(event["revision"] > 0, "fork publication has no directory revision")
            return
        inventory = event["inventory"]
        require(event["kind"] in {"copy", "reference"} and inventory["members"] >= 0 and digest(inventory["sha256"]),
                "fork has no complete inventory observation")
        require(event["kind"] == "reference" or not event["edges"], "independent fork inherits a physical edge")
        for edge in event["edges"]:
            fork_publication(edge["publication"], edge["owner"] + "/" + edge["name"])
    elif kind in FORK_EDGE:
        require(event["lineage"] and event["child"] and event["attempt"] and event["base_sequence"] >= 0
                and event["source_residence"] != event["child_residence"], "fork edge lacks its selected manifest")
        fork_publication(event["original"], event["source_residence"] + "/" + event["name"])
        if kind != "fork-edge-selected":
            fork_publication(event["replacement"], event["child_residence"] + "/" + event["name"])
            require(event["extent"] >= 0 and digest(event["sha256"]), "fork replacement lacks the compared bytes")
            member = event["pack_member"]
            if event["name"] != event["released"]:
                require(event["name"].startswith("~packs/") and member is not None and member["key"] == event["released"],
                        "fork replacement lacks the released pack member")
                offset, length = scope(member["range"], ("offset", "length"))
                require(0 <= offset <= offset + length <= event["extent"], "fork pack member exceeds the compared bytes")
            else:
                require(member is None, "loose fork edge masquerades as a pack member")
            if kind == "fork-edge-withdrawn":
                require(event["sequence"] > event["base_sequence"], "fork withdrawal lacks the new reference manifest")
    else:
        repair_anchor(event)
        target = event["target"]
        if kind == "repair-inventory-enrolled":
            require(target is None, "repair enrollment names a replay target")
        else:
            require(target["invocation"] and target["block"] > event["anchor_block"] and
                    target["boundary"] > event["anchor_boundary"] and digest(target["root"]),
                    "repair replay lacks its exact advancing target")


def verify_inventory_joins(captures):
    sources, destinations, selections, replacements, enrolled, staged = {}, {}, {}, {}, {}, {}
    effects = []
    for capture, events in enumerate(captures):
        for index, event in enumerate(events):
            kind = event["event"]
            where = (capture, index)
            if kind == "fork-source-inventory":
                sources[fork_inventory(event)] = (event, where)
            elif kind == "fork-inventory-verified":
                destinations[fork_inventory(event)] = (event, where)
            elif kind == "fork-edge-selected":
                selections[event["attempt"]] = (event, where)
            elif kind == "fork-replacement-verified":
                replacements[event["attempt"]] = (event, where)
            elif kind == "repair-inventory-enrolled":
                enrolled.setdefault(repair_anchor(event), (event, where))
            elif kind == "repair-inventory-verified":
                staged[event["target"]["invocation"]] = (event, where)
            if kind in FORK_INVENTORY | FORK_EDGE | REPAIR_INVENTORY:
                effects.append((event, where))

    def prior(observed, where, message):
        require(observed is not None, message)
        event, position = observed
        require(position[0] != where[0] or position[1] < where[1], message)
        return event

    joined = 0
    for event, where in effects:
        kind = event["event"]
        if kind == "fork-inventory-verified":
            message = "fork destination differs from its selected source inventory"
            source = prior(sources.get(fork_inventory(event)), where, message)
            require(all(source[field] == event[field] for field in ("kind", "inventory", "edges")), message)
        elif kind == "fork-directory-published":
            prior(destinations.get(fork_inventory(event)), where, "fork publication lacks its verified inventory")
        elif kind == "fork-replacement-verified":
            message = "fork replacement differs from its selected edge"
            source = prior(selections.get(event["attempt"]), where, message)
            require(all(event[field] == value for field, value in source.items() if field != "event"), message)
        elif kind == "fork-edge-withdrawn":
            message = "fork withdrawal lacks its exact replacement"
            proof = prior(replacements.get(event["attempt"]), where, message)
            require(all(event[field] == value for field, value in proof.items() if field != "event"), message)
        elif kind == "repair-inventory-verified":
            prior(enrolled.get(repair_anchor(event)), where, "repair verification lacks its durable enrollment")
        elif kind == "repair-replayed":
            message = "repair replay lacks its verified staged inventory"
            proof = prior(staged.get(event["target"]["invocation"]), where, message)
            require(all(event[field] == value for field, value in proof.items() if field != "event"), message)
        else:
            continue
        joined += 1
    return joined


def verify_native(events, *, verify_catalog_history=True, verify_certificate_history=True, verify_inventory_history=True):
    require(events, "the native trace is empty")
    deletes, trims, catalogs, captures, holds, attempts, reads = {}, set(), {}, {}, {}, {}, set()
    published_decisions, installed_grants, certificates = set(), set(), set()
    certified_stages, detached_stages = {}, {}
    preceding_stage = {
        "certified-catalog-capture": "certified-read-index",
        "certified-answer": "certified-catalog-capture",
    }
    for index, event in enumerate(events):
        try:
            kind = event["event"]
            if kind in {"retirement-recovery-dependency", "withdraw-detached-key", "settle-detached-version"} \
                    or (kind == "remove-version" and detached_effect(event)):
                attempt = detached_attempt(event)
                detached_stages.setdefault((kind, attempt), index)
                if kind == "retirement-recovery-dependency":
                    detached_fence(event)
                    start, through = event["interval"]
                    require(start <= event["position"] <= through and digest(event["decision"]),
                            "detached retirement lacks its certified recovery exclusion")
                    require(not event["uploads"] and all(first <= end and not first <= event["position"] <= end
                                                        for first, end in event["requires"]),
                            "detached retirement intersects a live read or upload")
                if kind == "settle-detached-version":
                    require(event["settled_revision"] >= event["revision"], "detached settlement precedes its grant")
            elif kind == "detached-recovery-fence":
                detached_fence(event)
                require(detached_effect(event) and event["root_incarnation"] == event["incarnation"],
                        "detached fence crosses a recovery history")
                detached_census(event)
                for candidate in event["candidates"]:
                    detached_candidate(candidate, event["residence"])
                    require(event["interval"][0] <= candidate["position"] <= event["interval"][1],
                            "detached census includes a position outside the fence")
            elif kind in {"authorize-version", "remove-version", "authorize-tombstones", "remove-tombstones",
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
                    paths = event["paths"]
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
            elif kind == "broker-certified-pointer":
                broker_pointer(event)
                require(event["range"] == [event["position"]] * 2 and digest(event["range_digest"]),
                        "detached pointer has no exact certified interval")
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
            elif kind in FORK_INVENTORY | FORK_EDGE | REPAIR_INVENTORY:
                verify_inventory_shape(event)
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
    for (kind, identity), index in detached_stages.items():
        prerequisite = {
            "withdraw-detached-key": "retirement-recovery-dependency",
            "remove-version": "withdraw-detached-key",
            "settle-detached-version": "remove-version",
        }.get(kind)
        prior = detached_stages.get((prerequisite, identity))
        require(prior is None or prior < index, f"{kind} precedes its detached deletion prerequisite")
    if verify_catalog_history:
        verify_catalogs(catalogs)
    if verify_certificate_history:
        verify_certified_reads(events)
    if verify_inventory_history:
        verify_inventory_joins([events])


def replica_scope(event):
    return scope(event, ("installation", "incarnation", "epoch", "stream"))


def exact_checkpoint(event, checkpoint, manifest):
    return (*replica_scope(event), checkpoint["block"], checkpoint["through"],
            tuple(checkpoint["root"]), tuple(manifest))


def broker_pointer(event):
    require(event["position"] >= 0 and event["key"].startswith(event["residence"] + "/")
            and digest(event["frame"]), "detached pointer has no exact committed frame")
    return (*replica_scope(event), event["residence"], event["position"], event["key"],
            json.dumps(event["publication"], sort_keys=True), tuple(event["frame"]))


def verify_joined(captures):
    """Join independently captured owner events by immutable identity and authority revision."""
    for events in captures:
        verify_native(events, verify_catalog_history=False, verify_certificate_history=False, verify_inventory_history=False)
    events = [event for capture in captures for event in capture]
    published, published_certified, catalogs, replay_holds, exclusions = {}, {}, {}, {}, {}
    censuses, fences_by_residence, grants, withdrawals, removals = {}, {}, {}, set(), set()
    certified_pointers, origins, body_grants = set(), {}, {}
    for event in events:
        kind = event["event"]
        if kind == "publish-checkpoint":
            identity = exact_checkpoint(event, event, event["manifest"])
            published[identity] = min(event["revision"], published.get(identity, event["revision"]))
            if event.get("decision") is not None:
                key = (identity, tuple(event["decision"]))
                published_certified[key] = min(event["revision"], published_certified.get(key, event["revision"]))
        elif kind in {"advance-floor", "close-catalog-admission", "catalog-hold", "catalog-release"}:
            catalogs.setdefault(scoped_catalog(event), []).append(event)
        elif kind == "detached-recovery-fence":
            fence = detached_fence(event)
            key = (*replica_scope(event), event["lineage"], event["residence"])
            fences_by_residence.setdefault(key, []).append(event)
            for candidate in event["candidates"]:
                entry = (*fence, detached_candidate(candidate, event["residence"]))
                censuses[entry] = min(event["revision"], censuses.get(entry, event["revision"]))
        elif kind == "broker-certified-pointer":
            certified_pointers.add((*broker_pointer(event), event["revision"]))
        elif kind == "admit-recovery":
            key = (*replica_scope(event), event["residence"], event["hold"])
            hold = scope(event, ("first_required", "through", "revision"))
            require(replay_holds.setdefault(key, hold) == hold, "replay hold changed its admitted range")
            if event.get("source_hold") is not None:
                require(origins.setdefault(key, event) == event, "broker admission changed its origin")
        elif kind == "retirement-recovery-dependency":
            attempt = detached_attempt(event)
            grant, candidate = attempt[:-2], attempt[-2]
            require(grants.setdefault(grant, candidate) == candidate,
                    "the detached grant changed its sealed frame or publication")
            publication = (*replica_scope(event), event["key"], candidate[1])
            exclusions[publication] = min(event["revision"], exclusions.get(publication, event["revision"]))
            position = (*replica_scope(event), event["lineage"], event["residence"], event["position"])
            body_grants[position] = min(event["revision"], body_grants.get(position, event["revision"]))
        elif kind == "withdraw-detached-key":
            withdrawals.add(detached_attempt(event))
        elif kind == "remove-version" and detached_effect(event):
            removals.add(detached_attempt(event))
    for fences in fences_by_residence.values():
        discoveries, previous = set(), None
        fences.sort(key=lambda event: event["revision"])
        for fence in fences:
            if previous is not None:
                require(fence["previous"] == {
                    "interval": previous["interval"], "digest": previous["anchor"],
                    "root_incarnation": previous["root_incarnation"],
                }, "detached census changes its preceding anchor")
            discoveries.update(detached_candidate(item, fence["residence"]) for item in fence["discovered"])
            candidates = {detached_candidate(item, fence["residence"]) for item in fence["candidates"]}
            require(candidates <= discoveries, "detached candidate lacks its exact censused publication")
            previous = fence
    for hold, event in origins.items():
        source = event["source_hold"]
        source_hold = (*replica_scope(event), source["residence"], source["hold"])
        first, through, revision = replay_holds[hold]
        require(event["residence"].rsplit("/", 1)[0] == "replay-bodies/" + source["residence"]
                and hold != source_hold and source["interval"][0] <= first == through <= source["interval"][1],
                "broker admission exceeds its originating hold")
        if source_hold in replay_holds:
            source_first, source_through, source_revision = replay_holds[source_hold]
            require([source_first, source_through] == source["interval"] and source_revision < revision,
                    "broker admission exceeds its originating hold")
        fence_key = (*replica_scope(event), event["lineage"], event["residence"])
        require(revision < body_grants.get((*fence_key, first), float("inf")), "broker admission follows the detached grant")
        closure = next((fence for fence in fences_by_residence.get(fence_key, [])
                        if fence["revision"] < revision and fence["interval"][0] <= first <= fence["interval"][1]), None)
        if closure is not None:
            previous = detached_census(closure).get(source["hold"])
            require(previous is not None and previous[0] <= first <= previous[1],
                    "broker admission reopened a closed recovery dependency")
    verify_catalogs(catalogs)
    ordered_fences = {ordered_object_fence(event) for event in events if event["event"] == "ordered-object-fence"}
    joined = verify_certified_reads(events) + verify_inventory_joins(captures)
    attempts = {detached_attempt(event) for event in events if event["event"] == "retirement-recovery-dependency"}
    require(removals <= attempts & withdrawals, "detached provider removal lacks its exact grant and key withdrawal")
    for event in events:
        kind = event["event"]
        if kind == "authorize-trim":
            checkpoint = event["checkpoint"]
            identity = exact_checkpoint(event, {
                "block": checkpoint["block"], "through": checkpoint["boundary"], "root": checkpoint["root_sha256"],
            }, event["manifest"])
        elif kind == "hosted-recovery-closure":
            identity = exact_checkpoint(event, event, event["manifest"])
        elif kind == "detached-recovery-fence":
            identity = detached_checkpoint(event)
            require(published_certified.get((identity, tuple(event["decision"])), float("inf")) <= event["revision"],
                    "detached census lacks its exact certified checkpoint")
        elif kind == "retirement-recovery-dependency":
            candidate = detached_candidate(event, event["residence"])
            entry = (*detached_fence(event), candidate)
            require(entry in censuses and censuses[entry] <= event["revision"],
                    "dependent retirement lacks its exact installed census and recovery exclusion")
            checkpoint = (detached_checkpoint(event), tuple(event["decision"]))
            require(checkpoint in published_certified and published_certified[checkpoint] <= event["revision"],
                    "detached retirement lacks its exact certified replacement checkpoint")
            joined += 1
            continue
        elif kind == "withdraw-detached-key":
            require(detached_attempt(event) in attempts, "key withdrawal lacks its exact detached grant")
            joined += 1
            continue
        elif kind == "remove-version" and detached_effect(event):
            joined += 1
            continue
        elif kind == "settle-detached-version":
            require(detached_attempt(event) in removals, "detached settlement lacks its exact provider removal")
            joined += 1
            continue
        elif kind == "read-retained-payload":
            hold = (*replica_scope(event), event["residence"], event["hold"])
            require(hold in replay_holds, "payload read lacks its exact replay hold")
            first, through, revision = replay_holds[hold]
            require(first <= event["position"] <= through and revision <= event["revision"],
                    "payload read is outside its admitted replay interval")
            if event.get("frame") is not None:
                require((*broker_pointer(event), event["revision"]) in certified_pointers,
                        "broker read lacks the exact certified pointer")
            if event.get("frame") is not None:
                require(hold in origins and origins[hold]["source_hold"] == event.get("source_hold"),
                        "broker read lacks its originating replay hold")
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
            if kind in {"install-body-census", "grant-detached-body"}:
                require(event["recovery_root"] == trace["recovery_root"] and event["decision"] == trace["decision"],
                        "body census names another committed recovery closure")
            if kind in {"grant-detached-body", "withdraw-body-key", "remove-detached-body", "settle-detached-body"}:
                require(event["unit"] == trace["detached_unit"], "detached effect names another publication")
                publication = "detached"
            if kind in {"select-fork-inventory", "verify-fork-inventory", "verify-fork-replacement", "withdraw-fork-edge"}:
                require(event["destination"] == fork, "fork inventory names another residence")
            if kind in {"select-fork-inventory", "verify-fork-inventory"}:
                require(event["inventory"] == trace["fork_inventory"], "fork inventory differs from the selected closure")
                publication = "closure"
            if kind in {"verify-fork-replacement", "withdraw-fork-edge"}:
                require(event["pack_member"] == trace["fork_pack_member"], "fork edge names another pack member")
                publication = "pack-member"
            if kind in {"enroll-repair-inventory", "verify-repair-inventory", "replay-repair-inventory"}:
                require(event["inventory"] == trace["repair_inventory"], "repair inventory differs from the enrolled primary")
                publication = "primary"
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
