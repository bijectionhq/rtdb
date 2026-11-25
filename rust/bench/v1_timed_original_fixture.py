#!/usr/bin/env python3
"""Private, loopback-only clock authorities for local StreamJournal qualification."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import stat
import struct
import subprocess
import tempfile
import threading
import time
import uuid


PURPOSE = "bijection.rtdb.durable-acceptance-clock.v4"
RECEIVER_DOMAIN = b"rtdb/native/receiver-clock/v1\0"
KEEPER_DOMAIN = b"logkeeper/stream/original-clock/v1\0"
PUBLIC_KEY_PREFIX = bytes.fromhex("302a300506032b6570032100")
CLOCK_ERROR_NS = 1_000_000
STAGE_MARGIN_NS = 100_000_000


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def keypair(openssl, path):
    subprocess.run([
        openssl, "genpkey", "-algorithm", "ED25519", "-outform", "DER", "-out", path,
    ], check=True)
    public = subprocess.check_output([
        openssl, "pkey", "-inform", "DER", "-in", path, "-pubout", "-outform", "DER",
    ])
    if len(public) != len(PUBLIC_KEY_PREFIX) + 32 or not public.startswith(PUBLIC_KEY_PREFIX):
        raise ValueError("Expected an OpenSSL Ed25519 public key")
    return public[len(PUBLIC_KEY_PREFIX):]


def signature(openssl, key, statement):
    with tempfile.NamedTemporaryFile(dir=key.parent) as challenge:
        challenge.write(statement)
        challenge.flush()
        result = subprocess.check_output([
            openssl, "pkeyutl", "-sign", "-rawin", "-inkey", key, "-keyform", "DER",
            "-in", challenge.name,
        ])
    if len(result) != 64:
        raise ValueError("Expected an Ed25519 signature")
    return result


def initialize(args, output):
    source = str(args.source)
    keepers = [json.loads(path.read_text()) for path in args.keeper]
    identities = [uuid.UUID(keeper["keeper"]) for keeper in keepers]
    if len(identities) != 3 or len(set(identities)) != 3 or not all(x.int for x in identities):
        raise ValueError("Provide three independently identified local keeper configurations")
    if not args.source.int or any(not keeper["listen"].startswith("127.0.0.1:") for keeper in keepers):
        raise ValueError("Use a nonzero source and IPv4 loopback keepers")
    output.mkdir(mode=0o700)
    issuer = output / "issuer.der"
    issuer_public = keypair(args.openssl, issuer)
    receiver_public = keypair(args.openssl, output / "receiver.der")
    clock_public = keypair(args.openssl, output / "keeper-clock.der")
    deployment = str(uuid.uuid4())
    domain = str(uuid.uuid4())
    scope = secrets.token_hex(32)
    evidence = secrets.token_hex(32)
    expires = time.time_ns() + args.lifetime_seconds * 1_000_000_000
    statement = json.dumps({
        "purpose": PURPOSE, "deployment": deployment, "source": source,
        "cell": "local-qualification", "rtdbImage": "local-qualification", "serviceScope": scope,
        "maxStageToDurableLagNs": STAGE_MARGIN_NS, "maxJournalClockErrorNs": CLOCK_ERROR_NS,
        "expiresAtNs": expires, "evidenceSha256": evidence,
    }, separators=(",", ":"))
    certificate = json.dumps({
        "statement": statement, "signature": signature(args.openssl, issuer, statement.encode()).hex(),
    }, separators=(",", ":"))
    envelope = output / "receiver-envelope.json"
    envelope.write_text(certificate)
    site = output / "receiver-installation.json"
    write_json(site, {
        "deployment": deployment, "source": source, "placement_epoch": 1, "certificate": certificate,
        "issuer": issuer_public.hex(), "image": "local-qualification", "cell": "local-qualification",
        "service_scope": scope,
    })
    receiver_profile = output / "receiver-profile.yaml"
    # JSON is valid YAML and preserves the exact quoted path and integer shapes.
    write_json(receiver_profile, {"fundedReceiverClock": {
        "installationFile": str(site), "domain": domain, "placementEpoch": 1, "purpose": PURPOSE,
        "transport": {"socket": str(output / "receiver.sock"), "verificationKey": receiver_public.hex(),
                      "evidenceSha256": evidence, "timeoutMillis": 1000},
    }})
    trust = output / "timed-trust.json"
    public_keepers = []
    for slot, (keeper, identity) in enumerate(zip(keepers, identities), start=1):
        signing_key = output / f"keeper-{slot}.der"
        public_keepers.append({"identity": str(identity), "verification_key": list(keypair(args.openssl, signing_key))})
        keeper["timed_original"] = {"trust_file": str(trust), "original": {
            "signer_pkcs8_file": str(signing_key), "receiver_certificate_file": str(envelope),
            "receiver_issuer_key": list(issuer_public), "deployment": deployment, "source": source,
            "cell": "local-qualification", "rtdb_image": "local-qualification", "service_scope": scope,
            "clock_socket": str(output / "keeper-clock.sock"), "clock_verification_key": list(clock_public),
            "clock_timeout_ms": 1000,
        }}
        write_json(output / f"keeper-{slot}.json", keeper)
    write_json(trust, {"keepers": public_keepers, "maximum_payload_bytes": 4 << 20})
    write_json(output / "clock-config.json", {
        "source": source, "domain": domain, "keepers": [str(identity) for identity in identities],
        "expires": expires, "evidence": evidence, "certificate": hashlib.sha256(certificate.encode()).hexdigest(),
    })
    write_json(output / "receiver-lineage.json", {"sequence": 0, "sample_ns": 0})


def ordered_receiver_reading(root, sampled):
    lineage = root / "receiver-lineage.json"
    previous = json.loads(lineage.read_text())
    if sampled < previous["sample_ns"]:
        raise ValueError("Receiver clock regressed")
    next_sequence = previous["sequence"] + 1
    with tempfile.NamedTemporaryFile(dir=root, delete=False, mode="w") as updated:
        json.dump({"sequence": next_sequence, "sample_ns": sampled}, updated)
        updated.flush()
        os.fsync(updated.fileno())
    os.replace(updated.name, lineage)
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return struct.pack(">QQQQ", 1, next_sequence, sampled, CLOCK_ERROR_NS)


def receive(stream, size):
    message = bytearray()
    while len(message) < size:
        part = stream.recv(size - len(message))
        if not part:
            raise ValueError("Incomplete clock challenge")
        message.extend(part)
    return bytes(message)


def serve_socket(listener, root, openssl, key_name, length, reading):
    while True:
        connection, _ = listener.accept()
        with connection:
            try:
                connection.settimeout(1)
                request = receive(connection, length)
                sample = reading(request)
                connection.sendall(sample + signature(openssl, root / key_name, request + sample))
            except (OSError, ValueError, subprocess.SubprocessError):
                # Admission on either side needs a positive signed sample.
                continue


def serve(args, root):
    settings = json.loads((root / "clock-config.json").read_text())
    source = uuid.UUID(settings["source"]).bytes
    domain = uuid.UUID(settings["domain"]).bytes
    cert = bytes.fromhex(settings["certificate"])
    evidence = bytes.fromhex(settings["evidence"])
    valid_keepers = {uuid.UUID(identity).bytes for identity in settings["keepers"]}
    receiver_prefix = RECEIVER_DOMAIN + PURPOSE.encode() + b"\0" + source + domain + cert + evidence

    def receiver_reading(request):
        sampled = time.time_ns()
        if not request.startswith(receiver_prefix) or sampled + CLOCK_ERROR_NS >= settings["expires"]:
            raise ValueError("Receiver challenge has another binding")
        return ordered_receiver_reading(root, sampled)

    def keeper_reading(request):
        body = request[len(KEEPER_DOMAIN):]
        keeper, original, observed = body[:16], body[16:120], body[120:152]
        sampled = time.time_ns()
        cutoff = struct.unpack(">q", original[64:72])[0]
        upper = sampled + CLOCK_ERROR_NS
        if (not request.startswith(KEEPER_DOMAIN) or keeper not in valid_keepers or original[:16] != source
                or original[72:] != cert or observed != evidence
                or upper >= settings["expires"] or (upper + 999) // 1000 >= cutoff):
            raise ValueError("Keeper challenge has another binding or a closed cutoff")
        return struct.pack(">QQ", sampled, CLOCK_ERROR_NS)

    with (root / "authority.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        sockets = []
        for name, key, length, reading in [
            ("receiver.sock", "receiver.der", len(receiver_prefix) + 32, receiver_reading),
            ("keeper-clock.sock", "keeper-clock.der", len(KEEPER_DOMAIN) + 16 + 104 + 32 + 32, keeper_reading),
        ]:
            path = root / name
            if path.is_socket():
                path.unlink()
            listener = socket.socket(socket.AF_UNIX)
            listener.bind(str(path))
            path.chmod(0o600)
            listener.listen(16)
            sockets.append((listener, key, length, reading))
        receiver, keeper = sockets
        threading.Thread(target=serve_socket, args=(keeper[0], root, args.openssl, *keeper[1:]), daemon=True).start()
        print(root / "receiver-profile.yaml", flush=True)
        serve_socket(receiver[0], root, args.openssl, *receiver[1:])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--openssl", default="openssl", help="OpenSSL 3 executable")
    modes = parser.add_subparsers(dest="mode", required=True)
    init = modes.add_parser("initialize")
    init.add_argument("directory", type=Path)
    init.add_argument("--source", type=uuid.UUID, required=True)
    init.add_argument("--keeper", type=Path, action="append", required=True)
    init.add_argument("--lifetime-seconds", type=int, choices=range(60, 7201), metavar="60..7200", default=3600)
    modes.add_parser("serve").add_argument("directory", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    root = args.directory.absolute()
    if args.mode == "initialize":
        initialize(args, root)
    else:
        if not stat.S_ISDIR(root.lstat().st_mode) or stat.S_IMODE(root.lstat().st_mode) != 0o700:
            raise ValueError("Expected the private qualification custody directory")
        serve(args, root)


if __name__ == "__main__":
    main()
