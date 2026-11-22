"""Validate recovery publication, replay holds and trim observations in one journal."""

import argparse
import json
from pathlib import Path


def digest(value):
    return isinstance(value, list) and len(value) == 32 and all(type(byte) is int and 0 <= byte <= 255 for byte in value)


def verify(trace):
    revisions = trace["revisions"]
    if not revisions or not trace["retained_floors"]:
        raise ValueError("the trace needs directory revisions and observed log floors")
    if revisions[0]["authorized_through"] is not None:
        raise ValueError("the trace starts after a trim was authorized")
    observed = {}
    scope = revisions[0]["scope"]
    previous_version = None
    previous_through = -1
    previous_block = -1
    previous_boundary = -1
    for record in revisions:
        version = record["version"]
        if record["scope"] != scope:
            raise ValueError(f"revision {version} names a different replica journal")
        if previous_version is not None and version != previous_version + 1:
            raise ValueError(f"revision {version} follows a gap")
        previous_version = version
        checkpoint = record["checkpoint"]
        manifest = record["manifest"]
        through = record["authorized_through"]
        through = -1 if through is None else through
        if checkpoint is not None:
            block, boundary = checkpoint["block"], checkpoint["boundary"]
            if block < previous_block or boundary < previous_boundary:
                raise ValueError(f"revision {version} rewinds the checkpoint")
            previous_block, previous_boundary = block, boundary
        elif previous_block >= 0:
            raise ValueError(f"revision {version} discards its checkpoint")
        if through < previous_through:
            raise ValueError(f"revision {version} rewinds the authorization")
        if any(first <= through for first in record["first_required"]):
            raise ValueError(f"revision {version} overlaps an authorized trim and a replay hold")
        if through > previous_through and (
            checkpoint is None or not digest(manifest) or not digest(checkpoint.get("root_sha256"))
            or through >= checkpoint["boundary"]
        ):
            raise ValueError(f"revision {version} authorizes a trim without a covering manifest")
        observed[version] = through
        previous_through = through

    previous_floor = None
    for version, floor in trace["retained_floors"]:
        if version not in observed:
            raise ValueError(f"the log floor references unknown revision {version}")
        if floor > observed[version] + 1:
            raise ValueError(f"the log floor at revision {version} exceeds the authorized trim")
        if previous_floor is not None and floor < previous_floor:
            raise ValueError(f"the log floor at revision {version} moved backwards")
        previous_floor = floor


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    args = parser.parse_args()
    try:
        verify(json.loads(args.trace.read_text()))
    except (KeyError, TypeError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    print("Recovery and trim trace accepted")
