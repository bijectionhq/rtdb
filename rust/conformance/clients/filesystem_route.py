import json
import os
from pathlib import Path


def sample(root=Path("/proc/1")):
    """Read the actual server mount namespace and stat its object root through procfs.
    This observes routing only; payload counters come from the FUSE backing operations.
    """
    with (root / "mountinfo").open("rb") as stream:
        raw = stream.read(1048577)
    if len(raw) > 1048576:
        raise ValueError("server mount inventory exceeds bound")
    return {"mountinfo": raw.decode(), "root_device": os.stat(root / "root/measured-objects").st_dev}


if __name__ == "__main__":
    print(json.dumps(sample()))
