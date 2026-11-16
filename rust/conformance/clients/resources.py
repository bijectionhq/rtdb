import json
import os
import sys
from pathlib import Path


def sample(root=Path("/proc/1")):
    status = {}
    for line in (root / "status").read_text().splitlines():
        key, _, value = line.partition(":")
        status[key] = value.strip()
    io = {key: int(value) for line in (root / "io").read_text().splitlines() for key, value in [line.split(":", 1)]}
    stat = (root / "stat").read_text()
    fields = stat[stat.rfind(")") + 2:].split()
    peak = status.get("VmHWM", "").split()
    if len(peak) != 2 or peak[1] != "kB":
        raise ValueError("kernel did not report peak process RSS")
    return {"schema_version": 1, "pid": 1, "process_start_ticks": int(fields[19]),
            "executable": os.readlink(root / "exe"), "peak_rss_bytes": int(peak[0]) * 1024,
            "peak_boundary": "engine_process_lifetime_through_sample", "process_io": io,
            "io_boundary": "linux_process_io; includes logs and runtime; not object-provider payload"}


if __name__ == "__main__":
    json.dump(sample(), sys.stdout, allow_nan=False)
    sys.stdout.write("\n")
