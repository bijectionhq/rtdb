"""Check one independently versioned RTDB contract and its proof controls."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from run import ROOT, clone_file, run_case, verify_verus


def check_lean(args, scratch, theorems):
    source = Path("verification/multi/lean")
    directory = scratch / "ordered-model"
    directory.mkdir()
    env = dict(os.environ, LEAN_PATH=str(directory))
    for module in ("OrderedCapacity", "OrderedJournal", "HeldWorldsOrder"):
        file = clone_file(source / f"{module}.lean", directory)
        proof = run_case([args.lean, "-DwarningAsError=true", "-o", f"{module}.olean", str(file)],
                         directory, args.evidence, module, env)
        if proof.returncode:
            raise ValueError(f"{module} failed; see {args.evidence}")
    audit = directory / "Audit.lean"
    audit.write_text("import HeldWorldsOrder\n" + "".join(f"#print axioms {name}\n" for name in theorems))
    checked = run_case([args.lean, "-DwarningAsError=true", str(audit)],
                       directory, args.evidence, "ordered-model-axioms", env)
    if checked.returncode:
        raise ValueError(f"ordered model theorem audit failed; see {args.evidence}")
    for name in theorems:
        axiom_line = re.search(rf"^'{re.escape(name)}' (?:depends on axioms: \[([^\]]*)\]|does not depend on any axioms)$",
                               checked.stdout, re.M)
        if not axiom_line or set(re.split(r",\s*", axiom_line[1] or "")) - {"", "propext", "Quot.sound"}:
            raise ValueError(f"{name}: unexpected axiom report")
    print(f"held-worlds model: {len(theorems)} exported theorems checked")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verus", required=True)
    parser.add_argument("--lean", required=True)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    if os.environ.get("BIJECTION_LOCAL_EXECUTION") != "owner-authorized":
        parser.error("local proof execution requires BIJECTION_LOCAL_EXECUTION=owner-authorized")
    directory = ROOT / "verification/contracts"
    source = (directory / "rtdb-consumer-v1.json").read_bytes()
    contract = json.loads(source)
    obligations = json.loads((directory / "rtdb-consumer-v1.proofs.json").read_text())
    if contract["id"] != obligations["contract"] or contract["guarantees"].keys() != obligations["proofs"].keys():
        raise ValueError("contract version and exported guarantees must match the proof obligations")
    required, lean = {}, []
    for proof in obligations["proofs"].values():
        name = proof["theorem"]
        if proof["engine"] == "verus":
            name = "proof::" + name.removeprefix("crate::")
            required.setdefault(proof["area"], {})[name] = proof["file"]
        elif proof["engine"] == "lean" and proof["area"] == "HeldWorldsOrder" and proof["file"] == "verification/multi/lean/HeldWorldsOrder.lean":
            lean.append(name)
        else:
            raise ValueError(f"unknown proof route: {proof}")
    for area in required:
        if not (ROOT / "verification/verus" / area / "area.json").is_file():
            raise ValueError(f"unknown Verus area: {area}")
    args.evidence = args.evidence.resolve()
    args.evidence.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="rtdb-consumer-contract-") as work:
        scratch = Path(work)
        verify_verus(args, scratch, required.keys(), required)
        check_lean(args, scratch, lean)
    record = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "contract": contract["id"],
        "contract_sha256": hashlib.sha256(source).hexdigest(),
        "guarantees": list(obligations["proofs"]),
    }
    (args.evidence / "contract-run.json").write_text(json.dumps(record, indent=2) + "\n")


if __name__ == "__main__":
    main()
