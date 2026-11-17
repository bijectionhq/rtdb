import argparse
import hashlib
import json
from pathlib import Path
import re
from zipfile import ZipFile

SOURCE = "java.base/sun/security/util/KnownOIDs.java"
SOURCE_SHA256 = "2a269aa108db2826558b1d0394a4562b4e589a76113dacf198ba8bc458954122"


def registry(source):
    block = source.split("public enum KnownOIDs {", 1)[1].split("private final String stdName;", 1)[0]
    block = re.sub(r"//[^\n]*|/\*.*?\*/", "", block, flags=re.S)
    pattern = re.compile(r"([\w$]+)\s*\(([^()]*)\)\s*[,;]", re.S)
    entries = []
    for match in pattern.finditer(block):
        values = [text or boolean for text, boolean in re.findall(r'"([^"\n]*)"|\b(false|true)\b', match[2])]
        assert values and re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", values[0])
        name = values[1] if len(values) > 1 else match[1]
        register = not (len(values) > 2 and values[2] == "false")
        aliases = values[2:] if len(values) > 2 and values[2] not in ("true", "false") else []
        entries.append((match[1], values[0], name, register, aliases))
    assert not pattern.sub("", block).strip()
    assert len(entries) == 280
    lookup = {}
    for entry in entries:
        _, oid, name, register, aliases = entry
        for key in [oid] + ([name] + aliases if register else []):
            assert key.isascii()
            key = key.upper()
            assert key not in lookup
            lookup[key] = entry
    assert len(lookup) == 589
    return entries, lookup


def oid_bytes(oid):
    arcs = [int(value) for value in oid.split(".")]
    values = [arcs[0] * 40 + arcs[1]] + arcs[2:]
    output = []
    for value in values:
        encoded = [value & 127]
        value >>= 7
        while value:
            encoded.append(128 | (value & 127))
            value >>= 7
        output.extend(reversed(encoded))
    return output


def rust_bytes(oid):
    return "&[" + ", ".join(f"0x{value:02x}" for value in oid_bytes(oid)) + "]"


def derive(archive, output):
    with ZipFile(archive) as sources:
        data = sources.read(SOURCE)
    assert hashlib.sha256(data).hexdigest() == SOURCE_SHA256
    entries, lookup = registry(data.decode())
    matches = []
    rows = ["symbol\toid\tstandard_name\tderived_name\treverse_oid"]
    for symbol, oid, name, _, _ in entries:
        derived = name.replace("-", "") + "withECDSA"
        target = lookup.get(derived.upper())
        reverse = target[1] if target else "-"
        rows.append("\t".join((symbol, oid, name, derived, reverse)))
        if target:
            matches.append((oid, target[1]))
    matches.sort(key=lambda pair: tuple(int(arc) for arc in pair[0].split(".")))
    assert len(matches) == 5
    assert not any(name.endswith("WITHECDSAWITHECDSA") for name in lookup)
    prfs = ["HmacSHA1", "HmacSHA224", "HmacSHA256", "HmacSHA384", "HmacSHA512", "HmacSHA512/224", "HmacSHA512/256"]
    for prf in prfs:
        for bits in (128, 256):
            assert (f"PBEWith{prf}AndAES_{bits}withECDSA").upper() not in lookup
    output.mkdir(parents=True, exist_ok=True)
    output.joinpath("known_names.tsv").write_text("\n".join(rows) + "\n")
    output.joinpath("names.rs").write_text("pub(super) const DERIVED: &[(&[u8], &[u8])] = &[\n" + "".join(f"    ({rust_bytes(source)}, {rust_bytes(target)}),\n" for source, target in matches) + "];\n")
    output.joinpath("names.tsv").write_text("digest_oid\tsignature_oid\n" + "".join(f"{source}\t{target}\n" for source, target in matches))
    output.joinpath("provenance.json").write_text(json.dumps({"source": SOURCE, "source_sha256": SOURCE_SHA256, "enum_entries": len(entries), "registry_keys": len(lookup), "derived_matches": len(matches), "double_suffix_matches": 0, "pbes2_matches": 0}, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("src_zip", type=Path)
    parser.add_argument("output", type=Path)
    options = parser.parse_args()
    derive(options.src_zip, options.output)
