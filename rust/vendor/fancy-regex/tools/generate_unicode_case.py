from pathlib import Path
import hashlib
import sys

source = Path(sys.argv[1]).read_bytes()
if hashlib.sha256(source).hexdigest() != "ff58e5823bd095166564a006e47d111130813dcf8bf234ef79fa51a870edb48f":
    raise SystemExit("Expected Unicode 16.0.0 UnicodeData.txt")
tables = {"UPPER": [], "LOWER": []}
for line in source.decode().splitlines():
    fields = line.split(";")
    for name, index in [("UPPER", 12), ("LOWER", 13)]:
        if fields[index]:
            tables[name].append((int(fields[0], 16), int(fields[index], 16)))
output = """// Copyright © 1991-2024 Unicode, Inc. Distributed under Unicode License V3; see LICENSE-UNICODE.
// Generated from Unicode 16.0.0 UnicodeData.txt by tools/generate_unicode_case.py.

pub(crate) fn equivalent(left: char, right: char) -> bool {
    let left = mapping(UPPER, left as u32);
    let right = mapping(UPPER, right as u32);
    left == right || mapping(LOWER, left) == mapping(LOWER, right)
}

fn mapping(table: &[(u32, u32)], value: u32) -> u32 {
    table.binary_search_by_key(&value, |entry| entry.0).map_or(value, |index| table[index].1)
}

"""
for name, table in tables.items():
    output += f"const {name}: &[(u32, u32)] = &[\n"
    for start in range(0, len(table), 4):
        output += "    " + " ".join(f"(0x{a:x}, 0x{b:x})," for a, b in table[start:start + 4]) + "\n"
    output += "];\n\n"
Path(__file__).resolve().parent.parent.joinpath("src/unicode_case.rs").write_text(output.rstrip() + "\n")
