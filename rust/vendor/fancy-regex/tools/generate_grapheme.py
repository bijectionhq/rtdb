from pathlib import Path
import hashlib
import sys

root = Path(sys.argv[1])
expected = {
    'UnicodeData': 'ff58e5823bd095166564a006e47d111130813dcf8bf234ef79fa51a870edb48f',
    'emoji-data': 'f1365a5173eee18e1f98b240cdc492e84a25f1ce7e0c9d1094eb29c41a22696a',
    'DerivedCoreProperties': '39d35161f2954497f69e08bdb9e701493f476a3d30222de20028feda36c1dabd',
}
source = {}
for name, digest in expected.items():
    data = root.joinpath(f'unicode16-{name}.txt').read_bytes()
    if hashlib.sha256(data).hexdigest() != digest:
        raise SystemExit(f'Expected Unicode 16.0.0 {name}.txt')
    source[name] = data.decode()

def properties(name):
    for line in source[name].splitlines():
        text = line.partition('#')[0].strip()
        if not text:
            continue
        fields = [field.strip() for field in text.split(';')]
        interval = fields[0].split('..')
        yield int(interval[0], 16), int(interval[-1], 16), fields[1:]

categories = ['Cn'] * 0x110000
for line in source['UnicodeData'].splitlines():
    fields = line.split(';')
    cp = int(fields[0], 16)
    if fields[1].endswith(', First>'):
        first = cp
    elif fields[1].endswith(', Last>'):
        categories[first:cp + 1] = [fields[2]] * (cp - first + 1)
    else:
        categories[cp] = fields[2]
pictographic = set()
for lo, hi, fields in properties('emoji-data'):
    if fields == ['Extended_Pictographic']:
        pictographic.update(range(lo, hi + 1))
excluded_spacing = {0x102b, 0x102c, 0x1038, *range(0x1062, 0x1065), *range(0x1067, 0x106e), 0x1083,
                    *range(0x1087, 0x108d), 0x108f, *range(0x109a, 0x109d), 0x1a61, 0x1a63, 0x1a64, 0xaa7b, 0xaa7d}
prepend_letters = {0x0d4e, 0x111c2, 0x111c3, 0x113d1, 0x1193f, 0x11941, 0x11a3a, *range(0x11a84, 0x11a8a), 0x11d46, 0x11f02}

def classify(cp, category):
    if cp < 0x7f:
        return 'Cr' if cp == 13 else 'Lf' if cp == 10 else 'Control' if cp < 32 else 'Other'
    if cp in pictographic:
        return 'Pictographic'
    if category in {'Cn', 'Cc', 'Zl', 'Zp', 'Cs'}:
        return 'Other' if cp == 0x378 else 'Control'
    if category == 'Cf':
        if cp == 0x200c or 0xe0020 <= cp <= 0xe007f:
            return 'Extend'
        if cp == 0x200d:
            return 'Zwj'
        if 0x600 <= cp <= 0x605 or cp in {0x6dd, 0x70f, 0x890, 0x891, 0x8e2, 0x110bd, 0x110cd}:
            return 'Prepend'
        return 'Control'
    if category in {'Mn', 'Me'}:
        return 'Extend'
    if category == 'Mc':
        return 'Other' if cp in excluded_spacing else 'Spacing'
    if category == 'So':
        return 'Regional' if 0x1f1e6 <= cp <= 0x1f1ff else 'Other'
    if category in {'Lm', 'Sk'}:
        return 'Extend' if cp in {0xff9e, 0xff9f} or 0x1f3fb <= cp <= 0x1f3ff else 'Other'
    if category == 'Lo':
        if cp in {0xe33, 0xeb3}:
            return 'Spacing'
        if 0x1100 <= cp <= 0x11ff:
            return 'L' if cp <= 0x115f else 'V' if cp <= 0x11a7 else 'T'
        if 0xac00 <= cp < 0xac00 + 11172:
            return 'Other'
        if 0xa960 <= cp <= 0xa97c:
            return 'L'
        if 0xd7b0 <= cp <= 0xd7c6 or cp == 0x16d63 or 0x16d67 <= cp <= 0x16d6a:
            return 'V'
        if 0xd7cb <= cp <= 0xd7fb:
            return 'T'
        if cp in prepend_letters:
            return 'Prepend'
    return 'Other'

def ranges(values):
    previous, start = values[0], 0
    for index, value in enumerate(values[1:], 1):
        if value != previous:
            yield start, index - 1, previous
            previous, start = value, index
    yield start, len(values) - 1, previous

output = '// Copyright © 1991-2024 Unicode, Inc. Distributed under Unicode License V3; see LICENSE-UNICODE.\n'
output += '// Generated from Unicode 16.0.0 by tools/generate_grapheme.py.\n\nuse super::Class;\n\n'
output += 'pub(super) const CLASSES: &[(u32, u32, Class)] = &[\n'
for lo, hi, value in ranges([classify(cp, category) for cp, category in enumerate(categories)]):
    if value != 'Other':
        output += f'    (0x{lo:x}, 0x{hi:x}, Class::{value}),\n'
output += '];\n'
for kind in ['Consonant', 'Linker', 'Extend']:
    output += f'\npub(super) const {kind.upper()}: &[(u32, u32)] = &[\n'
    for lo, hi, fields in properties('DerivedCoreProperties'):
        if fields == ['InCB', kind]:
            output += f'    (0x{lo:x}, 0x{hi:x}),\n'
    output += '];\n'
Path(__file__).resolve().parent.parent.joinpath('src/grapheme_data.rs').write_text(output)
