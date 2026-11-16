from pathlib import Path
import hashlib
import json
import re
import sys

root = Path(sys.argv[1])
checksums = {
    'unicode16-UnicodeData.txt': 'ff58e5823bd095166564a006e47d111130813dcf8bf234ef79fa51a870edb48f',
    'unicode16-PropList.txt': '53d614508e2a0b2305a8aa21cd60d993de9326cdf65993660dfcce4503548583',
    'unicode16-DerivedCoreProperties.txt': '39d35161f2954497f69e08bdb9e701493f476a3d30222de20028feda36c1dabd',
    'unicode16-Scripts.txt': '9e88f0a677df47311106340be8ede2ecdacd9c1c931831218d2be6d5508e0039',
    'unicode16-emoji-data.txt': 'f1365a5173eee18e1f98b240cdc492e84a25f1ce7e0c9d1094eb29c41a22696a',
    'unicode16-SpecialCasing.txt': '8d5de354eef79f2395a54c9c7dcebbaf3d30fc962d0f85611ea97aa973a0c451',
    'openjdk25-Character.java': 'bd864040a42498f6c15cfc61786570924e75072c61e576847c57434ded10ac52',
}
sources = {}
for name, expected in checksums.items():
    value = (root / name).read_bytes()
    if hashlib.sha256(value).hexdigest() != expected:
        raise SystemExit(f'Unexpected source digest: {name}')
    sources[name] = value.decode()


def union(*sequences):
    result = []
    for low, high in sorted(pair for sequence in sequences for pair in sequence):
        if result and low <= result[-1][1] + 1:
            result[-1] = (result[-1][0], max(high, result[-1][1]))
        else:
            result.append((low, high))
    return result


def difference(left, right):
    result = []
    position = 0
    for low, high in left:
        while position < len(right) and right[position][1] < low:
            position += 1
        cursor = low
        index = position
        while index < len(right) and right[index][0] <= high:
            if cursor < right[index][0]:
                result.append((cursor, right[index][0] - 1))
            cursor = max(cursor, right[index][1] + 1)
            index += 1
        if cursor <= high:
            result.append((cursor, high))
    return result


def properties(text):
    result = {}
    for line in text.splitlines():
        fields = [value.strip() for value in line.split('#')[0].split(';')]
        if len(fields) != 2:
            continue
        pair = [int(value, 16) for value in fields[0].split('..')]
        result.setdefault(fields[1], []).append((pair[0], pair[-1]))
    return {key: union(value) for key, value in result.items()}


categories = {}
names = {}
upper = {}
lower = {}
mirrored = []
range_start = None
for line in sources['unicode16-UnicodeData.txt'].splitlines():
    fields = line.split(';')
    cp = int(fields[0], 16)
    name = fields[1]
    if name.endswith(', First>'):
        range_start = cp
        continue
    low = range_start if name.endswith(', Last>') else cp
    range_start = None
    categories.setdefault(fields[2], []).append((low, cp))
    if fields[9] == 'Y':
        mirrored.append((low, cp))
    if fields[12]: upper[cp] = int(fields[12], 16)
    if fields[13]: lower[cp] = int(fields[13], 16)
    if name == '<control>':
        name = {7: 'BEL', 0x80: 'PADDING CHARACTER', 0x81: 'HIGH OCTET PRESET', 0x99: 'SINGLE GRAPHIC CHARACTER INTRODUCER'}.get(cp, fields[10])
    if name and not name.startswith('<'):
        names[name] = cp
categories = {key: union(value) for key, value in categories.items()}
all_values = [(0, 0x10ffff)]
assigned = union(*categories.values())
categories['Cn'] = difference(all_values, assigned)
for prefix in 'LMNZCPS':
    categories[prefix] = union(*(value for key, value in categories.items() if len(key) == 2 and key.startswith(prefix)))
categories['LC'] = union(*(categories[key] for key in ('Lu', 'Ll', 'Lt')))
categories['LD'] = union(categories['L'], categories['Nd'])
props = properties(sources['unicode16-PropList.txt'])
derived = properties(sources['unicode16-DerivedCoreProperties.txt'])
emoji = properties(sources['unicode16-emoji-data.txt'])
scripts = properties(sources['unicode16-Scripts.txt'])
scripts = {key.upper(): difference(value, categories['Cn']) for key, value in scripts.items()}
scripts['UNKNOWN'] = difference(all_values, union(*scripts.values()))

java = sources['openjdk25-Character.java']
block_aliases = {}
for body in re.findall(r'new UnicodeBlock\((.*?)\)', java, re.S):
    entries = re.findall(r'"([^"]+)"', body)
    if entries:
        for alias in entries: block_aliases[alias] = entries[0]
block_starts = re.search(r'int\[\] blockStarts = \{(.*?)\};', java, re.S).group(1)
block_starts = [int(value, 16) for value in re.findall(r'^\s*(0x[0-9A-Fa-f]+)', block_starts, re.M)]
block_values = re.search(r'UnicodeBlock\[\] blocks = \{(.*?)\};', java, re.S).group(1)
block_values = re.findall(r'^\s*([A-Z][A-Z0-9_]*|null)\s*[,\n]', block_values, re.M)
if len(block_values) != len(block_starts): raise SystemExit('Incomplete Java block table')
blocks = {}
for index, (start, name) in enumerate(zip(block_starts, block_values)):
    if name != 'null':
        blocks.setdefault(name, []).append((start, block_starts[index + 1] - 1 if index + 1 < len(block_starts) else 0x10ffff))
for name in block_aliases.values(): blocks.setdefault(name, [])
script_aliases = {name: name for name in scripts}
script_aliases.update(re.findall(r'aliases.put\("([^"]+)", ([A-Z_]+)\)', java))
if any(target not in scripts for target in script_aliases.values()): raise SystemExit('Unknown Java script alias target')

ignorable = union(categories['Cf'], [(0, 8), (14, 27), (127, 159)])
java_start = union(*(categories[key] for key in ('L', 'Nl', 'Sc', 'Pc')))
java_part = union(java_start, categories['Mn'], categories['Mc'], categories['Nd'], ignorable)
unicode_start = union(derived['ID_Start'], [(0x2e2f, 0x2e2f)])
unicode_part = union(unicode_start, derived['ID_Continue'], ignorable)
whitespace = union(categories['Z'], [(9, 13), (0x85, 0x85)])
java_whitespace = union(difference(categories['Z'], [(0xa0, 0xa0), (0x2007, 0x2007), (0x202f, 0x202f)]), [(9, 13), (28, 31)])
alpha = derived['Alphabetic']
case_letters = union(derived['Lowercase'], derived['Uppercase'], categories['Lt'])
hex_digit = union(categories['Nd'], [(0x41, 0x46), (0x61, 0x66), (0xff21, 0xff26), (0xff41, 0xff46)])
graph = difference(all_values, union(categories['Z'], categories['Cc'], categories['Cs'], categories['Cn']))
blank = union(categories['Zs'], [(9, 9)])
word = union(alpha, categories['M'], categories['Nd'], categories['Pc'], [(0x200c, 0x200d)])

sets = []
indices = {}
def register(value):
    value = tuple(union(value))
    if value not in indices:
        indices[value] = len(sets)
        sets.append(value)
    return indices[value]


def pair(value, insensitive=None):
    return (register(value), register(value if insensitive is None else insensitive))

properties_exact = {key: pair(value, categories['LC'] if key in ('Lu', 'Ll', 'Lt') else value) for key, value in categories.items()}
for key, value in {
    'L1': [(0, 255)], 'all': all_values, 'ASCII': [(0, 127)],
    'Alnum': [(48, 57), (65, 90), (97, 122)], 'Alpha': [(65, 90), (97, 122)],
    'Blank': [(9, 9), (32, 32)], 'Cntrl': [(0, 31), (127, 127)], 'Digit': [(48, 57)],
    'Graph': [(33, 126)], 'Lower': [(97, 122)], 'Print': [(32, 126)],
    'Punct': [(33, 47), (58, 64), (91, 96), (123, 126)],
    'Space': [(9, 13), (32, 32)], 'Upper': [(65, 90)], 'XDigit': [(48, 57), (65, 70), (97, 102)],
    'javaLowerCase': derived['Lowercase'], 'javaUpperCase': derived['Uppercase'], 'javaTitleCase': categories['Lt'],
    'javaAlphabetic': alpha, 'javaIdeographic': props['Ideographic'], 'javaDigit': categories['Nd'],
    'javaDefined': assigned, 'javaLetter': categories['L'], 'javaLetterOrDigit': categories['LD'],
    'javaJavaIdentifierStart': java_start, 'javaJavaIdentifierPart': java_part,
    'javaUnicodeIdentifierStart': unicode_start, 'javaUnicodeIdentifierPart': unicode_part,
    'javaIdentifierIgnorable': ignorable, 'javaSpaceChar': categories['Z'], 'javaWhitespace': java_whitespace,
    'javaISOControl': [(0, 31), (127, 159)], 'javaMirrored': mirrored,
}.items():
    ci = [(65, 90), (97, 122)] if key in ('Lower', 'Upper') else case_letters if key in ('javaLowerCase', 'javaUpperCase', 'javaTitleCase') else value
    properties_exact[key] = pair(value, ci)

posix = {}
for key, value in {
    'ALPHA': alpha, 'LOWER': derived['Lowercase'], 'UPPER': derived['Uppercase'], 'SPACE': whitespace,
    'PUNCT': categories['P'], 'XDIGIT': hex_digit, 'ALNUM': union(alpha, categories['Nd']),
    'CNTRL': categories['Cc'], 'DIGIT': categories['Nd'], 'BLANK': blank, 'GRAPH': graph,
    'PRINT': difference(union(graph, blank), categories['Cc']),
}.items(): posix[key] = pair(value, case_letters if key in ('LOWER', 'UPPER') else value)
unicode = dict(posix)
for key, value in {
    'ALPHABETIC': alpha, 'ASSIGNED': assigned, 'CONTROL': categories['Cc'], 'HEXDIGIT': hex_digit, 'HEX_DIGIT': hex_digit,
    'IDEOGRAPHIC': props['Ideographic'], 'JOINCONTROL': [(0x200c, 0x200d)], 'JOIN_CONTROL': [(0x200c, 0x200d)],
    'LETTER': categories['L'], 'LOWERCASE': derived['Lowercase'], 'UPPERCASE': derived['Uppercase'], 'TITLECASE': categories['Lt'],
    'NONCHARACTERCODEPOINT': props['Noncharacter_Code_Point'], 'NONCHARACTER_CODE_POINT': props['Noncharacter_Code_Point'],
    'PUNCTUATION': categories['P'], 'WHITESPACE': whitespace, 'WHITE_SPACE': whitespace, 'WORD': word,
    **{key.upper(): value for key, value in emoji.items()},
}.items(): unicode[key] = pair(value, case_letters if key in ('LOWERCASE', 'UPPERCASE', 'TITLECASE') else value)

block_indices = {key: register(value) for key, value in blocks.items()}
script_indices = {key: register(value) for key, value in scripts.items()}
assigned_index = register(assigned)
full_upper = {cp: [mapped] for cp, mapped in upper.items()}
full_lower = {cp: [mapped] for cp, mapped in lower.items()}
for line in sources['unicode16-SpecialCasing.txt'].splitlines():
    fields = [value.strip() for value in line.split('#')[0].split(';')]
    if len(fields) >= 5 and not fields[4]:
        full_upper[int(fields[0], 16)] = [int(value, 16) for value in fields[3].split()]
        full_lower[int(fields[0], 16)] = [int(value, 16) for value in fields[1].split()]
ascii_upper = [(cp, ''.join(chr(value) for value in values)) for cp, values in sorted(full_upper.items()) if cp > 127 and all(value < 128 for value in values)]
ascii_lower = [(cp, ''.join(chr(value) for value in values)) for cp, values in sorted(full_lower.items()) if cp > 127 and all(value < 128 for value in values)]
case_mappings = [(cp, upper.get(cp, cp), lower.get(cp, cp), lower.get(upper.get(cp, cp), upper.get(cp, cp))) for cp in sorted(upper.keys() | lower.keys())]

out = ['// Copyright © 1991-2024 Unicode, Inc. Distributed under Unicode License V3; see LICENSE-UNICODE.', '// Generated by tools/generate_java_unicode.py from digest-pinned Unicode 16.0 and Java 25 catalog data.', '']
def quote(value): return json.dumps(value, ensure_ascii=True)

def emit_entries(name, type_, entries):
    out.append(f'pub(super) const {name}: {type_} = &[{chr(10)}' + '\n'.join('    ' + value + ',' for value in entries) + '\n];\n')

emit_entries('SETS', '&[&[(u32, u32)]]', ['&[' + ', '.join(f'(0x{low:x}, 0x{high:x})' for low, high in values) + ']' for values in sets])
for name, values in [('PROPERTIES', properties_exact), ('POSIX', posix), ('UNICODE', unicode)]:
    emit_entries(name, '&[(&str, usize, usize)]', [f'({quote(key)}, {a}, {b})' for key, (a, b) in sorted(values.items())])
for name, values in [('BLOCK_ALIASES', {key: block_indices[value] for key, value in block_aliases.items()}), ('SCRIPT_ALIASES', {key: script_indices[value] for key, value in script_aliases.items()})]:
    emit_entries(name, '&[(&str, usize)]', [f'({quote(key)}, {value})' for key, value in sorted(values.items())])
emit_entries('BLOCK_NAMES', '&[(u32, u32, &str)]', [f'(0x{low:x}, 0x{high:x}, {quote(key.replace("_", " "))})' for low, high, key in sorted((low, high, key) for key, value in blocks.items() for low, high in value)])
emit_entries('NAMES', '&[(&str, u32)]', [f'({quote(key)}, 0x{value:x})' for key, value in sorted(names.items())])
emit_entries('NAMED_CODEPOINTS', '&[u32]', [f'0x{value:x}' for value in sorted(names.values())])
emit_entries('CASE_MAPPINGS', '&[(u32, u32, u32, u32)]', [f'(0x{cp:x}, 0x{up:x}, 0x{lo:x}, 0x{fold:x})' for cp, up, lo, fold in case_mappings])
emit_entries('ASCII_UPPER', '&[(u32, &str)]', [f'(0x{cp:x}, {quote(value)})' for cp, value in ascii_upper])
emit_entries('ASCII_LOWER', '&[(u32, &str)]', [f'(0x{cp:x}, {quote(value)})' for cp, value in ascii_lower])
out.append(f'pub(super) const ASSIGNED: usize = {assigned_index};\n')
Path(__file__).resolve().parent.parent.joinpath('src/string/java_unicode_data.rs').write_text('\n'.join(out))
print(f'Generated {len(names)} names, {len(block_aliases)} block aliases, {len(script_aliases)} script aliases, {len(sets)} range sets')
