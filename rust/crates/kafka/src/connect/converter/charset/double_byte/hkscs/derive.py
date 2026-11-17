import hashlib
import io
import re
import sys
import tarfile
import zipfile
from pathlib import Path

ARCHIVE_SHA256 = '8daf77d1aacffe38c9889689bc224a13557de77559d9a5bb91991e6a298baa0d'
SOURCE_ZIP_SHA256 = '3ed5aadae786d9e35101a67e94818c4590f5b8a81e7ee44ad4ce4add58881482'
archive, output = Path(sys.argv[1]), Path(sys.argv[2])
assert hashlib.file_digest(archive.open('rb'), 'sha256').hexdigest() == ARCHIVE_SHA256
output.mkdir(parents=True, exist_ok=True)
with tarfile.open(archive) as tar:
    source_member = next(member for member in tar if member.name.endswith('/lib/src.zip'))
    source_bytes = tar.extractfile(source_member).read()
    assert hashlib.sha256(source_bytes).hexdigest() == SOURCE_ZIP_SHA256
    for filename in ['LICENSE', 'ASSEMBLY_EXCEPTION', 'ADDITIONAL_LICENSE_INFO']:
        member = next(member for member in tar.getmembers() if member.name.endswith('/legal/java.base/' + filename))
        (output / filename).write_bytes(tar.extractfile(member).read())


def uncomment(source):
    return re.sub(r'/\*.*?\*/|//[^\n]*', '', source, flags=re.S)


def strings(expression):
    result = []
    escapes = {'b': '\b', 't': '\t', 'n': '\n', 'f': '\f', 'r': '\r', '"': '"', "'": "'", '\\': '\\'}
    for literal in re.findall(r'"((?:[^"\\]|\\.)*)"', expression):
        units, index = [], 0
        while index < len(literal):
            char = literal[index]
            index += 1
            if char == '\\':
                char = literal[index]
                index += 1
                if char == 'u':
                    char = chr(int(literal[index:index + 4], 16))
                    index += 4
                else:
                    char = escapes[char]
            units.append(char)
        result.append(''.join(units))
    return result


def string_rows(expression):
    rows, fragments, end = [], [], 0
    for token in re.finditer(r'"(?:[^"\\]|\\.)*"|null|[+,]', expression):
        assert not expression[end:token.start()].strip()
        value = token.group()
        end = token.end()
        if value == ',':
            assert fragments
            rows.append(None if fragments == [None] else ''.join(fragments))
            fragments = []
        elif value == 'null':
            assert not fragments
            fragments.append(None)
        elif value != '+':
            fragments.extend(strings(value))
    assert not expression[end:].strip()
    if fragments:
        rows.append(None if fragments == [None] else ''.join(fragments))
    return [None if row is None else [ord(char) for char in row] for row in rows]


with zipfile.ZipFile(io.BytesIO(source_bytes)) as source_zip:
    source = {name: source_zip.read(name) for name in source_zip.namelist() if name.endswith('.java') and ('/sun/nio/cs/' in name or name in ['java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java'])}
standard_path = 'java.base/sun/nio/cs/StandardCharsets.java'
extended_path = 'jdk.charsets/sun/nio/cs/ext/ExtendedCharsets.java'
standard, extended = (uncomment(source[path].decode()) for path in [standard_path, extended_path])
standard_aliases = dict(re.findall(r'new Object\[\]\s*\{\s*"([^"]+)",\s*"([^"]+)"', standard.split('class Aliases', 1)[1].split('class Classes', 1)[0]))
standard_classes = dict(re.findall(r'new Object\[\]\s*\{\s*"([^"]+)",\s*"([^"]+)"', standard.split('class Classes', 1)[1].split('class Cache', 1)[0]))
alias_methods = {name: strings(body) for name, body in re.findall(r'static String\[\] aliases_(\w+)(?:\(\)\s*\{\s*return|\s*=) new String\[\]\s*\{(.*?)\}', standard, flags=re.S)}
extended_classes, extended_aliases, extended_names = {}, {}, {}
for canonical, classname, body in re.findall(r'charset\("([^"]+)",\s*"([^"]+)",\s*new String\[\]\s*\{(.*?)\}\);', extended, flags=re.S):
    extended_classes.setdefault(canonical.lower(), classname)
    extended_names[classname] = (canonical, strings(body))
    for alias in strings(body):
        extended_aliases.setdefault(alias.lower(), canonical.lower())
assert 'deleteCharset(' not in extended


def resolve(name):
    lower = name.lower()
    canonical = standard_aliases.get(lower, lower)
    if canonical in standard_classes:
        return 'java.base', standard_classes[canonical]
    canonical = extended_aliases.get(lower, lower)
    return 'jdk.charsets', extended_classes.get(canonical)


classes = [
    ('Big5_HKSCS', 'HKSCSMapping', 'Big5'),
    ('Big5_HKSCS_2001', 'HKSCS2001Mapping', 'Big5'),
    ('MS950_HKSCS', 'HKSCSMapping', 'MS950'),
    ('MS950_HKSCS_XP', 'HKSCS_XPMapping', 'MS950'),
]
paths = {Path(path).stem: path for path in source}
headers = {}


def retain(path):
    original = source[path].decode()
    header = original[:original.index('*/') + 2]
    headers.setdefault(header, []).append(path)
    return uncomment(original)


bases = {}
for name in ['Big5', 'MS950']:
    text = retain(paths[name])
    holder = text.split('public static class DecodeHolder', 1)[1].split('public static class EncodeHolder', 1)[0]
    singles = [ord(char) for char in ''.join(strings(re.search(r'static final String b2cSBStr\s*=\s*(.*?);', holder, re.S).group(1)))]
    pairs = string_rows(re.search(r'static final String\[\] b2cStr\s*=\s*\{(.*?)\};', holder, re.S).group(1))
    assert singles == list(range(128)) + [0xfffd] * 128
    assert len(pairs) == 256 and all(row is None or len(row) == 191 for row in pairs)
    bases[name] = singles, pairs

overlays, overlay_rows = {}, []
for name in ['HKSCSMapping', 'HKSCS2001Mapping', 'HKSCS_XPMapping']:
    path = paths[name]
    text = retain(path)
    bmp = string_rows(re.search(r'String\[\] b2cBmpStr\s*=\s*new String\[\]\s*\{(.*?)\};', text, re.S).group(1))
    if name == 'HKSCS_XPMapping':
        assert 'String[] b2cSuppStr = null;' in text
        supp = [None] * 256
    else:
        supp = string_rows(re.search(r'String\[\] b2cSuppStr\s*=\s*new String\[\]\s*\{(.*?)\};', text, re.S).group(1))
    assert len(bmp) == len(supp) == 256
    assert all(row is None or len(row) == 191 for row in bmp + supp)
    assert all(not 0xd800 <= unit <= 0xdfff for row in bmp if row for unit in row)
    combined = []
    for bmp_row, supp_row in zip(bmp, supp):
        if bmp_row is None and supp_row is None:
            combined.append(None)
        else:
            combined.append([a if a != 0xfffd else 0x20000 + b if b != 0xfffd else 0xfffd
                             for a, b in zip(bmp_row or [0xfffd] * 191, supp_row or [0xfffd] * 191)])
    assert combined[0xf9][0xd6 - 0x40:0xdd - 0x40] == [0x7881, 0x92b9, 0x88cf, 0x58bb, 0x6052, 0x7ca7, 0x5afa]
    offsets, compact = [], []
    for row in combined:
        offsets.append(0xffff if row is None else len(compact))
        if row is not None:
            compact.append(row)
    blob = b''.join(unit.to_bytes(2, 'big') for unit in offsets)
    blob += b''.join(scalar.to_bytes(4, 'big') for row in compact for scalar in row)
    filename = name.lower() + '.bin'
    (output / filename).write_bytes(blob)
    overlays[name] = combined
    overlay_rows.append([name, path, hashlib.sha256(source[path]).hexdigest(), str(len(compact)), str(len(blob)), hashlib.sha256(blob).hexdigest()])
(output / 'overlays.tsv').write_text('class\tsource\tsource_sha256\tlead_rows\tbytes\tmapping_sha256\n' + ''.join('\t'.join(row) + '\n' for row in overlay_rows))

rows = []
for classname, overlay_name, base in classes:
    path = paths[classname]
    text = retain(path)
    canonical, constructor = re.search(r'super\("([^"]+)",\s*(.*?)\);', text).groups()
    provider = path.split('/')[0]
    if provider == 'java.base':
        assert constructor == f'StandardCharsets.aliases_{classname}()'
        aliases = alias_methods[classname]
    else:
        assert constructor == f'ExtendedCharsets.aliasesFor("{canonical}")'
        declared, aliases = extended_names[classname]
        assert declared == canonical
    assert re.search(r'public CharsetDecoder newDecoder\(\)\s*\{\s*return new Decoder\(this\);\s*\}', text)
    decoder = re.sub(r'\s+', ' ', text.split('class Decoder extends HKSCS.Decoder {', 1)[1].split('class Encoder', 1)[0])
    assert f'(DoubleByte.Decoder)new {base}().newDecoder();' in decoder
    assert f'initb2c(b2cBmp, {overlay_name}.b2cBmpStr);' in decoder
    if classname == 'MS950_HKSCS_XP':
        assert 'public char decodeDoubleEx(int b1, int b2) { return UNMAPPABLE_DECODING; }' in decoder
        assert 'super(cs, ms950, b2cBmp, null);' in decoder
        assert 'initb2c(b2cSupp' not in decoder
    else:
        assert f'initb2c(b2cSupp, {overlay_name}.b2cSuppStr);' in decoder
        assert f'super(cs, {base.lower()}, b2cBmp, b2cSupp);' in decoder
        assert 'decodeDoubleEx(' not in decoder
    singles, pairs = bases[base]
    overlay = overlays[overlay_name]
    single_probe = hashlib.sha256(''.join(chr(unit) for unit in singles).encode()).hexdigest()
    pair_probe = hashlib.sha256()
    for first in range(256):
        for second in range(256):
            if singles[first] != 0xfffd:
                scalars = [singles[first], singles[second]]
            else:
                scalar = 0xfffd
                if 0x40 <= second <= 0xfe:
                    if overlay[first] is not None:
                        scalar = overlay[first][second - 0x40]
                    if scalar == 0xfffd and pairs[first] is not None:
                        scalar = pairs[first][second - 0x40]
                scalars = [scalar]
            pair_probe.update(''.join(chr(scalar) for scalar in [*scalars, 0]).encode())
    names = sorted({name.lower() for name in [canonical, *aliases]})
    for name in names:
        assert resolve(name) == (provider, classname), (name, resolve(name), classname)
    actual_names = sorted(name for name in set(standard_aliases) | set(standard_classes) | set(extended_aliases) | set(extended_classes) if resolve(name) == (provider, classname))
    assert names == actual_names
    rows.append((canonical, classname, path, hashlib.sha256(source[path]).hexdigest(), overlay_name, base, single_probe, pair_probe.hexdigest(), '|'.join(aliases), names))
manifest = 'canonical\tclass\tsource\tsource_sha256\toverlay\tbase\tsingle_utf8_sha256\tpair_probe_utf8_sha256\taliases\n'
manifest += ''.join('\t'.join(row[:-1]) + '\n' for row in rows)
(output / 'charsets.tsv').write_text(manifest)
aliases = sorted((name, index) for index, row in enumerate(rows) for name in row[-1])
assert len(aliases) == len({name for name, _ in aliases}) == 14
(output / 'aliases.rs').write_text('pub(super) const ALIASES: &[(&str, usize)] = &[\n' + ''.join(f'    ("{name}", {index}),\n' for name, index in aliases) + '];\n')
tables = 'use super::{Base, Hkscs, Overlay};\n\npub(super) static OVERLAYS: &[Overlay] = &[\n'
tables += ''.join(f'    Overlay(include_bytes!("{row[0].lower()}.bin")),\n' for row in overlay_rows) + '];\n\npub(super) static TABLES: &[Hkscs] = &[\n'
for row in rows:
    index = next(index for index, overlay in enumerate(overlay_rows) if overlay[0] == row[4])
    base = 'Base::Big5(&super::super::big5::FAMILY)' if row[5] == 'Big5' else 'Base::Ms950(&super::super::tables::TABLES[19])'
    tables += f'    Hkscs {{ overlay: &OVERLAYS[{index}], base: {base} }},\n'
(output / 'tables.rs').write_text(tables + '];\n')
provenance_paths = [standard_path, extended_path, paths['HKSCS'], paths['DoubleByte'], paths['ArrayDecoder'], paths['CharsetMapping'], paths['Big5'], paths['MS950'], 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
for path in provenance_paths:
    if path not in [paths['Big5'], paths['MS950']]:
        retain(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
print('names', len(aliases), 'charsets', len(rows), 'overlays', len(overlay_rows), 'bytes', sum(int(row[4]) for row in overlay_rows))
for filename in ['aliases.rs', 'tables.rs', 'charsets.tsv', 'overlays.tsv', 'sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
for row in rows:
    print(row[0], row[6], row[7])
