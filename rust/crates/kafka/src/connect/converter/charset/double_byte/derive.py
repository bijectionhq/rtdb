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


with zipfile.ZipFile(io.BytesIO(source_bytes)) as zip:
    source = {name: zip.read(name) for name in zip.namelist() if name.endswith('.java') and ('/sun/nio/cs/' in name or name in ['java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java'])}
standard_path = 'java.base/sun/nio/cs/StandardCharsets.java'
extended_path = 'jdk.charsets/sun/nio/cs/ext/ExtendedCharsets.java'
standard, extended = (uncomment(source[path].decode()) for path in [standard_path, extended_path])
standard_aliases = dict(re.findall(r'new Object\[\]\s*\{\s*"([^"]+)",\s*"([^"]+)"', standard.split('class Aliases', 1)[1].split('class Classes', 1)[0]))
standard_classes = dict(re.findall(r'new Object\[\]\s*\{\s*"([^"]+)",\s*"([^"]+)"', standard.split('class Classes', 1)[1].split('class Cache', 1)[0]))
alias_methods = {name: strings(body) for name, body in re.findall(r'static String\[\] aliases_(\w+)(?:\(\)\s*\{\s*return|\s*=) new String\[\]\s*\{(.*?)\}', standard, flags=re.S)}
extended_classes, extended_aliases = {}, {}
for canonical, classname, body in re.findall(r'charset\("([^"]+)",\s*"([^"]+)",\s*new String\[\]\s*\{(.*?)\}\);', extended, flags=re.S):
    extended_classes.setdefault(canonical.lower(), classname)
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


rows, headers = [], {}
for classname in ['SJIS', 'MS932']:
    path = f'java.base/sun/nio/cs/{classname}.java'
    raw = source[path]
    original, text = raw.decode(), uncomment(raw.decode())
    canonical, constructor = re.search(r'super\("([^"]+)",\s*(.*?)\);', text).groups()
    assert constructor == f'StandardCharsets.aliases_{classname}()'
    decoder = re.search(r'public CharsetDecoder newDecoder\(\)\s*\{(.*?)\}', text, flags=re.S).group(1).strip()
    flags = re.fullmatch(r'return new DoubleByte.Decoder\(this, DecodeHolder.b2c, DecodeHolder.b2cSB, (0x[0-9a-f]+), (0x[0-9a-f]+), (true|false)\);', decoder)
    assert flags, path
    minimum, maximum = (int(value, 16) for value in flags.groups()[:2])
    ascii_flag = flags.group(3) == 'true'
    holder = text.split('public static class DecodeHolder', 1)[1].split('public static class EncodeHolder', 1)[0]
    singles = [ord(char) for char in ''.join(strings(re.search(r'static final String b2cSBStr\s*=\s*(.*?);', holder, flags=re.S).group(1)))]
    pairs = string_rows(re.search(r'static final String\[\] b2cStr\s*=\s*\{(.*?)\};', holder, flags=re.S).group(1))
    initialization = re.sub(r'\s+', ' ', holder[holder.index('static {'):]).strip()
    assert initialization == 'static { for (int i = 0; i < b2cStr.length; i++) { if (b2cStr[i] == null) b2c[i] = DoubleByte.B2C_UNMAPPABLE; else b2c[i] = b2cStr[i].toCharArray(); } b2cSB = b2cSBStr.toCharArray(); } }', path
    assert len(singles) == len(pairs) == 256
    assert not ascii_flag or singles[:128] == list(range(128))
    assert all(row is None or len(row) == maximum - minimum + 1 for row in pairs)
    assert all(not 0xd800 <= unit <= 0xdfff for row in [singles, *[row for row in pairs if row is not None]] for unit in row)
    offsets, compact = [], []
    for row in pairs:
        offsets.append(0xffff if row is None else len(compact))
        if row is not None:
            compact.append(row)
    blob = bytes([minimum, maximum]) + b''.join(unit.to_bytes(2, 'big') for unit in singles + offsets + [unit for row in compact for unit in row])
    filename = canonical.lower() + '.bin'
    (output / filename).write_bytes(blob)
    single_digest = hashlib.sha256(''.join(chr(unit) for unit in singles).encode('utf-8')).hexdigest()
    pair_probe = hashlib.sha256()
    for first in range(256):
        for second in range(256):
            if singles[first] != 0xfffd:
                units = [singles[first], singles[second]]
            else:
                mapped = pairs[first][second - minimum] if pairs[first] is not None and minimum <= second <= maximum else 0xfffd
                if mapped != 0xfffd:
                    units = [mapped]
                elif pairs[first] is None or pairs[second] is not None or singles[second] != 0xfffd:
                    units = [0xfffd, singles[second]]
                else:
                    units = [0xfffd]
            pair_probe.update(''.join(chr(unit) for unit in [*units, 0]).encode('utf-8'))
    aliases = alias_methods[classname]
    names = sorted({name.lower() for name in [canonical, *aliases]})
    for name in names:
        assert resolve(name) == ('java.base', classname), (name, resolve(name), classname)
    actual_names = sorted(name for name in set(standard_aliases) | set(standard_classes) | set(extended_aliases) | set(extended_classes) if resolve(name) == ('java.base', classname))
    assert names == actual_names
    header = original[:original.index('*/') + 2]
    headers.setdefault(header, []).append(path)
    rows.append((canonical, classname, path, hashlib.sha256(raw).hexdigest(), str(ascii_flag).lower(), f'{minimum:02x}', f'{maximum:02x}', str(len(compact)), str(len(blob)), hashlib.sha256(blob).hexdigest(), single_digest, pair_probe.hexdigest(), '|'.join(aliases), filename, names))
rows.sort(key=lambda row: row[0].lower())
manifest = 'canonical\tclass\tsource\tsource_sha256\tascii\ttrail_min\ttrail_max\tlead_rows\tbytes\tmapping_sha256\tsingle_utf8_sha256\tpair_probe_utf8_sha256\taliases\n'
manifest += ''.join('\t'.join(row[:-2]) + '\n' for row in rows)
(output / 'charsets.tsv').write_text(manifest)
aliases = sorted((name, index) for index, row in enumerate(rows) for name in row[-1])
assert len(aliases) == len({name for name, _ in aliases}) == 10
(output / 'aliases.rs').write_text('pub(super) const ALIASES: &[(&str, usize)] = &[\n' + ''.join(f'    ("{name}", {index}),\n' for name, index in aliases) + '];\n')
(output / 'tables.rs').write_text('use super::Mapping;\n\npub(super) static TABLES: &[Mapping] = &[\n' + ''.join(f'    Mapping(include_bytes!("{row[-2]}")),\n' for row in rows) + '];\n')
provenance_paths = [standard_path, extended_path, 'java.base/sun/nio/cs/DoubleByte.java', 'java.base/sun/nio/cs/ArrayDecoder.java', 'java.base/sun/nio/cs/CharsetMapping.java', 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
for path in provenance_paths:
    original = source[path].decode()
    header = original[:original.index('*/') + 2]
    headers.setdefault(header, []).append(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
for row in rows:
    print(row[0], 'lead rows', row[7], 'bytes', row[8], 'sha256', row[9], 'pair probe', row[11])
for filename in ['aliases.rs', 'tables.rs', 'charsets.tsv', 'sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
