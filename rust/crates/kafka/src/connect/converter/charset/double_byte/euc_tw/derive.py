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
    source = {name: source_zip.read(name) for name in source_zip.namelist() if name.endswith('.java') and ('/sun/nio/cs/' in name or name in ['java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'java.base/java/nio/charset/CharsetDecoder.java'])}
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


paths = {Path(path).stem: path for path in source}
headers = {}


def retain(path):
    original = source[path].decode()
    header = original[:original.index('*/') + 2]
    headers.setdefault(header, []).append(path)
    return uncomment(original)


def units(text, name):
    expression = re.search(r'\b' + name + r'\s*=\s*(.*?);', text, re.S).group(1)
    return [ord(char) for char in ''.join(strings(expression))]


standard = retain(paths['EUC_TW'])
decoder = standard.split('public static class Decoder extends CharsetDecoder', 1)[1].split('public static class Encoder', 1)[0]
assert 'ArrayDecoder' not in decoder
assert 'if ( sl - sp < 4)' in decoder
assert 'return CoderResult.malformedForLength(2);' in decoder
assert 'return CoderResult.malformedForLength(4);' in decoder
assert 'return CoderResult.malformedForLength(1);' in decoder
assert 'return CoderResult.unmappableForLength(2);' in decoder
assert 'Character.highSurrogate(0x20000 + c)' in decoder
assert 'Character.lowSurrogate(0x20000 + c)' in decoder
assert 'if (c == UNMAPPABLE_DECODING)' in decoder
assert 'if ((b2cIsSupp[index] & (1 << p)) == 0)' in decoder
selectors = {int(selector, 16): int(index) for selector, index in re.findall(r'cnspToIndex\[(0x[0-9a-f]+)\] = (\d+);', decoder)}
assert selectors == {0xa2: 1, 0xa3: 2, 0xa4: 3, 0xa5: 4, 0xa6: 5, 0xa7: 6, 0xaf: 7}
mapping = retain(paths['EUC_TWMapping'])
for name, value in [('b1Min', 0xa1), ('b1Max', 0xfe), ('b2Min', 0xa1), ('b2Max', 0xfe)]:
    assert int(re.search(r'\b' + name + r' = (0x[0-9a-f]+);', mapping).group(1), 16) == value
planes = string_rows(re.search(r'String\[\] b2c\s*=\s*\{(.*?)\};', mapping, re.S).group(1))
assert len(planes) == 8 and all(plane is not None and len(plane) == 8836 for plane in planes)
flags = b''.join(unit.to_bytes(2, 'big') for unit in units(mapping, 'b2cIsSuppStr'))
assert len(flags) == 17672
assert all(flag == 0 for flag in flags[8836:])
assert 'flag[off++] = (byte)(c >> 8);' in decoder and 'flag[off++] = (byte)(c & 0xff);' in decoder
for plane_index, plane in enumerate(planes):
    for index, value in enumerate(plane):
        if value != 0xfffd:
            supplementary = flags[index] & (1 << plane_index) != 0
            assert supplementary or not 0xd800 <= value <= 0xdfff
            assert plane_index != 0 or not supplementary
plane_bytes = b''.join(unit.to_bytes(2, 'big') for plane in planes for unit in plane)
(output / 'euc-tw-planes.bin').write_bytes(plane_bytes)
(output / 'euc-tw-supp.bin').write_bytes(flags)

ibm = retain(paths['IBM964'])
ibm_decoder = ibm.split('protected static class Decoder extends CharsetDecoder', 1)[1].split('protected static class Encoder', 1)[0]
assert 'ArrayDecoder' not in ibm_decoder
assert 'if (sl - sp < 4)' in ibm_decoder
assert 'return CoderResult.malformedForLength(3);' in ibm_decoder
assert 'return CoderResult.malformedForLength(4);' in ibm_decoder
assert 'byte1 == SS3' in ibm_decoder
assert 'byte1 == 0xa2' in ibm_decoder and 'byte1 == 0xac' in ibm_decoder and 'byte1 == 0xad' in ibm_decoder
ibm_singles = units(ibm_decoder, 'byteToCharTable')
ibm_planes = [units(ibm_decoder, name) for name in ['mappingTableG1', 'mappingTableG2a2', 'mappingTableG2ac', 'mappingTableG2ad']]
assert len(ibm_singles) == 160 and all(len(plane) == 8836 for plane in ibm_planes)
assert ibm_singles[:128] == list(range(128))
assert all(not 0xd800 <= unit <= 0xdfff for values in [ibm_singles, *ibm_planes] for unit in values)
ibm_bytes = b''.join(unit.to_bytes(2, 'big') for values in [ibm_singles, *ibm_planes] for unit in values)
(output / 'ibm964.bin').write_bytes(ibm_bytes)

charset_decoder = retain('java.base/java/nio/charset/CharsetDecoder.java')
assert 'cr = CoderResult.malformedForLength(in.remaining());' in charset_decoder
assert 'in.position(in.position() + cr.length());' in charset_decoder
string = retain('java.base/java/lang/String.java')
assert 'cd.decode(bb, cb, true)' in string
assert 'cd.onMalformedInput(CodingErrorAction.REPLACE)' in string
assert '.onUnmappableCharacter(CodingErrorAction.REPLACE);' in string


def legal(byte):
    return 0xa1 <= byte <= 0xfe


def pair(name, plane, first, second):
    if not legal(first) or not legal(second):
        return 0xfffd
    index = (first - 0xa1) * 94 + second - 0xa1
    value = (planes if name == 'EUC_TW' else ibm_planes)[plane][index]
    if value == 0xfffd:
        return value
    if name == 'EUC_TW' and flags[index] & (1 << plane):
        return 0x20000 + value
    return value


def decode_record(name, record):
    result, position = [], 0
    while position < len(record):
        first = record[position]
        position += 1
        if first == 0x8e:
            if len(record) - position < 3:
                result.append(0xfffd)
                break
            selector = record[position]
            position += 1
            plane = (selectors if name == 'EUC_TW' else {0xa2: 1, 0xac: 2, 0xad: 3}).get(selector)
            if plane is None:
                value = 0xfffd
            else:
                lead = record[position]
                position += 1
                if name == 'IBM964' and not legal(lead):
                    value = 0xfffd
                else:
                    trail = record[position]
                    position += 1
                    value = pair(name, plane, lead, trail)
        elif name == 'EUC_TW':
            if first < 0x80:
                value = first
            elif position == len(record):
                result.append(0xfffd)
                break
            else:
                second = record[position]
                if not legal(first) or not legal(second):
                    value = 0xfffd
                else:
                    position += 1
                    value = pair(name, 0, first, second)
        elif first == 0x8f:
            value = 0xfffd
        elif first < 0xa0:
            value = ibm_singles[first]
        elif not legal(first):
            value = 0xfffd
        elif position == len(record):
            result.append(0xfffd)
            break
        else:
            second = record[position]
            position += 1
            value = pair(name, 0, first, second)
        result.append(value)
    return ''.join(chr(value) for value in result).encode()


rows, plane_rows = [], []
for classname, variant in [('EUC_TW', 'Standard'), ('IBM964', 'Ibm964')]:
    path = paths[classname]
    text = uncomment(source[path].decode())
    canonical, constructor = re.search(r'super\("([^"]+)",\s*(.*?)\);', text).groups()
    if path.startswith('java.base/'):
        assert constructor == f'StandardCharsets.aliases_{classname}()'
        aliases = alias_methods[classname]
    else:
        assert constructor == f'ExtendedCharsets.aliasesFor("{canonical}")'
        declared, aliases = extended_names[classname]
        assert declared == canonical
    names = sorted({name.lower() for name in [canonical, *aliases]})
    for name in names:
        assert resolve(name) == (path.split('/')[0], classname), (name, resolve(name), classname)
    actual_names = sorted(name for name in set(standard_aliases) | set(standard_classes) | set(extended_aliases) | set(extended_classes) if resolve(name) == (path.split('/')[0], classname))
    assert names == actual_names
    single_probe = hashlib.sha256(b''.join(decode_record(classname, [byte]) for byte in range(256))).hexdigest()
    digest = hashlib.sha256()
    for first in range(256):
        for second in range(256):
            digest.update(decode_record(classname, [first, second]))
            digest.update(b'\0')
    pair_probe = digest.hexdigest()
    digest = hashlib.sha256()
    for selector in range(256):
        for lead, trail in [(0xa1, 0xa1), (0, 0x41), (0xa1, 0), (0x8e, 0xa2), (0xff, 0xff)]:
            digest.update(decode_record(classname, [0x8e, selector, lead, trail]))
            digest.update(b'\0')
    selector_probe = digest.hexdigest()
    active = selectors if classname == 'EUC_TW' else {0xa2: 1, 0xac: 2, 0xad: 3}
    for selector, index in active.items():
        digest = hashlib.sha256()
        for first in range(256):
            for second in range(256):
                digest.update(decode_record(classname, [0x8e, selector, first, second]))
                digest.update(b'\0')
        plane = (planes if classname == 'EUC_TW' else ibm_planes)[index]
        mapped = sum(value != 0xfffd for value in plane)
        supplementary = sum(value != 0xfffd and flags[position] & (1 << index) != 0 for position, value in enumerate(plane)) if classname == 'EUC_TW' else 0
        plane_rows.append((classname, f'{selector:02x}', str(index), str(mapped), str(supplementary), digest.hexdigest()))
    rows.append((canonical, classname, path, hashlib.sha256(source[path]).hexdigest(), variant, single_probe, pair_probe, selector_probe, '|'.join(aliases), names))
manifest = 'canonical\tclass\tsource\tsource_sha256\tvariant\tsingle_utf8_sha256\tpair_probe_utf8_sha256\tselector_probe_utf8_sha256\taliases\n'
manifest += ''.join('\t'.join(row[:-1]) + '\n' for row in rows)
(output / 'charsets.tsv').write_text(manifest)
(output / 'planes.tsv').write_text('class\tselector\tindex\tmapped\tsupplementary\tquartet_probe_utf8_sha256\n' + ''.join('\t'.join(row) + '\n' for row in plane_rows))
mapping_rows = []
for filename, classname, blob in [('euc-tw-planes.bin', 'EUC_TWMapping', plane_bytes), ('euc-tw-supp.bin', 'EUC_TWMapping', flags), ('ibm964.bin', 'IBM964', ibm_bytes)]:
    path = paths[classname]
    mapping_rows.append((filename, path, hashlib.sha256(source[path]).hexdigest(), str(len(blob)), hashlib.sha256(blob).hexdigest()))
(output / 'mappings.tsv').write_text('file\tsource\tsource_sha256\tbytes\tmapping_sha256\n' + ''.join('\t'.join(row) + '\n' for row in mapping_rows))
aliases = sorted((name, row[4]) for row in rows for name in row[-1])
assert len(aliases) == len({name for name, _ in aliases}) == 11
(output / 'aliases.rs').write_text('use super::EucTw;\n\npub(super) const ALIASES: &[(&str, EucTw)] = &[\n' + ''.join(f'    ("{name}", EucTw::{variant}),\n' for name, variant in aliases) + '];\n')
(output / 'tables.rs').write_text('use super::{IbmTables, StandardPlanes};\n\npub(super) static STANDARD: StandardPlanes = StandardPlanes {\n    planes: include_bytes!("euc-tw-planes.bin"),\n    supplementary: include_bytes!("euc-tw-supp.bin"),\n};\npub(super) static IBM964: IbmTables = IbmTables(include_bytes!("ibm964.bin"));\n')
provenance_paths = [standard_path, extended_path, paths['EUC_TW'], paths['EUC_TWMapping'], paths['IBM964'], paths['CharsetMapping'], 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'java.base/java/nio/charset/CharsetDecoder.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
for path in provenance_paths:
    if path not in [item for members in headers.values() for item in members]:
        retain(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
print('names', len(aliases), 'charsets', len(rows), 'mapping bytes', sum(int(row[3]) for row in mapping_rows))
for filename in ['aliases.rs', 'tables.rs', 'charsets.tsv', 'planes.tsv', 'mappings.tsv', 'sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
for row in plane_rows:
    print(*row)
