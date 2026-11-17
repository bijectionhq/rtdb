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


mapping_rows, mappings = [], {}
for classname in ['JIS_X_0208', 'JIS_X_0212', 'JIS_X_0208_MS5022X', 'JIS_X_0212_MS5022X', 'JIS_X_0208_MS932']:
    path = paths[classname]
    text = retain(path)
    assert 'new DoubleByte.Decoder_DBCSONLY(this, DecodeHolder.b2c, DecodeHolder.b2cSB, 0x21, 0x7e, false)' in text
    holder = text.split('public static class DecodeHolder', 1)[1].split('public static class EncodeHolder', 1)[0]
    singles = [ord(char) for char in ''.join(strings(re.search(r'String b2cSBStr\s*=\s*(.*?);', holder, re.S).group(1)))]
    pairs = string_rows(re.search(r'String\[\] b2cStr\s*=\s*\{(.*?)\};', holder, re.S).group(1))
    initialization = re.sub(r'\s+', ' ', holder[holder.index('static {'):]).strip()
    assert initialization == 'static { for (int i = 0; i < b2cStr.length; i++) { if (b2cStr[i] == null) b2c[i] = DoubleByte.B2C_UNMAPPABLE; else b2c[i] = b2cStr[i].toCharArray(); } b2cSB = b2cSBStr.toCharArray(); } }'
    assert len(singles) == len(pairs) == 256
    assert all(unit == 0xfffd for unit in singles)
    assert all(row is None or len(row) == 94 for row in pairs)
    offsets, compact = [], []
    for row in pairs:
        offsets.append(0xffff if row is None else len(compact))
        if row is not None:
            compact.append(row)
    units = singles + offsets + [unit for row in compact for unit in row]
    assert all(not 0xd800 <= unit <= 0xdfff for row in compact for unit in row)
    blob = bytes([0x21, 0x7e]) + b''.join(unit.to_bytes(2, 'big') for unit in units)
    filename = classname.lower() + '.bin'
    (output / filename).write_bytes(blob)
    mappings[classname] = pairs
    mapping_rows.append((classname, path, hashlib.sha256(source[path]).hexdigest(), filename, str(len(blob)), hashlib.sha256(blob).hexdigest(), str(len(compact))))

jp = retain(paths['ISO2022_JP'])
decoder = jp.split('static class Decoder extends CharsetDecoder', 1)[1].split('static class Encoder extends CharsetEncoder', 1)[0]
assert 'implements DelegatableDecoder' in decoder and 'ArrayDecoder' not in decoder
assert 'currentState = ASCII;' in decoder and 'previousState = ASCII;' in decoder
assert 'if ((b1 & 0x80) != 0)' in decoder
assert 'if (sp + inputSize + 2 > sl)' in decoder
assert 'b3 == \'(\' && dec0212 != null' in decoder
assert 'if (sp + inputSize + 1 > sl)' in decoder
assert 'c = dec0208.decodeDouble(b1,b2);' in decoder
assert 'c = dec0212.decodeDouble(b1,b2);' in decoder
assert 'if (b1 > 0x5f)' in decoder
assert 'da[dp++] = (char)(b1 + 0xff40);' in decoder
assert re.search(r'previousState = currentState;\s*currentState = SHIFTOUT;', decoder)
assert re.search(r'else if \(b1 == SI\) \{\s*currentState = previousState;', decoder)
assert 'return super.implFlush(out);' in decoder
jp2 = retain(paths['ISO2022_JP_2'])
assert 'new Decoder(this, Decoder.DEC0208, CoderHolder.DEC0212)' in jp2
assert 'new JIS_X_0212().newDecoder()' in jp2
ms20 = retain(paths['MS50220'])
assert 'new Decoder(this, Holder.DEC0208, Holder.DEC0212)' in ms20
assert 'new JIS_X_0208_MS5022X().newDecoder()' in ms20
assert 'new JIS_X_0212_MS5022X().newDecoder()' in ms20
ms21 = retain(paths['MS50221'])
assert 'extends MS50220' in ms21 and 'newDecoder' not in ms21
msiso = retain(paths['MSISO2022JP'])
assert 'new Decoder(this, CoderHolder.DEC0208, null)' in msiso
assert 'new JIS_X_0208_MS932().newDecoder()' in msiso
charset_decoder = retain('java.base/java/nio/charset/CharsetDecoder.java')
assert 'cr = CoderResult.malformedForLength(in.remaining());' in charset_decoder
assert 'in.position(in.position() + cr.length());' in charset_decoder
string = retain('java.base/java/lang/String.java')
assert 'cd.decode(bb, cb, true)' in string
assert 'cd.onMalformedInput(CodingErrorAction.REPLACE)' in string
assert '.onUnmappableCharacter(CodingErrorAction.REPLACE);' in string

configurations = {
    'Standard': ('JIS_X_0208', None),
    'Jp2': ('JIS_X_0208', 'JIS_X_0212'),
    'Ms5022x': ('JIS_X_0208_MS5022X', 'JIS_X_0212_MS5022X'),
    'MsIso': ('JIS_X_0208_MS932', None),
}


def pair(name, first, second):
    rows = mappings[name]
    return rows[first][second - 0x21] if rows[first] is not None and 0x21 <= second <= 0x7e else 0xfffd


def decode_record(variant, record):
    jis0208, jis0212 = configurations[variant]
    result, current, previous, position = [], 0, 0, 0
    while position < len(record):
        first = record[position]
        width = 1
        if first >= 0x80:
            result.append(0xfffd)
            position += 1
            continue
        if first == 0x1b:
            if len(record) - position < 3:
                result.append(0xfffd)
                break
            second = record[position + 1]
            width = 2
            valid = True
            if second == 0x28:
                third = record[position + 2]
                width = 3
                if third in [0x42, 0x4a, 0x49]:
                    current = {0x42: 0, 0x4a: 1, 0x49: 5}[third]
                else:
                    valid = False
            elif second == 0x24:
                third = record[position + 2]
                width = 3
                if third == 0x40:
                    current = 2
                elif third == 0x42:
                    current = 3
                elif third == 0x28 and jis0212 is not None:
                    if len(record) - position < 4:
                        result.append(0xfffd)
                        break
                    width = 4
                    if record[position + 3] == 0x44:
                        current = 4
                    else:
                        valid = False
                else:
                    valid = False
            else:
                valid = False
            if not valid:
                result.append(0xfffd)
            position += width
            continue
        if first == 0x0e:
            previous, current = current, 6
            position += 1
            continue
        if first == 0x0f:
            current = previous
            position += 1
            continue
        if current == 0:
            value = first
        elif current == 1:
            value = {0x5c: 0xa5, 0x7e: 0x203e}.get(first, first)
        elif current in [2, 3, 4]:
            if len(record) - position < 2:
                result.append(0xfffd)
                break
            width = 2
            value = pair(jis0212 if current == 4 else jis0208, first, record[position + 1])
        else:
            value = first + 0xff40 if first <= 0x5f else 0xfffd
        result.append(value)
        position += width
    return ''.join(chr(unit) for unit in result).encode()


prefixes = [('ascii', b''), ('roman', b'\x1b(J'), ('kana', b'\x1b(I'), ('jis1978', b'\x1b$@'), ('jis1983', b'\x1b$B'), ('jis0212', b'\x1b$(D'), ('shiftout', b'\x0e')]
mode_rows = []
for variant in configurations:
    for name, prefix in prefixes:
        single = hashlib.sha256(b''.join(decode_record(variant, prefix + bytes([byte])) for byte in range(256))).hexdigest()
        digests = []
        for escape in [b'', b'\x1b']:
            digest = hashlib.sha256()
            for first in range(256):
                for second in range(256):
                    digest.update(decode_record(variant, prefix + escape + bytes([first, second])))
                    digest.update(b'\0')
            digests.append(digest.hexdigest())
        ending = hashlib.sha256()
        for byte in range(256):
            ending.update(decode_record(variant, prefix + b'\x1b$(' + bytes([byte])))
            ending.update(b'\0')
        mode_rows.append((variant, name, prefix.hex(), single, *digests, ending.hexdigest()))

rows = []
for classname, variant in [('ISO2022_JP', 'Standard'), ('ISO2022_JP_2', 'Jp2'), ('MS50220', 'Ms5022x'), ('MS50221', 'Ms5022x'), ('MSISO2022JP', 'MsIso')]:
    path = paths[classname]
    canonical, aliases = extended_names[classname]
    assert re.search(r'super\("' + re.escape(canonical) + r'",\s*ExtendedCharsets.aliasesFor\("' + re.escape(canonical) + r'"\)\);', uncomment(source[path].decode()))
    names = sorted({name.lower() for name in [canonical, *aliases]})
    for name in names:
        assert resolve(name) == ('jdk.charsets', classname), (name, resolve(name), classname)
    actual_names = sorted(name for name in set(standard_aliases) | set(standard_classes) | set(extended_aliases) | set(extended_classes) if resolve(name) == ('jdk.charsets', classname))
    assert names == actual_names
    rows.append((canonical, classname, path, hashlib.sha256(source[path]).hexdigest(), variant, '|'.join(aliases), names))
(output / 'charsets.tsv').write_text('canonical\tclass\tsource\tsource_sha256\tvariant\taliases\n' + ''.join('\t'.join(row[:-1]) + '\n' for row in rows))
(output / 'modes.tsv').write_text('variant\tmode\tprefix_hex\tsingle_utf8_sha256\tpair_probe_utf8_sha256\tescape_probe_utf8_sha256\tjis0212_escape_probe_utf8_sha256\n' + ''.join('\t'.join(row) + '\n' for row in mode_rows))
(output / 'mappings.tsv').write_text('class\tsource\tsource_sha256\tfile\tbytes\tmapping_sha256\tlead_rows\n' + ''.join('\t'.join(row) + '\n' for row in mapping_rows))
aliases = sorted((name, row[4]) for row in rows for name in row[-1])
assert len(aliases) == len({name for name, _ in aliases}) == 17
(output / 'aliases.rs').write_text('use super::Iso2022Jp;\n\npub(super) const ALIASES: &[(&str, Iso2022Jp)] = &[\n' + ''.join(f'    ("{name}", Iso2022Jp::{variant}),\n' for name, variant in aliases) + '];\n')
(output / 'tables.rs').write_text('use super::Mapping;\n\n' + ''.join(f'pub(super) static {name}: Mapping = Mapping(include_bytes!("{classname.lower()}.bin"));\n' for name, classname in [('JIS0208', 'JIS_X_0208'), ('JIS0212', 'JIS_X_0212'), ('MS5022X_0208', 'JIS_X_0208_MS5022X'), ('MS5022X_0212', 'JIS_X_0212_MS5022X'), ('MS932_0208', 'JIS_X_0208_MS932')]))
provenance_paths = [standard_path, extended_path, *[paths[name] for name in ['ISO2022_JP', 'ISO2022_JP_2', 'MS50220', 'MS50221', 'MSISO2022JP', 'JIS_X_0208', 'JIS_X_0212', 'JIS_X_0208_MS5022X', 'JIS_X_0212_MS5022X', 'JIS_X_0208_MS932', 'DoubleByte', 'CharsetMapping']], 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'java.base/java/nio/charset/CharsetDecoder.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
for path in provenance_paths:
    if path not in [item for members in headers.values() for item in members]:
        retain(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
print('names', len(aliases), 'charsets', len(rows), 'mapping bytes', sum(int(row[4]) for row in mapping_rows))
for filename in ['aliases.rs', 'tables.rs', 'charsets.tsv', 'modes.tsv', 'mappings.tsv', 'sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
