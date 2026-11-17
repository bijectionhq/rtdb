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


double_byte = retain(paths['DoubleByte'])
assert 'return encodeChar(c) != UNMAPPABLE_ENCODING;' in double_byte
assert 'return c2b[c2bIndex[ch >> 8] + (ch & 0xff)];' in double_byte
assert 'if (b2c_ca[b >> 8][(b & 0xff) - b2Min] == c)' in double_byte
assert 'c2b[index] = b;' in double_byte
tables = {}
for name in ['JIS_X_0208', 'JIS_X_0212', 'IBM943']:
    text = retain(paths[name])
    holder = text.split('public static class DecodeHolder', 1)[1].split('public static class EncodeHolder', 1)[0]
    singles = units(holder, 'b2cSBStr')
    pairs = string_rows(re.search(r'String\[\] b2cStr\s*=\s*\{(.*?)\};', holder, re.S).group(1))
    low, high = (0x40, 0xfc) if name == 'IBM943' else (0x21, 0x7e)
    assert len(singles) == len(pairs) == 256
    assert all(row is None or len(row) == high - low + 1 for row in pairs)
    initialization = re.sub(r'\s+', ' ', holder[holder.index('static {'):]).strip()
    assert initialization == 'static { for (int i = 0; i < b2cStr.length; i++) { if (b2cStr[i] == null) b2c[i] = DoubleByte.B2C_UNMAPPABLE; else b2c[i] = b2cStr[i].toCharArray(); } b2cSB = b2cSBStr.toCharArray(); } }'
    if name != 'IBM943':
        assert all(row is None for index, row in enumerate(pairs) if not 0x21 <= index <= 0x7e)
    tables[name] = (singles, pairs, low, high)

ibm943 = uncomment(source[paths['IBM943']].decode())
assert 'return new DoubleByte.Encoder(this,  EncodeHolder.c2b, EncodeHolder.c2bIndex, false);' in ibm943
encoder = ibm943.split('public static class EncodeHolder', 1)[1]
removed, added = units(encoder, 'b2cNR'), units(encoder, 'c2bNR')
assert len(removed) % 2 == len(added) % 2 == 0
singles, pairs, low, high = tables['IBM943']
singles, pairs = singles[:], [None if row is None else row[:] for row in pairs]
for byte, char in zip(removed[::2], removed[1::2]):
    if byte < 256:
        if singles[byte] == char:
            singles[byte] = 0xfffd
    else:
        row, column = byte >> 8, (byte & 255) - low
        if pairs[row][column] == char:
            pairs[row][column] = 0xfffd
c2b = [0xfffd] * 65536
for byte, char in enumerate(singles):
    if char != 0xfffd:
        c2b[char] = byte
for first, row in enumerate(pairs):
    if row is not None:
        for second, char in enumerate(row, low):
            if char != 0xfffd:
                c2b[char] = first * 256 + second
before_additions = c2b[:]
for byte, char in zip(added[::2], added[1::2]):
    c2b[char] = byte
available = bytearray(8192)
for char, byte in enumerate(c2b):
    if byte != 0xfffd:
        available[char // 8] |= 0x80 >> (char % 8)
(output / 'ibm943-can-encode.bin').write_bytes(available)

jis0201 = units(retain(paths['JIS_X_0201']), 'b2cTable')
assert len(jis0201) == 256
jis0201 = jis0201[128:] + jis0201[:128]
assert all(jis0201[byte] == 0xfffd for byte in [*range(0x80, 0xa1), *range(0xe0, 256)])


def jis_pair(name, first, second):
    _, rows, low, high = tables[name]
    first, second = first - 0x80, second - 0x80
    if first < 0 or second < low or second > high or rows[first] is None:
        return 0xfffd
    return rows[first][second - low]


ibm29626c = retain(paths['IBM29626C'])
decoder = ibm29626c.split('static class Decoder extends CharsetDecoder', 1)[1].split('static class Encoder', 1)[0]
assert 'implements DelegatableDecoder' in decoder and 'ArrayDecoder' not in decoder
assert 'int idx = G2_b.indexOf(c);' in decoder and 'int idx = G3_b.indexOf(c);' in decoder
assert 'ch =  ibm943.canEncode(ch) ? ch : UNMAPPABLE_DECODING;' in decoder
assert "if (ch == '\\u2116')" in decoder
assert 'ch = decodeUDC(byte1, byte2, 0xe000);' in decoder
assert 'ch = decodeUDC(byte1, byte2, 0xe3ac);' in decoder
assert 'if (sp + 3 > sl)' in decoder and 'inputSize += 2;' in decoder
assert 'return CoderResult.unmappableForLength(inputSize);' in decoder
overlays = {}
for group in ['G2', 'G3']:
    keys, values = units(decoder, group + '_b'), units(decoder, group + '_c')
    assert len(keys) == len(values)
    overlays[group] = {}
    for byte, char in zip(keys, values):
        overlays[group].setdefault(byte, char)
assert overlays['G3'][0xf3b8] == 0x2116
g1_extra = units(decoder, 'g1_c')
assert g1_extra == [0xa2, 0xa3, 0xac, 0x5c, 0x7e]
g0 = [byte if byte not in [0x8e, 0x8f] else 0xfffd for byte in range(160)]
g2 = [g1_extra[byte - 0xe0] if 0xe0 <= byte <= 0xe4 else jis0201[byte] for byte in range(0xa1, 0xff)]
g1, g3 = [], []
for first in range(0xa1, 0xff):
    for second in range(0xa1, 0xff):
        key = first * 256 + second
        value = overlays['G2'].get(key, jis_pair('JIS_X_0208', first, second))
        if value == 0xfffd and first >= 0xf5:
            value = (first - 0xf5) * 94 + second - 0xa1 + 0xe000
        g1.append(value)
        if key in overlays['G3']:
            value = overlays['G3'][key]
        else:
            value = jis_pair('JIS_X_0212', first, second)
            if value == 0x2116 or c2b[value] == 0xfffd:
                value = 0xfffd
            if value == 0xfffd and first >= 0xf5:
                value = (first - 0xf5) * 94 + second - 0xa1 + 0xe3ac
        g3.append(value)
groups = {'IBM29626C': (g0, g1, g2, g3)}

ibm33722 = retain(paths['IBM33722'])
decoder33722 = ibm33722.split('protected static class Decoder extends CharsetDecoder', 1)[1].split('protected static class Encoder', 1)[0]
assert 'ArrayDecoder' not in decoder33722
assert 'return CoderResult.malformedForLength(1);' in decoder33722
assert 'return CoderResult.malformedForLength(2);' in decoder33722
assert 'return CoderResult.malformedForLength(3);' in decoder33722
assert 'if (sl - sp < 3)' in decoder33722
groups['IBM33722'] = tuple(units(decoder33722, name) for name in ['byteToCharTable', 'mappingTableG1', 'mappingTableG2', 'mappingTableG3'])
for name, group in groups.items():
    assert [len(values) for values in group] == [160, 8836, 94, 8836]
    assert all(not 0xd800 <= unit <= 0xdfff for values in group for unit in values)
    blob = b''.join(unit.to_bytes(2, 'big') for values in group for unit in values)
    (output / (name.lower() + '.bin')).write_bytes(blob)

charset_decoder = retain('java.base/java/nio/charset/CharsetDecoder.java')
assert 'cr = CoderResult.malformedForLength(in.remaining());' in charset_decoder
assert 'in.position(in.position() + cr.length());' in charset_decoder
string = retain('java.base/java/lang/String.java')
assert 'cd.decode(bb, cb, true)' in string
assert 'cd.onMalformedInput(CodingErrorAction.REPLACE)' in string
assert '.onUnmappableCharacter(CodingErrorAction.REPLACE);' in string


def decode_record(name, record):
    g0, g1, g2, g3 = groups[name]
    output_units, position = [], 0
    while position < len(record):
        first = record[position]
        position += 1
        if first == 0x8e:
            if position == len(record):
                output_units.append(0xfffd)
                break
            second = record[position]
            position += 1
            value = g2[second - 0xa1] if 0xa1 <= second <= 0xfe else 0xfffd
        elif first == 0x8f:
            if len(record) - position < 2:
                output_units.append(0xfffd)
                break
            second = record[position]
            position += 1
            if name == 'IBM33722' and not 0xa1 <= second <= 0xfe:
                value = 0xfffd
            else:
                third = record[position]
                position += 1
                value = g3[(second - 0xa1) * 94 + third - 0xa1] if 0xa1 <= second <= 0xfe and 0xa1 <= third <= 0xfe else 0xfffd
        elif first < 0xa0:
            value = g0[first]
        elif name == 'IBM33722' and first in [0xa0, 0xff]:
            value = 0xfffd
        elif position == len(record):
            output_units.append(0xfffd)
            break
        else:
            second = record[position]
            position += 1
            value = g1[(first - 0xa1) * 94 + second - 0xa1] if 0xa1 <= first <= 0xfe and 0xa1 <= second <= 0xfe else 0xfffd
        output_units.append(value)
    return ''.join(chr(unit) for unit in output_units).encode()


rows = []
for classname in ['IBM29626C', 'IBM33722']:
    path = paths[classname]
    text = uncomment(source[path].decode())
    canonical, constructor = re.search(r'super\("([^"]+)",\s*(.*?)\);', text).groups()
    assert constructor == f'ExtendedCharsets.aliasesFor("{canonical}")'
    declared, aliases = extended_names[classname]
    assert declared == canonical
    names = sorted({name.lower() for name in [canonical, *aliases]})
    for name in names:
        assert resolve(name) == ('jdk.charsets', classname), (name, resolve(name), classname)
    actual_names = sorted(name for name in set(standard_aliases) | set(standard_classes) | set(extended_aliases) | set(extended_classes) if resolve(name) == ('jdk.charsets', classname))
    assert names == actual_names
    single_probe = hashlib.sha256(b''.join(decode_record(classname, [byte]) for byte in range(256))).hexdigest()
    probes = []
    for prefix in [[], [0x8e], [0x8f]]:
        digest = hashlib.sha256()
        for first in range(256):
            for second in range(256):
                digest.update(decode_record(classname, [*prefix, first, second]))
                digest.update(b'\0')
        probes.append(digest.hexdigest())
    blob = (output / (classname.lower() + '.bin')).read_bytes()
    rows.append((canonical, classname, path, hashlib.sha256(source[path]).hexdigest(), str(len(blob)), hashlib.sha256(blob).hexdigest(), single_probe, *probes, '|'.join(aliases), names))
manifest = 'canonical\tclass\tsource\tsource_sha256\tbytes\tmapping_sha256\tsingle_utf8_sha256\tpair_probe_utf8_sha256\tss2_triple_utf8_sha256\tss3_triple_utf8_sha256\taliases\n'
manifest += ''.join('\t'.join(row[:-1]) + '\n' for row in rows)
(output / 'charsets.tsv').write_text(manifest)
aliases = sorted((name, row[1].replace('IBM', 'Ibm').replace('C', 'c')) for row in rows for name in row[-1])
assert len(aliases) == len({name for name, _ in aliases}) == 13
(output / 'aliases.rs').write_text('use super::IbmEuc;\n\npub(super) const ALIASES: &[(&str, IbmEuc)] = &[\n' + ''.join(f'    ("{name}", IbmEuc::{variant}),\n' for name, variant in aliases) + '];\n')
(output / 'tables.rs').write_text('use super::Tables;\n\n' + ''.join(f'pub(super) static {name}: Tables = Tables(include_bytes!("{name.lower()}.bin"));\n' for name in groups))
provenance_paths = [standard_path, extended_path, paths['IBM29626C'], paths['IBM33722'], paths['IBM943'], paths['JIS_X_0201'], paths['JIS_X_0208'], paths['JIS_X_0212'], paths['DoubleByte'], paths['SingleByte'], paths['CharsetMapping'], 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'java.base/java/nio/charset/CharsetDecoder.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
for path in provenance_paths:
    if path not in [item for members in headers.values() for item in members]:
        retain(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
print('names', len(aliases), 'charsets', len(rows), 'runtime bytes', sum(int(row[4]) for row in rows), 'availability bytes', len(available))
print('IBM943 available', sum(byte != 0xfffd for byte in c2b), 'NR removals', len(removed)//2, 'NR additions', len(added)//2, 'availability additions', sum(before == 0xfffd and after != 0xfffd for before, after in zip(before_additions, c2b)))
for filename in ['aliases.rs', 'tables.rs', 'charsets.tsv', 'sources.sha256', 'ibm943-can-encode.bin']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
for row in rows:
    print(row[0], 'single probe', row[6], 'pair probe', row[7], 'SS2 triple', row[8], 'SS3 triple', row[9])
