import hashlib
import io
import re
import struct
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
    member = next(member for member in tar if member.name.endswith('/lib/src.zip'))
    source_bytes = tar.extractfile(member).read()
    assert hashlib.sha256(source_bytes).hexdigest() == SOURCE_ZIP_SHA256
    for filename in ['LICENSE', 'ASSEMBLY_EXCEPTION', 'ADDITIONAL_LICENSE_INFO']:
        member = next(member for member in tar.getmembers() if member.name.endswith('/legal/java.base/' + filename))
        (output / filename).write_bytes(tar.extractfile(member).read())
with zipfile.ZipFile(io.BytesIO(source_bytes)) as source_zip:
    source = {name: source_zip.read(name) for name in source_zip.namelist() if name.endswith('.java')}
paths = {Path(path).stem: path for path in source if '/sun/nio/cs/' in path}
used = set()


def text(path):
    used.add(path)
    return re.sub(r'/\*.*?\*/|//[^\n]*', '', source[path].decode(), flags=re.S)


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
            units.append(ord(char))
        result.extend(units)
    return result


def string_rows(expression):
    rows, fragments, end = [], [], 0
    for token in re.finditer(r'"(?:[^"\\]|\\.)*"|null|[+,]', expression):
        assert not expression[end:token.start()].strip()
        value = token.group()
        end = token.end()
        if value == ',':
            assert fragments
            rows.append(None if fragments == [None] else fragments)
            fragments = []
        elif value == 'null':
            assert not fragments
            fragments.append(None)
        elif value != '+':
            fragments.extend(strings(value))
    assert not expression[end:].strip()
    if fragments:
        rows.append(None if fragments == [None] else fragments)
    return rows


mapping_specs = [
    ('SJIS', 'shift_jis.bin', 0x40, 0xfc),
    ('MS932', 'windows-31j.bin', 0x40, 0xfc),
    ('JIS_X_0208', 'euc_jp/jis_x_0208.bin', 0x21, 0x7e),
    ('JIS_X_0212', 'euc_jp/jis_x_0212.bin', 0x21, 0x7e),
    ('JIS_X_0208_Solaris', 'euc_jp/jis_x_0208_solaris.bin', 0x21, 0x7e),
    ('JIS_X_0212_Solaris', 'euc_jp/jis_x_0212_solaris.bin', 0x21, 0x7e),
    ('JIS_X_0208_MS5022X', 'iso2022jp/jis_x_0208_ms5022x.bin', 0x21, 0x7e),
    ('JIS_X_0212_MS5022X', 'iso2022jp/jis_x_0212_ms5022x.bin', 0x21, 0x7e),
    ('JIS_X_0208_MS932', 'iso2022jp/jis_x_0208_ms932.bin', 0x21, 0x7e),
]
mappings, mapping_rows = {}, []
for classname, filename, minimum, maximum in mapping_specs:
    path = paths[classname]
    body = text(path)
    holder = body.split('public static class DecodeHolder', 1)[1].split('public static class EncodeHolder', 1)[0]
    singles = strings(re.search(r'String b2cSBStr\s*=\s*(.*?);', holder, re.S).group(1))
    pairs = string_rows(re.search(r'String\[\] b2cStr\s*=\s*\{(.*?)\};', holder, re.S).group(1))
    initialization = re.sub(r'\s+', ' ', holder[holder.index('static {'):]).strip()
    assert initialization == 'static { for (int i = 0; i < b2cStr.length; i++) { if (b2cStr[i] == null) b2c[i] = DoubleByte.B2C_UNMAPPABLE; else b2c[i] = b2cStr[i].toCharArray(); } b2cSB = b2cSBStr.toCharArray(); } }'
    assert len(singles) == len(pairs) == 256
    assert all(row is None or len(row) == maximum - minimum + 1 for row in pairs)
    assert all(not 0xd800 <= unit <= 0xdfff for row in [singles, *[row for row in pairs if row is not None]] for unit in row)
    if classname in ['SJIS', 'MS932']:
        assert f'new DoubleByte.Decoder(this, DecodeHolder.b2c, DecodeHolder.b2cSB, 0x{minimum:x}, 0x{maximum:x}, true)' in body
    else:
        assert 'new DoubleByte.Decoder_DBCSONLY(this, DecodeHolder.b2c, DecodeHolder.b2cSB, 0x21, 0x7e, false)' in body
    offsets, compact = [], []
    for row in pairs:
        offsets.append(0xffff if row is None else len(compact))
        if row is not None:
            compact.append(row)
    blob = bytes([minimum, maximum]) + b''.join(unit.to_bytes(2, 'big') for unit in singles + offsets + [unit for row in compact for unit in row])
    mappings[classname] = singles, pairs, minimum, maximum
    mapping_rows.append((classname, filename, str(len(blob)), hashlib.sha256(blob).hexdigest(), path, hashlib.sha256(source[path]).hexdigest()))
    (output / filename).parent.mkdir(parents=True, exist_ok=True)
    (output / filename).write_bytes(blob)
body = text(paths['JIS_X_0201'])
units = strings(re.search(r'String b2cTable\s*=\s*(.*?);', body, re.S).group(1))
assert len(units) == 256
jis0201 = units[128:] + units[:128]
assert jis0201[0xa1:0xe0] == list(range(0xff61, 0xffa0))
assert all(not 0xd800 <= unit <= 0xdfff for unit in jis0201)
blob = b''.join(unit.to_bytes(2, 'big') for unit in jis0201)
filename = 'euc_jp/jis_x_0201.bin'
(output / filename).write_bytes(blob)
mapping_rows.insert(2, ('JIS_X_0201', filename, str(len(blob)), hashlib.sha256(blob).hexdigest(), paths['JIS_X_0201'], hashlib.sha256(source[paths['JIS_X_0201']]).hexdigest()))

double = text(paths['DoubleByte'])
buffer = double.split('CoderResult decodeBufferLoop', 1)[1].split('public CoderResult decodeLoop', 1)[0]
assert 'return crMalformedOrUnderFlow(b1);' in buffer
assert 'return crMalformedOrUnmappable(b1, b2);' in buffer
assert 'src.position(mark);' in buffer
assert re.search(r'crMalformedOrUnderFlow\(int b\)\s*\{\s*return CoderResult.UNDERFLOW;', double)
assert 'b2c[b1] == B2C_UNMAPPABLE' in double and 'b2c[b2] != B2C_UNMAPPABLE' in double
assert 'decodeSingle(b2) != UNMAPPABLE_DECODING' in double

euc = text(paths['EUC_JP'])
buffer = euc.split('CoderResult decodeBufferLoop', 1)[1].split('public CoderResult decodeLoop', 1)[0]
assert 'src.remaining() < 2' in buffer and 'src.remaining() < 1' in buffer
assert 'return CoderResult.unmappableForLength(inputSize);' in buffer
assert 'src.position(mark);' in buffer
assert 'return dec0208.decodeDouble(byte1 - 0x80, byte2 - 0x80);' in euc
assert 'return dec0201.decode((byte)byte2);' in euc
assert 'super(cs, 1.0f, 1.0f, DEC0201, DEC0208, null);' in text(paths['EUC_JP_LINUX'])
solaris = text(paths['EUC_JP_Open'])
assert 'DEC0212_Solaris' in solaris and 'DEC0208_Solaris.decodeDouble(byte1 - 0x80, byte2 - 0x80)' in solaris

iso = text(paths['ISO2022_JP'])
buffer = iso.split('CoderResult decodeBufferLoop', 1)[1].split('public CoderResult decodeLoop', 1)[0]
for fragment in ['src.remaining() < 2', 'b3 == \'(\' && dec0212 != null', 'return CoderResult.malformedForLength(inputSize);', 'return CoderResult.unmappableForLength(inputSize);', 'previousState = currentState;', 'currentState = previousState;', 'src.position(mark);']:
    assert fragment in buffer, fragment
assert 'new Decoder(this, Decoder.DEC0208, CoderHolder.DEC0212)' in text(paths['ISO2022_JP_2'])
ms = text(paths['MS50220'])
assert 'new Decoder(this, Holder.DEC0208, Holder.DEC0212)' in ms
assert 'new JIS_X_0208_MS5022X().newDecoder()' in ms and 'new JIS_X_0212_MS5022X().newDecoder()' in ms
assert 'extends MS50220' in text(paths['MS50221'])
assert 'new Decoder(this, CoderHolder.DEC0208, null)' in text(paths['MSISO2022JP'])
string = text('java.base/java/lang/String.java')
assert 'cd.decode(bb, cb, true)' in string and 'cd.onMalformedInput(CodingErrorAction.REPLACE)' in string
assert '.onUnmappableCharacter(CodingErrorAction.REPLACE);' in string
charset_decoder = text('java.base/java/nio/charset/CharsetDecoder.java')
assert 'cr = CoderResult.malformedForLength(in.remaining());' in charset_decoder
assert 'in.position(in.position() + cr.length());' in charset_decoder


def pair(classname, first, second):
    _, rows, minimum, maximum = mappings[classname]
    if not 0 <= first <= 255 or not minimum <= second <= maximum or rows[first] is None:
        return 0xfffd
    return rows[first][second - minimum]


def double_group(classname, data, position):
    singles, rows, _, _ = mappings[classname]
    first = data[position]
    if singles[first] != 0xfffd:
        return singles[first], 1, 0
    if position + 1 == len(data):
        return None, 0, 0
    second = data[position + 1]
    unit = pair(classname, first, second)
    if unit != 0xfffd:
        return unit, 2, 0
    return None, 1 if rows[first] is None or rows[second] is not None or singles[second] != 0xfffd else 2, 1 if rows[first] is None or rows[second] is not None or singles[second] != 0xfffd else 2


def euc_group(variant, data, position):
    first = data[position]
    if first < 0x80:
        return first, 1, 0
    width = 3 if first == 0x8f else 2
    if len(data) - position < width:
        return None, 0, 0
    second = data[position + 1]
    if first == 0x8f:
        unit = 0xfffd if variant == 'Linux' else pair('JIS_X_0212_Solaris' if variant == 'Solaris' else 'JIS_X_0212', second - 0x80, data[position + 2] - 0x80)
    else:
        unit = jis0201[second] if first == 0x8e and second >= 0x80 else 0xfffd if first == 0x8e else pair('JIS_X_0208', first - 0x80, second - 0x80)
        if unit == 0xfffd and variant == 'Solaris':
            unit = pair('JIS_X_0208_Solaris', first - 0x80, second - 0x80)
    return (unit, width, 0) if unit != 0xfffd else (None, width, 2)


iso_tables = {'Standard': ('JIS_X_0208', None), 'Jp2': ('JIS_X_0208', 'JIS_X_0212'), 'Ms5022x': ('JIS_X_0208_MS5022X', 'JIS_X_0212_MS5022X'), 'MsIso': ('JIS_X_0208_MS932', None)}


def iso_group(variant, data, position, state):
    current, previous = state
    first = data[position]
    if first >= 0x80:
        return None, 1, 1
    if first == 0x1b:
        if len(data) - position < 3:
            return None, 0, 0
        second, third = data[position + 1:position + 3]
        width, selected = 3, None
        if second == 0x28:
            selected = {0x42: 0, 0x4a: 1, 0x49: 5}.get(third)
        elif second == 0x24:
            if third in [0x40, 0x42]:
                selected = 2 if third == 0x40 else 3
            elif third == 0x28 and iso_tables[variant][1] is not None:
                if len(data) - position < 4:
                    return None, 0, 0
                width = 4
                if data[position + 3] == 0x44:
                    selected = 4
        else:
            width = 2
        if selected is None:
            return None, width, 1
        state[0] = selected
        return None, width, 0
    if first == 0x0e:
        state[:] = [6, current]
        return None, 1, 0
    if first == 0x0f:
        state[0] = previous
        return None, 1, 0
    if current == 0:
        return first, 1, 0
    if current == 1:
        return {0x5c: 0x00a5, 0x7e: 0x203e}.get(first, first), 1, 0
    if current in [5, 6]:
        return (first + 0xff40, 1, 0) if first <= 0x5f else (None, 1, 1)
    if position + 1 == len(data):
        return None, 0, 0
    unit = pair(iso_tables[variant][1 if current == 4 else 0], first, data[position + 1])
    return (unit, 2, 0) if unit != 0xfffd else (None, 2, 2)


def walk(family, variant, data, replace):
    position, units, state = 0, [], [0, 0]
    while position < len(data):
        if family == 'double':
            unit, width, error = double_group(variant, data, position)
        elif family == 'euc':
            unit, width, error = euc_group(variant, data, position)
        else:
            unit, width, error = iso_group(variant, data, position, state)
        if width == 0:
            if replace:
                units.append(0xfffd)
            return position, units, 0, 0
        if error:
            if not replace:
                return position, units, error, width
            units.append(0xfffd)
        elif unit is not None:
            units.append(unit)
        position += width
    return position, units, 0, 0


def probe_bytes(family, variant, data):
    position, units, error, width = walk(family, variant, data, False)
    assert len(units) <= position <= len(data)
    result = struct.pack('>IIBB', position, len(units), error, width)
    return result + (b''.join(unit.to_bytes(2, 'big') for unit in units) if family == 'euc' else b'')


def append_bytes(family, variant, data):
    _, units, _, _ = walk(family, variant, data, True)
    return b'caller:' + ''.join(chr(unit) for unit in units).encode() + b'\0'


prefixes = {
    'double': [('empty', b''), ('ascii', b'A')],
    'euc': [('empty', b''), ('ascii', b'A'), ('ss2', b'\x8e'), ('ss3', b'\x8f'), ('invalid', b'\x80'), ('hiragana', b'\xa4\xa2'), ('halfwidth', b'\x8e\xa6')],
    'iso': [('ascii', b''), ('roman', b'\x1b(J'), ('kana', b'\x1b(I'), ('jis1978', b'\x1b$@'), ('jis1983', b'\x1b$B'), ('jis0212', b'\x1b$(D'), ('so', b'\x0e')],
}
variants = {'double': ['SJIS', 'MS932'], 'euc': ['Standard', 'Linux', 'Solaris'], 'iso': list(iso_tables)}
rows = []
for family, candidates in variants.items():
    for variant in candidates:
        for mode, prefix in prefixes[family]:
            singles, pairs, appended = hashlib.sha256(), hashlib.sha256(), hashlib.sha256()
            for byte in range(256):
                singles.update(probe_bytes(family, variant, prefix + bytes([byte])))
            for first in range(256):
                for second in range(256):
                    data = prefix + bytes([first, second])
                    pairs.update(probe_bytes(family, variant, data))
                    appended.update(append_bytes(family, variant, data))
            rows.append((family, variant, mode, prefix.hex(), singles.hexdigest(), pairs.hexdigest(), appended.hexdigest()))
assert len(rows) == 53
(output / 'modes.tsv').write_text('family\tvariant\tmode\tprefix_hex\tsingle_probe_sha256\tpair_probe_sha256\tappend_utf8_sha256\n' + ''.join('\t'.join(row) + '\n' for row in rows))
(output / 'mappings.tsv').write_text('class\tfile\tbytes\tmapping_sha256\tsource\tsource_sha256\n' + ''.join('\t'.join(row) + '\n' for row in mapping_rows))
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in sorted(used)))
(output / 'COPYRIGHT').write_bytes(b'\n\n'.join(path.encode() + b'\n\n' + source[path][:source[path].index(b'*/') + 2] for path in sorted(used)) + b'\n')
for filename in ['modes.tsv', 'mappings.tsv', 'sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
print('Source-only derivation:', len(rows), 'modes,', len(used), 'sources,', sum(int(row[2]) for row in mapping_rows), 'mapping bytes')
