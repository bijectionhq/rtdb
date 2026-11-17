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
for classname, filename in [('EUC_CN', 'gb2312.bin'), ('EUC_KR', 'euc-kr.bin')]:
    path = paths[classname]
    text = retain(path)
    assert 'new DoubleByte.Decoder(this, DecodeHolder.b2c, DecodeHolder.b2cSB, 0xa1, 0xfe, true)' in text
    holder = text.split('public static class DecodeHolder', 1)[1].split('public static class EncodeHolder', 1)[0]
    singles = [ord(char) for char in ''.join(strings(re.search(r'String b2cSBStr\s*=\s*(.*?);', holder, re.S).group(1)))]
    pairs = string_rows(re.search(r'String\[\] b2cStr\s*=\s*\{(.*?)\};', holder, re.S).group(1))
    initialization = re.sub(r'\s+', ' ', holder[holder.index('static {'):]).strip()
    assert initialization == 'static { for (int i = 0; i < b2cStr.length; i++) { if (b2cStr[i] == null) b2c[i] = DoubleByte.B2C_UNMAPPABLE; else b2c[i] = b2cStr[i].toCharArray(); } b2cSB = b2cSBStr.toCharArray(); } }'
    assert len(singles) == len(pairs) == 256
    assert singles[:128] == list(range(128)) and singles[128:] == [0xfffd] * 128
    assert all(row is None or len(row) == 94 for row in pairs)
    offsets, compact = [], []
    for row in pairs:
        offsets.append(0xffff if row is None else len(compact))
        if row is not None:
            compact.append(row)
    assert all(not 0xd800 <= unit <= 0xdfff for row in compact for unit in row)
    units = singles + offsets + [unit for row in compact for unit in row]
    blob = bytes([0xa1, 0xfe]) + b''.join(unit.to_bytes(2, 'big') for unit in units)
    (output / filename).write_bytes(blob)
    mappings[classname] = pairs
    mapping_rows.append((filename, path, hashlib.sha256(source[path]).hexdigest(), str(len(blob)), hashlib.sha256(blob).hexdigest(), 'double-byte', str(len(compact)), '0'))

tw = retain(paths['EUC_TW'])
assert 'flag[off++] = (byte)(c >> 8);' in tw and 'flag[off++] = (byte)(c & 0xff);' in tw
single = tw.split('public static char decodeSingleOrReplace', 1)[1].split('static char[] decode', 1)[0]
assert 'if (b1 < b1Min || b1 > b1Max || b2 < b2Min || b2 > b2Max)' in single
assert 'if (c == UNMAPPABLE_DECODING)' in single
assert 'if ((b2cIsSupp[index] & (1 << p)) == 0)' in single
assert single.count('return replace;') == 3 and 'return c;' in single
mapping_path = paths['EUC_TWMapping']
mapping = retain(mapping_path)
for name, value in [('b1Min', 0xa1), ('b1Max', 0xfe), ('b2Min', 0xa1), ('b2Max', 0xfe)]:
    assert int(re.search(r'\b' + name + r' = (0x[0-9a-f]+);', mapping).group(1), 16) == value
planes = string_rows(re.search(r'String\[\] b2c\s*=\s*\{(.*?)\};', mapping, re.S).group(1))
assert len(planes) == 8 and all(plane is not None and len(plane) == 8836 for plane in planes)
flag_units = [ord(char) for char in ''.join(strings(re.search(r'\bb2cIsSuppStr\s*=\s*(.*?);', mapping, re.S).group(1)))]
flags = b''.join(unit.to_bytes(2, 'big') for unit in flag_units)
assert len(flags) == 17672 and all(flag == 0 for flag in flags[8836:])
for plane_index in range(3):
    rejected = sum(unit != 0xfffd and flags[index] & (1 << plane_index) != 0 for index, unit in enumerate(planes[plane_index]))
    assert rejected == [0, 0, 71][plane_index]
    filtered = [0xfffd if flags[index] & (1 << plane_index) else unit for index, unit in enumerate(planes[plane_index])]
    assert all(not 0xd800 <= unit <= 0xdfff for unit in filtered)
    filename = f'cns{plane_index + 1}.bin'
    blob = b''.join(unit.to_bytes(2, 'big') for unit in filtered)
    (output / filename).write_bytes(blob)
    mapping_rows.append((filename, mapping_path, hashlib.sha256(source[mapping_path]).hexdigest(), str(len(blob)), hashlib.sha256(blob).hexdigest(), 'cns-plane', str(sum(unit != 0xfffd for unit in filtered)), str(rejected)))

cn = retain(paths['ISO2022_CN'])
cn_decoder = cn.split('static class Decoder extends CharsetDecoder', 1)[1]
assert 'ArrayDecoder' not in cn_decoder
assert 'new EUC_CN().newDecoder()' in cn_decoder
assert cn_decoder.count('EUC_TW.Decoder.decodeSingleOrReplace') == 2
assert re.search(r'if \(b1 == ISO_ESC\)\s*\{\s*currentSODesig = SODesigGB;', cn_decoder)
assert 'if (sp + 2 > sl)' in cn_decoder and 'if (sp + 3 > sl)' in cn_decoder and 'if (sp + 4 > sl)' in cn_decoder
assert 'if ((b2 & (byte)0x80) != 0)' in cn_decoder and 'if ((b3 & (byte)0x80) != 0)' in cn_decoder
assert 'byte1 |= MSB;' in cn_decoder and 'byte2 |= MSB;' in cn_decoder
assert "if (b3 == 'A')" in cn_decoder
assert "if (b4 == 'A')" in cn_decoder and "else if (b4 == 'G')" in cn_decoder
assert "if (b4 != 'H')" in cn_decoder and "if (b4 != 'I')" in cn_decoder
assert 'p = 1;' in cn_decoder and 'p = 2;' in cn_decoder
assert 'da[dp++] = (char)(b1 & 0xff);' in cn_decoder
for classname in ['ISO2022_CN_CNS', 'ISO2022_CN_GB']:
    text = retain(paths[classname])
    assert 'return new ISO2022_CN.Decoder(this);' in text

kr = retain(paths['ISO2022_KR'])
kr_decoder = kr.split('private static class Decoder extends CharsetDecoder', 1)[1].split('private static class Encoder', 1)[0]
assert 'ArrayDecoder' not in kr_decoder
assert "new byte[] {'$', ')', 'C' }" in kr_decoder
assert 'new EUC_KR().newDecoder()' in kr_decoder
assert 'private static final byte minDesignatorLength = 3;' in kr_decoder
assert 'if (sl - sp - 1 < minDesignatorLength)' in kr_decoder
assert 'return CoderResult.malformedForLength(2);' in kr_decoder
assert 'KSC5601.decodeDouble((byte1 | MSB) & 0xFF, (byte2 | MSB) & 0xFF)' in kr_decoder
assert 'return REPLACE_CHAR;' in kr_decoder
assert 'da[dp++]=(char)(sa[sp] & 0xff);' in kr_decoder
base = retain(paths['ISO2022'])
assert 'abstract CharsetDecoder newDecoder();' in base
charset_decoder = retain('java.base/java/nio/charset/CharsetDecoder.java')
assert 'cr = CoderResult.malformedForLength(in.remaining());' in charset_decoder
assert 'in.position(in.position() + cr.length());' in charset_decoder
string = retain('java.base/java/lang/String.java')
assert 'cd.decode(bb, cb, true)' in string
assert 'cd.onMalformedInput(CodingErrorAction.REPLACE)' in string
assert '.onUnmappableCharacter(CodingErrorAction.REPLACE);' in string


def db_pair(classname, first, second):
    first, second = first | 0x80, second | 0x80
    row = mappings[classname][first]
    return row[second - 0xa1] if row is not None and 0xa1 <= second <= 0xfe else 0xfffd


def cns_pair(plane, first, second):
    first, second = first | 0x80, second | 0x80
    if not 0xa1 <= first <= 0xfe or not 0xa1 <= second <= 0xfe:
        return 0xfffd
    index = (first - 0xa1) * 94 + second - 0xa1
    unit = planes[plane][index]
    if unit == 0xfffd or flags[index] & (1 << plane):
        return 0xfffd
    return unit


def decode_cn(record):
    result, position, shiftout, designation = [], 0, False, 0
    while position < len(record):
        first = record[position]
        width = 1
        if first == 0x1b:
            designation = 0
            if len(record) - position < 2:
                result.append(0xfffd)
                break
            second, width = record[position + 1], 2
            if second == 0x24:
                if len(record) - position < 3:
                    result.append(0xfffd)
                    break
                third, width = record[position + 2], 3
                if third == 0x41:
                    pass
                elif third in [0x29, 0x2a, 0x2b]:
                    if len(record) - position < 4:
                        result.append(0xfffd)
                        break
                    fourth, width = record[position + 3], 4
                    if third == 0x29 and fourth == 0x47:
                        designation = 1
                    elif not ((third == 0x29 and fourth == 0x41) or (third == 0x2a and fourth == 0x48) or (third == 0x2b and fourth == 0x49)):
                        result.append(0xfffd)
                else:
                    result.append(0xfffd)
            elif second in [0x4e, 0x4f]:
                if len(record) - position < 4:
                    result.append(0xfffd)
                    break
                width = 4
                result.append(cns_pair(1 if second == 0x4e else 2, record[position + 2], record[position + 3]))
            else:
                result.append(0xfffd)
        elif first == 0x0e:
            shiftout = True
        elif first == 0x0f:
            shiftout = False
        elif not shiftout:
            result.append(first)
        else:
            if len(record) - position < 2:
                result.append(0xfffd)
                break
            width = 2
            second = record[position + 1]
            result.append(db_pair('EUC_CN', first, second) if designation == 0 else cns_pair(0, first, second))
        position += width
    return ''.join(chr(unit) for unit in result).encode()


def decode_kr(record):
    result, position, shiftout = [], 0, False
    while position < len(record):
        first, width = record[position], 1
        if first == 0x0e:
            shiftout = True
        elif first == 0x0f:
            shiftout = False
        elif first == 0x1b:
            if len(record) - position < 4:
                result.append(0xfffd)
                break
            if record[position + 1:position + 4] == b'$)C':
                width = 4
            elif record[position + 1] in [0x4e, 0x4f]:
                result.append(0xfffd)
                width = 4
            else:
                result.append(0xfffd)
                width = 2
        elif not shiftout:
            result.append(first)
        else:
            if len(record) - position < 2:
                result.append(0xfffd)
                break
            result.append(db_pair('EUC_KR', first, record[position + 1]))
            width = 2
        position += width
    return ''.join(chr(unit) for unit in result).encode()


mode_rows = []
for variant, decode, modes in [
    ('Cn', decode_cn, [('gb-in', b''), ('gb-out', b'\x0e'), ('cns-in', b'\x1b$)G'), ('cns-out', b'\x1b$)G\x0e')]),
    ('Kr', decode_kr, [('in', b''), ('out', b'\x0e')]),
]:
    for name, prefix in modes:
        singles = hashlib.sha256(b''.join(decode(prefix + bytes([byte])) for byte in range(256))).hexdigest()
        digests = []
        for escape in [b'', b'\x1b', b'\x1b$', b'\x1bN', b'\x1bO']:
            digest = hashlib.sha256()
            for first in range(256):
                for second in range(256):
                    digest.update(decode(prefix + escape + bytes([first, second])))
                    digest.update(b'\0')
            digests.append(digest.hexdigest())
        selectors = hashlib.sha256()
        for selector in range(256):
            for suffix in [b'!!', b')C', b'\0\xff', b'\x0e\x0f', b'\x1b\x1b']:
                selectors.update(decode(prefix + b'\x1b' + bytes([selector]) + suffix))
                selectors.update(b'\0')
        mode_rows.append((variant, name, prefix.hex(), singles, *digests, selectors.hexdigest()))

rows = []
for classname, variant in [('ISO2022_CN', 'Cn'), ('ISO2022_CN_CNS', 'Cn'), ('ISO2022_CN_GB', 'Cn'), ('ISO2022_KR', 'Kr')]:
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
(output / 'modes.tsv').write_text('variant\tmode\tprefix_hex\tsingle_utf8_sha256\tpair_probe_utf8_sha256\tescape_pair_probe_utf8_sha256\tdesignation_probe_utf8_sha256\tss2_probe_utf8_sha256\tss3_probe_utf8_sha256\tescape_selectors_probe_utf8_sha256\n' + ''.join('\t'.join(row) + '\n' for row in mode_rows))
(output / 'mappings.tsv').write_text('file\tsource\tsource_sha256\tbytes\tmapping_sha256\tformat\trows_or_mapped\tsupplementary_replaced\n' + ''.join('\t'.join(row) + '\n' for row in mapping_rows))
aliases = sorted((name, row[4]) for row in rows for name in row[-1])
assert len(aliases) == len({name for name, _ in aliases}) == 12
(output / 'aliases.rs').write_text('use super::Iso2022CnKr;\n\npub(super) const ALIASES: &[(&str, Iso2022CnKr)] = &[\n' + ''.join(f'    ("{name}", Iso2022CnKr::{variant}),\n' for name, variant in aliases) + '];\n')
(output / 'tables.rs').write_text('use super::{CnsPlane, Mapping};\n\npub(super) static GB2312: Mapping = Mapping(include_bytes!("gb2312.bin"));\npub(super) static KSC5601: Mapping = Mapping(include_bytes!("euc-kr.bin"));\n' + ''.join(f'pub(super) static CNS{index}: CnsPlane = CnsPlane(include_bytes!("cns{index}.bin"));\n' for index in range(1, 4)))
provenance_paths = [standard_path, extended_path, *[paths[name] for name in ['ISO2022_CN', 'ISO2022_CN_CNS', 'ISO2022_CN_GB', 'ISO2022_KR', 'ISO2022', 'EUC_CN', 'EUC_KR', 'EUC_TW', 'EUC_TWMapping', 'DoubleByte', 'CharsetMapping']], 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'java.base/java/nio/charset/CharsetDecoder.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
for path in provenance_paths:
    if path not in [item for members in headers.values() for item in members]:
        retain(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
print('names', len(aliases), 'charsets', len(rows), 'mapping bytes', sum(int(row[3]) for row in mapping_rows))
for filename in ['aliases.rs', 'tables.rs', 'charsets.tsv', 'modes.tsv', 'mappings.tsv', 'sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
