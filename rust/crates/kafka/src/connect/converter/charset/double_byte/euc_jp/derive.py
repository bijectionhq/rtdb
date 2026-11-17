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


euc = retain(paths['EUC_JP'])
decoder = euc.split('public static class Decoder extends CharsetDecoder', 1)[1].split('public static class Encoder', 1)[0]
assert 'implements DelegatableDecoder' in decoder and 'ArrayDecoder' not in decoder
assert 'if ((b1 & 0x80) == 0)' in decoder and 'outputChar = (char)b1;' in decoder
assert 'if (sp + 3 > sl)' in decoder and 'inputSize += 2;' in decoder
assert 'return CoderResult.unmappableForLength(inputSize);' in decoder
assert 'return dec0208.decodeDouble(byte1 - 0x80, byte2 - 0x80);' in decoder
assert 'return dec0201.decode((byte)byte2);' in decoder
assert 'outputChar = dec0212.decodeDouble(b1-0x80, b2-0x80);' in decoder
linux = retain(paths['EUC_JP_LINUX'])
assert 'super(cs, 1.0f, 1.0f, DEC0201, DEC0208, null);' in linux
solaris = retain(paths['EUC_JP_Open'])
assert 'super(cs, 0.5f, 1.0f, DEC0201, DEC0208, DEC0212_Solaris);' in solaris
assert re.search(r'char c = super.decodeDouble\(byte1, byte2\);\s*if \(c == UNMAPPABLE_DECODING\)\s*return DEC0208_Solaris.decodeDouble\(byte1 - 0x80, byte2 - 0x80\);\s*return c;', solaris)
charset_decoder = retain('java.base/java/nio/charset/CharsetDecoder.java')
assert 'cr = CoderResult.malformedForLength(in.remaining());' in charset_decoder
assert 'in.position(in.position() + cr.length());' in charset_decoder
string = retain('java.base/java/lang/String.java')
assert 'cd.decode(bb, cb, true)' in string
assert 'cd.onMalformedInput(CodingErrorAction.REPLACE)' in string
assert '.onUnmappableCharacter(CodingErrorAction.REPLACE);' in string

mapping_rows, mappings = [], {}
for classname in ['JIS_X_0201', 'JIS_X_0208', 'JIS_X_0212', 'JIS_X_0208_Solaris', 'JIS_X_0212_Solaris']:
    path = paths[classname]
    text = retain(path)
    filename = classname.lower() + '.bin'
    if classname == 'JIS_X_0201':
        units = [ord(char) for char in ''.join(strings(re.search(r'String b2cTable\s*=\s*(.*?);', text, re.S).group(1)))]
        assert len(units) == 256
        singles = units[128:] + units[:128]
        assert singles[:128] == list(range(128))
        assert singles[0xa1:0xe0] == list(range(0xff61, 0xffa0))
        assert all(singles[byte] == 0xfffd for byte in [*range(0x80, 0xa1), *range(0xe0, 256)])
        blob = b''.join(unit.to_bytes(2, 'big') for unit in singles)
        row_count = 0
        mappings[classname] = singles
    else:
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
        row_count = len(compact)
        mappings[classname] = pairs
    (output / filename).write_bytes(blob)
    mapping_rows.append((classname, path, hashlib.sha256(source[path]).hexdigest(), filename, str(len(blob)), hashlib.sha256(blob).hexdigest(), str(row_count)))


def pair(classname, first, second):
    if first < 0x80 or second < 0x80:
        return 0xfffd
    first, second = first - 0x80, second - 0x80
    rows = mappings[classname]
    return rows[first][second - 0x21] if rows[first] is not None and 0x21 <= second <= 0x7e else 0xfffd


def decode_record(variant, record):
    result, position = [], 0
    while position < len(record):
        first = record[position]
        if first < 0x80:
            result.append(first)
            position += 1
            continue
        width = 3 if first == 0x8f else 2
        if len(record) - position < width:
            result.append(0xfffd)
            break
        second = record[position + 1]
        if first == 0x8f:
            value = 0xfffd if variant == 'Linux' else pair('JIS_X_0212_Solaris' if variant == 'Solaris' else 'JIS_X_0212', second, record[position + 2])
        else:
            if first == 0x8e:
                value = mappings['JIS_X_0201'][second] if second >= 0x80 else 0xfffd
            else:
                value = pair('JIS_X_0208', first, second)
            if value == 0xfffd and variant == 'Solaris':
                value = pair('JIS_X_0208_Solaris', first, second)
        result.append(value)
        position += width
    return ''.join(chr(unit) for unit in result).encode()


rows = []
for classname, variant in [('EUC_JP', 'Standard'), ('EUC_JP_LINUX', 'Linux'), ('EUC_JP_Open', 'Solaris')]:
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
    single_probe = hashlib.sha256(b''.join(decode_record(variant, [byte]) for byte in range(256))).hexdigest()
    probes = []
    for prefix in [[], [0x8e], [0x8f]]:
        digest = hashlib.sha256()
        for first in range(256):
            for second in range(256):
                digest.update(decode_record(variant, [*prefix, first, second]))
                digest.update(b'\0')
        probes.append(digest.hexdigest())
    names = sorted({name.lower() for name in [canonical, *aliases]})
    for name in names:
        assert resolve(name) == (path.split('/')[0], classname), (name, resolve(name), classname)
    actual_names = sorted(name for name in set(standard_aliases) | set(standard_classes) | set(extended_aliases) | set(extended_classes) if resolve(name) == (path.split('/')[0], classname))
    assert names == actual_names
    rows.append((canonical, classname, path, hashlib.sha256(source[path]).hexdigest(), variant, single_probe, *probes, '|'.join(aliases), names))
manifest = 'canonical\tclass\tsource\tsource_sha256\tvariant\tsingle_utf8_sha256\tpair_probe_utf8_sha256\tss2_triple_utf8_sha256\tss3_triple_utf8_sha256\taliases\n'
manifest += ''.join('\t'.join(row[:-1]) + '\n' for row in rows)
(output / 'charsets.tsv').write_text(manifest)
(output / 'mappings.tsv').write_text('class\tsource\tsource_sha256\tfile\tbytes\tmapping_sha256\tlead_rows\n' + ''.join('\t'.join(row) + '\n' for row in mapping_rows))
aliases = sorted((name, row[4]) for row in rows for name in row[-1])
assert len(aliases) == len({name for name, _ in aliases}) == 14
(output / 'aliases.rs').write_text('use super::EucJp;\n\npub(super) const ALIASES: &[(&str, EucJp)] = &[\n' + ''.join(f'    ("{name}", EucJp::{variant}),\n' for name, variant in aliases) + '];\n')
(output / 'tables.rs').write_text('use super::Mapping;\n\npub(super) const JIS0201: &[u8; 512] = include_bytes!("jis_x_0201.bin");\n' + ''.join(f'pub(super) static {name}: Mapping = Mapping(include_bytes!("{classname.lower()}.bin"));\n' for name, classname in [('JIS0208', 'JIS_X_0208'), ('JIS0212', 'JIS_X_0212'), ('SOLARIS0208', 'JIS_X_0208_Solaris'), ('SOLARIS0212', 'JIS_X_0212_Solaris')]))
provenance_paths = [standard_path, extended_path, paths['EUC_JP'], paths['EUC_JP_LINUX'], paths['EUC_JP_Open'], paths['DoubleByte'], paths['SingleByte'], paths['CharsetMapping'], 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'java.base/java/nio/charset/CharsetDecoder.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
for path in provenance_paths:
    if path not in [item for members in headers.values() for item in members]:
        retain(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
print('names', len(aliases), 'charsets', len(rows), 'bytes', sum(int(row[4]) for row in mapping_rows))
for filename in ['aliases.rs', 'tables.rs', 'charsets.tsv', 'mappings.tsv', 'sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
for row in rows:
    print(row[0], 'single probe', row[5], 'pair probe', row[6], 'SS2 triple', row[7], 'SS3 triple', row[8])
