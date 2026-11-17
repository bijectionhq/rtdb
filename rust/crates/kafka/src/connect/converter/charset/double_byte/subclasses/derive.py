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


classes = ['JIS_X_0208', 'JIS_X_0212', 'IBM300', 'IBM834', 'IBM1383', 'IBM970']
paths = {Path(path).stem: path for path in source}
headers = {}


def retain(path):
    original = source[path].decode()
    header = original[:original.index('*/') + 2]
    headers.setdefault(header, []).append(path)
    return uncomment(original)


double_byte = retain(paths['DoubleByte'])
dbcs = double_byte.split('public static class Decoder_DBCSONLY extends Decoder {', 1)[1].split('public static class Decoder_EUC_SIM', 1)[0]
assert 'int decode(' not in dbcs
assert 'Arrays.fill(b2cSB_UNMAPPABLE, UNMAPPABLE_DECODING);' in dbcs
assert re.search(r'crMalformedOrUnmappable\(int b1, int b2\)\s*\{\s*return CoderResult.unmappableForLength\(2\);\s*\}', dbcs)
assert 'super(cs, 0.5f, 1.0f, b2c, b2cSB_UNMAPPABLE, b2Min, b2Max, isASCIICompatible);' in dbcs
assert 'super(cs, 0.5f, 1.0f, b2c, b2cSB_UNMAPPABLE, b2Min, b2Max, false);' in dbcs
euc = double_byte.split('public static class Decoder_EUC_SIM extends Decoder {', 1)[1].split('public static class Encoder', 1)[0]
assert 'private static final int SS2 = 0x8E;' in euc and 'private static final int SS3 = 0x8F;' in euc
assert re.search(r'if \(b1 == SS2 \|\| b1 == SS3\)\s*\{\s*sp--;\s*\}\s*c = repl;', euc)

rows = []
for classname in classes:
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
    decoder = re.sub(r'\s+', ' ', re.search(r'public CharsetDecoder newDecoder\(\)\s*\{(.*?)\}', text, re.S).group(1)).strip()
    if classname == 'IBM834':
        assert decoder == 'return new DoubleByte.Decoder_DBCSONLY( this, IBM933.DecodeHolder.b2c, null, 0x40, 0xfe);'
        table_source = paths['IBM933']
        table_text = retain(table_source)
        kind, minimum, maximum, ascii_flag = 'DbcsOnly', 0x40, 0xfe, False
    else:
        flags = re.fullmatch(r'return new DoubleByte.Decoder_(DBCSONLY|EUC_SIM)\(this, DecodeHolder.b2c, DecodeHolder.b2cSB, (0x[0-9a-f]+), (0x[0-9a-f]+), (true|false)\);', decoder)
        assert flags, path
        kind = 'DbcsOnly' if flags.group(1) == 'DBCSONLY' else 'EucSimple'
        minimum, maximum = (int(value, 16) for value in flags.groups()[1:3])
        ascii_flag = flags.group(4) == 'true'
        table_source, table_text = path, text
    holder = table_text.split('public static class DecodeHolder', 1)[1].split('public static class EncodeHolder', 1)[0]
    singles = [ord(char) for char in ''.join(strings(re.search(r'String b2cSBStr\s*=\s*(.*?);', holder, re.S).group(1)))]
    pairs = string_rows(re.search(r'String\[\] b2cStr\s*=\s*\{(.*?)\};', holder, re.S).group(1))
    initialization = re.sub(r'\s+', ' ', holder[holder.index('static {'):]).strip()
    assert initialization == 'static { for (int i = 0; i < b2cStr.length; i++) { if (b2cStr[i] == null) b2c[i] = DoubleByte.B2C_UNMAPPABLE; else b2c[i] = b2cStr[i].toCharArray(); } b2cSB = b2cSBStr.toCharArray(); } }'
    assert len(singles) == len(pairs) == 256
    assert all(row is None or len(row) == maximum - minimum + 1 for row in pairs)
    assert all(not 0xd800 <= unit <= 0xdfff for row in [singles, *[row for row in pairs if row is not None]] for unit in row)
    if kind == 'DbcsOnly':
        assert not ascii_flag
        effective_singles = [0xfffd] * 256
    else:
        assert ascii_flag and singles[:128] == list(range(128))
        assert singles == [byte if byte < 0xa0 and byte not in [0x8e, 0x8f] else 0xfffd for byte in range(256)]
        assert pairs[0x8e] is None and pairs[0x8f] is None
        effective_singles = singles
    offsets, compact = [], []
    for row in pairs:
        offsets.append(0xffff if row is None else len(compact))
        if row is not None:
            compact.append(row)
    assert len(compact) < 0xffff
    blob = bytes([minimum, maximum]) + b''.join(unit.to_bytes(2, 'big') for unit in singles + offsets + [unit for row in compact for unit in row])
    filename = canonical.lower() + '.bin'
    (output / filename).write_bytes(blob)
    single_probe = hashlib.sha256(''.join(chr(unit) for unit in effective_singles).encode()).hexdigest()
    pair_probe = hashlib.sha256()
    for first in range(256):
        for second in range(256):
            if effective_singles[first] != 0xfffd:
                units = [effective_singles[first], effective_singles[second]]
            else:
                unit = pairs[first][second - minimum] if pairs[first] is not None and minimum <= second <= maximum else 0xfffd
                units = [unit]
                if kind == 'EucSimple' and unit == 0xfffd and first in [0x8e, 0x8f]:
                    units.append(effective_singles[second])
            pair_probe.update(''.join(chr(unit) for unit in [*units, 0]).encode())
    names = sorted({name.lower() for name in [canonical, *aliases]})
    for name in names:
        assert resolve(name) == (provider, classname), (name, resolve(name), classname)
    actual_names = sorted(name for name in set(standard_aliases) | set(standard_classes) | set(extended_aliases) | set(extended_classes) if resolve(name) == (provider, classname))
    assert names == actual_names
    rows.append((canonical, classname, path, hashlib.sha256(source[path]).hexdigest(), kind, table_source, hashlib.sha256(source[table_source]).hexdigest(), str(ascii_flag).lower(), f'{minimum:02x}', f'{maximum:02x}', str(len(compact)), str(len(blob)), hashlib.sha256(blob).hexdigest(), single_probe, pair_probe.hexdigest(), '|'.join(aliases), filename, names))
rows.sort(key=lambda row: row[0].lower())
manifest = 'canonical\tclass\tsource\tsource_sha256\tdecoder\ttable_source\ttable_source_sha256\tascii\ttrail_min\ttrail_max\tlead_rows\tbytes\tmapping_sha256\tsingle_utf8_sha256\tpair_probe_utf8_sha256\taliases\n'
manifest += ''.join('\t'.join(row[:-2]) + '\n' for row in rows)
(output / 'charsets.tsv').write_text(manifest)
aliases = sorted((name, index) for index, row in enumerate(rows) for name in row[-1])
assert len(aliases) == len({name for name, _ in aliases}) == 36
(output / 'aliases.rs').write_text('pub(super) const ALIASES: &[(&str, usize)] = &[\n' + ''.join(f'    ("{name}", {index}),\n' for name, index in aliases) + '];\n')
tables = 'use super::{Decoder, Mapping};\n\npub(super) static MAPPINGS: &[Mapping] = &[\n'
tables += ''.join(f'    Mapping(include_bytes!("{row[-2]}")),\n' for row in rows) + '];\n\npub(super) static DECODERS: &[Decoder] = &[\n'
tables += ''.join(f'    Decoder::{row[4]}(&MAPPINGS[{index}]),\n' for index, row in enumerate(rows))
(output / 'tables.rs').write_text(tables + '];\n')
provenance_paths = [standard_path, extended_path, paths['DoubleByte'], paths['ArrayDecoder'], paths['CharsetMapping'], 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
for path in provenance_paths:
    if path != paths['DoubleByte']:
        retain(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
print('names', len(aliases), 'charsets', len(rows), 'bytes', sum(int(row[11]) for row in rows))
for filename in ['aliases.rs', 'tables.rs', 'charsets.tsv', 'sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
for row in rows:
    print(row[0], row[4], 'rows', row[10], 'single probe', row[13], 'pair probe', row[14])
