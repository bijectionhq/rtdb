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


direct_classes = ['SJIS', 'MS932', 'Big5', 'EUC_KR', 'EUC_CN', 'GBK', 'IBM1381', 'IBM942', 'IBM943', 'IBM948', 'IBM949', 'IBM950', 'Johab', 'PCK', 'MS936', 'MS949', 'MS950']
shared_classes = ['IBM942C', 'IBM943C', 'IBM949C']
class_paths = {Path(path).stem: path for path in source if Path(path).stem in direct_classes + shared_classes}
assert len(class_paths) == 20
mappings = {}
for classname in direct_classes:
    path = class_paths[classname]
    text = uncomment(source[path].decode())
    holder = text.split('public static class DecodeHolder', 1)[1].split('public static class EncodeHolder', 1)[0]
    singles = [ord(char) for char in ''.join(strings(re.search(r'static final String b2cSBStr\s*=\s*(.*?);', holder, flags=re.S).group(1)))]
    pairs = string_rows(re.search(r'static final String\[\] b2cStr\s*=\s*\{(.*?)\};', holder, flags=re.S).group(1))
    initialization = re.sub(r'\s+', ' ', holder[holder.index('static {'):]).strip()
    assert initialization == 'static { for (int i = 0; i < b2cStr.length; i++) { if (b2cStr[i] == null) b2c[i] = DoubleByte.B2C_UNMAPPABLE; else b2c[i] = b2cStr[i].toCharArray(); } b2cSB = b2cSBStr.toCharArray(); } }', path
    mappings[classname] = singles, pairs
for classname in shared_classes:
    text = uncomment(source[class_paths[classname]].decode())
    base = classname[:-1]
    singles, pairs = mappings[base]
    initialization = re.sub(r'\s+', ' ', text.split('static {', 1)[1].split('c2b =', 1)[0]).strip()
    if classname == 'IBM942C':
        expected = 'b2cSB = Arrays.copyOf(IBM942.DecodeHolder.b2cSB, IBM942.DecodeHolder.b2cSB.length); '
        expected += ' '.join(f'b2cSB[0x{byte:02x}] = 0x{byte:02x};' for byte in [0x1a, 0x1c, 0x5c, 0x7e, 0x7f])
        assert initialization == expected, classname
        singles = singles.copy()
        for byte in [0x1a, 0x1c, 0x5c, 0x7e, 0x7f]:
            singles[byte] = byte
    else:
        assert initialization == f'b2cSB = new char[0x100]; for (int i = 0; i < 0x80; i++) {{ b2cSB[i] = (char)i; }} for (int i = 0x80; i < 0x100; i++) {{ b2cSB[i] = {base}.DecodeHolder.b2cSB[i]; }}', classname
        singles = list(range(128)) + singles[128:]
    mappings[classname] = singles, pairs

rows, headers = [], {}
for classname in direct_classes + shared_classes:
    path = class_paths[classname]
    raw = source[path]
    original, text = raw.decode(), uncomment(raw.decode())
    canonical, constructor = re.search(r'super\("([^"]+)",\s*(.*?)\);', text).groups()
    provider = path.split('/')[0]
    if provider == 'java.base':
        assert constructor == f'StandardCharsets.aliases_{classname}()'
        aliases = alias_methods[classname]
    else:
        assert constructor == f'ExtendedCharsets.aliasesFor("{canonical}")'
        declared, aliases = extended_names[classname]
        assert declared == canonical
    decoder = re.sub(r'\s+', ' ', re.search(r'public CharsetDecoder newDecoder\(\)\s*\{(.*?)\}', text, flags=re.S).group(1)).strip()
    if classname in direct_classes:
        flags = re.fullmatch(r'return new DoubleByte.Decoder\(this, DecodeHolder.b2c, DecodeHolder.b2cSB, (0x[0-9a-f]+), (0x[0-9a-f]+), (true|false)\);', decoder)
        assert flags, path
        minimum, maximum = (int(value, 16) for value in flags.groups()[:2])
        ascii_flag = flags.group(3) == 'true'
    else:
        flags = re.fullmatch(r'return new DoubleByte.Decoder\(this, (IBM[0-9]+).DecodeHolder.b2c, Holder.b2cSB, (0x[0-9a-f]+), (0x[0-9a-f]+)\);', decoder)
        assert flags and flags.group(1) == classname[:-1], path
        minimum, maximum = (int(value, 16) for value in flags.groups()[1:])
        ascii_flag = False
    singles, pairs = mappings[classname]
    assert len(singles) == len(pairs) == 256
    assert not ascii_flag or singles[:128] == list(range(128))
    assert singles[0] == 0 and minimum > 0
    assert all(unit != 0xfffd for unit in singles[:128])
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
    names = sorted({name.lower() for name in [canonical, *aliases]})
    for name in names:
        assert resolve(name) == (provider, classname), (name, resolve(name), classname)
    actual_names = sorted(name for name in set(standard_aliases) | set(standard_classes) | set(extended_aliases) | set(extended_classes) if resolve(name) == (provider, classname))
    assert names == actual_names
    header = original[:original.index('*/') + 2]
    headers.setdefault(header, []).append(path)
    rows.append((canonical, classname, path, hashlib.sha256(raw).hexdigest(), str(ascii_flag).lower(), f'{minimum:02x}', f'{maximum:02x}', str(len(compact)), str(len(blob)), hashlib.sha256(blob).hexdigest(), single_digest, pair_probe.hexdigest(), '|'.join(aliases), filename, names))
rows.sort(key=lambda row: row[0].lower())
manifest = 'canonical\tclass\tsource\tsource_sha256\tascii\ttrail_min\ttrail_max\tlead_rows\tbytes\tmapping_sha256\tsingle_utf8_sha256\tpair_probe_utf8_sha256\taliases\n'
manifest += ''.join('\t'.join(row[:-2]) + '\n' for row in rows)
(output / 'charsets.tsv').write_text(manifest)
aliases = sorted((name, index) for index, row in enumerate(rows) for name in row[-1])
assert len(aliases) == len({name for name, _ in aliases}) == 100
assert len(rows) == 20
(output / 'aliases.rs').write_text('pub(super) const ALIASES: &[(&str, usize)] = &[\n' + ''.join(f'    ("{name}", {index}),\n' for name, index in aliases) + '];\n')
(output / 'tables.rs').write_text('use super::Mapping;\n\npub(super) static TABLES: [Mapping; 20] = [\n' + ''.join(f'    Mapping(include_bytes!("{row[-2]}")),\n' for row in rows) + '];\n')
provenance_paths = [standard_path, extended_path, 'java.base/sun/nio/cs/DoubleByte.java', 'java.base/sun/nio/cs/ArrayDecoder.java', 'java.base/sun/nio/cs/CharsetMapping.java', 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
for path in provenance_paths:
    original = source[path].decode()
    header = original[:original.index('*/') + 2]
    headers.setdefault(header, []).append(path)
solaris_path = 'jdk.charsets/sun/nio/cs/ext/Big5_Solaris.java'
solaris_original = source[solaris_path].decode()
solaris = uncomment(solaris_original)
assert re.search(r'public CharsetDecoder newDecoder\(\)\s*\{\s*return new DoubleByte.Decoder\(this, Holder.b2c, Holder.b2cSB, 0x40, 0xfe, true\);\s*\}', solaris)
assert re.search(r'public CharsetEncoder newEncoder\(\)\s*\{\s*return new DoubleByte.Encoder\(this, Holder.c2b, Holder.c2bIndex, true\);\s*\}', solaris)
initialization = re.sub(r'\s+', ' ', solaris.split('static {', 1)[1].split('c2b =', 1)[0]).strip()
expected = 'b2c = Big5.DecodeHolder.b2c.clone(); int[] sol = new int[] { 0xF9D6, 0x7881, 0xF9D7, 0x92B9, 0xF9D8, 0x88CF, 0xF9D9, 0x58BB, 0xF9DA, 0x6052, 0xF9DB, 0x7CA7, 0xF9DC, 0x5AFA }; if (b2c[0xf9] == DoubleByte.B2C_UNMAPPABLE) { b2c[0xf9] = new char[0xfe - 0x40 + 1]; Arrays.fill(b2c[0xf9], UNMAPPABLE_DECODING); } for (int i = 0; i < sol.length;) { b2c[0xf9][sol[i++] & 0xff - 0x40] = (char)sol[i++]; } b2cSB = Big5.DecodeHolder.b2cSB;'
assert initialization == expected
patch_array = re.search(r'int\[\] sol = new int\[\] \{(.*?)\};', solaris, re.S).group(1)
patch_numbers = [int(value, 16) for value in re.findall(r'0x[0-9A-Fa-f]+', patch_array)]
points, replacements = patch_numbers[::2], patch_numbers[1::2]
assert points == list(range(0xf9d6, 0xf9dd))
singles, pairs = mappings['Big5']
assert pairs[0xf9] is not None
base_units = [pairs[0xf9][point & (0xff - 0x40)] for point in points]
assert base_units == [0xfffd] * 7
assert all(pairs[point & 255] is not None for point in points)
assert all(not 0xd800 <= unit <= 0xdfff for unit in replacements)
solaris_canonical, solaris_aliases = extended_names['Big5_Solaris']
solaris_names = sorted({name.lower() for name in [solaris_canonical, *solaris_aliases]})
assert solaris_names == ['big5_solaris', 'x-big5-solaris']
for name in solaris_names:
    assert resolve(name) == ('jdk.charsets', 'Big5_Solaris')
(output / 'big5').mkdir(exist_ok=True)
patches = 'pub(super) const LEAD: u8 = 0xf9;\npub(super) const FIRST: u8 = 0xd6;\npub(super) const LAST: u8 = 0xdc;\n'
patches += 'pub(super) const BASE_UNIT: u16 = 0xfffd;\n'
patches += 'pub(super) const SOLARIS_UNITS: [u16; 7] = [' + ', '.join(f'0x{unit:04x}' for unit in replacements) + '];\n'
(output / 'big5' / 'patches.rs').write_text(patches)
patched_pairs = pairs.copy()
patched_pairs[0xf9] = pairs[0xf9].copy()
for index, point in enumerate(points):
    patched_pairs[0xf9][(point & 255) - 0x40] = replacements[index]
post_activation_probe = hashlib.sha256()
for first in range(256):
    for second in range(256):
        if singles[first] != 0xfffd:
            units = [singles[first], singles[second]]
        else:
            mapped = patched_pairs[first][second - 0x40] if patched_pairs[first] is not None and 0x40 <= second <= 0xfe else 0xfffd
            if mapped != 0xfffd:
                units = [mapped]
            elif patched_pairs[first] is None or patched_pairs[second] is not None or singles[second] != 0xfffd:
                units = [0xfffd, singles[second]]
            else:
                units = [0xfffd]
        post_activation_probe.update(''.join(chr(unit) for unit in [*units, 0]).encode('utf-8'))
solaris_manifest = 'canonical\tclass\tsource\tsource_sha256\tbase_source\tbase_source_sha256\tlead\tfirst\tlast\tbase_unit\tsolaris_units\taliases\tpost_activation_pair_probe_utf8_sha256\n'
solaris_manifest += '\t'.join([solaris_canonical, 'Big5_Solaris', solaris_path, hashlib.sha256(source[solaris_path]).hexdigest(), class_paths['Big5'], hashlib.sha256(source[class_paths['Big5']]).hexdigest(), 'f9', 'd6', 'dc', 'fffd', '|'.join(f'{unit:04x}' for unit in replacements), '|'.join(solaris_aliases), post_activation_probe.hexdigest()]) + '\n'
(output / 'big5-solaris.tsv').write_text(solaris_manifest)
solaris_sources = [solaris_path, class_paths['Big5'], 'java.base/sun/nio/cs/DoubleByte.java', 'java.base/java/lang/String.java', extended_path, 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java', 'java.base/sun/nio/cs/Big5_HKSCS.java', 'jdk.charsets/sun/nio/cs/ext/Big5_HKSCS_2001.java', 'java.base/sun/nio/cs/HKSCS.java']
(output / 'big5-solaris.sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in solaris_sources))
headers.setdefault(solaris_original[:solaris_original.index('*/') + 2], []).append(solaris_path)

(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
print('names', len(aliases), 'tables', len(rows), 'bytes', sum(int(row[8]) for row in rows))
for row in rows:
    print(row[0], 'lead rows', row[7], 'bytes', row[8], 'sha256', row[9], 'pair probe', row[11])
for filename in ['aliases.rs', 'tables.rs', 'charsets.tsv', 'sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())

for filename in ['big5/patches.rs', 'big5-solaris.tsv', 'big5-solaris.sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
