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


def strings(expression):
    literals = re.findall(r'"((?:[^"\\]|\\.)*)"', expression)
    result = []
    escapes = {'b': '\b', 't': '\t', 'n': '\n', 'f': '\f', 'r': '\r', '"': '"', "'": "'", '\\': '\\'}
    for literal in literals:
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


def uncomment(source):
    return re.sub(r'/\*.*?\*/|//[^\n]*', '', source, flags=re.S)


with zipfile.ZipFile(io.BytesIO(source_bytes)) as zip:
    source = {name: zip.read(name) for name in zip.namelist() if name.endswith('.java') and ('/sun/nio/cs/' in name or name in ['java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java'])}

standard_path = 'java.base/sun/nio/cs/StandardCharsets.java'
extended_path = 'jdk.charsets/sun/nio/cs/ext/ExtendedCharsets.java'
standard, extended = (uncomment(source[path].decode()) for path in [standard_path, extended_path])
standard_aliases = dict(re.findall(r'new Object\[\]\s*\{\s*"([^"]+)",\s*"([^"]+)"', standard.split('class Aliases', 1)[1].split('class Classes', 1)[0]))
standard_classes = dict(re.findall(r'new Object\[\]\s*\{\s*"([^"]+)",\s*"([^"]+)"', standard.split('class Classes', 1)[1].split('class Cache', 1)[0]))
alias_methods = {name: strings(body) for name, body in re.findall(r'static String\[\] aliases_(\w+)\(\)\s*\{\s*return new String\[\]\s*\{(.*?)\}', standard, flags=re.S)}
extended_classes, extended_aliases, extended_names = {}, {}, {}
for canonical, classname, body in re.findall(r'charset\("([^"]+)",\s*"([^"]+)",\s*new String\[\]\s*\{(.*?)\}\);', extended, flags=re.S):
    aliases = strings(body)
    extended_classes.setdefault(canonical.lower(), classname)
    extended_names[classname] = (canonical, aliases)
    for alias in aliases:
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
for path, raw in sorted(source.items()):
    original = raw.decode()
    if 'new SingleByte.Decoder' not in original:
        continue
    text = uncomment(original)
    classname = Path(path).stem
    canonical, constructor = re.search(r'super\("([^"]+)",\s*(.*?)\);', text).groups()
    decoder = re.search(r'public CharsetDecoder newDecoder\(\)\s*\{(.*?)\}', text, flags=re.S).group(1).strip()
    flags = re.fullmatch(r'return new SingleByte.Decoder\(this, Holder.b2c, (true|false), (true|false)\);', decoder)
    assert flags, path
    ascii_flag, latin1_flag = (flag == 'true' for flag in flags.groups())
    assert re.search(r'private static final char\[\] b2c = b2cTable.toCharArray\(\);', text), path
    assert not re.search(r'(?<!\w)b2c\s*\[', text), path
    mutation = re.search(r'b2cMap\[', text)
    if mutation:
        clone = text.index('b2cMap = b2cTable.toCharArray();')
        assert clone < mutation.start(), path
    expression = re.search(r'private static final String b2cTable\s*=\s*(.*?);', text, flags=re.S).group(1)
    units = [ord(char) for char in ''.join(strings(expression))]
    assert len(units) == 256, path
    unsigned = units[128:] + units[:128]
    assert not any(0xd800 <= unit <= 0xdfff for unit in unsigned), path
    assert not ascii_flag or unsigned[:128] == list(range(128)), path
    assert not latin1_flag or max(unsigned) <= 255, path
    provider = path.split('/')[0]
    if provider == 'java.base':
        assert constructor == f'StandardCharsets.aliases_{classname}()'
        aliases = alias_methods[classname]
        assert standard_classes[canonical.lower()] == classname
    else:
        assert constructor == f'ExtendedCharsets.aliasesFor("{canonical}")'
        declared, aliases = extended_names[classname]
        assert declared == canonical
    for name in [canonical, *aliases]:
        assert resolve(name) == (provider, classname), (name, resolve(name), classname)
    names = sorted({name.lower() for name in [canonical, *aliases]})
    actual_names = sorted(name for name in set(standard_aliases) | set(standard_classes) | set(extended_aliases) | set(extended_classes) if resolve(name) == (provider, classname))
    assert names == actual_names, (classname, names, actual_names)
    mapping = b''.join(unit.to_bytes(2, 'big') for unit in unsigned)
    decoded = ''.join(chr(unit) for unit in unsigned).encode('utf-8')
    header = original[:original.index('*/') + 2]
    headers.setdefault(header, []).append(path)
    rows.append((canonical, classname, path, hashlib.sha256(raw).hexdigest(), str(ascii_flag).lower(), str(latin1_flag).lower(), hashlib.sha256(mapping).hexdigest(), hashlib.sha256(decoded).hexdigest(), '|'.join(aliases), mapping, names))
rows.sort(key=lambda row: row[0].lower())
assert len(rows) == 102
blob = b''.join(row[-2] for row in rows)
(output / 'java25-single-byte.bin').write_bytes(blob)
manifest = 'canonical\tclass\tsource\tsource_sha256\tascii\tlatin1\tmapping_sha256\tdecoded_utf8_sha256\taliases\n'
manifest += ''.join('\t'.join(row[:-2]) + '\n' for row in rows)
(output / 'charsets.tsv').write_text(manifest)
aliases = sorted((name, index) for index, row in enumerate(rows) for name in row[-1])
assert len({name for name, _ in aliases}) == len(aliases)
(output / 'aliases.rs').write_text('pub(super) const ALIASES: &[(&str, usize)] = &[\n' + ''.join(f'    ("{name}", {index}),\n' for name, index in aliases) + '];\n')
provenance_paths = [standard_path, extended_path, 'java.base/sun/nio/cs/SingleByte.java', 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
for path in provenance_paths:
    original = source[path].decode()
    header = original[:original.index('*/') + 2]
    headers.setdefault(header, []).append(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
print('charsets',len(rows),'names',len(aliases),'bytes',len(blob),'sha256',hashlib.sha256(blob).hexdigest())
for filename in ['aliases.rs','charsets.tsv','sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
print('ASCII compatible',sum(row[4]=='true' for row in rows),'Latin1 decodable',sum(row[5]=='true' for row in rows))
