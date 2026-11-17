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


with zipfile.ZipFile(io.BytesIO(source_bytes)) as source_zip:
    source = {name: source_zip.read(name) for name in source_zip.namelist() if name.endswith('.java') and ('/sun/nio/cs/' in name or name in ['java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'java.base/java/nio/charset/CharsetDecoder.java'])}
standard_path = 'java.base/sun/nio/cs/StandardCharsets.java'
extended_path = 'jdk.charsets/sun/nio/cs/ext/ExtendedCharsets.java'
standard, extended = (uncomment(source[path].decode()) for path in [standard_path, extended_path])
standard_aliases = dict(re.findall(r'new Object\[\]\s*\{\s*"([^"]+)",\s*"([^"]+)"', standard.split('class Aliases', 1)[1].split('class Classes', 1)[0]))
standard_classes = dict(re.findall(r'new Object\[\]\s*\{\s*"([^"]+)",\s*"([^"]+)"', standard.split('class Classes', 1)[1].split('class Cache', 1)[0]))
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


path = 'jdk.charsets/sun/nio/cs/ext/ISCII91.java'
text = uncomment(source[path].decode())
expression = re.search(r'char\[\] directMapTable\s*=\s*\{(.*?)\};', text, re.S).group(1)
units = []
for part in expression.split(','):
    part = part.strip()
    if not part:
        continue
    if part.startswith('(char)'):
        unit = int(part[6:], 16)
    else:
        assert part[0] == part[-1] == "'", part
        literal = part[1:-1]
        if literal.startswith('\\u'):
            unit = int(literal[2:], 16)
        elif literal.startswith('\\') and literal[1:].isdigit():
            unit = int(literal[1:], 8)
        elif literal.startswith('\\'):
            unit = ord({'\\': '\\', "'": "'", 'n': '\n', 'r': '\r', 't': '\t', 'b': '\b', 'f': '\f'}[literal[1:]])
        else:
            unit = ord(literal)
    units.append(unit)
assert len(units) == 255 and units[:128] == list(range(128))
assert all(not 0xd800 <= unit <= 0xdfff for unit in units)
blob = b''.join(unit.to_bytes(2, 'big') for unit in units)
(output / 'direct-map.bin').write_bytes(blob)
decoder = text.split('private static class Decoder extends CharsetDecoder', 1)[1].split('private static class Encoder', 1)[0]
assert 'ArrayDecoder' not in decoder
assert decoder.count('index = ( index < 0 )? ( index + 255 ):index;') == 2
assert 'private char contextChar = INVALID_CHAR;' in decoder
assert 'private boolean needFlushing = false;' in decoder
assert 'out.put(contextChar);' in decoder
assert "if(contextChar == '\\ufffd')" in decoder
assert 'return CoderResult.unmappableForLength(1);' in decoder
assert 'da[dp++] = ZWNJ_CHAR;' in decoder and 'da[dp++] = ZWJ_CHAR;' in decoder
assert 'contextChar = currentChar;\n                            sp++;\n                            continue;' in decoder
string = uncomment(source['java.base/java/lang/String.java'].decode())
assert 'cd.decode(bb, cb, true)' in string and 'cd.flush(cb)' in string
assert 'cd.onMalformedInput(CodingErrorAction.REPLACE)' in string
assert '.onUnmappableCharacter(CodingErrorAction.REPLACE);' in string
charset_decoder = uncomment(source['java.base/java/nio/charset/CharsetDecoder.java'].decode())
assert 'in.position(in.position() + cr.length());' in charset_decoder

compositions = {0x0901: 0x0950, 0x0907: 0x090c, 0x0908: 0x0961, 0x090b: 0x0960, 0x093f: 0x0962, 0x0940: 0x0963, 0x0943: 0x0944, 0x0964: 0x093d}
for base, composed in compositions.items():
    assert re.search(r"case '\\u" + f'{base:04x}' + r"':\s*da\[dp\+\+\] = '\\u" + f'{composed:04x}' + "';", decoder)


def decode_record(record):
    result, context, pending = [], 0xffff, False
    for byte in record:
        current = units[byte if byte < 128 else byte - 1]
        if context == 0xfffd:
            result.append(0xfffd)
            context, pending = 0xffff, False
            continue
        if current in compositions:
            if pending:
                result.append(context)
            context, pending = current, True
            continue
        if current == 0x093c:
            if context in compositions:
                result.append(compositions[context])
            elif pending:
                result.append(context)
                context = current
                continue
            else:
                result.append(0x200d if context == 0x094d else current)
        elif current == 0x094d:
            if pending:
                result.append(context)
                context = current
                continue
            result.append(0x200c if context == 0x094d else current)
        elif current == 0xffff:
            if pending:
                result.append(context)
                context = current
                continue
            result.append(0xfffd)
            continue
        else:
            if pending:
                result.append(context)
                context = current
                continue
            result.append(current)
        context, pending = current, False
    if pending:
        result.append(context)
    assert len(result) <= len(record)
    return ''.join(chr(unit) for unit in result).encode()


prefixes = [('empty', b''), ('halant-emitted', b'\xe8'), ('nukta-emitted', b'\xe9'), ('atr-emitted', b'\xef'), ('ext-emitted', b'\xf0')]
prefixes += [(f'base-{base:04x}-pending', bytes([byte])) for byte in range(256) if (base := units[byte if byte < 128 else byte - 1]) in compositions]
prefixes += [('ascii-pending', b'\xa1A'), ('halant-pending', b'\xa1\xe8'), ('nukta-pending', b'\xa1\xe9'), ('invalid-pending', b'\xa1\x81'), ('atr-pending', b'\xa1\xef'), ('ext-pending', b'\xa1\xf0'), ('halant-after-error', b'\xe8\x81')]
assert len(prefixes) == 20
context_rows = []
for name, prefix in prefixes:
    singles = hashlib.sha256(b''.join(decode_record(prefix + bytes([byte])) for byte in range(256))).hexdigest()
    digest = hashlib.sha256()
    for first in range(256):
        for second in range(256):
            digest.update(decode_record(prefix + bytes([first, second])))
            digest.update(b'\0')
    context_rows.append((name, prefix.hex(), singles, digest.hexdigest()))

canonical, aliases = extended_names['ISCII91']
assert canonical == 'x-ISCII91'
names = sorted({name.lower() for name in [canonical, *aliases]})
assert len(names) == 6
for name in names:
    assert resolve(name) == ('jdk.charsets', 'ISCII91')
actual_names = sorted(name for name in set(standard_aliases) | set(standard_classes) | set(extended_aliases) | set(extended_classes) if resolve(name) == ('jdk.charsets', 'ISCII91'))
assert names == actual_names
(output / 'aliases.rs').write_text('pub(super) const ALIASES: &[&str] = &[\n' + ''.join(f'    "{name}",\n' for name in names) + '];\n')
(output / 'charsets.tsv').write_text('canonical\tclass\tsource\tsource_sha256\taliases\n' + '\t'.join((canonical, 'ISCII91', path, hashlib.sha256(source[path]).hexdigest(), '|'.join(aliases))) + '\n')
(output / 'contexts.tsv').write_text('context\tprefix_hex\tsingle_utf8_sha256\tpair_probe_utf8_sha256\n' + ''.join('\t'.join(row) + '\n' for row in context_rows))
(output / 'mappings.tsv').write_text('file\tsource\tsource_sha256\tbytes\tmapping_sha256\tunits\n' + '\t'.join(('direct-map.bin', path, hashlib.sha256(source[path]).hexdigest(), str(len(blob)), hashlib.sha256(blob).hexdigest(), str(len(units)))) + '\n')
provenance_paths = [standard_path, extended_path, path, 'java.base/java/lang/String.java', 'java.base/java/nio/charset/Charset.java', 'java.base/java/nio/charset/CharsetDecoder.java', 'jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java']
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in provenance_paths))
headers = {}
for path in provenance_paths:
    original = source[path].decode()
    header = original[:original.index('*/') + 2]
    headers.setdefault(header, []).append(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(paths) + '\n\n' + header for header, paths in headers.items()) + '\n')
print('names', len(names), 'mapping bytes', len(blob), 'contexts', len(context_rows))
for filename in ['aliases.rs', 'charsets.tsv', 'contexts.tsv', 'mappings.tsv', 'sources.sha256']:
    print(filename, hashlib.sha256((output / filename).read_bytes()).hexdigest())
