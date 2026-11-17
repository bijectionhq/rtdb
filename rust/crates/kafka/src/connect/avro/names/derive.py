import hashlib
import io
import json
import re
import sys
import zipfile
from pathlib import Path

SOURCE_ZIP_SHA256 = '3ed5aadae786d9e35101a67e94818c4590f5b8a81e7ee44ad4ce4add58881482'
AVRO_JAR_SHA256 = '0151e4f35e319f3291b7defc90e879109ad4a4b90c6a465e06617f7f2dc436b7'
EXPECTED_TABLE_SHA256 = 'ae0fd805864b967c6563eda7fdd00287e1c8bbc66bcc88096178af04e9420f5c'
SOURCES = {
    'java.base/java/lang/Character.java': 'bd864040a42498f6c15cfc61786570924e75072c61e576847c57434ded10ac52',
    'java.base/java/lang/CharacterData.java': '04ee9000969ba7e09b62f1b33296aab1ad01fd33318ba5618be7bf9fadf0e55c',
    'java.base/java/lang/CharacterData00.java': '7ca89f8177f7df516091a495de1e53105c085cb487ecf083561158f4a8dff941',
    'java.base/java/lang/CharacterDataLatin1.java': 'd34c8903d1caa4befafacd761494f927c7eb453d4cd01e727b64a5f487b0f047',
    'apache-avro-1.11.4/org/apache/avro/Schema.java': 'eb45c1650db37862bf6b663663cf00f5632e4a5dc72f39382fb2b92acfefd08c',
    'apache-avro-1.11.4/META-INF/LICENSE': 'cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30',
    'apache-avro-1.11.4/META-INF/NOTICE': 'dbb4103a03e368193b797525f71369b36da0bc27514870fa34c9b5a49107fc75',
    'unicode-16.0.0/UnicodeData.txt': 'ff58e5823bd095166564a006e47d111130813dcf8bf234ef79fa51a870edb48f',
    'unicode-16.0.0/LICENSE': 'f5062c9a188d81dfe66b56db4182dcf9e4b17c0d9b0d311a8e20b3a1b075c443',
    'legal/java.base/LICENSE': '4b9abebc4338048a7c2dc184e9f800deb349366bdf28eb23c2677a77b4c87726',
    'legal/java.base/ASSEMBLY_EXCEPTION': '75292f03bf23d3db7c985aecc191029b93883200721ed23ed34a2e601463df33',
    'legal/java.base/ADDITIONAL_LICENSE_INFO': 'a69bce275ba7a3570af6579cb0f55682cd75fedfcd49e0e8e9022270c447c916',
}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def method(source, signature):
    start = source.index(signature)
    start = source.index('{', start) + 1
    depth = 1
    for end in range(start, len(source)):
        depth += (source[end] == '{') - (source[end] == '}')
        if depth == 0:
            return re.sub(r'\s+', '', source[start:end])
    raise ValueError(signature)


def string_units(expression):
    units = []
    for literal in re.findall(r'"((?:[^"\\]|\\.)*)"', expression):
        offset = 0
        while offset < len(literal):
            if literal[offset] != '\\':
                units.append(ord(literal[offset]))
                offset += 1
                continue
            offset += 1
            escape = literal[offset]
            offset += 1
            if escape == 'u':
                units.append(int(literal[offset:offset + 4], 16))
                offset += 4
            elif escape in '01234567':
                start = offset - 1
                maximum = 3 if escape < '4' else 2
                while offset < len(literal) and offset - start < maximum and literal[offset] in '01234567':
                    offset += 1
                units.append(int(literal[start:offset], 8))
            else:
                units.append(ord({'n': '\n', 'r': '\r', 't': '\t', 'b': '\b', 'f': '\f', '\\': '\\', '"': '"', "'": "'"}[escape]))
    assert all(0 <= unit <= 65535 for unit in units)
    return units


def plane_zero_types(source):
    assert method(source, 'int getProperties(int ch)') == 'charoffset=(char)ch;intprops=A[Y[X[offset>>5]|((offset>>1)&0xF)]|(offset&0x1)];returnprops;'
    assert method(source, 'int getType(int ch)') == 'intprops=getProperties(ch);return(props&0x1F);'
    x = string_units(re.search(r'char X\[\] = \((.*?)\)\.toCharArray', source, re.S)[1])
    y = string_units(re.search(r'char Y\[\] = \((.*?)\)\.toCharArray', source, re.S)[1])
    encoded = string_units(re.search(r'String A_DATA =\s*(.*?);', source, re.S)[1])
    a = [(encoded[index] << 16) | encoded[index + 1] for index in range(0, len(encoded), 2)]
    assert len(x) == 2048 and len(encoded) == 2112 and len(a) == 1056
    return [a[y[x[unit >> 5] | ((unit >> 1) & 15)] | (unit & 1)] & 31 for unit in range(65536)]


def latin1_types(source):
    assert method(source, 'int getProperties(int ch)') == 'charoffset=(char)ch;intprops=A[offset];returnprops;'
    assert method(source, 'int getType(int ch)') == 'intprops=getProperties(ch);return(props&0x1F);'
    expression = re.search(r'int A\[\] = \{(.*?)\};', source, re.S)[1]
    expression = re.sub(r'//[^\n]*', '', expression)
    values = [int(value, 16) & 0xffffffff for value in re.findall(r'-?0x[0-9a-fA-F]+', expression)]
    assert len(values) == 256
    return [value & 31 for value in values]


def unicode_expectations(source):
    flags = bytearray(65536)
    pending = None
    for line in source.splitlines():
        fields = line.split(';')
        assert len(fields) == 15
        unit = int(fields[0], 16)
        category = fields[2]
        if fields[1].endswith(', First>'):
            assert pending is None
            pending = (unit, category)
            continue
        if fields[1].endswith(', Last>'):
            assert pending is not None and pending[1] == category
            start = pending[0]
            pending = None
        else:
            assert pending is None
            start = unit
        if start <= 65535:
            mask = 3 if category in {'Lu', 'Ll', 'Lt', 'Lm', 'Lo'} else 2 if category == 'Nd' else 0
            for code_unit in range(start, min(unit, 65535) + 1):
                flags[code_unit] = mask
    assert pending is None
    flags[95] = 3
    return flags


source_zip, avro_jar, unicode_data, jdk_legal, unicode_license, output = map(Path, sys.argv[1:])
zip_bytes = source_zip.read_bytes()
jar_bytes = avro_jar.read_bytes()
assert sha256(zip_bytes) == SOURCE_ZIP_SHA256
assert sha256(jar_bytes) == AVRO_JAR_SHA256
source = {}
with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
    for path in SOURCES:
        if path.startswith('java.base/'):
            source[path] = archive.read(path)
with zipfile.ZipFile(io.BytesIO(jar_bytes)) as archive:
    for path in SOURCES:
        if path.startswith('apache-avro-1.11.4/'):
            source[path] = archive.read(path.removeprefix('apache-avro-1.11.4/'))
source['unicode-16.0.0/UnicodeData.txt'] = unicode_data.read_bytes()
source['unicode-16.0.0/LICENSE'] = unicode_license.read_bytes()
for path in SOURCES:
    if path.startswith('legal/java.base/'):
        source[path] = (jdk_legal / Path(path).name).read_bytes()
assert set(source) == set(SOURCES)
for path, digest in SOURCES.items():
    assert sha256(source[path]) == digest, path

character = source['java.base/java/lang/Character.java'].decode()
assert 'Unicode Standard, version 16.0.' in character
for name, value in [('UPPERCASE_LETTER', 1), ('LOWERCASE_LETTER', 2), ('TITLECASE_LETTER', 3), ('MODIFIER_LETTER', 4), ('OTHER_LETTER', 5), ('DECIMAL_DIGIT_NUMBER', 9), ('SURROGATE', 19)]:
    assert re.search(r'public static final byte ' + name + r' = ' + str(value) + r';', character)
assert method(character, 'public static boolean isLetter(char ch)') == 'returnisLetter((int)ch);'
assert method(character, 'public static boolean isLetterOrDigit(char ch)') == 'returnisLetterOrDigit((int)ch);'
letters = '(1<<Character.UPPERCASE_LETTER)|(1<<Character.LOWERCASE_LETTER)|(1<<Character.TITLECASE_LETTER)|(1<<Character.MODIFIER_LETTER)|(1<<Character.OTHER_LETTER)'
assert method(character, 'public static boolean isLetter(int codePoint)') == 'return(((' + letters + ')>>getType(codePoint))&1)!=0;'
assert method(character, 'public static boolean isLetterOrDigit(int codePoint)') == 'return(((' + letters + '|(1<<Character.DECIMAL_DIGIT_NUMBER))>>getType(codePoint))&1)!=0;'
assert method(character, 'public static int getType(int codePoint)') == 'returnCharacterData.of(codePoint).getType(codePoint);'
dispatch = source['java.base/java/lang/CharacterData.java'].decode()
assert dispatch.index('if (ch >>> 8 == 0)') < dispatch.index('return CharacterDataLatin1.instance;') < dispatch.index('case 0 -> CharacterData00.instance;')

avro = source['apache-avro-1.11.4/org/apache/avro/Schema.java'].decode()
validation = method(avro, 'private static String validateName(String name)')
assert "charfirst=name.charAt(0);if(!(Character.isLetter(first)||first=='_'))" in validation
assert "for(inti=1;i<length;i++){charc=name.charAt(i);if(!(Character.isLetterOrDigit(c)||c=='_'))" in validation
assert 'if(length==0)' in validation

categories = plane_zero_types(source['java.base/java/lang/CharacterData00.java'].decode())
assert categories[:256] == latin1_types(source['java.base/java/lang/CharacterDataLatin1.java'].decode())
assert set(categories[0xd800:0xe000]) == {19}
flags = bytearray(3 if category in {1, 2, 3, 4, 5} or unit == 95 else 2 if category == 9 else 0 for unit, category in enumerate(categories))
expected = unicode_expectations(source['unicode-16.0.0/UnicodeData.txt'].decode())
assert flags == expected
assert sum(bool(flag & 1) for flag in flags) == 48974
assert sum(bool(flag & 2) for flag in flags) == 49344
packed = bytes(sum(flags[index + step] << (step * 2) for step in range(4)) for index in range(0, 65536, 4))
assert len(packed) == 16384 and sha256(packed) == EXPECTED_TABLE_SHA256
ranges = []
start = 0
for end in range(1, 65537):
    if end == 65536 or expected[end] != expected[start]:
        ranges.append((start, end - 1, expected[start]))
        start = end
expectation_text = 'first\tlast\tflags\n' + ''.join(f'{first:04X}\t{last:04X}\t{mask}\n' for first, last, mask in ranges)
audit = {
    'profile': 'Eclipse Temurin OpenJDK 25.0.1+8 Linux x64, Apache Avro 1.11.4',
    'units': 65536,
    'packing': 'four UTF16 units per byte; unit index modulo4 chooses its low-to-high two-bit slot; bit0 first, bit1 later',
    'bytes': len(packed),
    'sha256': sha256(packed),
    'first_units': sum(bool(flag & 1) for flag in flags),
    'later_units': sum(bool(flag & 2) for flag in flags),
    'decimal_digit_units': categories.count(9),
    'latin1_units_compared': 256,
    'surrogate_units_rejected': 2048,
    'unicode_expectation_units_compared': len(expected),
    'unicode_expectation_ranges': len(ranges),
    'unicode_expectation_sha256': sha256(expectation_text.encode()),
    'source_zip_sha256': SOURCE_ZIP_SHA256,
    'avro_sources_jar_sha256': AVRO_JAR_SHA256,
    'host_unicode_tables': False,
    'java_or_rust_execution': False,
}
output.mkdir(parents=True, exist_ok=True)
artifacts = {
    'bmp-flags.bin': packed,
    'expected-ranges.tsv': expectation_text.encode(),
    'audit.json': (json.dumps(audit, indent=2) + '\n').encode(),
    'sources.sha256': ''.join(digest + '  ' + path + '\n' for path, digest in sorted(SOURCES.items())).encode(),
    'COPYRIGHT': b'\n\n'.join(path.encode() + b'\n\n' + data[:data.index(b'*/') + 2] for path, data in sorted(source.items()) if path.endswith('.java')) + b'\n',
    'LICENSE': source['legal/java.base/LICENSE'],
    'ASSEMBLY_EXCEPTION': source['legal/java.base/ASSEMBLY_EXCEPTION'],
    'ADDITIONAL_LICENSE_INFO': source['legal/java.base/ADDITIONAL_LICENSE_INFO'],
    'LICENSE-APACHE': source['apache-avro-1.11.4/META-INF/LICENSE'],
    'NOTICE-APACHE': source['apache-avro-1.11.4/META-INF/NOTICE'],
    'LICENSE-UNICODE': source['unicode-16.0.0/LICENSE'],
}
for path, data in artifacts.items():
    (output / path).write_bytes(data)
(output / 'artifacts.sha256').write_text(''.join(sha256(data) + '  ' + path + '\n' for path, data in sorted(artifacts.items())))
print('Source-only derivation:', len(packed), 'table bytes,', len(expected), 'independent expected units,', len(SOURCES), 'pinned sources')
