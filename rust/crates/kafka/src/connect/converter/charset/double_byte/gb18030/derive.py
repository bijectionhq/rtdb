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
    members = tar.getmembers()
    source_bytes = tar.extractfile(next(member for member in members if member.name.endswith('/lib/src.zip'))).read()
    assert hashlib.sha256(source_bytes).hexdigest() == SOURCE_ZIP_SHA256
    for filename in ['LICENSE', 'ASSEMBLY_EXCEPTION', 'ADDITIONAL_LICENSE_INFO']:
        member = next(member for member in members if member.name.endswith('/legal/java.base/' + filename))
        (output / filename).write_bytes(tar.extractfile(member).read())

paths = [
    'java.base/sun/nio/cs/GB18030.java',
    'java.base/sun/nio/cs/StandardCharsets.java',
    'java.base/java/lang/String.java',
    'java.base/java/nio/charset/Charset.java',
    'java.base/java/nio/charset/CharsetDecoder.java',
]
with zipfile.ZipFile(io.BytesIO(source_bytes)) as source_zip:
    originals = {path: source_zip.read(path) for path in paths}


def uncomment(source):
    return re.sub(r'/\*.*?\*/|//[^\n]*', '', source, flags=re.S)


def literal(token):
    body, result, index = token[1:-1], [], 0
    escapes = {'b': 8, 't': 9, 'n': 10, 'f': 12, 'r': 13, '"': 34, "'": 39, '\\': 92}
    while index < len(body):
        char = body[index]
        index += 1
        if char != '\\':
            result.append(ord(char))
            continue
        char = body[index]
        index += 1
        if char == 'u':
            while body[index] == 'u':
                index += 1
            result.append(int(body[index:index + 4], 16))
            index += 4
        elif char in '01234567':
            digits = char
            while len(digits) < (3 if char <= '3' else 2) and index < len(body) and body[index] in '01234567':
                digits += body[index]
                index += 1
            result.append(int(digits, 8))
        else:
            result.append(escapes[char])
    return result


def expression(text):
    tokens, end = [], 0
    for token in re.finditer(r'"(?:[^"\\]|\\.)*"|IS_2000|[()+?:]', text):
        assert not text[end:token.start()].strip()
        tokens.append(token.group())
        end = token.end()
    assert not text[end:].strip()
    position = 0

    def concatenate():
        nonlocal position
        result = []
        while True:
            token = tokens[position]
            position += 1
            if token == '(':
                assert tokens[position:position + 2] == ['IS_2000', '?']
                position += 2
                concatenate()
                assert tokens[position] == ':'
                position += 1
                result.extend(concatenate())
                assert tokens[position] == ')'
                position += 1
            else:
                assert token.startswith('"')
                result.extend(literal(token))
            if position == len(tokens) or tokens[position] != '+':
                return result
            position += 1

    result = concatenate()
    assert position == len(tokens)
    return result


source = uncomment(originals[paths[0]].decode())
standard = uncomment(originals[paths[1]].decode())
assert '"2000".equals(System.getProperty("jdk.charset.GB18030", ""))' in source
assert re.sub(r'\s+', '', re.search(r'aliases_GB18030\(\).*?new String\[\]\s*\{(.*?)\}', standard, re.S).group(1)) == 'GB18030.IS_2000?"gb18030-2000":"gb18030-2022"'
assert re.search(r'new Object\[\]\s*\{\s*"gb18030",\s*"GB18030"', standard)
assert 'csn.equals("gb18030-2022") && !GB18030.IS_2000' in standard
assert 'csn.equals("gb18030-2000") && GB18030.IS_2000 ? "gb18030" : csn' in standard
assert 'new Decoder(this)' in source and 'ArrayDecoder' not in source
string = uncomment(originals['java.base/java/lang/String.java'].decode())
assert 'cd.decode(bb, cb, true)' in string and 'cd.flush(cb)' in string
assert 'cd.onMalformedInput(CodingErrorAction.REPLACE)' in string
assert '.onUnmappableCharacter(CodingErrorAction.REPLACE);' in string
assert 'in.position(in.position() + cr.length());' in uncomment(originals['java.base/java/nio/charset/CharsetDecoder.java'].decode())


def table(index_name, rows_name):
    body = re.search(r'\b' + index_name + r'\[\]\s*=\s*\{(.*?)\};', source, re.S).group(1)
    indices = [int(value.strip(), 0) for value in body.split(',') if value.strip()]
    body = re.search(r'\b' + rows_name + r'\[\]\s*=\s*\{(.*?)\};', source, re.S).group(1)
    names = [name.strip() for name in body.split(',') if name.strip()]
    rows = [expression(re.search(r'\b' + name + r'\s*=\s*(.*?);', source, re.S).group(1)) for name in names]
    assert len(indices) == 256
    return indices, rows


pair_indices, pair_rows = table('index1', 'index2')
offset_indices, offset_rows = table('decoderIndex1', 'decoderIndex2')
assert len(pair_rows) == 8 and len(offset_rows) == 6


def pair(first, second):
    index = pair_indices[first]
    return pair_rows[index >> 4][(index & 15) * 191 + second - 0x40]


def offset_table(offset):
    index = offset_indices[(offset >> 8) & 255]
    return offset_rows[index >> 4][(index & 15) * 256 + (offset & 255)]


def four(offset):
    if offset <= 0x4a62:
        return offset_table(offset)
    if offset <= 0x82bc:
        return offset_table(offset) if 0x4a71 <= offset <= 0x4a78 else offset + 0x5543
    if offset <= 0x830d:
        return offset_table(offset)
    if offset <= 0x93a8:
        return offset + 0x6557
    if offset <= 0x99fb:
        return offset_table(offset)
    if 0x2e248 <= offset < 0x12e248:
        return offset - 0x1e248
    return 0xfffd


decoder = re.sub(r'\s+', '', source.split('private static class Decoder', 1)[1].split('private static class Encoder', 1)[0])
for constraint in ['if(sl-sp<4)', 'if(byte3<0x81||byte3>0xfe)', 'malformedForLength(3)', 'if(byte4<0x30||byte4>0x39)', 'malformedForLength(4)', 'if(offset<=0x4A62)', 'offset<=0x82BC', 'offset>=0x4A71&&offset<=0x4A78&&!IS_2000', 'offset>=0x82BD&&offset<=0x830D', 'offset>=0x830D&&offset<=0x93A8', 'offset>=0x93A9&&offset<=0x99FB', 'offset>=0x2E248&&offset<0x12E248', 'offset-=0x1e248']:
    assert constraint in decoder, constraint

mapping_rows = []
for filename, units, shape in [
    ('pairs.bin', [pair(first, second) for first in range(0x81, 0xff) for second in range(0x40, 0xff)], '126x191'),
    ('bmp.bin', [four(offset) for offset in range(0x99fc)], '39420'),
]:
    assert all(0 <= unit <= 0xffff and not 0xd800 <= unit <= 0xdfff for unit in units)
    blob = b''.join(unit.to_bytes(2, 'big') for unit in units)
    (output / filename).write_bytes(blob)
    mapping_rows.append((filename, str(len(blob)), hashlib.sha256(blob).hexdigest(), shape))
(output / 'mappings.tsv').write_text('resource\tbytes\tsha256\tshape\n' + ''.join('\t'.join(row) + '\n' for row in mapping_rows))


def decode(data):
    position, result = 0, []
    while position < len(data):
        first = data[position]
        if first < 128:
            result.append(first)
            position += 1
            continue
        if first < 0x81 or first > 0xfe:
            result.append(0xfffd)
            position += 1
            continue
        if len(data) - position < 2:
            result.append(0xfffd)
            break
        second = data[position + 1]
        if second < 0x30:
            result.append(0xfffd)
            position += 1
        elif second <= 0x39:
            if len(data) - position < 4:
                result.append(0xfffd)
                break
            third, fourth = data[position + 2:position + 4]
            if third < 0x81 or third > 0xfe:
                result.append(0xfffd)
                position += 3
            elif fourth < 0x30 or fourth > 0x39:
                result.append(0xfffd)
                position += 4
            else:
                result.append(four((((first - 0x81) * 10 + second - 0x30) * 126 + third - 0x81) * 10 + fourth - 0x30))
                position += 4
        elif second < 0x40 or second in (0x7f, 0xff):
            result.append(0xfffd)
            position += 2
        else:
            result.append(pair(first, second))
            position += 2
    return ''.join(map(chr, result)).encode('utf-8')


def digest(records):
    result, count = hashlib.sha256(), 0
    for record in records:
        result.update(decode(record))
        result.update(b'\0')
        count += 1
    return str(count), result.hexdigest()


probes = [
    ('single', digest(bytes([first]) for first in range(256))),
    ('pairs', digest(bytes([first, second]) for first in range(256) for second in range(256))),
    ('four-byte-space', digest(bytes([first, second, third, fourth]) for first in range(0x81, 0xff) for second in range(0x30, 0x3a) for third in range(0x81, 0xff) for fourth in range(0x30, 0x3a))),
    ('four-byte-suffixes', digest(bytes([first, second, third, fourth]) for first in [0x81, 0x84, 0x90, 0xe3, 0xfe] for second in [0x30, 0x39] for third in range(256) for fourth in range(256))),
    ('three-byte-prefixes', digest(bytes([first, second, third]) for first in [0x81, 0xfe] for second in range(256) for third in range(256))),
]
(output / 'probes.tsv').write_text('probe\trecords\tsha256\n' + ''.join(name + '\t' + '\t'.join(values) + '\n' for name, values in probes))
(output / 'charsets.tsv').write_text('canonical\tclass\tprovider\tstandard\taliases\nGB18030\tGB18030\tjava.base\t2022\tgb18030-2022\n')
(output / 'sources.sha256').write_text(''.join(hashlib.sha256(originals[path]).hexdigest() + '  ' + path + '\n' for path in paths))
headers = {}
for path, original in originals.items():
    text = original.decode()
    header = text[:text.index('*/') + 2]
    headers.setdefault(header, []).append(path)
(output / 'COPYRIGHT').write_text('\n\n'.join('\n'.join(names) + '\n\n' + header for header, names in headers.items()) + '\n')
