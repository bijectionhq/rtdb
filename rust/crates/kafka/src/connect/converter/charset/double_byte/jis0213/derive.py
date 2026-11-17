import hashlib
import io
import json
import re
import struct
import sys
import tarfile
import zipfile
from pathlib import Path

ARCHIVE_SHA256 = "8daf77d1aacffe38c9889689bc224a13557de77559d9a5bb91991e6a298baa0d"
SOURCE_SHA256 = "3ed5aadae786d9e35101a67e94818c4590f5b8a81e7ee44ad4ce4add58881482"
RESOURCE_SHA256 = "9a103ec5e953cb5f8dd205c928abde3b333086a1e784825c940a6acc2e1c0020"
RESOURCE_PATH = "/jdk.charsets/sun/nio/cs/ext/sjis0213.dat"
SOURCES = [
    "java.base/jdk/internal/jimage/ImageHeader.java",
    "java.base/jdk/internal/jimage/ImageLocation.java",
    "java.base/jdk/internal/jimage/BasicImageReader.java",
    "java.base/jdk/internal/jimage/ImageStringsReader.java",
    "java.base/jdk/internal/jimage/decompressor/CompressedResourceHeader.java",
    "java.base/jdk/internal/jimage/decompressor/Decompressor.java",
    "java.base/jdk/internal/jimage/decompressor/ZipDecompressor.java",
    "java.base/sun/nio/cs/CharsetMapping.java",
    "java.base/sun/nio/cs/MS932.java",
    "java.base/sun/nio/cs/DoubleByte.java",
    "java.base/sun/nio/cs/StandardCharsets.java",
    "java.base/java/lang/String.java",
    "java.base/java/nio/charset/Charset.java",
    "java.base/java/nio/charset/CharsetDecoder.java",
    "jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java",
    "jdk.charsets/sun/nio/cs/ext/ExtendedCharsets.java",
    "jdk.charsets/sun/nio/cs/ext/SJIS_0213.java",
    "jdk.charsets/sun/nio/cs/ext/MS932_0213.java",
]


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_exact(stream, size):
    data = stream.read(size)
    require(len(data) == size, "Short data read")
    return data


def member(tar, suffix):
    matches = [item for item in tar.getmembers() if item.name.endswith(suffix)]
    require(len(matches) == 1, "Archive member is not unique: " + suffix)
    return matches[0]


def mapping_shape(data):
    require(len(data) >= 4, "Missing mapping payload length")
    payload_size = int.from_bytes(data[:4], "big")
    require(payload_size == len(data) - 4, "Mapping payload length mismatch")
    position = 4
    tables = {}
    blocks = []

    def u16():
        nonlocal position
        require(position + 2 <= len(data), "Truncated mapping integer")
        result = int.from_bytes(data[position:position + 2], "big")
        position += 2
        return result

    while position < len(data):
        start = position
        tag = u16()
        require(tag in (1, 2, 3, 5, 6, 7, 8), "Unknown mapping block")
        require(tag not in tables, "Duplicate mapping block")
        bounds = [u16() for _ in range(4)] if tag in (2, 3) else None
        count = u16()
        units = [u16() for _ in range(count)]
        tables[tag] = units
        block = {"tag": tag, "file_offset": start, "bytes": position - start,
                 "u16_count": count, "sha256": sha256(data[start:position])}
        if bounds is not None:
            low1, high1, low2, high2 = bounds
            require(0 <= low1 <= high1 <= 255 and 0 <= low2 <= high2 <= 255,
                    "Invalid mapping byte bounds")
            require(count == (high1 - low1 + 1) * (high2 - low2 + 1),
                    "Double-byte mapping dimensions disagree")
            block["bounds_b1_min_max_b2_min_max"] = bounds
        if tag in (1, 2, 3):
            require(all(not 0xD800 <= unit <= 0xDFFF for unit in units),
                    "Unexpected BMP surrogate mapping")
            block["mapped_bmp_units"] = sum(unit != 0xFFFD for unit in units)
        if tag in (5, 6):
            require(count % 2 == 0, "Odd supplementary table length")
            keys = units[:count // 2]
            require(all(left < right for left, right in zip(keys, keys[1:])),
                    "Unsorted supplementary mapping keys")
            block["pairs"] = count // 2
        if tag == 7:
            require(count % 3 == 0, "Incomplete composite mapping")
            keys = units[::3]
            require(all(left < right for left, right in zip(keys, keys[1:])),
                    "Unsorted composite mapping keys")
            require(all(not 0xD800 <= units[index] <= 0xDFFF
                        for index in range(count) if index % 3 != 0),
                    "Unexpected composite surrogate")
            block["triples"] = count // 3
        blocks.append(block)
    require(set(tables) == {1, 2, 3, 5, 6, 7, 8}, "Missing mapping block")
    require(len(tables[1]) == len(tables[8]) == 256, "Wrong single/index size")
    supplementary = tables[5]
    half = len(supplementary) // 2
    examples = {
        "single_5c": tables[1][0x5C],
        "supplementary_first": [supplementary[0], supplementary[half] + 0x20000],
        "composite_first": tables[7][:3],
    }
    return {"payload_bytes": payload_size, "bytes_consumed": position,
            "block_order": [block["tag"] for block in blocks],
            "blocks": blocks, "examples": examples}


def main():
    archive, output = map(Path, sys.argv[1:3])
    with archive.open("rb") as stream:
        require(hashlib.file_digest(stream, "sha256").hexdigest() == ARCHIVE_SHA256,
                "Pinned archive hash mismatch")
    with tarfile.open(archive) as tar:
        source_member = member(tar, "/lib/src.zip")
        source_data = tar.extractfile(source_member).read()
        require(sha256(source_data) == SOURCE_SHA256, "Pinned source ZIP mismatch")
        with zipfile.ZipFile(io.BytesIO(source_data)) as source_zip:
            sources = {path: source_zip.read(path) for path in SOURCES}
        modules = member(tar, "/lib/modules")
        stream = tar.extractfile(modules)
        modules_hash = hashlib.file_digest(stream, "sha256").hexdigest()
        stream.seek(0)
        raw_header = read_exact(stream, 28)
        require(struct.unpack("<I", raw_header[:4])[0] == 0xCAFEDADA,
                "Pinned Linux x64 image must have little-endian jimage magic")
        magic, version, flags, resource_count, table_length, locations_size, strings_size = struct.unpack("<7I", raw_header)
        require(version == 0x10000 and flags == 0, "Unexpected jimage version or flags")
        index_size = 28 + table_length * 8 + locations_size + strings_size
        require(index_size <= modules.size, "Jimage index exceeds member size")
        index = raw_header + read_exact(stream, index_size - 28)
        locations_offset = 28 + table_length * 8
        strings_offset = locations_offset + locations_size
        locations = index[locations_offset:strings_offset]
        strings = index[strings_offset:index_size]

        def string(offset):
            require(0 <= offset < len(strings), "String offset exceeds table")
            end = strings.find(b"\0", offset)
            require(end >= 0, "Unterminated image string")
            return strings[offset:end]

        matches = []
        for slot in range(table_length):
            location = struct.unpack_from("<I", index, 28 + table_length * 4 + slot * 4)[0]
            if location == 0:
                continue
            attributes = [0] * 8
            position = location
            while True:
                require(position < len(locations), "Location exceeds table")
                tag = locations[position]
                position += 1
                if tag <= 7:
                    break
                kind, length = tag >> 3, (tag & 7) + 1
                require(kind < 8 and position + length <= len(locations),
                        "Invalid image location attribute")
                attributes[kind] = int.from_bytes(locations[position:position + length], "big")
                position += length
            if string(attributes[3]) != b"sjis0213":
                continue
            module, parent, base, extension = [string(attributes[kind]).decode("ascii") for kind in range(1, 5)]
            full_name = ("/" + module + "/" if module else "") + (parent + "/" if parent else "") + base + ("." + extension if extension else "")
            if full_name == RESOURCE_PATH:
                matches.append((slot, location, attributes))
        require(len(matches) == 1, "Target image resource is not unique")
        slot, location, attributes = matches[0]
        require(attributes[6] == 0, "Compressed target requires a separately reviewed decompressor")
        absolute_offset = index_size + attributes[5]
        require(absolute_offset + attributes[7] <= modules.size, "Resource exceeds image")
        stream.seek(absolute_offset)
        resource = read_exact(stream, attributes[7])
        require(sha256(resource) == RESOURCE_SHA256, "Pinned resource hash mismatch")
        shape = mapping_shape(resource)
        legal = {}
        for name in ["LICENSE", "ASSEMBLY_EXCEPTION", "ADDITIONAL_LICENSE_INFO"]:
            item = member(tar, "/legal/java.base/" + name)
            legal[name] = tar.extractfile(item).read()

    provenance = {
        "archive_sha256": ARCHIVE_SHA256, "source_zip_member": source_member.name,
        "source_zip_sha256": SOURCE_SHA256, "image_member": modules.name,
        "image_bytes": modules.size, "image_sha256": modules_hash,
        "image_byte_order": "little", "image_version": "1.0", "image_flags": flags,
        "image_resource_count": resource_count, "image_table_length": table_length,
        "image_locations_bytes": locations_size, "image_strings_bytes": strings_size,
        "image_index_bytes": index_size, "resource_path": RESOURCE_PATH,
        "resource_offset_table_slot": slot, "resource_location_offset": location,
        "resource_attributes": attributes, "resource_content_offset": attributes[5],
        "resource_image_offset": absolute_offset, "resource_compressed_bytes": attributes[6],
        "resource_bytes": len(resource), "resource_sha256": sha256(resource),
        "resource_decompression": "none; compressed-size attribute is zero",
        "mapping_shape": shape,
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "sjis0213.dat").write_bytes(resource)
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    (output / "sources.sha256").write_text("".join(sha256(data) + "  " + path + "\n" for path, data in sources.items()))
    (output / "COPYRIGHT").write_bytes(b"\n\n".join(path.encode() + b"\n\n" + data[:data.index(b"*/") + 2] for path, data in sources.items()) + b"\n")
    for name, data in legal.items():
        (output / name).write_bytes(data)
    derive(sources, resource, output)


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
        require(not expression[end:token.start()].strip(), 'Unparsed table expression')
        value = token.group()
        end = token.end()
        if value == ',':
            require(bool(fragments), 'Empty table entry')
            rows.append(None if fragments == [None] else ''.join(fragments))
            fragments = []
        elif value == 'null':
            require(not fragments, 'Mixed null and string entry')
            fragments.append(None)
        elif value != '+':
            fragments.extend(strings(value))
    require(not expression[end:].strip(), 'Unparsed table suffix')
    if fragments:
        rows.append(None if fragments == [None] else ''.join(fragments))
    return [None if row is None else [ord(char) for char in row] for row in rows]


def derive(sources, resource, output):
    source = {path: uncomment(data.decode()) for path, data in sources.items()}
    ms_path = 'java.base/sun/nio/cs/MS932.java'
    ms = source[ms_path]
    require('new DoubleByte.Decoder(this, DecodeHolder.b2c, DecodeHolder.b2cSB, 0x40, 0xfc, true)' in ms,
            'Unexpected MS932 decoder')
    holder = ms.split('public static class DecodeHolder', 1)[1].split('public static class EncodeHolder', 1)[0]
    singles = [ord(char) for char in ''.join(strings(re.search(r'String b2cSBStr\s*=\s*(.*?);', holder, re.S).group(1)))]
    rows = string_rows(re.search(r'String\[\] b2cStr\s*=\s*\{(.*?)\};', holder, re.S).group(1))
    initialization = re.sub(r'\s+', ' ', holder[holder.index('static {'):]).strip()
    require(initialization == 'static { for (int i = 0; i < b2cStr.length; i++) { if (b2cStr[i] == null) b2c[i] = DoubleByte.B2C_UNMAPPABLE; else b2c[i] = b2cStr[i].toCharArray(); } b2cSB = b2cSBStr.toCharArray(); } }',
            'Unexpected MS932 table initialization')
    require(len(singles) == len(rows) == 256, 'Unexpected MS932 table dimensions')
    require(all(row is None or len(row) == 189 for row in rows), 'Unexpected MS932 row width')
    offsets, compact = [], []
    for row in rows:
        offsets.append(0xffff if row is None else len(compact))
        if row is not None:
            compact.append(row)
    units = singles + offsets + [unit for row in compact for unit in row]
    require(all(not 0xd800 <= unit <= 0xdfff for unit in units), 'MS932 surrogate mapping')
    ms_blob = bytes([0x40, 0xfc]) + b''.join(unit.to_bytes(2, 'big') for unit in units)
    (output / 'windows-31j.bin').write_bytes(ms_blob)

    mapping = source['java.base/sun/nio/cs/CharsetMapping.java']
    for statement in ['if (b2 >= b2Min && b2 < b2Max)', 'dbSegSize = b2Max - b2Min + 1;',
                      'Character.toChars(b2cSupp[end + i] + 0x20000, cc, 0);']:
        require(statement in mapping, 'Changed CharsetMapping semantics')
    sjis = source['jdk.charsets/sun/nio/cs/ext/SJIS_0213.java']
    sjis_decoder = sjis.split('static class Decoder extends CharsetDecoder', 1)[1].split('static class Encoder extends CharsetEncoder', 1)[0]
    require('ArrayDecoder' not in sjis_decoder, 'Changed SJIS_0213 decoder route')
    for statement in ['if (sl - sp < 2)', 'if (decodeSingle(b2) == UNMAPPABLE)',
                      'return CoderResult.unmappableForLength(2);', 'return CoderResult.unmappableForLength(1);',
                      'if (Holder.mapping.decodeSurrogate(db, cc) != null)',
                      'if (Holder.mapping.decodeComposite(comp, cc) != null)']:
        require(statement in sjis_decoder, 'Changed SJIS_0213 decode semantics')
    ms0213 = source['jdk.charsets/sun/nio/cs/ext/MS932_0213.java']
    ms_decoder = ms0213.split('static class Decoder extends SJIS_0213.Decoder', 1)[1].split('static class Encoder', 1)[0]
    require('decodeSingle' not in ms_decoder, 'MS932_0213 changed single-byte dispatch')
    require(re.search(r'char c = decMS932.decodeDouble\(b1, b2\);\s*if \(c == UNMAPPABLE_DECODING\)\s*return super.decodeDouble\(b1, b2\);\s*return c;', ms_decoder),
            'MS932_0213 changed BMP precedence')
    require('b2 < b2Min || b2 > b2Max)' in source['java.base/sun/nio/cs/DoubleByte.java'],
            'Changed DoubleByte trail bounds')
    string = source['java.base/java/lang/String.java']
    for statement in ['cd.decode(bb, cb, true)', 'cd.onMalformedInput(CodingErrorAction.REPLACE)',
                      '.onUnmappableCharacter(CodingErrorAction.REPLACE);']:
        require(statement in string, 'Changed String decoding route')
    decoder = source['java.base/java/nio/charset/CharsetDecoder.java']
    require('cr = CoderResult.malformedForLength(in.remaining());' in decoder, 'Changed terminal underflow')
    require('in.position(in.position() + cr.length());' in decoder, 'Changed replacement consumption')

    shape = mapping_shape(resource)
    blocks = {block['tag']: block for block in shape['blocks']}
    def array(tag):
        block = blocks[tag]
        start = block['file_offset'] + (12 if tag in (2, 3) else 4)
        return list(struct.unpack_from('>' + str(block['u16_count']) + 'H', resource, start))
    single, db1, db2, supp, comp = (array(tag) for tag in [1, 2, 3, 5, 7])
    require(blocks[2]['bounds_b1_min_max_b2_min_max'] == [0x81, 0x9f, 0x40, 0xfe], 'Changed first JIS row bounds')
    require(blocks[3]['bounds_b1_min_max_b2_min_max'] == [0xe0, 0xfc, 0x40, 0xfe], 'Changed second JIS row bounds')
    supplementary = dict(zip(supp[:len(supp) // 2], [unit + 0x20000 for unit in supp[len(supp) // 2:]]))
    composites = {comp[i]: comp[i + 1:i + 3] for i in range(0, len(comp), 3)}
    require(len(supplementary) == 303 and len(composites) == 25, 'Changed JIS extended tables')

    def bmp(variant, first, second):
        if variant == 'Windows' and rows[first] is not None and 0x40 <= second <= 0xfc:
            value = rows[first][second - 0x40]
            if value != 0xfffd:
                return value
        if 0x40 <= second < 0xfe:
            if 0x81 <= first <= 0x9f:
                return db1[(first - 0x81) * 191 + second - 0x40]
            if 0xe0 <= first <= 0xfc:
                return db2[(first - 0xe0) * 191 + second - 0x40]
        return 0xfffd

    def decode_record(variant, record):
        result, position = [], 0
        while position < len(record):
            first = record[position]
            if single[first] != 0xfffd:
                result.append(single[first])
                position += 1
                continue
            if len(record) - position == 1:
                result.append(0xfffd)
                break
            second = record[position + 1]
            unit = bmp(variant, first, second)
            key = first * 256 + second
            if unit != 0xfffd:
                result.append(unit)
            elif key in supplementary:
                result.append(supplementary[key])
            elif key in composites:
                result.extend(composites[key])
            else:
                result.append(0xfffd)
                if single[second] != 0xfffd:
                    position += 1
                    continue
            position += 2
        return ''.join(chr(unit) for unit in result).encode('utf-8')

    for key in supplementary.keys() | composites.keys():
        require(single[key >> 8] == 0xfffd and bmp('Standard', key >> 8, key & 255) == 0xfffd,
                'Extended key shadowed by a standard BMP mapping')

    extended = source['jdk.charsets/sun/nio/cs/ext/ExtendedCharsets.java']
    standard = source['java.base/sun/nio/cs/StandardCharsets.java']
    standard_aliases = dict(re.findall(r'new Object\[\]\s*\{\s*"([^"]+)",\s*"([^"]+)"', standard.split('class Aliases', 1)[1].split('class Classes', 1)[0]))
    standard_classes = dict(re.findall(r'new Object\[\]\s*\{\s*"([^"]+)",\s*"([^"]+)"', standard.split('class Classes', 1)[1].split('class Cache', 1)[0]))
    names, ext_aliases, ext_classes = {}, {}, {}
    for canonical, classname, body in re.findall(r'charset\("([^"]+)",\s*"([^"]+)",\s*new String\[\]\s*\{(.*?)\}\);', extended, flags=re.S):
        names[classname] = (canonical, strings(body))
        ext_classes.setdefault(canonical.lower(), classname)
        for alias in strings(body):
            ext_aliases.setdefault(alias.lower(), canonical.lower())
    require('deleteCharset(' not in extended, 'Changed provider registration lifecycle')
    aliases, manifest, probes = {}, [], []
    for classname, variant in [('SJIS_0213', 'Standard'), ('MS932_0213', 'Windows')]:
        canonical, alternate = names[classname]
        for name in [canonical] + alternate:
            lower = name.lower()
            require(standard_aliases.get(lower, lower) not in standard_classes, 'Standard provider shadows alias')
            require(ext_classes[ext_aliases.get(lower, lower)] == classname, 'Extended provider shadows alias')
            require(lower not in aliases, 'Duplicate JIS0213 alias')
            aliases[lower] = variant
        path = 'jdk.charsets/sun/nio/cs/ext/' + classname + '.java'
        manifest.append((canonical, classname, path, sha256(sources[path]), variant, '|'.join(alternate)))
        singles_hash = sha256(b''.join(decode_record(variant, bytes([byte])) for byte in range(256)))
        pairs_hash = sha256(b''.join(decode_record(variant, bytes([first, second])) + b'\0' for first in range(256) for second in range(256)))
        recovery_hash = sha256(b''.join(decode_record(variant, bytes([first, second]) + b'A\x82\xf5\x87\xa0') + b'\0' for first in range(256) for second in range(256)))
        probes.append((variant, singles_hash, pairs_hash, recovery_hash))
    require(len(aliases) == 13, 'Unexpected JIS0213 alias count')
    (output / 'charsets.tsv').write_text('canonical\tclass\tsource\tsource_sha256\tvariant\taliases\n' + ''.join('\t'.join(row) + '\n' for row in manifest))
    (output / 'probes.tsv').write_text('variant\tsingle_sha256\tpair_sha256\trecovery_sha256\n' + ''.join('\t'.join(row) + '\n' for row in probes))
    (output / 'aliases.rs').write_text('use super::Jis0213;\n\npub(super) const ALIASES: &[(&str, Jis0213)] = &[\n' + ''.join(f'    ("{name}", Jis0213::{variant}),\n' for name, variant in sorted(aliases.items())) + '];\n')
    constants = [('SINGLE', blocks[1]['file_offset'] + 4), ('DB1', blocks[2]['file_offset'] + 12),
                 ('DB2', blocks[3]['file_offset'] + 12), ('SUPPLEMENTARY', blocks[5]['file_offset'] + 4),
                 ('SUPPLEMENTARY_COUNT', len(supp) // 2), ('COMPOSITE', blocks[7]['file_offset'] + 4),
                 ('COMPOSITE_COUNT', len(comp) // 3)]
    (output / 'tables.rs').write_text('use super::{JisMapping, Mapping};\n\npub(super) const JIS: JisMapping = JisMapping(include_bytes!("sjis0213.dat"));\npub(super) const MS932: Mapping = Mapping(include_bytes!("windows-31j.bin"));\n\n' + ''.join(f'pub(super) const {name}: usize = {value};\n' for name, value in constants))
    (output / 'mappings.tsv').write_text('file\tbytes\tsha256\tsource\tsource_sha256\n' + '\t'.join(['sjis0213.dat', str(len(resource)), sha256(resource), RESOURCE_PATH, sha256(resource)]) + '\n' + '\t'.join(['windows-31j.bin', str(len(ms_blob)), sha256(ms_blob), ms_path, sha256(sources[ms_path])]) + '\n')
    print(json.dumps({'aliases': len(aliases), 'windows_bytes': len(ms_blob), 'windows_rows': len(compact), 'probes': probes}))


if __name__ == '__main__':
    main()
