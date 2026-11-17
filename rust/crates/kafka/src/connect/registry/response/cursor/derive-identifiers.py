import hashlib, json, re, sys, zipfile
from pathlib import Path
archive, decimal_source, output_directory = map(Path, sys.argv[1:])
assert hashlib.sha256(archive.read_bytes()).hexdigest() == "3ed5aadae786d9e35101a67e94818c4590f5b8a81e7ee44ad4ce4add58881482"
with zipfile.ZipFile(archive) as z:
    raw = z.read("java.base/java/lang/CharacterData00.java")
    latin_raw = z.read("java.base/java/lang/CharacterDataLatin1.java")
source = raw.decode()
def units(expression):
    output = []
    for literal in re.findall(r'"((?:[^"\\]|\\.)*)"', expression):
        at = 0
        while at < len(literal):
            if literal[at] != "\\": output.append(ord(literal[at])); at += 1; continue
            at += 1
            char = literal[at]; at += 1
            if char == "u": output.append(int(literal[at:at+4],16)); at += 4
            elif char in "01234567":
                begin = at-1; limit = 3 if char < "4" else 2
                while at < len(literal) and at-begin < limit and literal[at] in "01234567": at += 1
                output.append(int(literal[begin:at],8))
            else: output.append(ord({"n":"\n","r":"\r","t":"\t","b":"\b","f":"\f","\\":"\\",'"':'"',"'":"'"}[char]))
    return output
assert "A[Y[X[offset>>5]|((offset>>1)&0xF)]|(offset&0x1)]" in source
assert "return ((props & 0x00003000) != 0);" in source
x=units(re.search(r"char X\[\] = \((.*?)\)\.toCharArray",source,re.S)[1])
y=units(re.search(r"char Y\[\] = \((.*?)\)\.toCharArray",source,re.S)[1])
a=units(re.search(r"String A_DATA =\s*(.*?);",source,re.S)[1])
a=[(a[i]<<16)|a[i+1] for i in range(0,len(a),2)]
props=[a[y[x[c>>5]|((c>>1)&15)]|(c&1)] for c in range(65536)]
latin = latin_raw.decode()
expression = re.sub(r"//[^\n]*", "", re.search(r"int A\[\] = \{(.*?)\};", latin, re.S)[1])
latin_props = [int(item,16) & 0xffffffff for item in re.findall(r"-?0x[0-9a-fA-F]+",expression)]
assert len(latin_props) == 256
assert all((p & 0x3000) == (q & 0x3000) for p,q in zip(props,latin_props))
latin_digits = [int(item) for item in re.findall(r"-?\d+",re.search(r"DIGITS = new byte\[\] \{(.*?)\};",latin,re.S)[1])]
assert len(latin_digits) == 256
assert all((v if 0 <= v < 10 else None) == (c-48 if 48 <= c <= 57 else None) for c,v in enumerate(latin_digits))
bitmap=bytearray(8192)
for c,p in enumerate(props):
    if p&0x3000: bitmap[c>>3] |= 1<<(c&7)
output=output_directory / "identifier-parts.bin"
output.write_bytes(bitmap)
text=decimal_source.read_text()
zeroes=[0x30]+[int(v,16) for v in re.findall(r"0x[0-9a-f]+",text.split("const ZEROES:")[1].split("];",1)[0])]
digits={zero+v:v for zero in zeroes for v in range(10)}
assert set(digits)=={c for c,p in enumerate(props) if p&31==9}
assert "if (kind == Character.DECIMAL_DIGIT_NUMBER)" in source
assert "value = ch + ((val & 0x3E0) >> 5) & 0x1F;" in source
for c,v in digits.items():
    offset=(props[c]&0x3e0)>>5
    assert ((c+offset)&31)==v
(output_directory / "identifier-derivation.json").write_text(json.dumps({"source_sha256":hashlib.sha256(raw).hexdigest(),"latin1_source_sha256":hashlib.sha256(latin_raw).hexdigest(),"bitmap_sha256":hashlib.sha256(bitmap).hexdigest(),"identifier_unit_count":sum(bool(p&0x3000) for p in props),"verified_decimal_units":len(digits)},indent=2)+"\n")
