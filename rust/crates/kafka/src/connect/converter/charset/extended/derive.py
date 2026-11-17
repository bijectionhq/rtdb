import hashlib
import io
import json
import re
import sys
import tarfile
import zipfile
from pathlib import Path

archive, output = Path(sys.argv[1]), Path(sys.argv[2])
assert hashlib.file_digest(archive.open('rb'), 'sha256').hexdigest() == '8daf77d1aacffe38c9889689bc224a13557de77559d9a5bb91991e6a298baa0d'
output.mkdir(parents=True, exist_ok=True)
with tarfile.open(archive) as tar:
    member = next(member for member in tar if member.name.endswith('/lib/src.zip'))
    source_bytes = tar.extractfile(member).read()
    assert hashlib.sha256(source_bytes).hexdigest() == '3ed5aadae786d9e35101a67e94818c4590f5b8a81e7ee44ad4ce4add58881482'
    for filename in ['LICENSE', 'ASSEMBLY_EXCEPTION', 'ADDITIONAL_LICENSE_INFO']:
        member = next(member for member in tar.getmembers() if member.name.endswith('/legal/java.base/' + filename))
        (output / filename).write_bytes(tar.extractfile(member).read())
with zipfile.ZipFile(io.BytesIO(source_bytes)) as source_zip:
    source = {name: source_zip.read(name) for name in source_zip.namelist() if name.endswith('.java')}
used = set()


def text(path):
    used.add(path)
    return source[path].decode()


provider = re.sub(r'/\*.*?\*/|//[^\n]*', '', text('jdk.charsets/sun/nio/cs/ext/ExtendedCharsets.java'), flags=re.S)
canonical, aliases = {}, {}
for name, classname, body in re.findall(r'charset\("([^"\\]+)",\s*"([^"\\]+)",\s*new String\[\]\s*\{(.*?)\}\);', provider, re.S):
    assert name.isascii()
    key = name.lower()
    assert key not in canonical
    canonical[key] = classname
    for alias in re.findall(r'"([^"\\]+)"', body):
        assert alias.isascii()
        aliases.setdefault(alias.lower(), key)
assert len(canonical) == 113 and len(aliases) == 459
assert not any(value in provider for value in ['deleteCharset(', 'System.', 'GetPropertyAction'])
rows = [(name, aliases.get(name, name)) for name in sorted(canonical.keys() | aliases.keys())]
assert len(rows) == 550 and all(target in canonical for _, target in rows)
(output / 'names.rs').write_text('pub(super) static NAMES: &[(&str, &str)] = &[\n' + ''.join(f'    ("{name}", "{target}"),\n' for name, target in rows) + '];\n')
(output / 'names.tsv').write_text('name\tcanonical\tclass\n' + ''.join(name + '\t' + target + '\t' + canonical[target] + '\n' for name, target in rows))

s=text('java.base/java/lang/CharacterData00.java')
def literals(body):
 out=[]
 for literal in re.findall(r'"((?:[^"\\]|\\.)*)"',body):
  i=0
  while i<len(literal):
   if literal[i]!='\\':out.append(ord(literal[i]));i+=1;continue
   i+=1;c=literal[i];i+=1
   if c=='u':out.append(int(literal[i:i+4],16));i+=4
   elif c in '01234567':
    start=i-1;limit=3 if c<'4' else 2
    while i<len(literal) and i-start<limit and literal[i] in '01234567':i+=1
    out.append(int(literal[start:i],8))
   else:out.append(ord({'n':'\n','r':'\r','t':'\t','b':'\b','f':'\f','\\':'\\','"':'"',"'":"'"}[c]))
 return out
X=literals(re.search(r'char X\[\] = \((.*?)\)\.toCharArray',s,re.S)[1])
Y=literals(re.search(r'char Y\[\] = \((.*?)\)\.toCharArray',s,re.S)[1])
a=literals(re.search(r'String A_DATA =\s*(.*?);',s,re.S)[1]);A=[hi<<16|lo for hi,lo in zip(a[::2],a[1::2])]
assert len(X)==2048 and len(A)==1056
lowerbody=s.split('int toLowerCase(int ch)',1)[1].split('int toUpperCase(int ch)',1)[0]
upperbody=s.split('int toUpperCase(int ch)',1)[1].split('int toTitleCase(int ch)',1)[0]
lower={int(a,16):int(b,16) for a,b in re.findall(r'case 0x([0-9A-F]+): mapChar = 0x([0-9A-F]+); break;',lowerbody)}
upper={int(a,16):int(b,16) for a,b in re.findall(r'case 0x([0-9A-F]+): mapChar = 0x([0-9A-F]+); break;',upperbody)}
tables={0:(X,Y,A)}
for plane in [1,2,3,14]:
 t=text(f'java.base/java/lang/CharacterData{plane:02X}.java')
 x=literals(re.search(r'char X\[\] = \((.*?)\)\.toCharArray',t,re.S)[1])
 y=literals(re.search(r'char Y\[\] = \((.*?)\)\.toCharArray',t,re.S)[1])
 d=literals(re.search(r'String A_DATA =\s*(.*?);',t,re.S)[1]);a=[hi<<16|lo for hi,lo in zip(d[::2],d[1::2])]
 n=int(re.search(r'int A\[\] = new int\[([0-9]+)\]',t)[1])
 assert len(x)==2048 and len(a)==n
 for method in ['toUpperCase','toLowerCase']:
  body=t.split(f'int {method}(int ch)',1)[1].split('return ',1)[0]
  assert 'switch' not in body and '0x07FC0000' not in body
 tables[plane]=(x,y,a)
def props(c):
 x,y,a=tables[c>>16];c &=65535
 return a[y[x[c>>5]|((c>>1)&15)]|(c&1)]
def signed32(n):return n-2**32 if n&2**31 else n
def tocase(c,uppercase):
 if c<=255:
  if uppercase:
   if c<97:return c
   u=c&0xdf
   return u if u<=90 or 0xc0<=u<=0xde and u!=0xd7 else 0x178 if c==0xff else 0x39c if c==0xb5 else c
  if c<65:return c
  lo=c|0x20
  return lo if lo<=122 or 0xe0<=lo<=0xfe and lo!=0xf7 else c
 if c>>16 not in tables:return c
 v=props(c)
 if not v&(0x10000 if uppercase else 0x20000):return c
 if c<65536 and v&0x7fc0000==0x7fc0000:return (upper if uppercase else lower).get(c,c)
 delta=signed32((v<<5)&0xffffffff)>>23
 return c-delta if uppercase else c+delta
result=[]
for c in range(128,0x110000):
 folded=tocase(tocase(c,True),False)
 if 0<=folded<128:result.append({'codepoint':f'U+{c:04X}','upper':f'U+{tocase(c,True):04X}','folded_ascii':chr(folded),'properties':f'0x{props(c):08x}'})
assert all(tocase(c,True)>=0x10000 and tocase(c,False)>=0x10000 for c in range(0x10000,0x110000))
assert all(tocase(c,True)==c and tocase(c,False)==c for c in range(0xd800,0xe000))
assert [x['codepoint'] for x in result]==['U+0130','U+0131','U+017F','U+212A']
(output / 'case-audit.json').write_text(json.dumps(result,indent=2)+'\n')


for path in ['jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java', 'java.base/java/nio/charset/Charset.java', 'java.base/sun/nio/cs/StandardCharsets.java', 'java.base/java/lang/String.java', 'java.base/java/lang/StringLatin1.java', 'java.base/java/lang/StringUTF16.java', 'java.base/java/lang/Character.java', 'java.base/java/lang/CharacterData.java', 'java.base/java/lang/CharacterDataLatin1.java', 'java.base/java/lang/CharacterDataPrivateUse.java', 'java.base/java/lang/CharacterDataUndefined.java']:
    text(path)
assert 'String.CASE_INSENSITIVE_ORDER' in source['jdk.charsets/sun/nio/cs/ext/AbstractCharsetProvider.java'].decode()
comparison = source['java.base/java/lang/StringUTF16.java'].decode().split('private static int compareCodePointCI', 1)[1].split('private static int codePointIncluding', 1)[0]
assert comparison.index('Character.toUpperCase') < comparison.index('Character.toLowerCase')
(output / 'sources.sha256').write_text(''.join(f'{hashlib.sha256(source[path]).hexdigest()}  {path}\n' for path in sorted(used)))
(output / 'COPYRIGHT').write_bytes(b'\n\n'.join(path.encode() + b'\n\n' + source[path][:source[path].index(b'*/') + 2] for path in sorted(used)) + b'\n')
print('Source-only derivation:', len(rows), 'extension keys,', len(result), 'non-ASCII equivalents,', len(used), 'source files')
