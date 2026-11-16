use std::borrow::Cow;
use unicode_normalization::UnicodeNormalization;
use crate::{Anomaly, Result};

/// Validated components of a Java URI spelling, borrowed without normalization or name resolution.
/// Parsing allocates no component storage; decoded accessors allocate at most the raw component's byte length.
#[derive(Clone, Copy, Debug)]
pub struct UriParts<'a> {
    text: &'a str,
    scheme: Option<&'a str>,
    user_info: Option<&'a str>,
    host: Option<&'a str>,
    port: Option<i32>,
    body: UriBody<'a>,
    fragment: Option<&'a str>,
}

#[derive(Clone, Copy, Debug)]
enum UriBody<'a> {
    Opaque(&'a str),
    Hierarchical { authority: Option<&'a str>, path: &'a str, query: Option<&'a str> },
}

impl<'a> UriParts<'a> {
    /// Applies OpenJDK 25 URI(String) syntax, including registry-authority fallback.
    /// Opaque URIs have no path or query; absent and empty components remain distinct.
    /// The input must be representable as UTF-8; isolated UTF-16 surrogates need a wider carrier.
    pub fn parse(text: &'a str) -> Result<Self> {
        let (body, fragment) = text.split_once('#').map_or((text, None), |(body, fragment)| (body, Some(fragment)));
        let mut parts = Self { text, scheme: None, user_info: None, host: None, port: None, body: UriBody::Hierarchical { authority: None, path: "", query: None }, fragment };
        let mut rest = body;
        if let Some(index) = body.find([':', '/', '?']) {
            if body.as_bytes()[index] == b':' {
                let scheme = &body[..index];
                if !scheme.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
                    || !scheme.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"+-.".contains(&byte)) {
                    return Err(invalid("Illegal scheme name"));
                }
                parts.scheme = Some(scheme);
                rest = &body[index + 1..];
                if !rest.starts_with('/') {
                    if rest.is_empty() { return Err(invalid("Expected scheme-specific part")); }
                    check(rest, Component::Uric)?;
                    if let Some(fragment) = fragment { check(fragment, Component::Uric)?; }
                    parts.body = UriBody::Opaque(rest);
                    return Ok(parts);
                }
            }
        }
        let mut raw_authority = None;
        if let Some(authority_and_path) = rest.strip_prefix("//") {
            let end = authority_and_path.find(['/', '?']).unwrap_or(authority_and_path.len());
            let authority = &authority_and_path[..end];
            rest = &authority_and_path[end..];
            if authority.is_empty() {
                if rest.is_empty() && fragment.is_none() { return Err(invalid("Expected authority")); }
            } else {
                raw_authority = Some(authority);
                let (user_info, host, port) = authority_parts(authority)?;
                parts.user_info = user_info;
                parts.host = host;
                parts.port = port;
            }
        }
        let (path, query) = rest.split_once('?').map_or((rest, None), |(path, query)| (path, Some(query)));
        check(path, Component::Path)?;
        if let Some(query) = query { check(query, Component::Uric)?; }
        if let Some(fragment) = fragment { check(fragment, Component::Uric)?; }
        parts.body = UriBody::Hierarchical { authority: raw_authority, path, query };
        Ok(parts)
    }

    /// Bounds normalization scratch and the returned ASCII string, excluding the borrowed input.
    /// Retain the charge through encoding until the returned string acquires its own owner.
    pub fn ascii_allocation_bound(&self) -> Result<usize> {
        self.text.len().checked_mul(128).and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| invalid("URI ASCII rendering exceeds addressable memory"))
    }

    /// Applies Java URI.toASCIIString: Unicode 16 NFC followed by UTF-8 percent encoding.
    /// ASCII input keeps its spelling; callers admit `ascii_allocation_bound` before encoding.
    pub fn to_ascii_string(&self) -> String {
        if self.text.is_ascii() { return self.text.into(); }
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let mut text = String::new();
        for character in self.text.nfc() {
            let mut bytes = [0;4];
            for byte in character.encode_utf8(&mut bytes).bytes() {
                if byte.is_ascii() { text.push(char::from(byte)); }
                else { text.push('%'); text.push(char::from(HEX[usize::from(byte >> 4)])); text.push(char::from(HEX[usize::from(byte & 15)])); }
            }
        }
        text
    }

    pub fn scheme(&self) -> Option<&'a str> { self.scheme }
    /// Host spelling is preserved, including IPv6 brackets and scope identifiers; registry authorities have no host.
    pub fn host(&self) -> Option<&'a str> { self.host }
    pub fn port(&self) -> Option<i32> { self.port }
    /// Decodes percent escapes as Java UTF-8, replacing malformed byte sequences; '+' remains literal.
    pub fn user_info(&self) -> Option<Cow<'a, str>> { self.user_info.map(decode) }
    /// Uses the same percent-decoding contract as `user_info`.
    pub fn path(&self) -> Option<Cow<'a, str>> { match self.body { UriBody::Hierarchical { path, .. } => Some(decode(path)), UriBody::Opaque(_) => None } }
    /// Uses the same percent-decoding contract as `user_info`, including escapes inside brackets.
    pub fn query(&self) -> Option<Cow<'a, str>> { match self.body { UriBody::Hierarchical { query, .. } => query.map(decode), UriBody::Opaque(_) => None } }
    /// Uses the same percent-decoding contract as `query`.
    pub fn fragment(&self) -> Option<Cow<'a, str>> { self.fragment.map(decode) }

    /// Java URI object equality, folding ASCII scheme/host case and raw percent-escape hex case.
    /// Escapes remain encoded and path segments remain unnormalized; vector hashes have a separate byte contract.
    pub fn equivalent(&self, other: &Self) -> bool {
        if !ascii_equal(self.scheme,other.scheme) || !raw_equal(self.fragment,other.fragment) { return false; }
        match (self.body,other.body) {
            (UriBody::Opaque(left),UriBody::Opaque(right)) => raw_equal(Some(left),Some(right)),
            (UriBody::Hierarchical { authority: left,path: lp,query: lq },UriBody::Hierarchical { authority: right,path: rp,query: rq }) => {
                raw_equal(Some(lp),Some(rp)) && raw_equal(lq,rq)
                    && if self.host.is_some() && other.host.is_some() {
                        raw_equal(self.user_info,other.user_info) && ascii_equal(self.host,other.host) && self.port == other.port
                    } else { self.host.is_none() && other.host.is_none() && raw_equal(left,right) }
            }
            _ => false,
        }
    }

    /// Java URI object hash, using UTF-16 components and the same case folding as `equivalent`.
    pub fn java_hash_code(&self) -> i32 {
        let mut hash = hash_case(0,self.scheme);
        hash = hash_component(hash,self.fragment);
        match self.body {
            UriBody::Opaque(value) => hash_component(hash,Some(value)),
            UriBody::Hierarchical { authority,path,query } => {
                hash = hash_component(hash,Some(path));
                hash = hash_component(hash,query);
                if self.host.is_some() {
                    hash = hash_component(hash,self.user_info);
                    hash = hash_case(hash,self.host);
                    hash.wrapping_add(1949i32.wrapping_mul(self.port.unwrap_or(-1)))
                } else { hash_component(hash,authority) }
            }
        }
    }
}

fn hash_case(hash: i32,value: Option<&str>) -> i32 {
    value.map_or(hash,|value|value.encode_utf16().fold(hash,|hash,ch| {
        let ch = if (u16::from(b'A')..=u16::from(b'Z')).contains(&ch) { ch + 32 } else { ch };
        hash.wrapping_mul(31).wrapping_add(i32::from(ch))
    }))
}
fn hash_component(hash: i32,value: Option<&str>) -> i32 {
    let Some(value) = value else { return hash; };
    let mut escaped = 0;let mut component = 0i32;
    for ch in value.encode_utf16() {
        let ch = if escaped != 0 {
            escaped -= 1;if (u16::from(b'a')..=u16::from(b'f')).contains(&ch) { ch - 32 } else { ch }
        } else { if ch == u16::from(b'%') { escaped = 2; }ch };
        component = component.wrapping_mul(31).wrapping_add(i32::from(ch));
    }
    hash.wrapping_mul(127).wrapping_add(component)
}

fn ascii_equal(left: Option<&str>,right: Option<&str>) -> bool {
    match (left,right) { (Some(left),Some(right)) => left.eq_ignore_ascii_case(right), (None,None) => true, _ => false }
}
fn raw_equal(left: Option<&str>,right: Option<&str>) -> bool {
    let (left,right) = match (left,right) { (Some(left),Some(right)) => (left.as_bytes(),right.as_bytes()), (None,None) => return true, _ => return false };
    if left.len() != right.len() { return false; }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] { return false; }
        if left[index] == b'%' {
            if !left[index + 1..index + 3].eq_ignore_ascii_case(&right[index + 1..index + 3]) { return false; }
            index += 3;
        } else { index += 1; }
    }
    true
}

#[derive(Clone, Copy)]
enum Component { Path, Uric, User, Registry, Server }

fn ascii_allowed(byte: u8, component: Component) -> bool {
    byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) || match component {
        Component::Path => b":@&=+$,;/".contains(&byte),
        Component::Uric => b";/?:@&=+$,[]".contains(&byte),
        Component::User => b";:&=+$,".contains(&byte),
        Component::Registry => b";:@&=+$,".contains(&byte),
        Component::Server => b";:@&=+$,[]%".contains(&byte),
    }
}

fn scan(text: &str, component: Component) -> Result<usize> {
    let mut offset = 0;
    while offset < text.len() {
        let byte = text.as_bytes()[offset];
        if ascii_allowed(byte, component) { offset += 1; }
        else if byte == b'%' {
            if text.as_bytes().get(offset + 1..offset + 3).is_none_or(|pair| !pair.iter().all(u8::is_ascii_hexdigit)) {
                return Err(invalid("Malformed escape pair"));
            }
            offset += 3;
        } else if byte >= 128 {
            let character = text[offset..].chars().next().ok_or_else(|| invalid("Missing URI character"))?;
            if matches!(character as u32, 0x80..=0x9f | 0xa0 | 0x1680 | 0x2000..=0x200a | 0x2028 | 0x2029 | 0x202f | 0x205f | 0x3000) { break; }
            offset += character.len_utf8();
        } else { break; }
    }
    Ok(offset)
}

fn check(text: &str, component: Component) -> Result<()> {
    if scan(text, component)? == text.len() { Ok(()) } else { Err(invalid("Illegal URI character")) }
}

type Authority<'a> = (Option<&'a str>, Option<&'a str>, Option<i32>);
fn authority_parts(text: &str) -> Result<Authority<'_>> {
    let registry = scan(text, Component::Registry)? == text.len();
    let server = scan(text, Component::Server)? == text.len();
    if server {
        if let Some((user, host, port)) = server_parts(text) { return Ok((user, Some(host), port)); }
    }
    if registry { Ok((None, None, None)) } else { Err(invalid("Malformed authority")) }
}

fn server_parts(text: &str) -> Option<(Option<&str>, &str, Option<i32>)> {
    let (user, rest) = if let Some((user, rest)) = text.split_once('@') {
        if scan(user, Component::User).ok()? != user.len() { return None; }
        (Some(user), rest)
    } else { (None, text) };
    let (host, port) = if rest.starts_with('[') {
        let close = rest.find(']')?;
        let address = &rest[1..close];
        let address = if let Some((address, scope)) = address.split_once('%') {
            if scope.is_empty() || !scope.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"_.".contains(&byte)) { return None; }
            address
        } else { address };
        if !ipv6(address) { return None; }
        (&rest[..=close], &rest[close + 1..])
    } else {
        let end = rest.find(':').unwrap_or(rest.len());
        let host = &rest[..end];
        if !ipv4(host) && !hostname(host) { return None; }
        (host, &rest[end..])
    };
    let port = if port.is_empty() || port == ":" { None } else {
        let digits = port.strip_prefix(':')?;
        if !digits.bytes().all(|byte| byte.is_ascii_digit()) { return None; }
        Some(digits.bytes().try_fold(0i32, |value, byte| value.checked_mul(10)?.checked_add(i32::from(byte - b'0')))?)
    };
    Some((user, host, port))
}

fn hostname(text: &str) -> bool {
    let text = text.strip_suffix('.').unwrap_or(text);
    let multiple = text.contains('.');
    let mut final_label = "";
    for label in text.split('.') {
        if !label.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
            || !label.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
            || !label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-') { return false; }
        final_label = label;
    }
    !multiple || final_label.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
}

fn ipv4(text: &str) -> bool {
    let mut count = 0;
    for part in text.split('.') {
        if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) { return false; }
        if part.bytes().try_fold(0u8, |value, byte| value.checked_mul(10)?.checked_add(byte - b'0')).is_none() { return false; }
        count += 1;
        if count > 4 { return false; }
    }
    count == 4
}

fn ipv6(text: &str) -> bool {
    if text.is_empty() { return false; }
    let (left, right) = text.split_once("::").map_or((text, None), |(left, right)| (left, Some(right)));
    if right.is_some_and(|right| right.contains("::")) { return false; }
    let mut groups = 0;
    for (side, is_last) in [(left, right.is_none()), (right.unwrap_or(""), true)] {
        if side.is_empty() { continue; }
        let mut parts = side.split(':').peekable();
        while let Some(part) = parts.next() {
            if part.contains('.') {
                if !is_last || parts.peek().is_some() || !ipv4(part) { return false; }
                groups += 2;
            } else {
                if part.is_empty() || part.len() > 4 || !part.bytes().all(|byte| byte.is_ascii_hexdigit()) { return false; }
                groups += 1;
            }
            if groups > 8 { return false; }
        }
    }
    if right.is_some() { groups < 8 } else { groups == 8 }
}

fn decode(text: &str) -> Cow<'_, str> {
    if !text.contains('%') { return Cow::Borrowed(text); }
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('%') {
        output.push_str(&rest[..start]);
        rest = &rest[start..];
        let mut end = 0;
        while rest.as_bytes().get(end) == Some(&b'%') { end += 3; }
        let bytes = &rest.as_bytes()[..end];
        let byte = |index: usize| (hex(bytes[index * 3 + 1]) << 4) | hex(bytes[index * 3 + 2]);
        crate::utf8::append_java_utf8(&mut output,end / 3,byte);
        rest = &rest[end..];
    }
    output.push_str(rest);
    Cow::Owned(output)
}

fn hex(byte: u8) -> u8 { match byte { b'0'..=b'9' => byte - b'0', b'a'..=b'f' => byte - b'a' + 10, _ => byte - b'A' + 10 } }
fn invalid(reason: &str) -> Anomaly { Anomaly::incorrect("xtdb/invalid-uri", reason) }
