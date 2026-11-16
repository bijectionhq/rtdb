use rtdb_types::{Result,UriParts,decode_java_utf8};
use super::{invalid,tagged};
use rtdb_format::transit::TransitValue as T;

#[derive(Clone,Debug,PartialEq,Eq)]
pub(crate) enum Path { Unix(Vec<u8>),Jrt(String) }

impl Path {
    pub(super) fn read(text: &str) -> Result<Self> {
        let uri = UriParts::parse(text)?;
        let Some(scheme) = uri.scheme() else { return Ok(Self::Unix(unix_string(text)?)); };
        let rest = text.get(scheme.len() + 1..).ok_or_else(invalid)?;
        if (rest.starts_with("//") && !rest.starts_with("///")) || uri.query().is_some() || uri.fragment().is_some() { return Err(invalid()); }
        let decoded = uri.path().ok_or_else(invalid)?;
        if decoded.is_empty() { return Err(invalid()); }
        if scheme.eq_ignore_ascii_case("file") {
            if text.starts_with("file:///") {
                let raw = &rest[2..];
                let raw = if raw.len() > 1 { raw.strip_suffix('/').unwrap_or(raw) } else { raw };
                let mut bytes = Vec::with_capacity(raw.len());let mut index = 0;
                while index < raw.len() {
                    let value = raw.as_bytes()[index];
                    let value = if value == b'%' {
                        let value = (hex(raw.as_bytes()[index + 1]) << 4) | hex(raw.as_bytes()[index + 2]);index += 3;value
                    } else { if !value.is_ascii() { return Err(invalid()); }index += 1;value };
                    if value == 0 { return Err(invalid()); }
                    if value != b'/' || bytes.last() != Some(&b'/') { bytes.push(value); }
                }
                return Ok(Self::Unix(bytes));
            }
            return Ok(Self::Unix(unix_string(&decoded)?));
        }
        if scheme.eq_ignore_ascii_case("jrt") {
            if !decoded.starts_with('/') || decoded.contains("..") { return Err(invalid()); }
            return Ok(Self::Jrt(jrt_string(&format!("/modules{decoded}"))?));
        }
        Err(invalid())
    }
    pub(super) fn wire(&self) -> T { tagged("xtdb/path",T::String(self.text())) }
    pub(super) fn text(&self) -> String { match self { Self::Unix(value) => decode_java_utf8(value),Self::Jrt(value) => value.clone() } }
    pub(super) fn java_hash(&self) -> i32 {
        match self {
            Self::Unix(value) => value.iter().fold(0i32,|hash,byte|hash.wrapping_mul(31).wrapping_add(i32::from(*byte))),
            Self::Jrt(value) => super::hash::string(value),
        }
    }
}
fn hex(byte: u8) -> u8 { match byte { b'0'..=b'9' => byte - b'0',b'a'..=b'f' => byte - b'a' + 10,_ => byte - b'A' + 10 } }
fn unix_string(text: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(text.len());
    for byte in text.bytes() {
        if byte == 0 { return Err(invalid()); }
        if byte != b'/' || bytes.last() != Some(&b'/') { bytes.push(byte); }
    }
    if bytes.len() > 1 && bytes.last() == Some(&b'/') { bytes.pop(); }Ok(bytes)
}
fn jrt_string(text: &str) -> Result<String> {
    let mut previous = '\0';
    for (index,ch) in text.char_indices() {
        let start = if ch == '\\' || ch == '\0' { Some(index) } else if ch == '/' && previous == '/' { Some(index - 1) } else { None };
        if let Some(start) = start {
            let mut output = text[..start].to_owned();let mut previous = '\0';
            for ch in text[start..].chars() {
                let ch = if ch == '\\' { '/' } else { ch };
                if ch == '\0' { return Err(invalid()); }
                if ch != '/' || previous != '/' { output.push(ch);previous = ch; }
            }
            if output.len() > 1 && output.ends_with('/') { output.pop(); }return Ok(output);
        }
        previous = ch;
    }
    Ok((if text.len() > 1 { text.strip_suffix('/').unwrap_or(text) } else { text }).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unix_uri_spelling_selects_raw_bytes_or_decoded_text_before_normalization() {
        for (input,output) in [("a//b/","a/b"),("a/../b","a/../b"),("a%2Fb","a%2Fb"),("a?b#c","a?b#c"),("file:/a%20b","/a b"),("file:///a%2Fb","/a/b"),("file:///a///","/a/"),("file:///a%2F","/a/"),("FILE:///é","/é")] { assert_eq!(Path::read(input).unwrap().text(),output,"{input}"); }
        assert_ne!(Path::read("file:///%ff").unwrap(),Path::read("file:/%ff").unwrap());
        assert_eq!(Path::read("file:///%ff").unwrap().text(),Path::read("file:/%ff").unwrap().text());
        assert_eq!(Path::read("file:///%ed%a0%80").unwrap().text(),"/\u{fffd}");
    }
    #[test]
    fn runtime_image_paths_keep_their_provider_kind_and_normalization() {
        assert_eq!(Path::read("jrt:/java.base").unwrap().text(),"/modules/java.base");
        assert_eq!(Path::read("jrt:/a%5Cb//").unwrap().text(),"/modules/a/b");
        assert_eq!(Path::read("jrt:/a/%5Cb").unwrap().text(),"/modules/a//b");
        assert_ne!(Path::read("jrt:/java.base").unwrap(),Path::read("/modules/java.base").unwrap());
    }
    #[test]
    fn uri_and_provider_constraints_apply_before_path_publication() {
        for text in ["a b","file:///é","file://host/a","file:a","file:/a?","file:/a#","file:/%00","file:///%00","jrt:/a..b","jrt:/%2e%2e","jrt:/%00","jrt:foo","https://example.com/a"] { assert!(Path::read(text).is_err(),"{text}"); }
    }
}
