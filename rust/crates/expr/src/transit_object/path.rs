use rtdb_types::{Result,UriParts,decode_java_utf8};
use super::{invalid,tagged};
use rtdb_format::transit::TransitValue as T;
mod windows;
use windows::WindowsPath;

#[derive(Clone,Debug,PartialEq,Eq)]
pub(crate) enum Path { Unix(Vec<u8>), Windows(WindowsPath), Jrt(String) }

#[derive(Clone, Copy)]
enum FileProvider { Unix, Windows }

impl Path {
    pub(super) fn read_wire(representation: &T) -> Result<Self> {
        match representation { T::String(text) => Self::read(text), _ => Err(invalid()) }
    }
    pub(super) fn read(text: &str) -> Result<Self> {
        Self::read_with_provider(text, if cfg!(windows) { FileProvider::Windows } else { FileProvider::Unix })
    }
    fn read_with_provider(text: &str, provider: FileProvider) -> Result<Self> {
        let uri = UriParts::parse(text)?;
        let Some(scheme) = uri.scheme() else {
            return match provider {
                FileProvider::Unix => unix_string(text).map(Self::Unix),
                FileProvider::Windows => WindowsPath::parse(text).map(Self::Windows),
            };
        };
        if scheme.eq_ignore_ascii_case("file") && matches!(provider, FileProvider::Windows) {
            return WindowsPath::from_uri(uri).map(Self::Windows);
        }
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
    pub(super) fn text(&self) -> String {
        match self {
            Self::Unix(value) => decode_java_utf8(value),
            Self::Windows(value) => value.text().to_owned(),
            Self::Jrt(value) => value.clone(),
        }
    }
    pub(super) fn text_allocation_bound(&self) -> Option<usize> {
        match self {
            Self::Unix(value) => value.len().checked_mul(8)?.checked_add(64),
            Self::Windows(value) => Some(value.text().len()),
            Self::Jrt(value) => Some(value.len()),
        }
    }
    pub(super) fn is_root_only(&self) -> bool {
        match self {
            Self::Unix(value) => value.as_slice() == b"/",
            Self::Windows(value) => value.is_root_only(),
            Self::Jrt(value) => value == "/",
        }
    }
    pub(super) fn java_hash(&self) -> i32 {
        match self {
            Self::Unix(value) => value.iter().fold(0i32,|hash,byte|hash.wrapping_mul(31).wrapping_add(i32::from(*byte))),
            Self::Windows(value) => value.java_hash(),
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
    fn unix(text: &str) -> Result<Path> { Path::read_with_provider(text, FileProvider::Unix) }
    fn windows(text: &str) -> Result<Path> { Path::read_with_provider(text, FileProvider::Windows) }
    #[test]
    fn unix_uri_spelling_selects_raw_bytes_or_decoded_text_before_normalization() {
        for (input,output) in [("a//b/","a/b"),("a/../b","a/../b"),("a%2Fb","a%2Fb"),("a?b#c","a?b#c"),("file:/a%20b","/a b"),("file:///a%2Fb","/a/b"),("file:///a///","/a/"),("file:///a%2F","/a/"),("FILE:///é","/é")] { assert_eq!(unix(input).unwrap().text(),output,"{input}"); }
        assert_ne!(unix("file:///%ff").unwrap(),unix("file:/%ff").unwrap());
        assert_eq!(unix("file:///%ff").unwrap().text(),unix("file:/%ff").unwrap().text());
        assert_eq!(unix("file:///%ed%a0%80").unwrap().text(),"/\u{fffd}");
    }
    #[test]
    fn runtime_image_paths_keep_their_provider_kind_and_normalization() {
        assert_eq!(Path::read("jrt:/java.base").unwrap().text(),"/modules/java.base");
        assert_eq!(Path::read("jrt:/a%5Cb//").unwrap().text(),"/modules/a/b");
        assert_eq!(Path::read("jrt:/a/%5Cb").unwrap().text(),"/modules/a//b");
        assert_ne!(Path::read("jrt:/java.base").unwrap(),unix("/modules/java.base").unwrap());
    }
    #[test]
    fn uri_and_provider_constraints_apply_before_path_publication() {
        for text in ["a b","file:///é","file://host/a","file:a","file:/a?","file:/a#","file:/%00","file:///%00","jrt:/a..b","jrt:/%2e%2e","jrt:/%00","jrt:foo","https://example.com/a"] { assert!(unix(text).is_err(),"{text}"); }
    }

    #[test]
    fn windows_uri_construction_retains_drive_and_unc_roots() {
        for (input, output, root_only) in [
            ("relative//a/", r"relative\a", false),
            ("/", r"\", true),
            ("file:///C:/", r"C:\", true),
            ("file:/c:tail", r"c:tail", false),
            ("file:///C:/group%20dir/Shard/", r"C:\group dir\Shard", false),
            ("file://Box/Share/", r"\\Box\Share\", true),
            ("file:////box/share/a//b/", r"\\box\share\a\b", false),
            ("file://[fe80::1%eth0]/disk", r"\\fe80--1seth0.ipv6-literal.net\disk\", true),
            ("file:///C:/%5C%5Cname", r"C:\name", false),
        ] {
            let path = windows(input).unwrap();
            assert_eq!(path.text(), output, "{input}");
            assert_eq!(path.is_root_only(), root_only, "{input}");
        }
        for input in ["C:/drive", r"a\b", "file://box/", "file://bad_host/share", "file://@box/share",
                      "file://box:80/share", "file:///C:/trailing%20", "file:///C:/bad%3Fname", "file:///C:/bad%00name"] {
            assert!(windows(input).is_err(), "{input}");
        }
    }

    #[test]
    fn windows_paths_compare_and_hash_java_uppercase_code_units() {
        let lower = windows("file:///c:/straße/é").unwrap();
        let upper = windows("file:///C:/STRAßE/É").unwrap();
        assert_eq!(lower, upper);
        assert_ne!(lower.text(), upper.text());
        assert_eq!(lower.java_hash(), upper.java_hash());
        assert_ne!(lower, windows("file:///C:/STRASSE/É").unwrap());
        assert_ne!(windows("file:///C:/𐐨").unwrap(), windows("file:///C:/𐐀").unwrap());
        assert_ne!(windows("relative").unwrap(), unix("relative").unwrap());
        assert_ne!(windows("jrt:/a").unwrap(), windows("file:/modules/a").unwrap());
    }

    #[test]
    fn windows_parser_restricts_extended_prefixes_to_their_root_kind() {
        assert_eq!(WindowsPath::parse(r"\\?\C:\folder").unwrap().text(), r"C:\folder");
        assert_eq!(WindowsPath::parse(r"\\?\UNC\host\share").unwrap().text(), r"\\host\share\");
        assert!(WindowsPath::parse(r"\\?\relative").is_err());
        assert!(WindowsPath::parse(r"\\?\UNC\host").is_err());
        assert_eq!(WindowsPath::parse(r"\\host \share \").unwrap().text(), r"\\host \share \");
    }
}
