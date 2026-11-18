use fancy_regex::java_uppercase_unit;
use rtdb_types::{Result, UriParts};

use super::invalid;

#[derive(Clone, Debug)]
pub(crate) struct WindowsPath {
    text: String,
    root_bytes: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Root { Unc, Absolute, DriveRelative, DirectoryRelative, Relative }

impl WindowsPath {
    pub(super) fn from_uri(uri: UriParts<'_>) -> Result<Self> {
        if uri.query().is_some() || uri.fragment().is_some() { return Err(invalid()); }
        let path = uri.path().filter(|path| !path.is_empty()).ok_or_else(invalid)?;
        if uri.raw_authority().is_some() {
            let host = uri.host().ok_or_else(invalid)?;
            if uri.raw_user_info().is_some() || uri.port().is_some() { return Err(invalid()); }
            let mut unc = String::from("\\\\");
            if let Some(ipv6) = host.strip_prefix('[').and_then(|host| host.strip_suffix(']')) {
                unc.extend(ipv6.chars().map(|unit| match unit { ':' => '-', '%' => 's', unit => unit }));
                unc.push_str(".ipv6-literal.net");
            } else {
                unc.push_str(host);
            }
            unc.push_str(&path);
            return Self::parse(&unc);
        }
        if path.encode_utf16().nth(2) == Some(u16::from(b':')) {
            return Self::parse(path.strip_prefix('/').unwrap_or(&path));
        }
        Self::parse(&path)
    }

    pub(super) fn parse(input: &str) -> Result<Self> {
        let (expected, input) = match input.strip_prefix("\\\\?\\") {
            Some(path) => match path.strip_prefix("UNC\\") {
                Some(path) => (Some(Root::Unc), path),
                None => (Some(Root::Absolute), path),
            },
            None => (None, input),
        };
        let mut text = String::new();
        let (kind, remaining) = if expected == Some(Root::Unc) || input.bytes().take(2).filter(|&byte| is_separator(char::from(byte))).count() == 2 {
            let (host, after_host) = take_name(input)?;
            let (share, rest) = take_name(after_host)?;
            text.push_str("\\\\");
            text.push_str(host);
            text.push('\\');
            text.push_str(share);
            text.push('\\');
            (Root::Unc, rest)
        } else if matches!(input.as_bytes(), [drive, b':', ..] if drive.is_ascii_alphabetic()) {
            let (drive, rest) = input.split_at(2);
            text.push_str(drive);
            if rest.starts_with(is_separator) {
                text.push('\\');
                (Root::Absolute, rest)
            } else {
                (Root::DriveRelative, rest)
            }
        } else if input.starts_with(is_separator) {
            text.push('\\');
            (Root::DirectoryRelative, input)
        } else {
            (Root::Relative, input)
        };
        if expected.is_some_and(|expected| expected != kind) { return Err(invalid()); }
        let root_bytes = text.len();
        for part in remaining.split(is_separator).filter(|part| !part.is_empty()) {
            if part.ends_with(' ') || invalid_name(part) { return Err(invalid()); }
            if text.len() > root_bytes { text.push('\\'); }
            text.push_str(part);
        }
        Ok(Self { text, root_bytes })
    }

    pub(super) fn text(&self) -> &str { &self.text }

    pub(super) fn is_root_only(&self) -> bool { self.root_bytes != 0 && self.root_bytes == self.text.len() }

    fn uppercase_units(&self) -> impl Iterator<Item = u16> + '_ {
        self.text.encode_utf16().map(java_uppercase_unit)
    }

    pub(super) fn java_hash(&self) -> i32 {
        self.uppercase_units().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit)))
    }
}

impl PartialEq for WindowsPath {
    fn eq(&self, other: &Self) -> bool { self.uppercase_units().eq(other.uppercase_units()) }
}

impl Eq for WindowsPath {}

fn is_separator(unit: char) -> bool { matches!(unit, '/' | '\\') }

fn invalid_name(text: &str) -> bool {
    text.bytes().any(|unit| unit < b' ' || b"<>:\"|?*".contains(&unit))
}

fn take_name(input: &str) -> Result<(&str, &str)> {
    let input = input.trim_start_matches(is_separator);
    let (name, rest) = input.split_at(input.find(is_separator).unwrap_or(input.len()));
    if name.is_empty() || invalid_name(name) { return Err(invalid()); }
    Ok((name, rest))
}
