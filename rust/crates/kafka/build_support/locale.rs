use std::{collections::BTreeSet, fmt::Write};

#[path = "locale_class.rs"]
mod class;

type Result<T> = std::result::Result<T, String>;

pub const ARCHIVE_SHA256: &str = "953468a698924bbf0a70b14cc900365d8311028209cf58473e0678a7f2042458";
pub const MAX_ARCHIVE_BYTES: usize = 33_554_432;

fn text(bytes: &[u8]) -> Result<&str> {
    let end = bytes.iter().position(|byte| *byte == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).map_err(|_| "Locale archive header is not UTF8".into())
}
fn octal(bytes: &[u8]) -> Result<usize> {
    usize::from_str_radix(text(bytes)?.trim(), 8).map_err(|_| "Locale archive header has an invalid octal field".into())
}

fn contents(bytes: &[u8]) -> Result<Vec<(String, &[u8])>> {
    if bytes.len() > MAX_ARCHIVE_BYTES || bytes.len() % 512 != 0 { return Err("Locale archive exceeds its bound or lacks block alignment".into()); }
    let mut entries = Vec::new();
    let mut names = BTreeSet::new();
    let mut position = 0usize;
    loop {
        let header = bytes.get(position..position + 512).ok_or("Locale archive lacks its terminal zero blocks")?;
        if header.iter().all(|byte| *byte == 0) {
            if bytes.len() - position < 1024 || bytes[position..].iter().any(|byte| *byte != 0) { return Err("Locale archive has an invalid terminal block".into()); }
            break;
        }
        if entries.len() >= 4096 { return Err("Locale archive exceeds its entry bound".into()); }
        let actual = header.iter().enumerate().map(|(index, byte)| if (148..156).contains(&index) { 32 } else { usize::from(*byte) }).sum::<usize>();
        if octal(&header[148..156])? != actual || &header[257..263] != b"ustar\0" || &header[263..265] != b"00" { return Err("Locale archive header checksum or USTAR version is invalid".into()); }
        if header[156] != 0 && header[156] != b'0' || !text(&header[157..257])?.is_empty() { return Err("Locale archive contains a nonregular entry".into()); }
        let prefix = text(&header[345..500])?;
        let short = text(&header[..100])?;
        let name = if prefix.is_empty() { short.to_owned() } else { format!("{prefix}/{short}") };
        if name.starts_with('/') || name.split('/').any(|part| part.is_empty() || part == "." || part == "..") || !names.insert(name.clone()) { return Err("Locale archive entry name is invalid or duplicated".into()); }
        let length = octal(&header[124..136])?;
        if length > 1_048_576 { return Err("Locale archive entry exceeds one MiB".into()); }
        let start = position + 512;
        let end = start.checked_add(length).ok_or("Locale archive entry length overflow")?;
        let body = bytes.get(start..end).ok_or("Truncated locale archive entry")?;
        entries.push((name, body));
        position = end.checked_add(511).ok_or("Locale archive padding overflow")? / 512 * 512;
    }
    Ok(entries)
}

fn data_bytes(value: &class::Data) -> Result<usize> {
    let strings: &[String] = match value { class::Data::Text(value) => std::slice::from_ref(value), class::Data::Array(values) => values };
    strings.iter().try_fold(64usize, |bytes, value| bytes.checked_add(value.len()).and_then(|bytes| bytes.checked_add(64)).ok_or_else(|| "Locale resource size overflow".into()))
}

struct SourceText(String);
impl std::fmt::Write for SourceText {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        if self.0.len().checked_add(value.len()).is_none_or(|length| length > 268_435_456) { return Err(std::fmt::Error); }
        self.0.push_str(value);
        Ok(())
    }
}

fn write_data(out: &mut SourceText, value: &class::Data) -> std::fmt::Result {
    match value {
        class::Data::Text(value) => write!(out, "LocaleValue::Text({value:?})"),
        class::Data::Array(values) => write!(out, "LocaleValue::Array(&{values:?})"),
    }
}

pub fn source(bytes: &[u8]) -> Result<String> {
    let mut bundles = Vec::new();
    let mut decoded_bytes = 0usize;
    let mut names = BTreeSet::new();
    for (entry, bytes) in contents(bytes)? {
        let Some((module, path)) = entry.split_once('/') else { return Err("Locale archive entry lacks its module".into()); };
        if !matches!(module, "java.base" | "jdk.localedata") { return Err("Locale archive has an unexpected module".into()); }
        if path.starts_with("legal/") { continue; }
        let Some(path) = path.strip_prefix("classes/").and_then(|path| path.strip_suffix(".class")) else { return Err("Locale archive contains unexpected content".into()); };
        if !(path.starts_with("sun/text/resources/") && path.rsplit('/').next().is_some_and(|name| name.starts_with("FormatData"))
            || path.starts_with("sun/util/resources/") && path.rsplit('/').next().is_some_and(|name| name.starts_with("TimeZoneNames") || name == "CalendarData")
            || matches!(path, "sun/util/cldr/CLDRBaseLocaleDataMetaInfo" | "sun/util/cldr/CLDRBaseLocaleDataMetaInfo$CLDRMapHolder" | "sun/util/resources/cldr/provider/CLDRLocaleDataMetaInfo")) {
            return Err("Locale archive class is outside the retained data families".into());
        }
        let bundle = class::decode(bytes).map_err(|cause| format!("{entry}: {cause}"))?;
        if bundle.name != path || !names.insert(bundle.name.clone()) { return Err("Locale resource identity does not match its archive entry".into()); }
        for (key, value) in bundle.entries.iter().chain(bundle.maps.values().flatten()) {
            decoded_bytes = decoded_bytes.checked_add(key.len()).and_then(|bytes| bytes.checked_add(data_bytes(value).ok()?)).filter(|bytes| *bytes <= 134_217_728).ok_or("Expanded locale resources exceed 128 MiB")?;
        }
        bundles.push(bundle);
    }
    if bundles.is_empty() { return Err("Locale archive has no resource bundles".into()); }
    bundles.sort_by(|left, right| left.name.cmp(&right.name));
    emit(bundles).map_err(|_| "Generated locale source exceeds 256 MiB".into())
}

fn emit(bundles: Vec<class::Bundle>) -> std::result::Result<String, std::fmt::Error> {
    let mut out = SourceText(String::new());
    writeln!(out, "pub(super) const LOCALE_DATA_SHA256: &str = {ARCHIVE_SHA256:?};")?;
    out.write_str("static LOCALE_BUNDLES: &[LocaleBundle] = &[\n")?;
    for bundle in bundles {
        writeln!(out, "LocaleBundle {{ name: {:?}, entries: &[", bundle.name)?;
        for (key, value) in bundle.entries {
            write!(out, "({key:?},")?; write_data(&mut out, &value)?; out.write_str("),\n")?;
        }
        out.write_str("], maps: &[\n")?;
        for (name, entries) in bundle.maps {
            writeln!(out, "({name:?}, &[")?;
            for (key, value) in entries { write!(out, "({key:?},")?; write_data(&mut out, &value)?; out.write_str("),\n")?; }
            out.write_str("]),\n")?;
        }
        writeln!(out, "], available: {:?}, nonlikely_script: {:?} }},", bundle.available, bundle.nonlikely_script)?;
    }
    out.write_str("];\n")?;
    Ok(out.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_reader_requires_complete_terminal_blocks_and_bounded_headers() {
        assert!(contents(&vec![0; 512]).is_err());
        assert!(contents(&vec![0; 1024]).unwrap().is_empty());
        let mut nonterminal = vec![0; 1536];
        nonterminal[1024] = 1;
        assert!(contents(&nonterminal).is_err());
        assert!(source(&vec![0; 1024]).is_err());
    }
}
