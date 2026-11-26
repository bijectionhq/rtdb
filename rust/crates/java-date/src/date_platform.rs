use std::{collections::BTreeMap,io::Read,path::{Path,PathBuf}};
use rtdb_types::{LegacyTimeZone,Result};
use super::{date_environment::DateEnvironmentInput,error};

pub(super) fn capture(initialized_at: i64,observed_at: i64,constructed_at: i64) -> Result<DateEnvironmentInput> {
    let locale = host_locale()?;
    let (language,script,region,variant) = posix_parts(&locale);
    let timezone = host_timezone(observed_at)?;
    Ok(DateEnvironmentInput { language,script,region,variant,unicode: BTreeMap::new(),timezone,initialized_at,observed_at,constructed_at })
}

/// The language of the JVM's `Locale.getDefault()`, which `String.toUpperCase()` and `toLowerCase()` use.
/// On Linux it comes from the startup environment's `LC_MESSAGES` category, so `LC_ALL`, then `LC_MESSAGES`, then `LANG`;
/// a `C` or `POSIX` locale, or one the host cannot load, gives `en`. On macOS it is the first preferred language.
/// - Errors: a host locale name over 4096 bytes or not UTF-8; on other hosts, always.
pub fn process_language() -> Result<String> { display_locale().map(|locale|posix_parts(&locale).0) }

#[cfg(target_os = "linux")]
fn host_locale() -> Result<String> { category_locale(libc::LC_CTYPE) }
#[cfg(target_os = "linux")]
fn display_locale() -> Result<String> { category_locale(libc::LC_MESSAGES) }
/// The name glibc's `_NL_LOCALE_NAME(category)` gives for the environment's locale, or `C` when it cannot be loaded.
#[cfg(target_os = "linux")]
fn category_locale(category: libc::c_int) -> Result<String> {
    struct Locale(libc::locale_t);
    impl Drop for Locale { fn drop(&mut self) { unsafe { libc::freelocale(self.0) }; } }
    let locale = unsafe { libc::newlocale(libc::LC_ALL_MASK,c"".as_ptr(),std::ptr::null_mut()) };
    if locale.is_null() { return Ok("C".into()); }
    let locale = Locale(locale);
    let name = unsafe { libc::nl_langinfo_l((category << 16) | 0xffff,locale.0) };
    bounded_c_string(name,4096).map(|value|if value.is_empty() { "C".into() } else { value })
}
#[cfg(target_os = "macos")]
fn host_locale() -> Result<String> { macos::locale().or_else(||std::env::var("LANG").ok()).map_or_else(||Ok("en".into()),|locale| {
    if locale.len() > 4096 { Err(limit()) } else { Ok(locale) }
}) }
#[cfg(target_os = "macos")]
fn display_locale() -> Result<String> { host_locale() }
#[cfg(not(any(target_os = "linux",target_os = "macos")))]
fn host_locale() -> Result<String> { Err(error("unsupported-date-environment","Process locale capture requires a Linux or macOS adapter; supply explicit DateEnvironmentInput")) }
#[cfg(not(any(target_os = "linux",target_os = "macos")))]
fn display_locale() -> Result<String> { host_locale() }

#[cfg(unix)]
fn bounded_c_string(value: *const libc::c_char,maximum: usize) -> Result<String> {
    if value.is_null() { return Ok(String::new()); }
    let length = unsafe { libc::strnlen(value,maximum + 1) };
    if length > maximum { return Err(limit()); }
    let bytes = unsafe { std::slice::from_raw_parts(value.cast::<u8>(),length) };
    String::from_utf8(bytes.to_vec()).map_err(|_|error("invalid-date-environment","Host locale identifier is not UTF-8"))
}

fn posix_parts(locale: &str) -> (String,String,String,String) {
    let locale = if cfg!(target_os = "linux") && matches!(locale,"C" | "POSIX") { "en_US" } else { locale };
    let (base,suffix) = locale.find(['.','@']).map_or((locale,""),|at|(&locale[..at],&locale[at..]));
    let base = ALIASES.iter().chain(if cfg!(target_os = "linux") { LINUX_ALIASES.iter() } else { [].iter() }).find(|(alias,_)|*alias == base).map_or(base,|(_,full)|*full);
    let (base,alias_suffix) = base.find(['.','@']).map_or((base,None),|at|(&base[..at],Some(&base[at..])));
    let suffix = alias_suffix.unwrap_or(suffix);
    let (language,region) = base.split_once('_').unwrap_or((base,""));
    let language = match language { "C" | "POSIX" => "en","cz" => "cs","sh" => "sr","su" => "fi",other => other };
    let region = match region { "RN" if cfg!(target_os = "linux") => "US","YU" => "CS",other => other };
    let modifier = suffix.split_once('@').map_or("",|(_,modifier)|modifier);
    let script = match modifier {
        "cyrillic" if cfg!(target_os = "linux") => "Cyrl","devanagari" if cfg!(target_os = "linux") => "Deva",
        "iqtelif" | "latin" if cfg!(target_os = "linux") => "Latn",
        "Arab" | "Cyrl" | "Deva" | "Ethi" | "Hans" | "Hant" | "Latn" | "Sund" | "Syrc" | "Tfng" => modifier,_ => "",
    };
    (language.into(),script.into(),region.into(),if modifier == "nynorsk" { "NY".into() } else { String::new() })
}

pub(super) fn host_timezone(now: i64) -> Result<String> {
    default_timezone_id(std::env::var("TZ").ok().as_deref(),Path::new("/etc/localtime"),Path::new("/usr/share/zoneinfo"),now)
}

/// The ID OpenJDK's `TimeZone.getDefault()` takes on Linux and macOS without `user.timezone`.
/// A non-empty `tz` wins; otherwise `localtime` names it, by the `zoneinfo/` part of its link or by the file under `zoneinfo` with the same bytes.
/// A leading `:`, and `posix/` on Linux, are dropped; no source gives `GMT`, and an unknown ID gives the host's `GMT±hh:mm` offset at `now` (Date milliseconds).
/// - Errors: zone data beyond the bounded capture limits, or a non-Unix host.
pub fn default_timezone_id(tz: Option<&str>,localtime: &Path,zoneinfo: &Path,now: i64) -> Result<String> {
    let mut zone = match tz.filter(|value|!value.is_empty()) {
        Some(value) => { if value.len() > 4096 { return Err(limit()); }Some(value.to_owned()) },
        None => platform_timezone(localtime,zoneinfo)?,
    };
    if let Some(value) = &mut zone {
        if value.starts_with(':') { value.remove(0); }
        if cfg!(target_os = "linux") && value.starts_with("posix/") { value.drain(..6); }
    }
    let Some(zone) = zone else { return Ok("GMT".into()); };
    let parsed = LegacyTimeZone::parse(&zone)?;
    if parsed.to_string() != "GMT" || zone == "GMT" { return Ok(zone); }
    host_offset(now)
}
fn platform_timezone(path: &Path,root: &Path) -> Result<Option<String>> {
    if let Ok(link) = std::fs::read_link(path) {
        let text = normalized_path(&link);
        if let Some((_,zone)) = text.split_once("zoneinfo/") { return Ok(Some(zone.into())); }
    }
    let Some(bytes) = read_zone(path)? else { return Ok(None); };
    for name in ["UTC","GMT"] {
        if read_zone(&root.join(name))?.is_some_and(|value|value == bytes) { return Ok(Some(name.into())); }
    }
    let mut remaining = 16_384usize;
    find_zone(root,root,&bytes,0,&mut remaining)
}
fn find_zone(root: &Path,directory: &Path,expected: &[u8],depth: usize,remaining: &mut usize) -> Result<Option<String>> {
    if depth > 16 { return Err(limit()); }
    let Ok(entries) = std::fs::read_dir(directory) else { return Ok(None); };
    for entry in entries {
        if *remaining == 0 { return Err(limit()); }*remaining -= 1;
        let Ok(entry) = entry else { continue; };
        let name = entry.file_name();let name = name.to_string_lossy();
        if name.starts_with('.') || matches!(name.as_ref(),"ROC" | "posixrules" | "localtime") { continue; }
        let path = entry.path();let Ok(metadata) = std::fs::metadata(&path) else { continue; };
        if metadata.is_dir() { if let Some(zone) = find_zone(root,&path,expected,depth + 1,remaining)? { return Ok(Some(zone)); } }
        else if metadata.is_file() && metadata.len() == expected.len() as u64 && read_zone(&path)?.is_some_and(|bytes|bytes == expected) {
            return Ok(path.strip_prefix(root).ok().and_then(|path|path.to_str()).map(str::to_owned));
        }
    }
    Ok(None)
}
#[cfg(unix)]
fn read_zone(path: &Path) -> Result<Option<Vec<u8>>> {
    use std::os::unix::fs::OpenOptionsExt;
    let Ok(file) = std::fs::OpenOptions::new().read(true).custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC).open(path) else { return Ok(None); };
    let Ok(metadata) = file.metadata() else { return Ok(None); };
    if !metadata.is_file() { return Ok(None); }
    if metadata.len() > 1 << 20 { return Err(limit()); }
    let mut bytes = Vec::with_capacity(metadata.len() as usize + 1);
    if file.take((1 << 20) + 1).read_to_end(&mut bytes).is_err() { return Ok(None); }
    if bytes.len() > 1 << 20 { return Err(limit()); }
    Ok(Some(bytes))
}
#[cfg(not(unix))]
fn read_zone(_path: &Path) -> Result<Option<Vec<u8>>> { Err(error("unsupported-date-environment","Host timezone files require a Unix capture adapter")) }
fn normalized_path(path: &Path) -> String {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component { std::path::Component::ParentDir => { result.pop(); },std::path::Component::CurDir => (),other => result.push(other.as_os_str()) }
    }
    result.to_string_lossy().into_owned()
}
#[cfg(unix)]
fn host_offset(now: i64) -> Result<String> {
    let time = libc::time_t::try_from(now.div_euclid(1000)).map_err(|_|limit())?;
    let mut local = std::mem::MaybeUninit::<libc::tm>::uninit();
    if unsafe { libc::localtime_r(&time,local.as_mut_ptr()) }.is_null() { return Ok("GMT".into()); }
    let local = unsafe { local.assume_init() };
    let mut output = [0 as libc::c_char;6];
    if unsafe { libc::strftime(output.as_mut_ptr(),output.len(),c"%z".as_ptr(),&local) } != 5 { return Ok("GMT".into()); }
    let value = bounded_c_string(output.as_ptr(),5)?;
    if value == "+0000" || value == "-0000" { Ok("GMT".into()) } else { Ok(format!("GMT{}:{}",&value[..3],&value[3..])) }
}
#[cfg(not(unix))]
fn host_offset(_now: i64) -> Result<String> { Err(error("unsupported-date-environment","Host timezone fallback requires a native platform adapter")) }
fn limit() -> rtdb_types::Anomaly { error("invalid-date-environment","Host locale or timezone data exceeds its bounded capture limits") }

const ALIASES: &[(&str,&str)] = &[
    ("ar","ar_EG"),("be","be_BY"),("bg","bg_BG"),("br","br_FR"),("ca","ca_ES"),("cs","cs_CZ"),("cz","cs_CZ"),
    ("da","da_DK"),("de","de_DE"),("el","el_GR"),("en","en_US"),("eo","eo"),("es","es_ES"),("et","et_EE"),("eu","eu_ES"),
    ("fi","fi_FI"),("fr","fr_FR"),("ga","ga_IE"),("gl","gl_ES"),("he","iw_IL"),("hr","hr_HR"),("hu","hu_HU"),
    ("id","in_ID"),("in","in_ID"),("is","is_IS"),("it","it_IT"),("iw","iw_IL"),("ja","ja_JP"),("kl","kl_GL"),
    ("ko","ko_KR"),("lt","lt_LT"),("lv","lv_LV"),("mk","mk_MK"),("nl","nl_NL"),("no","no_NO"),("pl","pl_PL"),
    ("pt","pt_PT"),("ro","ro_RO"),("ru","ru_RU"),("se","se_NO"),("sk","sk_SK"),("sl","sl_SI"),("sq","sq_AL"),
    ("sr","sr_CS"),("su","fi_FI"),("sv","sv_SE"),("th","th_TH"),("tr","tr_TR"),("uk","uk_UA"),("vi","vi_VN"),("wa","wa_BE"),("zh","zh_CN"),
    #[cfg(target_os = "linux")] ("hs","en_US"),#[cfg(target_os = "linux")] ("ua","en_US"),
    #[cfg(target_os = "linux")] ("bokmal","nb_NO"),#[cfg(target_os = "linux")] ("bokmål","nb_NO"),
    #[cfg(not(target_os = "linux"))] ("big5","zh_TW.Big5"),#[cfg(not(target_os = "linux"))] ("chinese","zh_CN"),
    #[cfg(not(target_os = "linux"))] ("iso_8859_1","en_US.ISO8859-1"),#[cfg(not(target_os = "linux"))] ("iso_8859_15","en_US.ISO8859-15"),
    #[cfg(not(target_os = "linux"))] ("japanese","ja_JP"),
    #[cfg(not(target_os = "linux"))] ("no_NY","no_NO@nynorsk"),#[cfg(not(target_os = "linux"))] ("sr_SP","sr_YU"),#[cfg(not(target_os = "linux"))] ("tchinese","zh_TW"),
];

const LINUX_ALIASES: &[(&str,&str)] = &[
    ("catalan","ca_ES"),("croatian","hr_HR"),("czech","cs_CZ"),("danish","da_DK"),("dansk","da_DK"),("deutsch","de_DE"),("dutch","nl_NL"),
    ("eesti","et_EE"),("estonian","et_EE"),("finnish","fi_FI"),("français","fr_FR"),("french","fr_FR"),("galego","gl_ES"),("galician","gl_ES"),
    ("german","de_DE"),("greek","el_GR"),("hebrew","iw_IL"),("hrvatski","hr_HR"),("hungarian","hu_HU"),("icelandic","is_IS"),
    ("italian","it_IT"),("japanese","ja_JP"),("korean","ko_KR"),("lithuanian","lt_LT"),("norwegian","no_NO"),("nynorsk","nn_NO"),
    ("polish","pl_PL"),("portuguese","pt_PT"),("romanian","ro_RO"),("russian","ru_RU"),("slovak","sk_SK"),("slovene","sl_SI"),
    ("slovenian","sl_SI"),("spanish","es_ES"),("swedish","sv_SE"),("thai","th_TH"),("turkish","tr_TR"),
];

#[cfg(target_os = "macos")]
mod macos {
    use std::ffi::{c_void,c_char};
    type Ref = *const c_void;
    #[link(name = "CoreFoundation",kind = "framework")]
    unsafe extern "C" {
        fn CFLocaleCopyPreferredLanguages() -> Ref;fn CFArrayGetCount(array: Ref) -> isize;fn CFArrayGetValueAtIndex(array: Ref,index: isize) -> Ref;
        fn CFLocaleCopyCurrent() -> Ref;fn CFLocaleGetIdentifier(locale: Ref) -> Ref;
        fn CFStringGetCString(string: Ref,buffer: *mut c_char,size: isize,encoding: u32) -> u8;fn CFStringGetSystemEncoding() -> u32;fn CFRelease(value: Ref);
    }
    struct Owned(Ref);
    impl Drop for Owned { fn drop(&mut self) { unsafe { CFRelease(self.0) }; } }
    fn owned(value: Ref) -> Option<Owned> { (!value.is_null()).then_some(Owned(value)) }
    fn text(value: Ref) -> Option<String> {
        if value.is_null() { return None; }
        let mut buffer = [0 as libc::c_char;128];
        if unsafe { CFStringGetCString(value,buffer.as_mut_ptr(),buffer.len() as isize,CFStringGetSystemEncoding()) } == 0 { return None; }
        super::bounded_c_string(buffer.as_ptr(),127).ok()
    }
    pub fn locale() -> Option<String> {
        let languages = owned(unsafe { CFLocaleCopyPreferredLanguages() })?;
        if unsafe { CFArrayGetCount(languages.0) } <= 0 { return None; }
        let mut language = text(unsafe { CFArrayGetValueAtIndex(languages.0,0) })?;
        if language.find('-').is_none_or(|at|language.len() - at == 5) {
            if let Some(locale) = owned(unsafe { CFLocaleCopyCurrent() }).and_then(|locale|text(unsafe { CFLocaleGetIdentifier(locale.0) })) {
                if let Some((_,region)) = locale.rsplit_once('_') { if language.len() + region.len() + 1 < 128 { language.push('-');language.push_str(region); } }
            }
        }
        let language = language.split('@').next()?;
        let mut parts = language.split(['-','_']);let language = parts.next()?;
        let second = parts.next().unwrap_or("");let third = parts.next().unwrap_or("");
        Some(if second.len() == 4 { if third.is_empty() { format!("{language}@{second}") } else { format!("{language}_{third}@{second}") } }
            else if second.is_empty() { language.into() } else { format!("{language}_{second}") })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn posix_locale_mapping_uses_reference_aliases_and_known_modifiers() {
        assert_eq!(posix_parts("sr_RS.UTF-8@latin"),("sr".into(),if cfg!(target_os = "linux") { "Latn".into() } else { String::new() },"RS".into(),String::new()));
        assert_eq!(posix_parts("no_NO@nynorsk"),("no".into(),String::new(),"NO".into(),"NY".into()));
        assert_eq!(posix_parts("fr_FR.UTF-8@unknown"),("fr".into(),String::new(),"FR".into(),String::new()));
        assert_eq!(posix_parts("C.UTF-8"),("en".into(),String::new(),String::new(),String::new()));
    }
    #[cfg(unix)]
    #[test]
    fn copied_timezone_files_are_compared_by_actual_payload_and_skip_special_names() {
        let directory = tempfile::tempdir().unwrap();let root = directory.path();
        std::fs::create_dir(root.join("Area")).unwrap();std::fs::write(root.join("localtime"),b"same").unwrap();std::fs::write(root.join("Area/City"),b"same").unwrap();
        assert_eq!(find_zone(root,root,b"same",0,&mut 100).unwrap(),Some("Area/City".into()));
        assert_eq!(find_zone(root,root,b"different",0,&mut 100).unwrap(),None);
        assert!(find_zone(root,root,b"same",0,&mut 0).is_err());
    }
}
