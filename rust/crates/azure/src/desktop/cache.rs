use azure_core::credentials::Secret;
use super::{RefreshTokenCache, invalid};

const MAX_CACHE_BYTES: usize = 1024 * 1024;

pub(super) struct NativeCache {
    #[cfg(windows)]
    path: Option<std::path::PathBuf>,
}

impl NativeCache {
    pub fn new() -> Self {
        Self {
            #[cfg(windows)]
            path: std::env::var_os("USERPROFILE").map(|home| std::path::PathBuf::from(home).join("AppData/Local/.IdentityService/azure-toolkit.cache.nocae")),
        }
    }
}

impl RefreshTokenCache for NativeCache {
    fn read(&self) -> azure_core::Result<Secret> {
        #[cfg(windows)]
        let bytes = windows::read(self.path.as_deref())?;
        #[cfg(target_os = "macos")]
        let bytes = macos::read()?;
        #[cfg(target_os = "linux")]
        let bytes = linux::read()?;
        #[cfg(not(any(windows, target_os = "macos", target_os = "linux")))]
        let bytes: Vec<u8> = return Err(invalid("IntelliJ credential cache is unavailable on this platform"));
        parse_refresh_token(&bytes)
    }
}

fn parse_refresh_token(bytes: &[u8]) -> azure_core::Result<Secret> {
    if bytes.len() > MAX_CACHE_BYTES { return Err(invalid("IntelliJ credential cache exceeds its byte limit")); }
    let text = rtdb_types::decode_java_utf8(bytes);
    use super::json::{Reader, Token};
    let mut reader = Reader::new(&text);
    if reader.next()? != Token::Object { return Err(invalid("IntelliJ credential cache requires an object")); }
    while reader.next()? != Token::EndObject {
        let refresh = reader.field() == Some("RefreshToken");
        reader.next()?;
        if refresh {
            reader.next()?;
            reader.next()?;
            while reader.next()? != Token::EndObject {
                let secret = reader.field() == Some("secret");
                reader.next()?;
                if secret {
                    return reader.string().filter(|value| !value.is_empty()).map(|value| Secret::new(value.to_owned()))
                        .ok_or_else(|| invalid("IntelliJ credential cache has no refresh token"));
                }
                reader.skip_children()?;
            }
        } else { reader.skip_children()?; }
    }
    Err(invalid("IntelliJ credential cache has no refresh token"))
}

#[cfg(target_os = "macos")]
mod macos {
    use std::{ffi::c_void, ptr};
    use super::{MAX_CACHE_BYTES, invalid};

    #[link(name = "Security", kind = "framework")]
    unsafe extern "C" {
        fn SecKeychainFindGenericPassword(keychain: *const c_void, service_length: u32, service: *const u8, account_length: u32, account: *const u8, length: *mut u32, data: *mut *mut c_void, item: *mut *mut c_void) -> i32;
        fn SecKeychainItemFreeContent(attributes: *const c_void, data: *mut c_void) -> i32;
    }

    struct Password(*mut c_void);
    impl Drop for Password {
        fn drop(&mut self) {
            if !self.0.is_null() { unsafe { SecKeychainItemFreeContent(ptr::null(), self.0); } }
        }
    }

    pub fn read() -> azure_core::Result<Vec<u8>> {
        let service = b"Microsoft.Developer.IdentityService";
        let account = b"azure-toolkit.cache.nocae";
        let mut length = 0;
        let mut password = Password(ptr::null_mut());
        let status = unsafe { SecKeychainFindGenericPassword(ptr::null(), service.len() as u32, service.as_ptr(), account.len() as u32, account.as_ptr(), &mut length, &mut password.0, ptr::null_mut()) };
        if status != 0 || password.0.is_null() { return Err(invalid("IntelliJ credential is unavailable in the macOS keychain")); }
        if length as usize > MAX_CACHE_BYTES { return Err(invalid("IntelliJ credential cache exceeds its byte limit")); }
        Ok(unsafe { std::slice::from_raw_parts(password.0.cast::<u8>(), length as usize) }.to_vec())
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{ffi::{c_char, c_int, c_void}, ptr};
    use libloading::Library;
    use super::{MAX_CACHE_BYTES, invalid};

    type NewSchema = unsafe extern "C" fn(*const c_char, c_int, ...) -> *mut c_void;
    type Lookup = unsafe extern "C" fn(*const c_void, *mut c_void, *mut *mut c_void, ...) -> *mut c_char;
    type Free = unsafe extern "C" fn(*mut c_void);

    struct NativeValue { pointer: *mut c_void, free: Free }
    impl Drop for NativeValue {
        fn drop(&mut self) { if !self.pointer.is_null() { unsafe { (self.free)(self.pointer); } } }
    }

    pub fn read() -> azure_core::Result<Vec<u8>> {
        let library = unsafe { Library::new("libsecret-1.so.0") }.map_err(|_| invalid("IntelliJ credential requires libsecret"))?;
        let schema_new = unsafe { library.get::<NewSchema>(b"secret_schema_new\0") }.map_err(|_| invalid("IntelliJ libsecret schema function is unavailable"))?;
        let lookup = unsafe { library.get::<Lookup>(b"secret_password_lookup_sync\0") }.map_err(|_| invalid("IntelliJ libsecret lookup function is unavailable"))?;
        let schema_free = *unsafe { library.get::<Free>(b"secret_schema_unref\0") }.map_err(|_| invalid("IntelliJ libsecret cleanup function is unavailable"))?;
        let password_free = *unsafe { library.get::<Free>(b"secret_password_free\0") }.map_err(|_| invalid("IntelliJ libsecret cleanup function is unavailable"))?;
        let error_free = *unsafe { library.get::<Free>(b"g_error_free\0") }.map_err(|_| invalid("IntelliJ libsecret error function is unavailable"))?;
        let schema = NativeValue { pointer: unsafe { schema_new(c"com.intellij.credentialStore.Credential".as_ptr(), 0, c"service".as_ptr(), 0 as c_int, c"account".as_ptr(), 0 as c_int, ptr::null::<c_char>()) }, free: schema_free };
        if schema.pointer.is_null() { return Err(invalid("IntelliJ libsecret schema could not be created")); }
        let mut error = NativeValue { pointer: ptr::null_mut(), free: error_free };
        let password = NativeValue { pointer: unsafe { lookup(schema.pointer, ptr::null_mut(), &mut error.pointer, c"service".as_ptr(), c"Microsoft.Developer.IdentityService".as_ptr(), c"account".as_ptr(), c"azure-toolkit.cache".as_ptr(), ptr::null::<c_char>()) }.cast(), free: password_free };
        if !error.pointer.is_null() || password.pointer.is_null() { return Err(invalid("IntelliJ credential is unavailable in the Linux keyring")); }
        let length = unsafe { libc::strnlen(password.pointer.cast(), MAX_CACHE_BYTES + 1) };
        if length > MAX_CACHE_BYTES { return Err(invalid("IntelliJ credential cache exceeds its byte limit")); }
        Ok(unsafe { std::slice::from_raw_parts(password.pointer.cast::<u8>(), length) }.to_vec())
    }
}

#[cfg(windows)]
mod windows {
    use std::{ffi::c_void, io::Read, path::Path, ptr};
    use super::{MAX_CACHE_BYTES, invalid};

    #[repr(C)]
    struct Blob { length: u32, data: *mut u8 }

    #[link(name = "crypt32")]
    unsafe extern "system" {
        fn CryptUnprotectData(input: *const Blob, description: *mut *mut u16, entropy: *const Blob, reserved: *const c_void, prompt: *const c_void, flags: u32, output: *mut Blob) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LocalFree(memory: *mut c_void) -> *mut c_void;
    }

    struct Decrypted(Blob);
    impl Drop for Decrypted {
        fn drop(&mut self) { if !self.0.data.is_null() { unsafe { LocalFree(self.0.data.cast()); } } }
    }

    pub fn read(path: Option<&Path>) -> azure_core::Result<Vec<u8>> {
        let path = path.ok_or_else(|| invalid("IntelliJ credential cache home directory is unavailable"))?;
        let file = std::fs::File::open(path).map_err(|_| invalid("IntelliJ credential cache file is unavailable"))?;
        let metadata = file.metadata().map_err(|_| invalid("IntelliJ credential cache file metadata is unavailable"))?;
        if !metadata.is_file() || metadata.len() > MAX_CACHE_BYTES as u64 { return Err(invalid("IntelliJ credential cache file exceeds its byte limit")); }
        let mut bytes = Vec::new();
        file.take(MAX_CACHE_BYTES as u64 + 1).read_to_end(&mut bytes).map_err(|_| invalid("IntelliJ credential cache file could not be read"))?;
        if bytes.len() > MAX_CACHE_BYTES { return Err(invalid("IntelliJ credential cache file exceeds its byte limit")); }
        let input = Blob { length: bytes.len() as u32, data: bytes.as_mut_ptr() };
        let mut output = Decrypted(Blob { length: 0, data: ptr::null_mut() });
        let decrypted = unsafe { CryptUnprotectData(&input, ptr::null_mut(), ptr::null(), ptr::null(), ptr::null(), 0, &mut output.0) };
        if decrypted == 0 || output.0.data.is_null() { return Err(invalid("IntelliJ credential cache file could not be decrypted")); }
        if output.0.length as usize > MAX_CACHE_BYTES { return Err(invalid("IntelliJ credential cache exceeds its byte limit")); }
        Ok(unsafe { std::slice::from_raw_parts(output.0.data, output.0.length as usize) }.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intellij_cache_retains_the_first_account_and_first_secret_in_wire_order() {
        let bytes = br#"{"other":{"secret":"ignored"},"RefreshToken":{"z":{"secret":"first","secret":"duplicate"},"a":{"secret":"second"}}}"#;
        assert_eq!(parse_refresh_token(bytes).unwrap().secret(), "first");
        assert_eq!(parse_refresh_token(br#"{"RefreshToken":{"x":{"secret":"ok",BROKEN"#).unwrap().secret(), "ok");
        for bytes in [br#"{"RefreshToken":{"first":{},"second":{"secret":"unused"}}}"#.as_slice(), br#"{"RefreshToken":{"first":{"secret":null},"second":{"secret":"unused"}}}"#, br#"{"RefreshToken":{}}"#, br#"{"RefreshToken":{"first":{"secret":""}}}"#] {
            assert!(parse_refresh_token(bytes).is_err());
        }
    }

    #[test]
    fn intellij_cache_scalar_coercion_and_java_utf8_replacement_are_preserved() {
        for (bytes, expected) in [(br#"{"RefreshToken":{"first":{"secret":true}}}"#.as_slice(), "true"), (br#"{"RefreshToken":{"first":{"secret":123}}}"#, "123")] {
            assert_eq!(parse_refresh_token(bytes).unwrap().secret(), expected);
        }
        assert_eq!(parse_refresh_token(b"{\"RefreshToken\":{\"first\":{\"secret\":\"\xed\xa0\x80\"}}}").unwrap().secret(), "\u{fffd}");
        assert!(parse_refresh_token(&vec![b' '; MAX_CACHE_BYTES + 1]).is_err());
        let error = parse_refresh_token(br#"{"RefreshToken":{"secret-value":[]}}"#).unwrap_err();
        assert!(!error.to_string().contains("secret-value"));
    }
}
