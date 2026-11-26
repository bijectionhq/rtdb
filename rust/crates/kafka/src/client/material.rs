use std::{collections::BTreeMap, ffi::{CStr, c_void}, fs::File, io::{self, Read}, sync::Arc};
use rdkafka::config::{SslContextConfigurer, SslMaterial, SslMaterialKind, SslMaterialLease, SslMaterialSource};
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Anomaly, Result};
use zeroize::Zeroizing;
use crate::tls::NativeTls;

mod directory;

const MATERIALS: [(SslMaterialKind, &str); 5] = [
    (SslMaterialKind::CertificateAuthorities, "ssl.ca.location"),
    (SslMaterialKind::Revocations, "ssl.crl.location"),
    (SslMaterialKind::CertificateChain, "ssl.certificate.location"),
    (SslMaterialKind::PrivateKey, "ssl.key.location"),
    (SslMaterialKind::KeyStore, "ssl.keystore.location"),
];

pub(crate) struct Configuration {
    owner: Arc<NativeTls>,
    files: [Option<FileSnapshot>; 5],
    charge: Arc<MemoryReservation>,
}
struct FileSnapshot { path: Box<str>, contents: Contents }
enum Contents { Bytes(AdmittedBytes), Unavailable(i32), Unmanaged }
struct AdmittedBytes { bytes: Zeroizing<Vec<u8>>, charge: Arc<MemoryReservation> }

const DECODE_FIXED_BYTES: usize = 64 << 10;
const DECODE_BYTES_PER_INPUT_BYTE: usize = 512;
const PBE_SCRATCH_BYTES: usize = 33 << 20;

enum DecodeAdmission {
    Loading { retained: Arc<MemoryReservation>, _scratch: Arc<MemoryReservation> },
    Retained { _charge: Arc<MemoryReservation> },
}
impl SslMaterialLease for DecodeAdmission {
    fn loaded(&mut self) {
        if let Self::Loading { retained, .. } = self { *self = Self::Retained { _charge: Arc::clone(retained) }; }
    }
}

struct DirectoryConstruction {
    decoded: Option<Box<dyn SslMaterialLease>>,
    constructor: Option<Arc<MemoryReservation>>,
}
impl SslMaterialLease for DirectoryConstruction {
    fn loaded(&mut self) {
        if let Some(decoded) = &mut self.decoded { decoded.loaded(); }
        self.constructor = None;
    }
}

impl Configuration {
    pub(crate) fn capture(owner: Arc<NativeTls>, properties: &BTreeMap<String, String>, reserve: &mut impl FnMut(usize) -> Result<Arc<MemoryReservation>>) -> Result<Arc<Self>> {
        let size = MATERIALS.iter().try_fold(1024usize, |size, (_, name)| size.checked_add(properties.get(*name).map_or(0, String::len)).ok_or_else(overflow))?;
        let charge = reserve(size)?;
        let mut files = std::array::from_fn(|_| None);
        for (index, (kind, name)) in MATERIALS.iter().enumerate() {
            if let Some(path) = properties.get(*name) {
                let contents = if *kind == SslMaterialKind::CertificateAuthorities && path == "probe" { Contents::Unmanaged }
                    else { snapshot(path, reserve)? };
                files[index] = Some(FileSnapshot { path: path.clone().into_boxed_str(), contents });
            }
        }
        Ok(Arc::new(Self { owner, files, charge }))
    }

    pub(crate) fn reservations(&self) -> impl Iterator<Item = &Arc<MemoryReservation>> {
        self.owner.reservations().iter().chain(std::iter::once(&self.charge)).chain(self.files.iter().flatten().filter_map(|file| match &file.contents {
            Contents::Bytes(bytes) => Some(&bytes.charge), _ => None,
        }))
    }

    fn directory_candidate(&self) -> Option<&str> {
        match &self.files[0] {
            Some(FileSnapshot { path, contents: Contents::Unmanaged }) if path.as_ref() != "probe" => Some(path),
            _ => None,
        }
    }

    fn admit_native(&self) -> Result<Option<Box<dyn SslMaterialLease>>> {
        let decoded = self.admit_decoded()?;
        let Some(path) = self.directory_candidate() else { return Ok(decoded); };
        let constructor = self.charge.budget().reserve(directory::metadata_bound(path.len())?)?;
        Ok(Some(Box::new(DirectoryConstruction { decoded, constructor: Some(constructor) })))
    }

    fn admit_decoded(&self) -> Result<Option<Box<dyn SslMaterialLease>>> {
        let mut bytes = 0usize;
        let mut present = false;
        let mut pbe = false;
        for ((kind, _), file) in MATERIALS.iter().zip(&self.files) {
            if let Some(FileSnapshot { contents: Contents::Bytes(input), .. }) = file {
                present = true;
                pbe |= matches!(kind, SslMaterialKind::PrivateKey | SslMaterialKind::KeyStore);
                bytes = bytes.checked_add(input.bytes.len()).ok_or_else(overflow)?;
            }
        }
        if !present { return Ok(None); }
        let bytes = bytes.checked_mul(DECODE_BYTES_PER_INPUT_BYTE).and_then(|bytes| bytes.checked_add(DECODE_FIXED_BYTES)).ok_or_else(overflow)?;
        let retained = self.charge.budget().reserve(bytes)?;
        let admission = if pbe {
            DecodeAdmission::Loading { retained, _scratch: self.charge.budget().reserve(PBE_SCRATCH_BYTES)? }
        } else { DecodeAdmission::Retained { _charge: retained } };
        Ok(Some(Box::new(admission)))
    }
}

impl SslMaterialSource for Configuration {
    fn admit_context(&self) -> std::result::Result<Option<Box<dyn SslMaterialLease>>, String> { self.admit_native().map_err(|error| error.to_string()) }

    fn material(&self, kind: SslMaterialKind, path: &CStr) -> SslMaterial<'_> {
        let index = MATERIALS.iter().position(|(candidate, _)| *candidate == kind).expect("complete native material kinds");
        match &self.files[index] {
            Some(file) if file.path.as_bytes() == path.to_bytes() => match &file.contents {
                Contents::Bytes(bytes) => SslMaterial::Bytes(&bytes.bytes),
                Contents::Unavailable(error) => SslMaterial::Unavailable(*error),
                Contents::Unmanaged => SslMaterial::Unmanaged,
            },
            _ => SslMaterial::Unavailable(libc::EINVAL),
        }
    }
}

impl SslContextConfigurer for Configuration {
    fn material_source(&self) -> Option<&dyn SslMaterialSource> { Some(self) }
    fn replaces_default_trust(&self) -> bool { self.owner.replaces_default_trust() }
    unsafe fn library_context(&self) -> *mut c_void { unsafe { self.owner.library_context() } }
    unsafe fn configure(&self, context: *mut c_void) -> std::result::Result<(), String> {
        unsafe { self.owner.configure(context)?; }
        if let Some(path) = self.directory_candidate() {
            unsafe { directory::install(context.cast(), Arc::clone(&self.owner), self.charge.budget(), path.len()) }.map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

fn snapshot(path: &str, reserve: &mut impl FnMut(usize) -> Result<Arc<MemoryReservation>>) -> Result<Contents> {
    let metadata = match std::fs::metadata(path) { Ok(metadata) => metadata, Err(error) => return Ok(unavailable(error)) };
    if !metadata.is_file() { return Ok(Contents::Unmanaged); }
    let mut file = match File::open(path) { Ok(file) => file, Err(error) => return Ok(unavailable(error)) };
    let metadata = match file.metadata() { Ok(metadata) => metadata, Err(error) => return Ok(unavailable(error)) };
    let length = usize::try_from(metadata.len()).ok().filter(|length| *length <= i32::MAX as usize).ok_or_else(overflow)?;
    let charge = reserve(length.checked_add(128).ok_or_else(overflow)?)?;
    let mut bytes = Zeroizing::new(vec![0; length]);
    if let Err(error) = file.read_exact(&mut bytes) { return Ok(unavailable(error)); }
    match file.read(&mut [0]) {
        Ok(0) => Ok(Contents::Bytes(AdmittedBytes { bytes, charge })),
        Ok(_) => Ok(Contents::Unavailable(libc::EAGAIN)),
        Err(error) => Ok(unavailable(error)),
    }
}

fn unavailable(error: io::Error) -> Contents { Contents::Unavailable(error.raw_os_error().unwrap_or(libc::EIO)) }
fn overflow() -> Anomaly {
    Anomaly::incorrect("rtdb/memory-budget-exceeded", "Native TLS snapshot exceeds its representable allocation bound").with_kind(rtdb_types::AnomalyKind::ResourceExhausted)
}

#[cfg(test)]
mod tests;
