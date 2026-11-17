use super::*;
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{asn1::{Asn1Time, Asn1Object, Asn1OctetString}, hash::MessageDigest, ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod}, x509::{X509Crl, X509CrlBuilder, X509Extension, X509NameRef, X509Object, store::X509Lookup}};
use rdkafka::config::{SslContextConfigurer, SslMaterialSource};
use crate::client::material::{Configuration, tests::{owner, support::Credential}};
use std::{collections::BTreeMap, ffi::CString, path::Path, sync::Barrier};

struct Configured { context: SslContext, _configuration: Arc<Configuration> }
impl std::ops::Deref for Configured { type Target = SslContext; fn deref(&self) -> &Self::Target { &self.context } }

fn configured(path: &Path, memory: &Arc<MemoryBudget>) -> (Configured, *mut openssl_sys::X509_LOOKUP) {
    let properties = BTreeMap::from([("ssl.ca.location".into(), path.to_str().unwrap().into())]);
    let configuration = Configuration::capture(owner(memory), &properties, &mut |bytes| memory.reserve(bytes)).unwrap();
    let mut lease = configuration.admit_context().unwrap().unwrap();
    let mut context = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
    let lookup = context.cert_store_mut().add_lookup(X509Lookup::hash_dir()).unwrap();
    lookup.add_dir(path.to_str().unwrap(), SslFiletype::PEM).unwrap();
    let lookup = lookup.as_ptr();
    unsafe { configuration.configure(context.as_ptr().cast()).unwrap(); }
    lease.loaded();
    (Configured { context: context.build(), _configuration: configuration }, lookup)
}

unsafe fn find(lookup: *mut openssl_sys::X509_LOOKUP, kind: i32, name: &X509NameRef) -> Option<X509Object> {
    let object = unsafe { X509_OBJECT_new() };
    assert!(!object.is_null());
    let found = unsafe { X509_LOOKUP_by_subject(lookup, kind, name.as_ptr(), object) };
    if found == 1 {
        assert_eq!(unsafe { X509_OBJECT_up_ref_count(object) }, 1);
        Some(unsafe { X509Object::from_ptr(object) })
    } else {
        assert_eq!(found, 0);
        unsafe { openssl_sys::X509_OBJECT_free(object); }
        None
    }
}
fn certificate_path(directory: &Path, credential: &Credential, suffix: usize) -> std::path::PathBuf {
    directory.join(format!("{:08x}.{suffix}", credential.certificate.subject_name_hash()))
}
fn crl(issuer: &Credential, number: i64) -> Vec<u8> {
    let mut value = X509CrlBuilder::new().unwrap();
    value.set_issuer_name(issuer.certificate.subject_name()).unwrap();
    value.set_last_update(&Asn1Time::from_unix(1_700_000_000 + number).unwrap()).unwrap();
    value.set_next_update(&Asn1Time::from_unix(1_800_000_000 + number).unwrap()).unwrap();
    for (oid, der) in [("2.5.29.35", vec![0x30, 0]), ("2.5.29.20", vec![2, 1, number as u8])] {
        value.append_extension(X509Extension::new_from_der(&Asn1Object::from_str(oid).unwrap(), false, &Asn1OctetString::new_from_bytes(&der).unwrap()).unwrap()).unwrap();
    }
    value.sign(&issuer.key, MessageDigest::sha256()).unwrap();
    value.build().unwrap().to_pem().unwrap()
}
fn source(memory: &Arc<MemoryBudget>) -> DirectorySource {
    DirectorySource { owner: owner(memory), metadata: Mutex::new(memory.reserve(1024).unwrap()) }
}

#[test]
fn lookup_observes_certificates_created_after_context_configuration() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("late root", false, None);
    let (context, lookup) = configured(directory.path(), &memory);
    assert!(unsafe { find(lookup, 1, root.certificate.subject_name()) }.is_none());
    assert!(context.cert_store().all_certificates().is_empty());
    std::fs::write(certificate_path(directory.path(), &root, 0), root.certificate.to_pem().unwrap()).unwrap();
    let found = unsafe { find(lookup, 1, root.certificate.subject_name()) }.unwrap();
    assert_eq!(found.x509().unwrap().to_der().unwrap(), root.certificate.to_der().unwrap());
    assert_eq!(context.cert_store().objects().len(), 1);
    drop(context); assert!(memory.used_bytes() > 0);
    drop(found); assert_eq!(memory.used_bytes(), 0);
}

#[cfg(unix)]
#[test]
fn lookup_uses_known_names_in_execute_only_directories() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("execute only", false, None);
    std::fs::write(certificate_path(directory.path(), &root, 0), root.certificate.to_pem().unwrap()).unwrap();
    std::fs::write(directory.path().join("unrelated"), b"not a certificate").unwrap();
    let (context, lookup) = configured(directory.path(), &memory);
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o111)).unwrap();
    let found = unsafe { find(lookup, 1, root.certificate.subject_name()) };
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(found.is_some());
    drop(found); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn crl_lookup_observes_new_suffixes_and_retains_each_import() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("CRL root", false, None);
    let (context, lookup) = configured(directory.path(), &memory);
    let path = |suffix| directory.path().join(format!("{:08x}.r{suffix}", root.certificate.subject_name_hash()));
    std::fs::write(path(0), crl(&root, 0)).unwrap();
    let first = unsafe { find(lookup, 2, root.certificate.subject_name()) }.unwrap();
    let retained = memory.used_bytes(); assert_eq!(context.cert_store().objects().len(), 1);
    std::fs::write(path(1), crl(&root, 1)).unwrap();
    let second = unsafe { find(lookup, 2, root.certificate.subject_name()) }.unwrap();
    assert_eq!(context.cert_store().objects().len(), 2); assert!(memory.used_bytes() > retained);
    drop(first); drop(second); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn repeated_imports_release_duplicate_decodes_and_metadata_grants() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("duplicate root", false, None);
    std::fs::write(certificate_path(directory.path(), &root, 0), root.certificate.to_pem().unwrap()).unwrap();
    let (context, lookup) = configured(directory.path(), &memory);
    drop(unsafe { find(lookup, 1, root.certificate.subject_name()) }.unwrap());
    let retained = memory.used_bytes();
    for _ in 0..4 { drop(unsafe { find(lookup, 1, root.certificate.subject_name()) }.unwrap()); assert_eq!(memory.used_bytes(), retained); }
    assert_eq!(context.cert_store().objects().len(), 1);
    drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn malformed_later_pem_preserves_imported_object_and_its_admission() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("partial root", false, None);
    let mut bytes = root.certificate.to_pem().unwrap(); bytes.extend_from_slice(b"-----BEGIN CERTIFICATE-----\nnot-base64\n-----END CERTIFICATE-----\n");
    std::fs::write(certificate_path(directory.path(), &root, 0), &bytes).unwrap();
    let (context, lookup) = configured(directory.path(), &memory); let baseline = memory.used_bytes();
    assert!(unsafe { find(lookup, 1, root.certificate.subject_name()) }.is_none());
    assert_eq!(context.cert_store().objects().len(), 1); assert!(memory.used_bytes() > baseline);
    let retained = context.cert_store().objects()[0].x509().unwrap().to_owned();
    drop(context); assert!(memory.used_bytes() > 0);
    drop(retained); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn denied_lookup_does_not_return_a_previously_cached_match() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("budget root", false, None);
    std::fs::write(certificate_path(directory.path(), &root, 0), root.certificate.to_pem().unwrap()).unwrap();
    let (context, lookup) = configured(directory.path(), &memory);
    drop(unsafe { find(lookup, 1, root.certificate.subject_name()) }.unwrap());
    let retained = memory.used_bytes(); let blocker = memory.reserve(memory.limit_bytes() - retained).unwrap();
    assert!(unsafe { find(lookup, 1, root.certificate.subject_name()) }.is_none());
    assert_eq!(context.cert_store().objects().len(), 1);
    drop(blocker); assert_eq!(memory.used_bytes(), retained);
    drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn raw_and_decoded_admission_denial_precedes_native_parsing() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("unparsed root", false, None);
    let bytes = root.certificate.to_pem().unwrap();
    std::fs::write(certificate_path(directory.path(), &root, 0), &bytes).unwrap();
    let (context, lookup) = configured(directory.path(), &memory);
    let baseline = memory.used_bytes();
    let path_allowance = directory.path().to_str().unwrap().len() * 4 + 4096 + 64;
    for available in [path_allowance, path_allowance + bytes.len() + 512] {
        let blocker = memory.reserve(memory.limit_bytes() - baseline - available).unwrap();
        assert!(unsafe { find(lookup, 1, root.certificate.subject_name()) }.is_none());
        assert!(context.cert_store().objects().is_empty());
        drop(blocker); assert_eq!(memory.used_bytes(), baseline);
    }
    drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn cache_metadata_denial_drops_an_already_parsed_certificate() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("metadata root", false, None);
    let bytes = root.certificate.to_pem().unwrap();
    std::fs::write(certificate_path(directory.path(), &root, 0), &bytes).unwrap();
    let (context, lookup) = configured(directory.path(), &memory); let baseline = memory.used_bytes();
    let available = directory.path().to_str().unwrap().len() * 4 + 4096 + 64 + bytes.len() + 512 + bytes.len() * DECODE_BYTES_PER_INPUT_BYTE + DECODE_FIXED_BYTES;
    let blocker = memory.reserve(memory.limit_bytes() - baseline - available).unwrap();
    assert!(unsafe { find(lookup, 1, root.certificate.subject_name()) }.is_none());
    assert!(context.cert_store().objects().is_empty());
    drop(blocker); assert_eq!(memory.used_bytes(), baseline);
    drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn concurrent_duplicate_imports_publish_one_admitted_cache_object() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("concurrent root", false, None);
    std::fs::write(certificate_path(directory.path(), &root, 0), root.certificate.to_pem().unwrap()).unwrap();
    let (context, lookup) = configured(directory.path(), &memory); let lookup = lookup as usize;
    let barrier = Barrier::new(4);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let barrier = &barrier; let name = root.certificate.subject_name();
            scope.spawn(move || { barrier.wait(); drop(unsafe { find(lookup as *mut _, 1, name) }.unwrap()); });
        }
    });
    assert_eq!(context.cert_store().objects().len(), 1);
    let retained = memory.used_bytes();
    drop(unsafe { find(lookup as *mut _, 1, root.certificate.subject_name()) }.unwrap());
    assert_eq!(memory.used_bytes(), retained);
    drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn concurrent_metadata_rollback_releases_only_its_own_grant() {
    let memory = MemoryBudget::new(128 << 20); let source = source(&memory); let baseline = memory.used_bytes();
    let barrier = Barrier::new(2);
    std::thread::scope(|scope| {
        for (bytes, retained) in [(400, false), (700, true)] {
            let source = &source; let barrier = &barrier;
            scope.spawn(move || {
                let grant = source.reserve(bytes).unwrap(); barrier.wait();
                let bytes = unsafe { *Box::from_raw(grant.cast::<usize>()) };
                source.settle(bytes, retained);
            });
        }
    });
    assert_eq!(memory.used_bytes(), baseline + 700);
    drop(source); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_metadata_growth_preserves_existing_admission() {
    let memory = MemoryBudget::new(128 << 20); let source = source(&memory); let baseline = memory.used_bytes();
    let blocker = memory.reserve(memory.limit_bytes() - baseline - 63).unwrap();
    assert!(source.reserve(1).is_err()); assert_eq!(memory.used_bytes(), memory.limit_bytes() - 63);
    drop(blocker); assert_eq!(memory.used_bytes(), baseline);
    drop(source); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn directory_read_retains_bytes_until_close_and_rejects_nonregular_paths() {
    let memory = MemoryBudget::new(128 << 20); let source = source(&memory); let baseline = memory.used_bytes();
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("input");
    std::fs::write(&path, b"immutable input").unwrap();
    let read = source.read(&CString::new(path.to_str().unwrap()).unwrap()).ok().unwrap();
    std::fs::write(&path, b"replacement").unwrap(); assert_eq!(read.bytes, b"immutable input");
    drop(read); assert_eq!(memory.used_bytes(), baseline);
    assert!(matches!(source.read(&CString::new(directory.path().to_str().unwrap()).unwrap()), Err(ReadError::Io(_))));
    drop(source); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn absent_hash_lookup_does_not_retain_a_source_or_create_a_lookup() {
    let memory = MemoryBudget::new(128 << 20); let owner = owner(&memory); let baseline = memory.used_bytes();
    let context = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
    unsafe { install(context.as_ptr(), Arc::clone(&owner), &memory, 32).unwrap(); }
    assert_eq!(memory.used_bytes(), baseline);
    let context = context.build();
    assert!(context.cert_store().all_certificates().is_empty());
    drop(context); drop(owner); assert_eq!(memory.used_bytes(), 0);
}


#[test]
fn duplicate_crl_still_admits_its_new_suffix_metadata() {
    unsafe extern "C" fn deny_retained_metadata(arg: *mut c_void, bytes: usize) -> *mut c_void {
        if bytes < 4096 { ptr::null_mut() } else { unsafe { reserve(arg, bytes) } }
    }
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("duplicate CRL", false, None); let bytes = crl(&root, 1);
    let mut context = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
    let lookup = context.cert_store_mut().add_lookup(X509Lookup::hash_dir()).unwrap();
    lookup.add_dir(directory.path().to_str().unwrap(), SslFiletype::PEM).unwrap();
    let lookup = lookup.as_ptr();
    let source = Box::into_raw(Box::new(source(&memory)));
    let callbacks = NativeSource { arg: source.cast(), open, close, retain, reserve: deny_retained_metadata, settle, free_source };
    assert_eq!(unsafe { X509_STORE_set0_hash_dir_source(context.cert_store().as_ptr(), &callbacks) }, 1);
    let existing = X509Crl::from_pem(&bytes).unwrap();
    assert_eq!(unsafe { X509_STORE_add_crl(context.cert_store().as_ptr(), existing.as_ptr()) }, 1);
    let context = context.build();
    let path = directory.path().join(format!("{:08x}.r0", root.certificate.subject_name_hash()));
    std::fs::write(path, &bytes).unwrap();
    let baseline = memory.used_bytes();
    for _ in 0..2 {
        assert!(unsafe { find(lookup, 2, root.certificate.subject_name()) }.is_none());
        assert_eq!(context.cert_store().objects().len(), 1);
        assert_eq!(memory.used_bytes(), baseline);
    }
    drop(existing); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn duplicate_source_installation_leaves_the_original_owner_intact() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let (context, _) = configured(directory.path(), &memory);
    let owner = owner(&memory); let baseline = memory.used_bytes();
    assert!(unsafe { install(context.as_ptr(), Arc::clone(&owner), &memory, directory.path().to_str().unwrap().len()) }.is_err());
    assert_eq!(memory.used_bytes(), baseline);
    drop(owner); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn production_client_installs_directory_admission_before_its_first_lookup() {
    use rdkafka::{ClientConfig, producer::BaseProducer};
    struct Inspect { configuration: Arc<Configuration>, root: openssl::x509::X509 }
    impl SslContextConfigurer for Inspect {
        fn material_source(&self) -> Option<&dyn SslMaterialSource> { Some(self.configuration.as_ref()) }
        fn replaces_default_trust(&self) -> bool { self.configuration.replaces_default_trust() }
        unsafe fn library_context(&self) -> *mut c_void { unsafe { self.configuration.library_context() } }
        unsafe fn configure(&self, context: *mut c_void) -> std::result::Result<(), String> {
            unsafe { self.configuration.configure(context)?; }
            let context = unsafe { openssl::ssl::SslContextRef::from_ptr(context.cast()) };
            let charge = self.configuration.charge.budget().reserve(64 << 10).unwrap();
            let selected = crate::tls::validation::pkix::verify_store(context.cert_store(), &[self.root.clone()], &Asn1Time::days_from_now(0).unwrap(), &charge).unwrap();
            assert_eq!(selected.certificates[0].to_der().unwrap(), self.root.to_der().unwrap());
            Ok(())
        }
    }
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let root = Credential::new("native directory client", false, None);
    let properties = BTreeMap::from([("security.protocol".into(), "SSL".into()), ("ssl.ca.location".into(), directory.path().to_str().unwrap().into())]);
    let snapshot = Configuration::capture(owner(&memory), &properties, &mut |bytes| memory.reserve(bytes)).unwrap();
    std::fs::write(certificate_path(directory.path(), &root, 0), root.certificate.to_pem().unwrap()).unwrap();
    let mut configuration = ClientConfig::new();
    configuration.set("security.protocol", "SSL").set("ssl.ca.location", directory.path().to_str().unwrap());
    configuration.set_ssl_context_configurer(Arc::new(Inspect { configuration: snapshot, root: root.certificate }));
    let producer: BaseProducer = configuration.create().unwrap();
    drop(configuration); assert!(memory.used_bytes() > 0);
    drop(producer); assert_eq!(memory.used_bytes(), 0);
}


#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
#[test]
fn colon_rich_existing_directory_admits_registration_before_native_construction() {
    let directory = tempfile::tempdir().unwrap(); let mut path = directory.path().to_owned();
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for start in (0..1300usize).step_by(75) {
        let mut component = String::new();
        for index in start..(start + 75).min(1300) {
            if index != start { component.push(':'); }
            component.push(ALPHABET[index / ALPHABET.len()] as char);
            component.push(ALPHABET[index % ALPHABET.len()] as char);
        }
        path.push(component); std::fs::create_dir(&path).unwrap();
    }
    let path = path.to_str().unwrap();
    assert!(path.len() < 4096); assert!(std::fs::metadata(path).unwrap().is_dir());
    let segments = path.split(':').collect::<std::collections::BTreeSet<_>>().len();
    assert!(segments >= 1280);
    let minimum_registration = 64 * segments + path.len() + segments * std::mem::size_of::<*const c_void>();
    let available = DECODE_FIXED_BYTES + 4 * path.len();
    assert!(available < minimum_registration);
    let memory = MemoryBudget::new(128 << 20);
    let properties = BTreeMap::from([("ssl.ca.location".into(), path.into())]);
    let configuration = Configuration::capture(owner(&memory), &properties, &mut |bytes| memory.reserve(bytes)).unwrap();
    assert!(matches!(configuration.material(rdkafka::config::SslMaterialKind::CertificateAuthorities, &CString::new(path).unwrap()), rdkafka::config::SslMaterial::Unmanaged));
    let baseline = memory.used_bytes();
    let blocker = memory.reserve(memory.limit_bytes() - baseline - available).unwrap();
    let error = configuration.admit_context().err().unwrap();
    assert!(error.contains("rtdb/memory-budget-exceeded"), "{error}");
    assert_eq!(memory.used_bytes(), memory.limit_bytes() - available);
    drop(blocker); assert_eq!(memory.used_bytes(), baseline);
    drop(configuration); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn registration_allowance_checks_overflow_and_releases_exact_constructor_ownership() {
    assert_eq!(metadata_bound(0).unwrap(), DECODE_FIXED_BYTES);
    assert!(metadata_bound(usize::MAX).is_err());
    let largest = (usize::MAX - DECODE_FIXED_BYTES) / 128;
    assert!(metadata_bound(largest).is_ok());
    assert!(metadata_bound(largest + 1).is_err());
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().to_str().unwrap();
    let memory = MemoryBudget::new(128 << 20);
    let properties = BTreeMap::from([("ssl.ca.location".into(), path.into())]);
    let configuration = Configuration::capture(owner(&memory), &properties, &mut |bytes| memory.reserve(bytes)).unwrap();
    let baseline = memory.used_bytes(); let allowance = DECODE_FIXED_BYTES + 128 * path.len();
    let blocker = memory.reserve(memory.limit_bytes() - baseline - allowance).unwrap();
    let mut lease = configuration.admit_context().unwrap().unwrap();
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
    lease.loaded(); assert_eq!(memory.used_bytes(), memory.limit_bytes() - allowance);
    drop(lease); drop(blocker); assert_eq!(memory.used_bytes(), baseline);
    drop(configuration); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn installed_source_closes_directory_registration() {
    let directory = tempfile::tempdir().unwrap(); let extra = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let (context, lookup) = configured(directory.path(), &memory);
    let result = unsafe { openssl_sys::X509_LOOKUP_add_dir(lookup, CString::new(extra.path().to_str().unwrap()).unwrap().as_ptr(), SslFiletype::PEM.as_raw()) };
    assert_eq!(result, 0);
    drop(context); assert_eq!(memory.used_bytes(), 0);
}

unsafe extern "C" {
    fn X509_STORE_add_crl(store: *mut openssl_sys::X509_STORE, crl: *mut openssl_sys::X509_CRL) -> i32;
    fn X509_OBJECT_new() -> *mut openssl_sys::X509_OBJECT;
    fn X509_OBJECT_up_ref_count(object: *mut openssl_sys::X509_OBJECT) -> i32;
    fn X509_LOOKUP_by_subject(lookup: *mut openssl_sys::X509_LOOKUP, kind: i32, name: *const openssl_sys::X509_NAME, object: *mut openssl_sys::X509_OBJECT) -> i32;
}
