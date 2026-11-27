use std::cmp::Ordering;
use rtdb_tls::{CertificateAlternativeNames, CertificateBasicConstraints, CertificateKeyUsage, DistinguishedName};
use crate::tls::{public_key, security};
use super::*;
mod tree;

struct Candidate<'a> { certificate: &'a X509Ref, encoded: Vec<u8>, hash: i32 }
struct Subject { certificate: usize, canonical: String, _charge: Arc<MemoryReservation> }
struct Frame { candidates: Vec<usize>, next: usize, intermediates: usize, _charge: Arc<MemoryReservation> }
struct Builder<'a, 'source> {
    source: &'a Search<'source>,
    candidates: Vec<Candidate<'a>>,
    anchors: Vec<usize>,
    trusted: Vec<usize>,
    subjects: Vec<Subject>,
    _charge: Arc<MemoryReservation>,
}

pub(super) fn verify(source: &Search<'_>) -> Result<Vec<X509>> { Builder::new(source)?.verify() }

pub(super) fn ordered(source: &Search<'_>, length: usize) -> Result<Vec<X509>> {
    let builder = Builder::new(source)?;
    let last = source.peer[length - 1];
    for &index in &builder.anchors {
        let anchor = builder.candidates[index].certificate;
        if !source.same_name(last.issuer_name(), anchor.subject_name())? || !ordered_anchor(last, anchor, source.charge)? { continue; }
        let _attempt = source.charge.budget().reserve(256)?;
        let chain = source.peer[..length].iter().copied().chain(std::iter::once(anchor)).collect::<Vec<_>>();
        match source.validate(&chain) {
            Ok(()) => return Ok(chain.into_iter().map(X509Ref::to_owned).collect()),
            Err(TlsConfigError::Invalid(_)) => {},
            Err(TlsConfigError::Other(error)) => return Err(error),
        }
    }
    Err(invalid("TLS PKIX ordered path validation failed"))
}

impl<'a, 'source> Builder<'a, 'source> {
    fn new(source: &'a Search<'source>) -> Result<Self> {
        let bound = source.peer.iter().map(|certificate| &**certificate).chain(source.anchors.iter()).try_fold(65_536usize, |bound, certificate| {
            let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), ptr::null_mut()) };
            usize::try_from(length).ok().and_then(|length| length.checked_mul(4)).and_then(|length| length.checked_add(512))
                .and_then(|length| bound.checked_add(length)).ok_or_else(|| invalid("TLS builder metadata exceeds allocation range"))
        })?;
        let charge = source.charge.budget().reserve(bound)?;
        let candidates = source.peer.iter().map(|certificate| &**certificate).chain(source.anchors.iter()).map(|certificate| {
            let encoded = certificate.to_der().map_err(invalid)?;
            let hash = encoded.iter().fold(1i32, |hash, byte| hash.wrapping_mul(31).wrapping_add(i32::from(*byte as i8)));
            Ok(Candidate { certificate, encoded, hash })
        }).collect::<Result<Vec<_>>>()?;
        let mut builder = Self { source, candidates, anchors: Vec::new(), trusted: Vec::new(), subjects: Vec::new(), _charge: charge };
        let certificates = builder.certificate_order(16, source.peer.len()..builder.candidates.len());
        let anchors = builder.identity_order(16, &certificates);
        builder.anchors = builder.identity_order(capacity(anchors.len().max(12))?, &anchors);
        builder.trusted = builder.certificate_order(capacity(builder.anchors.len())?, builder.anchors.iter().copied());
        for index in &builder.anchors {
            let name = builder.name(builder.candidates[*index].certificate.subject_name())?;
            if builder.subjects.iter().any(|subject| subject.canonical == name.canonical()) { continue; }
            let bound = name.canonical().len().checked_mul(2).and_then(|length| length.checked_add(128))
                .ok_or_else(|| invalid("TLS trusted subject exceeds allocation range"))?;
            let charge = source.charge.budget().reserve(bound)?;
            builder.subjects.push(Subject { certificate: *index, canonical: name.canonical().to_owned(), _charge: charge });
        }
        let indices = (0..builder.subjects.len()).collect::<Vec<_>>();
        let order = rtdb_types::java_hash_map_order(capacity(builder.anchors.len())?, indices.iter(),
            |index| builder.subjects[*index].canonical.encode_utf16().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit))),
            |left, right| builder.identity(builder.subjects[*left].certificate).cmp(&builder.identity(builder.subjects[*right].certificate)))
            .ok_or_else(|| invalid("TLS trusted-subject map exceeds allocation range"))?;
        let mut subjects = builder.subjects.into_iter().map(Some).collect::<Vec<_>>();
        builder.subjects = order.into_iter().map(|index| subjects[index].take().expect("distinct trusted subject")).collect();
        Ok(builder)
    }

    fn verify(&self) -> Result<Vec<X509>> {
        self.forward(0, true)?;
        let mut path = vec![0];
        let mut frames = vec![self.frame(&path, 0)?];
        while let Some(frame) = frames.last_mut() {
            let Some(index) = frame.candidates.get(frame.next).copied() else { frames.pop(); path.pop(); continue; };
            frame.next += 1;
            let intermediates = frame.intermediates;
            let _attempt = self.source.charge.budget().reserve(256)?;
            match self.forward(index, false) {
                Err(TlsConfigError::Invalid(_)) => continue,
                Err(TlsConfigError::Other(error)) => return Err(error),
                Ok(()) => {},
            }
            let candidate = self.candidates[index].certificate;
            if let Some(anchor) = self.anchor(index) {
                let chain = path.iter().map(|index| self.candidates[*index].certificate)
                    .chain(std::iter::once(self.candidates[anchor].certificate)).collect::<Vec<_>>();
                match self.source.validate(&chain) {
                    Ok(()) => return Ok(chain.into_iter().map(X509Ref::to_owned).collect()),
                    Err(TlsConfigError::Invalid(_)) => continue,
                    Err(TlsConfigError::Other(error)) => return Err(error),
                }
            }
            let self_issued = self.source.same_name(candidate.subject_name(), candidate.issuer_name())?;
            let previous = self.candidates[*path.last().expect("nonempty builder path")].certificate;
            if self_issued && self.source.same_name(previous.subject_name(), previous.issuer_name())? {
                frames.pop(); path.pop(); continue;
            }
            let count = intermediates + usize::from(!self_issued);
            path.push(index);
            match self.frame(&path, count) {
                Ok(frame) => frames.push(frame),
                Err(TlsConfigError::Invalid(_)) => { path.pop(); },
                Err(TlsConfigError::Other(error)) => return Err(error),
            }
        }
        Err(invalid("TLS PKIX path building found no permitted path"))
    }

    fn frame(&self, path: &[usize], intermediates: usize) -> TlsConfigResult<Frame> {
        let bound = self.candidates.len().checked_mul(512).and_then(|length| length.checked_add(65_536))
            .ok_or_else(|| TlsConfigError::invalid("TLS builder candidates exceed allocation range"))?;
        let charge = self.source.charge.budget().reserve(bound).map_err(TlsConfigError::Other)?;
        let previous = *path.last().expect("nonempty builder path");
        let mut selected = Vec::new();
        for index in &self.trusted {
            if self.matches(*index, previous, None)? { selected.push(*index); }
        }
        if intermediates < 5 {
            let mut certificates = Vec::new();
            for index in 0..self.source.peer.len() {
                match self.matches(index, previous, Some(intermediates)) {
                    Ok(true) => certificates.push(index),
                    Ok(false) => {},
                    Err(TlsConfigError::Invalid(_)) => {},
                    Err(error) => return Err(error),
                }
            }
            selected.extend(self.certificate_order(16, certificates));
        }
        let identifiers = self.identifiers(previous)?;
        let candidates = tree::ordered(selected, |left, right| self.compare(identifiers.authority_key_identifier(), left, right))?;
        let mut selected = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let mut repeated = false;
            for previous in path.iter().rev() {
                match self.repeated(*previous, candidate) {
                    Ok(true) => { repeated = true; break; },
                    Ok(false) => {},
                    Err(TlsConfigError::Invalid(_)) => { repeated = true; break; },
                    Err(error) => return Err(error),
                }
            }
            if !repeated { selected.push(candidate); }
        }
        Ok(Frame { candidates: selected, next: 0, intermediates, _charge: charge })
    }

    fn matches(&self, index: usize, previous: usize, intermediates: Option<usize>) -> TlsConfigResult<bool> {
        let candidate = &self.candidates[index];
        let child = self.candidates[previous].certificate;
        if !self.source.same_name(candidate.certificate.subject_name(), child.issuer_name())? { return Ok(false); }
        if candidate.certificate.version() < 2 && (candidate.certificate.not_before().compare(child.not_before()).map_err(TlsConfigError::invalid)?.is_gt()
            || candidate.certificate.not_after().compare(child.not_after()).map_err(TlsConfigError::invalid)?.is_lt()) { return Ok(false); }
        if let Some(intermediates) = intermediates {
            if candidate.certificate.not_before().compare(self.source.now).map_err(TlsConfigError::invalid)?.is_gt()
                || candidate.certificate.not_after().compare(self.source.now).map_err(TlsConfigError::invalid)?.is_lt() { return Ok(false); }
            let constraints = CertificateBasicConstraints::decode(&candidate.encoded, self.source.charge.budget()).map_err(TlsConfigError::from_tls)?;
            if constraints.path_length() < intermediates as i32 { return Ok(false); }
        }
        Ok(true)
    }

    fn forward(&self, index: usize, initial: bool) -> TlsConfigResult<()> {
        let candidate = &self.candidates[index];
        security::verify_untrusted_certificate(candidate.certificate).map_err(TlsConfigError::from_invalid_anomaly)?;
        if self.anchor(index).is_some() { return Ok(()); }
        super::super::super::critical_extensions(candidate.certificate, &[Nid::BASIC_CONSTRAINTS, Nid::NAME_CONSTRAINTS,
            Nid::CERTIFICATE_POLICIES, Nid::POLICY_MAPPINGS, Nid::POLICY_CONSTRAINTS, Nid::INHIBIT_ANY_POLICY,
            Nid::SUBJECT_ALT_NAME, Nid::KEY_USAGE, Nid::EXT_KEY_USAGE]).map_err(TlsConfigError::from_invalid_anomaly)?;
        if !initial {
            let path_length = CertificateBasicConstraints::decode(&candidate.encoded, self.source.charge.budget()).map_err(TlsConfigError::from_tls)?.path_length();
            if path_length == -1 { return Err(TlsConfigError::invalid("TLS builder candidate is not a certificate authority")); }
            let usage = CertificateKeyUsage::decode(&candidate.encoded, self.source.charge.budget()).map_err(TlsConfigError::from_tls)?;
            if !usage.permits_certificate_signing() { return Err(TlsConfigError::invalid("TLS builder candidate excludes certificate signing")); }
        }
        Ok(())
    }

    fn compare(&self, key: Option<&[u8]>, left: usize, right: usize) -> TlsConfigResult<Ordering> {
        if self.candidates[left].encoded == self.candidates[right].encoded { return Ok(Ordering::Equal); }
        if let Some(key) = key {
            if self.identifiers(left)?.subject_key_identifier() == Some(key) { return Ok(Ordering::Less); }
            if self.identifiers(right)?.subject_key_identifier() == Some(key) { return Ok(Ordering::Greater); }
        }
        let left = self.name(self.candidates[left].certificate.issuer_name())?;
        let right = self.name(self.candidates[right].certificate.issuer_name())?;
        if self.subjects.iter().any(|subject| subject.canonical == left.canonical()) { return Ok(Ordering::Less); }
        if self.subjects.iter().any(|subject| subject.canonical == right.canonical()) { return Ok(Ordering::Greater); }
        for subject in &self.subjects {
            let anchor = self.name(self.candidates[subject.certificate].certificate.subject_name())?;
            match (anchor.distance_to_common_non_geographical_ancestor(&left), anchor.distance_to_common_non_geographical_ancestor(&right)) {
                (Some(left), Some(right)) => return Ok(if left > right { Ordering::Greater } else { Ordering::Less }),
                (Some(_), None) => return Ok(Ordering::Less),
                (None, Some(_)) => return Ok(Ordering::Greater),
                (None, None) => {},
            }
        }
        Ok(Ordering::Less)
    }

    fn repeated(&self, left: usize, right: usize) -> TlsConfigResult<bool> {
        let left = &self.candidates[left]; let right = &self.candidates[right];
        if left.encoded == right.encoded { return Ok(true); }
        if !self.source.same_name(left.certificate.subject_name(), right.certificate.subject_name())? || !public_key::same_typed(left.certificate, right.certificate, self.source.charge.budget())? { return Ok(false); }
        let left = CertificateAlternativeNames::decode(&left.encoded, self.source.charge.budget()).map_err(TlsConfigError::from_tls)?;
        let right = CertificateAlternativeNames::decode(&right.encoded, self.source.charge.budget()).map_err(TlsConfigError::from_tls)?;
        Ok(left.extension_value() == right.extension_value())
    }

    fn name(&self, name: &X509NameRef) -> TlsConfigResult<DistinguishedName> {
        DistinguishedName::decode(&name.to_der().map_err(TlsConfigError::invalid)?, self.source.charge.budget()).map_err(TlsConfigError::from_tls)
    }
    fn identifiers(&self, index: usize) -> TlsConfigResult<CertificateIdentifiers> {
        CertificateIdentifiers::decode(&self.candidates[index].encoded, self.source.charge.budget()).map_err(TlsConfigError::from_tls)
    }
    fn anchor(&self, index: usize) -> Option<usize> {
        self.anchors.iter().copied().find(|anchor| self.candidates[*anchor].encoded == self.candidates[index].encoded)
    }
    fn identity(&self, index: usize) -> usize { self.candidates[index].certificate.as_ptr() as usize }
    fn certificate_order(&self, initial: usize, indices: impl IntoIterator<Item = usize>) -> Vec<usize> {
        let mut unique = Vec::new();
        for index in indices {
            if !unique.iter().any(|previous: &usize| self.candidates[*previous].encoded == self.candidates[index].encoded) { unique.push(index); }
        }
        let order = rtdb_types::java_hash_map_order(initial, unique.iter(), |index| self.candidates[*index].hash,
            |left, right| self.identity(*left).cmp(&self.identity(*right))).expect("admitted Java certificate map capacity");
        order.into_iter().map(|index| unique[index]).collect()
    }
    fn identity_order(&self, initial: usize, indices: &[usize]) -> Vec<usize> {
        let order = rtdb_types::java_hash_map_order(initial, indices.iter(), |index| (self.identity(*index) as i32) & i32::MAX,
            |left, right| self.identity(*left).cmp(&self.identity(*right))).expect("admitted Java anchor map capacity");
        order.into_iter().map(|index| indices[index]).collect()
    }
}

fn capacity(size: usize) -> Result<usize> {
    size.checked_mul(4).map(|size| size.div_ceil(3)).ok_or_else(|| invalid("TLS Java collection capacity exceeds allocation range"))
}

#[cfg(test)]
mod tests;
