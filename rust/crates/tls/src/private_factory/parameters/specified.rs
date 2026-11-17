use super::*;
mod names;

pub(super) const OID: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3];

pub(super) fn name(child: DigestName) -> DigestName {
    if let DigestName::Oid(oid) = child {
        if let Some((_, result)) = names::DERIVED.iter().find(|(digest, _)| *digest == oid.as_slice()) {
            return DigestName::Oid(Zeroizing::new(result.to_vec()));
        }
    }
    DigestName::Unmapped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_names_match_the_complete_pinned_known_oid_registry() {
        let rows = include_str!("specified/known_names.tsv").lines().skip(1).collect::<Vec<_>>();
        assert_eq!(rows.len(), 280);
        assert_eq!(rows.iter().filter(|row| row.split('\t').nth(4).unwrap() != "-").count(), 5);
        assert_eq!(names::DERIVED.len(), 5);
        for &(digest, signature) in names::DERIVED {
            assert!(matches!(name(DigestName::Oid(Zeroizing::new(digest.to_vec()))), DigestName::Oid(oid) if oid.as_slice() == signature));
            assert!(matches!(name(DigestName::Oid(Zeroizing::new(signature.to_vec()))), DigestName::Unmapped));
        }
        for oid in [OID, &[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 5], &[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 7], &[0x2a, 3, 4]] {
            assert!(matches!(name(DigestName::Oid(Zeroizing::new(oid.to_vec()))), DigestName::Unmapped));
        }
        assert!(matches!(name(DigestName::Unmapped), DigestName::Unmapped));
    }
}
