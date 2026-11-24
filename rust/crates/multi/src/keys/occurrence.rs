use openssl::symm::{Cipher, Crypter, Mode};
use ring::hmac;
use rtdb_types::{Anomaly, AssertionId, PlacedId, Result};

/// Key material from the original authority's registered mint namespace.
pub struct AssertionMintKey {
    namespace: [u8; 16],
    key: [u8; 32],
}

impl AssertionMintKey {
    pub fn new(namespace: [u8; 16], key: [u8; 32]) -> Self {
        Self { namespace, key }
    }

    /// The authority must durably reserve the counter before releasing the ID.
    pub fn id_for_counter(&self, counter: u128) -> Result<AssertionId> {
        let failed = |_| Anomaly::fault(
            "rtdb.multi/assertion-mint", "Could not encrypt the reserved assertion counter",
        );
        let mut cipher = Crypter::new(Cipher::aes_256_ecb(), Mode::Encrypt, &self.key, None)
            .map_err(failed)?;
        cipher.pad(false);
        let mut encrypted = [0; 32];
        let written = cipher.update(&counter.to_be_bytes(), &mut encrypted).map_err(failed)?;
        if written > 16 {
            return Err(Anomaly::fault(
                "rtdb.multi/assertion-mint", "Assertion counter encryption exceeded one block",
            ));
        }
        let written = written + cipher.finalize(&mut encrypted[written..]).map_err(failed)?;
        let mut block = [0; 16];
        if written != block.len() {
            return Err(Anomaly::fault(
                "rtdb.multi/assertion-mint", "Assertion counter encryption did not produce one block",
            ));
        }
        block.copy_from_slice(&encrypted[..16]);
        Ok(AssertionId::from_mint_block(self.namespace, block))
    }
}

pub enum AssertionRoute<'a> {
    BlindIncarnation(&'a [u8]),
    ProposalForm(&'a [u8]),
}

pub struct AssertionRouteKey(hmac::Key);

impl AssertionRouteKey {
    pub fn new(key: &[u8; 32]) -> Self {
        Self(hmac::Key::new(hmac::HMAC_SHA256, key))
    }

    pub fn place(
        &self,
        relation_incarnation: &[u8],
        address: AssertionRoute<'_>,
        assertion: AssertionId,
    ) -> PlacedId {
        let (tag, address) = match address {
            AssertionRoute::BlindIncarnation(handle) => (0, handle),
            AssertionRoute::ProposalForm(form) => (1, form),
        };
        let mut mac = hmac::Context::with_key(&self.0);
        mac.update(b"rtdb/placed-id/route/v1\0");
        mac.update(&(relation_incarnation.len() as u64).to_be_bytes());
        mac.update(relation_incarnation);
        mac.update(&[tag]);
        mac.update(&(address.len() as u64).to_be_bytes());
        mac.update(address);
        let mut route = [0; 32];
        route.copy_from_slice(mac.sign().as_ref());
        PlacedId { route, assertion }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assertion_counter_is_one_aes_256_block() {
        let key = AssertionMintKey::new([0x42; 16], std::array::from_fn(|i| i as u8));
        let bytes = key.id_for_counter(0x00112233445566778899aabbccddeeff).unwrap().to_bytes();
        assert_eq!(&bytes[..16], &[0x42; 16]);
        assert_eq!(&bytes[16..], &[0x8e, 0xa2, 0xb7, 0xca, 0x51, 0x67, 0x45, 0xbf,
                                  0xea, 0xfc, 0x49, 0x90, 0x4b, 0x49, 0x60, 0x89]);
    }

    #[test]
    fn route_binds_component_boundaries_and_the_address_form_tag() {
        let key = AssertionRouteKey::new(&[1; 32]);
        let id = AssertionId::from_bytes([2; 32]);
        let blind = key.place(b"relation", AssertionRoute::BlindIncarnation(b"row"), id);
        let proposal = key.place(b"relation", AssertionRoute::ProposalForm(b"row"), id);
        let other = key.place(b"relationr", AssertionRoute::BlindIncarnation(b"ow"), id);
        let later = key.place(b"relation", AssertionRoute::BlindIncarnation(b"row"),
                              AssertionId::from_bytes([3; 32]));
        assert_ne!(blind.route, proposal.route);
        assert_ne!(blind.route, other.route);
        assert_eq!(blind.route, later.route);
    }
}
