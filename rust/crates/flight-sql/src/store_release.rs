use tonic::{Status, metadata::{Ascii, MetadataMap, MetadataValue}};

pub const HEADER: &str = "x-rtdb-store-release";
pub const LINKED_RELEASE: &str = ::store::CONTRACT_VERSION;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreRelease(&'static str);

impl StoreRelease {
    const LINKED: Self = Self(LINKED_RELEASE);

    pub(crate) fn negotiate(headers: &MetadataMap, verified_credential: bool) -> Result<Self, Status> {
        if !verified_credential {
            return Err(Status::unauthenticated("A store release handshake requires verified credentials"));
        }
        let mut offers = headers.get_all(HEADER).iter();
        let offer = offers.next().filter(|_| offers.next().is_none())
            .ok_or_else(|| Status::invalid_argument("A store release handshake requires exactly one release"))?;
        let linked = Self::LINKED;
        if offer.as_encoded_bytes() != linked.0.as_bytes() {
            return Err(Status::failed_precondition("The store release does not match the linked release"));
        }
        Ok(linked)
    }

    pub(crate) fn require(binding: Option<Self>) -> Result<(), Status> {
        if binding != Some(Self::LINKED) {
            return Err(Status::permission_denied("The store action requires a release-checked Flight session"));
        }
        Ok(())
    }

    pub(crate) fn metadata(self) -> MetadataValue<Ascii> {
        MetadataValue::from_static(self.0)
    }
}
