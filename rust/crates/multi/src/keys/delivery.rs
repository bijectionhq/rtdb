use std::sync::Arc;

use ring::digest::{SHA256, digest};
use rtdb_storage::{Error, ObjectKey, Result, sealing::ObjectCipher};

use super::{KEY_BYTES, KeyService, UnitOperation, UnitOutput};

/// The deployment key remains the custody owner for objects sealed under it.
pub(crate) struct DeploymentDelivery {
    keys: Arc<dyn KeyService>,
    id: String,
    binding: [u8; KEY_BYTES],
}

impl DeploymentDelivery {
    pub(crate) fn new(keys: Arc<dyn KeyService>, id: String, material: &[u8; KEY_BYTES]) -> Arc<Self> {
        let binding = digest(&SHA256, material).as_ref().try_into().expect("SHA-256 width");
        Arc::new(Self { keys, id, binding })
    }
}

#[async_trait::async_trait]
impl ObjectCipher for DeploymentDelivery {
    async fn operate(&self, _: &ObjectKey, operation: UnitOperation) -> Result<UnitOutput> {
        if !matches!(operation, UnitOperation::Grant) {
            return Err(Error::Task("Deployment delivery authority only confirms grants".into()));
        }
        self.keys.grant_deployment_key(&self.id, self.binding).await
    }
}

pub(super) fn require_deployment_key(material: Option<[u8; KEY_BYTES]>, binding: [u8; KEY_BYTES]) -> Result<()> {
    let mut current = material.ok_or(Error::ErasedUnderView)?;
    let matches = digest(&SHA256, &current).as_ref() == binding.as_slice();
    current.fill(0);
    if !matches { return Err(Error::ErasedUnderView); }
    Ok(())
}
