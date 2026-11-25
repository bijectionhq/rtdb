use crate::{
    family_custody::framed_digest,
    recovery::classic_provider::{locator_digest, mismatch},
};

use super::*;

#[derive(Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Registration {
    key: String,
    creator: String,
    address_sha256: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Cohort {
    binding_sha256: [u8; 32],
    registrations: Vec<Registration>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Disposition {
    OriginalProducer,
    RecoveredProvider,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Terminal {
    registration: Registration,
    cutoff: TransactionKey,
    binding_sha256: [u8; 32],
    #[serde(rename = "disposition")]
    _disposition: Disposition,
}

impl Application {
    pub(super) fn read_provider_release(
        &self,
        index: &BranchIndex,
        view: &BranchView<'_>,
        selection: &ReleaseSelection,
        epoch: u64,
        cutoff: TransactionKey,
        binding_sha256: [u8; 32],
    ) -> Result<(TransactionKey, [u8; 32])> {
        let claim = selection.registration()?;
        let owner = claim.owner();
        let authority = self.authority;
        let table = &self.layout.closure.table;
        let registry = self.layout.external_key_recovery.as_ref().ok_or_else(mismatch)?;
        let registry = format!("{}.{}", registry.schema, registry.table);
        let cutoff_view = self.view(index, cutoff)?;
        let cohort = PointSelector {
            table: table.clone(),
            id: EntityId::String(format!("classic-key-delivery-cohort.v1/{epoch:020}")),
        };
        let (row, cells) = point(&cutoff_view, &cohort, cutoff.system_time)?;
        let body = binary(cells, "binding")?;
        if body.len() > MAX_INVENTORY_BYTES
            || transaction(row)? != cutoff
            || self.stamp(table, row, cells)?.position.is_none()
        {
            return Err(mismatch());
        }
        let cohort: Cohort = serde_json::from_slice(body).map_err(|_| mismatch())?;
        if cohort.binding_sha256 != binding_sha256
            || cohort.registrations.len() > MAX_ROWS
            || cohort
                .registrations
                .windows(2)
                .any(|pair| pair[0].key >= pair[1].key)
        {
            return Err(mismatch());
        }
        let selected = cohort
            .registrations
            .binary_search_by(|registration| registration.key.cmp(&selection.locator))
            .ok()
            .map(|index| &cohort.registrations[index])
            .ok_or_else(mismatch)?;
        if owner.deployment != authority
            || owner.installation != *authority.as_bytes()
            || selected.address_sha256 != sha256(&selection.address)
            || selected.creator.is_empty()
            || selected.creator.len() > 128
            || owner.original_writer
                != framed_digest(
                    b"rtdb.multi/carrier-writer/v1\0",
                    &[&owner.installation, selected.creator.as_bytes()],
                )
            || owner.registry
                != framed_digest(
                    b"rtdb.multi/carrier-registry/v1\0",
                    &[
                        &owner.source_history,
                        &owner.installation,
                        registry.as_bytes(),
                    ],
                )
        {
            return Err(mismatch());
        }
        let terminal = PointSelector {
            table: table.clone(),
            id: EntityId::String(format!(
                "classic-key-delivery-settled.v1/{epoch:020}/{}",
                locator_digest(&selection.locator)
            )),
        };
        let (row, cells) = point(view, &terminal, self.cut.system_time)?;
        let stamp = self.stamp(table, row, cells)?;
        let body = binary(cells, "binding")?;
        if body.len() > 4096 {
            return Err(mismatch());
        }
        let terminal: Terminal = serde_json::from_slice(body).map_err(|_| mismatch())?;
        if &terminal.registration != selected
            || terminal.cutoff != cutoff
            || terminal.binding_sha256 != binding_sha256
            || !before(cutoff, stamp.written)
            || !stamp.written.at_or_before(self.cut)
            || stamp.position.is_none()
        {
            return Err(mismatch());
        }
        Ok((stamp.written, sha256(body)))
    }
}
