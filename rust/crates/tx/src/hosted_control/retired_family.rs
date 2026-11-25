use std::fmt::Write;
use std::sync::Arc;

use rtdb_columnar::ArrowCell;
use rtdb_query::{QueryContext, read_hosted_control_point, owned_row_event_position};
use rtdb_types::{AssertionFamilyFenceMarker, Position, RootLandingBinding, RootRowCarrier, TransactionKey, TxId};
use sha2::{Digest, Sha256};

use crate::{TxOp, source::Operation};

use super::*;

const BODY: usize = 716;
const FRAME: usize = BODY + 32;

pub(crate) struct RetiredFamilyFence<'a> {
    enrollment: &'a [u8; FRAME],
    installation: [u8; 16],
    family: [u8; 16],
    position: Position,
    fence: TransactionKey,
    through: TransactionKey,
}

fn key(bytes: [u8; 16]) -> TransactionKey {
    let (id, time) = bytes.split_first_chunk::<8>().expect("transaction frame");
    TransactionKey { tx_id: TxId(i64::from_be_bytes(*id)), system_time: Timestamp(i64::from_be_bytes(time.try_into().expect("transaction time"))) }
}

fn row_id<'a>(row: &OwnedDocumentRow<'a>) -> Option<&'a str> {
    match *row {
        OwnedDocumentRow::Arrow(page, index) if page.legacy_owned_presence().is_none() => {
            let array = page.array();
            array.fields().iter().zip(array.columns()).find(|(field, _)| field.name() == "_id")
                .and_then(|(field, array)| ArrowCell { field, array: array.as_ref(), row: index }.utf8())
        }
        _ => row.string_id(),
    }
}

fn name_hex(value: &str, width: usize) -> bool {
    value.len() == width && value.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn row_journal_relation(name: &str) -> bool {
    let Some(name) = name.strip_prefix("fact_") else { return false; };
    name_hex(name, 64) || name.split_once("_t_").is_some_and(|(tablet, label)| name_hex(tablet, 32) && name_hex(label, 32))
}

fn only_columns(row: &OwnedDocumentRow<'_>, columns: &[&str]) -> bool {
    match row {
        OwnedDocumentRow::Arrow(page, _) if page.legacy_owned_presence().is_none() =>
            page.columns().iter().all(|name| columns.contains(&name.as_str())),
        _ => row.only_columns(columns),
    }
}

impl<'a> RetiredFamilyFence<'a> {
    fn decode(enrollment: &'a [u8]) -> Result<Self> {
        let enrollment: &[u8; FRAME] = enrollment.try_into().map_err(|_| authority_required())?;
        if &enrollment[..4] != b"RFC1" { return Err(authority_required()); }
        let root = RootLandingBinding::from_bytes(enrollment[404..652].try_into().expect("root binding width"))
            .map_err(|_| authority_required())?;
        Ok(Self {
            enrollment,
            installation: enrollment[92..108].try_into().expect("installation width"),
            family: enrollment[348..364].try_into().expect("family width"),
            position: root.position,
            fence: key(enrollment[220..236].try_into().expect("fence width")),
            through: key(enrollment[236..252].try_into().expect("frontier width")),
        })
    }

    pub(crate) fn from_control(control: &'a HostedControlWrite) -> Result<Option<Self>> {
        let Some(row) = control.source_row() else { return Ok(None); };
        let Some(value) = row.get("retired_family_grant") else { return Ok(None); };
        let Value::Binary(enrollment) = value else { return Err(authority_required()); };
        let proof = Self::decode(enrollment)?;
        if row.len() != 2 || control.expected() != Some(proof.fence.system_time) { return Err(authority_required()); }
        Ok(Some(proof))
    }

    pub(crate) fn require_batch(&self, operations: &[Operation], retirement: &str) -> Result<()> {
        let address = retirement.strip_prefix("row/").filter(|address| !address.is_empty())
            .ok_or_else(authority_required)?;
        let address = RootRowCarrier::decode_named_address(address).ok_or_else(authority_required)?;
        let marker = RootRowCarrier::parse(&address).ok_or_else(authority_required)?.retirement_key();
        let mut marker_seen = false;
        let mut row_marker_seen = false;
        for operation in operations {
            match operation {
                Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheck { .. }) => continue,
                Operation::Public(TxOp::HostedControl(control)) if control.is_check() => continue,
                Operation::Public(TxOp::HostedControl(control)) if control.is_assertion_family_marker() && !marker_seen => {
                    let EntityId::String(id) = control.id() else { return Err(authority_required()); };
                    let family = AssertionFamilyFenceMarker::family_id(id).map_err(|_| authority_required())?;
                    let replacement = control.replacement().ok_or_else(authority_required)?;
                    let Some(Value::Binary(body)) = replacement.get("body") else { return Err(authority_required()); };
                    let marker = AssertionFamilyFenceMarker::decode(body).map_err(|_| authority_required())?;
                    if family != self.family || marker.winner != self.position || marker.selected != self.position {
                        return Err(authority_required());
                    }
                    marker_seen = true;
                    continue;
                }
                Operation::Public(TxOp::OwnedErase { relation, .. }) if family_root_erase(relation) => continue,
                _ => (),
            }
            let write = owned_document_write(operation).filter(|write| write.relation.is_root_data())
                .ok_or_else(authority_required)?;
            let name = write.relation.name();
            if family_search_erase_marker(&write) { continue; }
            let admitted = match name {
                "fact_unit_fences" =>
                    write.create_only && write.documents.row_count() == 1
                        && write.documents.rows().all(|row| row_id(&row) == Some(retirement)
                            && only_columns(&row, &["_id", "unit_phase", "assertion_vault_inventory"])),
                "fact_row_retirements" => write.create_only && write.documents.row_count() == 1
                    && write.documents.rows().all(|row| row_id(&row) == Some(retirement)
                        && only_columns(&row, &["_id", "pending"])),
                "fact_attempts" => write.create_only && write.documents.rows().all(|row| only_columns(&row, &["_id", "fingerprint"])),
                "fact_publications" | "fact_public_root_versions" | "fact_row_probe_floors" | "fact_row_publications" =>
                    !write.create_only && write.documents.rows().all(|row| only_columns(&row, &["_id"])),
                _ if !row_marker_seen && row_journal_relation(name) => {
                    row_marker_seen = true;
                    write.create_only && write.documents.row_count() == 1
                        && write.documents.rows().all(|row| only_columns(&row, &["_id"]) && row.binary_id() == Some(marker.as_slice()))
                }
                _ => false,
            };
            if !admitted { return Err(authority_required()); }
        }
        if !marker_seen { return Err(authority_required()); }
        Ok(())
    }

    pub(crate) async fn current(&self, context: &Arc<QueryContext>, installation: &rtdb_columnar::OwnedBatch) -> Result<bool> {
        let row = installation.batch();
        let cell = |name| ArrowCell::named(row, name, 0);
        if cell("kind").and_then(ArrowCell::utf8) != Some("hosted")
            || cell("incarnation").and_then(ArrowCell::binary) != Some(self.installation.as_slice())
            || cell("writer").and_then(ArrowCell::utf8) != Some("")
            || cell("placement_assignment").and_then(ArrowCell::binary) != Some(&[][..])
            || owned_row_event_position(row, 0)?.transaction != self.fence.tx_id
        {
            return Ok(false);
        }
        let restore = TableRef::new("public", "bj_rtdb_restore_hold");
        let held = read_hosted_control_point(Arc::clone(context), restore.clone(), &EntityId::String("restore".into()), true).await?;
        if held.row.is_some() { return Ok(false); }
        let fence = read_hosted_control_point(
            Arc::clone(context), restore.clone(), &EntityId::String("archive-source-fence.v1".into()), true,
        ).await?;
        if fence.receipt_tx_id != Some(self.fence.tx_id.0)
            || fence.row.as_ref().map(|(_, revision)| *revision) != Some(self.fence.system_time)
        {
            return Ok(false);
        }
        let mut ordered = None;
        let locators: [(&str, [u8; 32]); 2] = [
            ("fenced-family-custody/", Sha256::digest(&self.enrollment[..BODY]).into()),
            ("fenced-family-replay/", self.enrollment[284..316].try_into().expect("replay digest width")),
        ];
        for (prefix, digest) in locators {
            let mut id = String::from(prefix);
            for byte in digest { write!(id, "{byte:02x}").expect("writing to a string"); }
            let point = read_hosted_control_point(Arc::clone(context), restore.clone(), &EntityId::String(id), true).await?;
            let (Some(transaction), Some((row, revision))) = (point.receipt_tx_id, point.row) else { return Ok(false); };
            if transaction <= self.through.tx_id.0 || revision <= self.through.system_time
                || ArrowCell::named(row.batch(), "binding", 0).and_then(ArrowCell::binary) != Some(self.enrollment.as_slice())
                || ordered.replace((transaction, revision)).is_some_and(|prior| prior != (transaction, revision))
            {
                return Ok(false);
            }
        }
        let custody = read_hosted_control_point(
            Arc::clone(context), TableRef::new("public", "bj_rtdb_custody_binding"), &EntityId::String("authority".into()), true,
        ).await?;
        Ok(custody.row.is_some_and(|(row, _)| {
            ArrowCell::named(row.batch(), "fingerprint", 0).and_then(ArrowCell::binary) == Some(&self.enrollment[140..172])
        }))
    }
}

pub fn bind_retired_family_authority(
    controls: &mut [HostedControlWrite], authority: HostedAuthority, enrollment: &[u8],
) -> Result<()> {
    let fence = RetiredFamilyFence::decode(enrollment)?;
    let (index, control) = installed_authority_control(controls.iter())?;
    if control.expected() != Some(fence.fence.system_time)
        || authority.revision != fence.fence.system_time || authority.incarnation != fence.installation
        || authority.writer != [0; 16]
    {
        return Err(authority_required());
    }
    controls[index].document = HostedControlDocument::Row(StructFields::from([
        ("_id".into(), Value::Utf8(INSTALLATION_AUTHORITY_ID.into())),
        ("retired_family_grant".into(), Value::Binary(enrollment.to_vec())),
    ]));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use rtdb_types::{AssertionId, RootLandingTarget};

    use super::*;

    #[test]
    fn a_retired_fence_credential_cannot_replace_an_ordinary_root_row() -> Result<()> {
        let position = Position { line_generation: [1; 16], publication: 5, ordinal: NonZeroU64::MIN };
        let binding = RootLandingBinding {
            position, assertion: AssertionId::from_bytes([2; 32]), stage: [3; 16],
            target: RootLandingTarget { receipt: [4; 32], review_slot: 0, address_extent: [5; 32], disclosure: [6; 32] },
            payload_digest: [7; 32], link_digest: [8; 32],
        };
        let mut enrollment = [0; FRAME];
        enrollment[..4].copy_from_slice(b"RFC1");
        enrollment[92..108].fill(9);
        enrollment[220..228].copy_from_slice(&1_i64.to_be_bytes());
        enrollment[228..236].copy_from_slice(&6_i64.to_be_bytes());
        enrollment[404..652].copy_from_slice(&binding.to_bytes());
        let proof = RetiredFamilyFence::decode(&enrollment)?;
        let operation = Operation::Public(TxOp::OwnedErase {
            relation: OwnedRelation::new("fact_unit_column_payloads")?,
            id: EntityId::String("another-carrier".into()),
        });
        assert!(proof.require_batch(&[operation], "row/exact").is_err());
        assert!(proof.require_batch(&[], "row/exact").is_err());

        let relation = OwnedRelation::new(INSTALLATION_AUTHORITY_RELATION)?;
        let scope = PlacedAttemptScope::new("retired-family-test", 1)?;
        let expected = Some(Timestamp(6));
        let locator = StructFields::from([("_id".into(), Value::Utf8(INSTALLATION_AUTHORITY_ID.into()))]);
        let mut controls = [HostedControlWrite::check(relation.clone(), scope, expected, locator)?];
        let mut header = [0; HostedAuthority::WIDTH];
        header[..8].copy_from_slice(&6_i64.to_be_bytes());
        header[8..24].fill(9);
        bind_retired_family_authority(&mut controls, HostedAuthority::decode(&header)?, &enrollment)?;
        let document = controls[0].source_row().unwrap().clone();
        assert!(HostedControlWrite::check(relation.clone(), scope, expected, document.clone()).is_err());
        let restored = HostedControlWrite::source_check(relation, scope, expected, document)?;
        assert!(RetiredFamilyFence::from_control(&restored)?.is_some());
        Ok(())
    }
}
