use rtdb_crypt::CustodyVault;
use rtdb_types::{Anomaly, Result, StructFields, Value};
use sha2::{Digest, Sha256};

use super::{Cell, CustodyPayload, CustodyPlacement, CustodyPlacementCommitments, Op, Row, Transact};

pub async fn prepare_custody(request: &mut Transact, vault: &CustodyVault) -> Result<()> {
    let fingerprint = request.logical_sha256()?;
    request.custody_request_digest = Some(fingerprint);
    for action in request.ops.iter().chain(&request.otherwise) {
        if let Op::CreateCustodyGroup { group } = action {
            if let Err(error) = vault.create(*group).await
                && error.code != "rtdb/erased-under-view"
            {
                return Err(error);
            }
        }
    }
    for (arm, actions) in [&mut request.ops, &mut request.otherwise].into_iter().enumerate() {
        if let Err(error) = prepare_arm(actions, vault, fingerprint, arm).await {
            // The fold can recover the exact receipt from the logical digest after key destruction.
            if error.code == "rtdb/erased-under-view" { return Ok(()); }
            return Err(error);
        }
    }
    Ok(())
}

async fn prepare_arm(ops: &mut [Op], vault: &CustodyVault, fingerprint: [u8; 32], arm: usize) -> Result<()> {
    for (action, op) in ops.iter_mut().enumerate() {
        match op {
            Op::Put { rows, .. } => {
                for (index, row) in rows.iter_mut().enumerate() {
                    if !row.protected_columns.is_empty() {
                        seal_row(row, vault, binding(fingerprint, arm, action, index)).await?;
                    }
                }
            }
            Op::PlaceCustodyRow { row, placement, .. } => {
                prepare_placement(row, placement)?;
                seal_row(row, vault, binding(fingerprint, arm, action, 0)).await?;
            }
            Op::LandCustodyBranch { placements, .. } => {
                for (index, reviewed) in placements.iter_mut().enumerate() {
                    prepare_placement(&reviewed.row, &mut reviewed.placement)?;
                    seal_row(&mut reviewed.row, vault, binding(fingerprint, arm, action, index)).await?;
                }
            }
            _ => (),
        }
    }
    Ok(())
}

fn prepare_placement(row: &Row, placement: &mut CustodyPlacement) -> Result<()> {
    let expected = 1 + usize::from(placement.link_column.is_some());
    if placement.reservation.is_some() && placement.derived.is_some()
        || (placement.reservation.is_some() || placement.derived.is_some()) != placement.link_column.is_some()
        || row.protected_columns.len() != expected
        || placement.link_column.as_ref() == Some(&placement.carrier_column)
    {
        return Err(invalid_carrier());
    }
    let digest = |column: &str| {
        if !row.protected_columns.iter().any(|protected| protected == column) {
            return Err(invalid_carrier());
        }
        match row.cells.get(column) {
            Some(Cell::Value(Value::Binary(bytes) | Value::FixedSizeBinary(bytes))) if !bytes.is_empty() => {
                Ok(Sha256::digest(bytes).into())
            }
            _ => Err(invalid_carrier()),
        }
    };
    placement.commitments = Some(CustodyPlacementCommitments {
        carrier_sha256: digest(&placement.carrier_column)?,
        link_sha256: placement.link_column.as_deref().map(digest).transpose()?,
    });
    Ok(())
}

async fn seal_row(row: &mut Row, vault: &CustodyVault, binding: [u8; 32]) -> Result<()> {
    let group = row.custody_group.ok_or_else(missing_group)?;
    let mut protected = StructFields::new();
    for column in &row.protected_columns {
        let Some(Cell::Value(value)) = row.cells.remove(column) else {
            return Err(Anomaly::incorrect("rtdb/custody-payload", "A protected column requires a literal cell"));
        };
        protected.insert(column.clone(), value);
    }
    let clear = serde_json::to_vec(&protected).map_err(payload_error)?;
    let envelope = vault.seal(group, binding, &clear).await?;
    row.custody_payload = Some(CustodyPayload { binding, envelope });
    Ok(())
}

fn binding(fingerprint: [u8; 32], arm: usize, operation: usize, row: usize) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rtdb/v1/custody-row\0");
    hash.update(fingerprint);
    for index in [arm, operation, row] {
        hash.update((index as u64).to_be_bytes());
    }
    hash.finalize().into()
}

fn invalid_carrier() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/custody-placement",
        "Placement requires exact protected binary carrier and reserved link columns",
    )
}

fn missing_group() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/custody-payload",
        "A protected payload requires a custody group",
    )
}

fn payload_error(error: serde_json::Error) -> Anomaly {
    Anomaly::fault("rtdb/custody-payload", error.to_string())
}
