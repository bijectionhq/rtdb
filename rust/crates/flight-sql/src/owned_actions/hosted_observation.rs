use super::*;
use rtdb_node::AppliedOwnedPoint;

pub(crate) const OBSERVE_HELD_HOSTED_V1: &str = "rtdb.owned.ObserveHeldHosted.v1";
pub(crate) const CONFIRM_APPLIED_HOSTED_V6: &str = "rtdb.owned.ConfirmAppliedHosted.v6";
pub(crate) const CONFIRM_HOSTED_SOURCE_V1: &str = "rtdb.owned.ConfirmHostedSource.v1";

pub(super) fn applied_read_floor(body: &[u8]) -> Result<(rtdb_node::AppliedReadFloor, &[u8]), Status> {
    let (history, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Applied read requires a history"))?;
    let (&count, mut body) = body.split_first().ok_or_else(|| invalid("Applied read requires a transaction count"))?;
    if !(1..=4).contains(&count) { return Err(invalid("Invalid applied read transaction count")); }
    let mut transactions = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        let (offset, rest) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Applied read requires a floor transaction"))?;
        let (time, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Applied read requires an exact floor time"))?;
        transactions.push(rtdb_types::TransactionKey {
            tx_id: rtdb_types::TxId(i64::from_be_bytes(*offset)),
            system_time: Timestamp(i64::from_be_bytes(*time)),
        });
        body = rest;
    }
    let floor = rtdb_node::AppliedReadFloor::new(Uuid::from_bytes(*history), transactions).map_err(status)?;
    Ok((floor, body))
}

pub(super) fn confirmation_reply(
    call: &Call, confirmation: Option<rtdb_node::AppliedReadConfirmation>,
) -> Result<ActionResult, Status> {
    let bytes = match confirmation {
        Some(confirmation) => {
            let mut bytes = Vec::with_capacity(24);
            bytes.extend_from_slice(&confirmation.covered.tx_id.0.to_be_bytes());
            bytes.extend_from_slice(&confirmation.covered.system_time.0.to_be_bytes());
            bytes.extend_from_slice(&confirmation.observed_at.0.to_be_bytes());
            Bytes::from(bytes)
        }
        None => Bytes::from_static(&[0]),
    };
    Ok(ActionResult { body: wire::retain(bytes, call.reserve(24)?) })
}

fn applied_point(body: &[u8]) -> Result<(Option<AppliedOwnedPoint>, &[u8]), Status> {
    let (kind, body) = body.split_first().ok_or_else(|| invalid("Applied confirmation requires a point flag"))?;
    match kind {
        0 => return Ok((None, body)),
        1 | 2 => (),
        _ => return Err(invalid("Applied confirmation supports at most one point")),
    }
    let (relation, body) = split_relation(body)?;
    let (size, body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Missing applied point key size"))?;
    let size = usize::from(u16::from_be_bytes(*size));
    let typed = *kind == 2;
    let tag_bytes = usize::from(typed);
    if !(1 + tag_bytes..=4096 + tag_bytes).contains(&size) {
        return Err(invalid("Applied point key exceeds its bound"));
    }
    let (id, body) = body.split_at_checked(size).ok_or_else(|| invalid("Truncated applied point key"))?;
    let id = if typed {
        EntityId::from_handle_bytes(id).map_err(status)?
    } else {
        EntityId::String(std::str::from_utf8(id).map_err(|_| invalid("Applied point key is not UTF-8"))?.to_owned())
    };
    let (valid_at, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing applied point valid time"))?;
    let (present, body) = body.split_first().ok_or_else(|| invalid("Missing applied point revision flag"))?;
    let (expected, body) = match present {
        0 => (None, body),
        1 => {
            let (revision, rest) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Truncated applied point revision"))?;
            let (position, rest) = rest.split_first_chunk::<16>().ok_or_else(|| invalid("Truncated applied point position"))?;
            (super::owned_point_expectation(Some(Timestamp(i64::from_be_bytes(*revision))), Some(position))?, rest)
        },
        _ => return Err(invalid("Invalid applied point revision flag")),
    };
    Ok((Some(AppliedOwnedPoint {
        relation, id,
        valid_at: Timestamp(i64::from_be_bytes(*valid_at)), expected,
    }), body))
}

impl Service {
    pub(crate) async fn confirm_hosted_source_action(&self, call: &Call, body: &[u8], stream: bool)
        -> Result<OwnedIpcReply, Status> {
        self.hosted_control_policy(call)?;
        let (history, suffix) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("The hosted source confirmation requires a history"))?;
        let history = Uuid::from_bytes(*history);
        let floor = if suffix == [0] {
            None
        } else {
            let (floor, rest) = applied_read_floor(body)?;
            if !rest.is_empty() { return Err(invalid("Unexpected hosted source confirmation data")); }
            Some(floor)
        };
        let connection = self.connection(call, &call.database).await?;
        let native = connection.borrow().await?;
        require_idle(&native)?;
        let applied = native.confirm_hosted_source_prefix(history, floor.as_ref()).await.map_err(status)?;
        let rows = RecordBatch::try_from_iter_with_nullable([
            ("tx_id", Arc::new(Int64Array::from(vec![applied.map(|key| key.tx_id.0)])) as ArrayRef, true),
            ("system_time_micros", Arc::new(Int64Array::from(vec![applied.map(|key| key.system_time.0)])), true),
        ]).map_err(arrow)?;
        self.encode_owned_stream_reply(call.memory(), rows.schema_ref(), std::slice::from_ref(&rows), stream)
    }

    pub(crate) async fn confirm_applied_hosted_action(
        &self,
        call: &Call,
        envelope: &Bytes,
    ) -> Result<ActionResult, Status> {
        let policy = self.hosted_control_policy(call)?;
        let (handle, body) = envelope.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Applied confirmation requires a held handle"))?;
        let (floor, body) = applied_read_floor(body)?;
        let (deadline, body) = body.split_first_chunk::<8>()
            .ok_or_else(|| invalid("Applied confirmation requires a deadline"))?;
        let held = self.retained_owned_read(call, &Bytes::copy_from_slice(handle))?;
        let _framing = call.reserve(MAX_COMMIT_GROUPS * 512 + 8192)?;
        let (point, body) = applied_point(body)?;
        let groups = owned_groups(body, true, false)?;
        hosted_control::admit_control_targets(policy, groups.iter().map(|(relation, _)| relation))?;
        let scope = self.hosted_control_scope(call).await?;
        let mut rows_left = MAX_GROUPED_COMMIT_ROWS;
        let control_frame = call.retain_request_bytes(envelope.clone());
        let (controls, _control_charges) =
            self.decode_hosted_control_groups(call, &control_frame, groups, scope, &mut rows_left, true)?;
        let until = Timestamp(i64::from_be_bytes(*deadline));
        let covered = held.snapshot.confirm_applied_hosted_read(scope, &floor, until, &controls, point.as_ref())
            .await.map_err(status)?;
        confirmation_reply(call, covered)
    }

    pub(crate) async fn observe_held_hosted_action(
        &self,
        call: &Call,
        envelope: &Bytes,
    ) -> Result<ActionResult, Status> {
        let policy = self.hosted_control_policy(call)?;
        let (handle, body) = envelope
            .split_first_chunk::<16>()
            .ok_or_else(|| invalid("Hosted observation requires a held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (witnesses, body, _witness_charge) = self.owned_witness_prefix(call, body, false)?;
        if witnesses
            .iter()
            .any(|(origin, _)| !Arc::ptr_eq(origin, &held))
        {
            return Err(invalid("Hosted observations must share one original hold"));
        }
        let (deadline, body) = body
            .split_first_chunk::<8>()
            .ok_or_else(|| invalid("Hosted observation requires a deadline"))?;
        let _framing = call.reserve(MAX_COMMIT_GROUPS * 512)?;
        let groups = owned_groups(body, true, false)?;
        hosted_control::admit_control_targets(policy, groups.iter().map(|(relation, _)| relation))?;
        let scope = self.hosted_control_scope(call).await?;
        let mut rows_left = MAX_GROUPED_COMMIT_ROWS;
        let control_frame = call.retain_request_bytes(envelope.clone());
        let (controls, _control_charges) =
            self.decode_hosted_control_groups(call, &control_frame, groups, scope, &mut rows_left, true)?;
        let witnesses = witnesses
            .into_iter()
            .map(|(_, evidence)| evidence)
            .collect::<Vec<_>>();
        let until = Timestamp(i64::from_be_bytes(*deadline));
        let covered = held
            .snapshot
            .observe_hosted_dependencies(scope, until, &witnesses, &controls)
            .await
            .map_err(status)?;
        let bytes = match covered {
            Some(covered) => Bytes::copy_from_slice(&covered.0.to_be_bytes()),
            None => Bytes::from_static(&[0]),
        };
        Ok(ActionResult {
            body: wire::retain(bytes, call.reserve(8)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applied_confirmation_distinguishes_no_point_from_exact_absence() {
        let (point, remaining) = applied_point(&[0, 7]).unwrap();
        assert!(point.is_none());
        assert_eq!(remaining, &[7]);

        let mut body = vec![1, 0, 1, b'r', 0, 1, b'x'];
        body.extend_from_slice(&17_i64.to_be_bytes());
        assert!(applied_point(&body).is_err());
        body.extend_from_slice(&[0, 7]);
        let (point, remaining) = applied_point(&body).unwrap();
        let point = point.unwrap();
        assert_eq!(point.relation.name(), "r");
        assert_eq!(point.id, EntityId::String("x".to_owned()));
        assert_eq!(point.valid_at, Timestamp(17));
        assert!(point.expected.is_none());
        assert_eq!(remaining, &[7]);
    }

    #[test]
    fn applied_confirmation_reads_the_complete_binary_key_before_the_original_point_stamp() {
        let mut body = vec![2, 0, 1, b'r', 0, 4, 4, 0, 255, 0];
        body.extend_from_slice(&17_i64.to_be_bytes());
        body.push(1);
        body.extend_from_slice(&13_i64.to_be_bytes());
        let position = rtdb_types::EventPosition { transaction: rtdb_types::TxId(6), ordinal: 8 };
        body.extend_from_slice(&position.to_bytes());
        body.extend_from_slice(&[0, 1, 9]);
        let (point, controls) = applied_point(&body).unwrap();
        let point = point.unwrap();
        assert_eq!(point.id, EntityId::Binary(vec![0, 255, 0]));
        assert_eq!(point.valid_at, Timestamp(17));
        assert_eq!(point.expected, Some(rtdb_types::OwnedPointVersion { history: Timestamp(13), position }));
        assert_eq!(controls, &[0, 1, 9]);

        body[6] = 5;
        let error = applied_point(&body[..10]).err().unwrap();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error.message(), "Invalid complete typed identity");

        body[0] = 1;
        let error = applied_point(&body).err().unwrap();
        assert_eq!(error.message(), "Applied point key is not UTF-8");
    }
}
