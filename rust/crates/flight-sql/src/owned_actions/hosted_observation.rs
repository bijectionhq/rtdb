use super::*;
use rtdb_node::AppliedOwnedPoint;

pub(crate) const OBSERVE_HELD_HOSTED_V1: &str = "rtdb.owned.ObserveHeldHosted.v1";
pub(crate) const CONFIRM_APPLIED_HOSTED_V3: &str = "rtdb.owned.ConfirmAppliedHosted.v3";

fn applied_point(body: &[u8]) -> Result<(Option<AppliedOwnedPoint>, &[u8]), Status> {
    let (kind, body) = body.split_first().ok_or_else(|| invalid("Applied confirmation requires a point flag"))?;
    match kind {
        0 => return Ok((None, body)),
        1 => (),
        _ => return Err(invalid("Applied confirmation supports at most one point")),
    }
    let (relation, body) = split_relation(body)?;
    let (size, body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Missing applied point key size"))?;
    let size = usize::from(u16::from_be_bytes(*size));
    if !(1..=4096).contains(&size) {
        return Err(invalid("Applied point key exceeds its bound"));
    }
    let (id, body) = body.split_at_checked(size).ok_or_else(|| invalid("Truncated applied point key"))?;
    let id = std::str::from_utf8(id).map_err(|_| invalid("Applied point key is not UTF-8"))?;
    let (valid_at, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing applied point valid time"))?;
    let (present, body) = body.split_first().ok_or_else(|| invalid("Missing applied point revision flag"))?;
    let (expected, body) = match present {
        0 => (None, body),
        1 => {
            let (revision, rest) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Truncated applied point revision"))?;
            (Some(Timestamp(i64::from_be_bytes(*revision))), rest)
        },
        _ => return Err(invalid("Invalid applied point revision flag")),
    };
    Ok((Some(AppliedOwnedPoint {
        relation, id: EntityId::String(id.to_owned()),
        valid_at: Timestamp(i64::from_be_bytes(*valid_at)), expected,
    }), body))
}

impl Service {
    pub(crate) async fn confirm_applied_hosted_action(
        &self,
        call: &Call,
        body: &[u8],
    ) -> Result<ActionResult, Status> {
        let policy = self.hosted_control_policy(call)?;
        let (handle, body) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Applied confirmation requires a held handle"))?;
        let (offset, body) = body.split_first_chunk::<8>()
            .ok_or_else(|| invalid("Applied confirmation requires a floor transaction"))?;
        let (time, body) = body.split_first_chunk::<8>()
            .ok_or_else(|| invalid("Applied confirmation requires an exact floor time"))?;
        let (deadline, body) = body.split_first_chunk::<8>()
            .ok_or_else(|| invalid("Applied confirmation requires a deadline"))?;
        let offset = i64::from_be_bytes(*offset);
        if offset < 0 { return Err(invalid("Applied confirmation requires a nonnegative transaction ID")); }
        let floor = rtdb_types::TransactionKey {
            tx_id: rtdb_types::TxId(offset),
            system_time: Timestamp(i64::from_be_bytes(*time)),
        };
        let held = self.retained_owned_read(call, &Bytes::copy_from_slice(handle))?;
        let _framing = call.reserve(MAX_COMMIT_GROUPS * 512 + 8192)?;
        let (point, body) = applied_point(body)?;
        let groups = owned_groups(body, true, false)?;
        hosted_control::admit_control_targets(policy, groups.iter().map(|(relation, _)| relation))?;
        let scope = self.hosted_control_scope(call).await?;
        let mut rows_left = MAX_GROUPED_COMMIT_ROWS;
        let (controls, _control_charges) =
            self.decode_hosted_control_groups(call, groups, scope, &mut rows_left, true)?;
        let until = Timestamp(i64::from_be_bytes(*deadline));
        let covered = held.snapshot.confirm_applied_hosted_read(scope, floor, until, &controls, point.as_ref())
            .await.map_err(status)?;
        let bytes = match covered {
            Some(covered) => {
                let mut bytes = Vec::with_capacity(16);
                bytes.extend_from_slice(&covered.tx_id.0.to_be_bytes());
                bytes.extend_from_slice(&covered.system_time.0.to_be_bytes());
                Bytes::from(bytes)
            }
            None => Bytes::from_static(&[0]),
        };
        Ok(ActionResult { body: wire::retain(bytes, call.reserve(16)?) })
    }

    pub(crate) async fn observe_held_hosted_action(
        &self,
        call: &Call,
        body: &[u8],
    ) -> Result<ActionResult, Status> {
        let policy = self.hosted_control_policy(call)?;
        let (handle, body) = body
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
        let (controls, _control_charges) =
            self.decode_hosted_control_groups(call, groups, scope, &mut rows_left, true)?;
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
}
