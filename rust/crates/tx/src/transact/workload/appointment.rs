use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundedAppointmentScope {
    pub fence: Fence,
    pub writer: Applied,
    pub key: [u8; 32],
    pub receipt: Point,
    pub intent_sha256: [u8; 32],
    pub funding_policy_sha256: [u8; 32],
}

impl FundedAppointmentScope {
    pub(super) fn valid(&self, incarnation: [u8; 16]) -> bool {
        self.read_authority().valid(incarnation)
            && self.key != [0; 32]
            && self.intent_sha256 != [0; 32]
            && self.funding_policy_sha256 != [0; 32]
            && self.receipt.valid_at < Timestamp::MAX
            && bounded(&self.receipt.table.schema) && bounded(&self.receipt.table.table)
            && match &self.receipt.id {
                EntityId::String(id) => bounded(id),
                EntityId::Keyword(id) => bounded(id.as_str()),
                EntityId::Binary(id) => (1..=256).contains(&id.len()),
                EntityId::Integer(_) | EntityId::Uuid(_) => true,
            }
    }

    fn read_authority(&self) -> Authority {
        Authority::ReadRoot { fence: self.fence.clone(), writer: self.writer.clone() }
    }
}

#[derive(Clone)]
pub struct ManagedAppointment {
    point: Point,
    scope: FundedAppointmentScope,
    namespace: [u8; 32],
    expires_at: Timestamp,
}

impl ManagedAppointment {
    pub fn new(point: Point, scope: FundedAppointmentScope, namespace: [u8; 32], expires_at: Timestamp) -> Result<Self> {
        if point.table == scope.receipt.table || namespace == [0; 32] { return Err(denied()); }
        Ok(Self { point, scope, namespace, expires_at })
    }

    pub fn fence(&self) -> &Fence { &self.scope.fence }

    pub fn namespace(&self) -> [u8; 32] { self.namespace }

    pub fn expires_at(&self) -> Timestamp { self.expires_at }

    pub fn claim_binding(&self) -> Result<[u8; 32]> {
        self.scope.read_authority().assignment_binding()?.ok_or_else(denied)
    }

    pub fn check_current(&self, state: &TransactState, now: Timestamp) -> Result<()> {
        if now >= self.expires_at || state.index().has_visibility_history() { return Err(denied()); }
        self.point.check_current(state, now, &self.scope.read_authority())
    }

    pub fn check_key(&self, key: &[u8]) -> Result<()> {
        if key != self.scope.key.as_slice() { return Err(denied()); }
        Ok(())
    }

    pub fn check_request(&self, state: &TransactState, tx: &Transact, command: AppointmentCommand) -> Result<()> {
        self.check_key(&tx.key)?;
        tx.check_appointment_shape(command)?;
        let receipt = &self.scope.receipt;
        tx.check_appointment_address(receipt)?;
        match command {
            AppointmentCommand::Prepare { intent, .. } | AppointmentCommand::Consume { intent, .. }
                if intent != self.scope.intent_sha256 => return Err(denied()),
            _ => {}
        }
        if !matches!(command, AppointmentCommand::Prepare { .. }) {
            self.point.check_write(state.index(), tx, false)?;
        }
        for op in tx.ops.iter().chain(&tx.otherwise) {
            let within_receipt_scope = match op {
                Op::Put { table, rows, .. } if *table == receipt.table => rows.iter().all(|row| row.id == receipt.id),
                Op::Delete { table, .. } if *table == receipt.table => false,
                _ => true,
            };
            if !within_receipt_scope { return Err(denied()); }
        }
        Ok(())
    }

    pub fn status(&self, state: &TransactState, key: &[u8]) -> Result<Receipt> {
        self.check_key(key)?;
        let receipt = state.tx_status_with_namespace(key, Some(self.namespace))?;
        let offer = receipt.decision_clock.filter(|offer| offer.is_appointment() && offer.belongs_to(self.namespace))
            .ok_or_else(unknown_receipt)?;
        offer.check_managed_appointment(&self.scope)?;
        Ok(receipt)
    }
}
