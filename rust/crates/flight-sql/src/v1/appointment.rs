use std::sync::Arc;

use prost::Message;
use rtdb_node::{Database, serving::FundingSubmission};
use rtdb_tx::transact::{
    AppointmentCommand, FUNDED_APPOINTMENT, FundedReceipt, FundingCommand, RELATIVE_VALID_TIME,
    Receipt, TIMED_TRANSACT, TRANSACT_REQUIRES, Transact, workload,
};
use rtdb_types::{Timestamp, rtdb_v1::Basis};
use tonic::Status;

use super::{Call, DatabaseAccess, Service, error, funding_owner, proto, transact};

pub(crate) const PREPARE: &str = "rtdb.v1.PrepareFundedAppointment";
pub(crate) const ACTIVATE: &str = "rtdb.v1.ActivateFundedAppointment";

pub(super) fn is_managed(call: &Call) -> bool {
    call.session.caller.principal.workload_grant().is_some_and(rtdb_auth::VerifiedWorkloadGrant::is_funded_appointment)
}

pub(super) fn denied() -> Status { error::anomaly(workload::denied()) }

pub(super) fn owner(call: &Call) -> Option<[u8; 32]> {
    let principal = &call.session.caller.principal;
    if principal.label_scope().is_some() || principal.workload_grant().is_some() { return None; }
    funding_owner(call)
}

impl Service {
    fn managed_appointment_attachment(&self, call: &Call) -> Result<(Arc<Database>, workload::ManagedAppointment), Status> {
        let caller = &call.session.caller;
        if !caller.verified_credential || caller.principal.label_scope().is_some()
            || !self.0.node.catalog().is_managed() || self.follower_only()
            || call.session.audience_source().is_some() || call.session.fresh_replica
        {
            return Err(denied());
        }
        let grant = caller.principal.workload_grant().ok_or_else(denied)?;
        let database = self.0.node.catalog().database(&call.database).map_err(error::anomaly)?;
        let driver = database.serving_driver().filter(|_| !database.read_only()).ok_or_else(denied)?;
        let appointment = grant.funded_appointment(&call.database, driver.incarnation(), self.0.node.now()).map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        Ok((database, appointment))
    }

    fn check_managed_appointment_attachment(
        &self, call: &Call, database: &Arc<Database>, expires_at: Timestamp,
    ) -> Result<(), Status> {
        self.check_attachment(call, database)?;
        if self.0.node.now() >= expires_at { return Err(denied()); }
        Ok(())
    }

    pub(super) async fn managed_appointment_capabilities(&self, call: &Call) -> Result<Vec<u8>, Status> {
        let (database, appointment) = self.managed_appointment_attachment(call)?;
        let driver = database.serving_driver().ok_or_else(denied)?;
        let ready = driver.managed_appointment_ready(&appointment).await.map_err(error::anomaly)?;
        let mut features = vec!["tx-status"];
        if ready && let Some(clock) = self.0.node.funded_offer_clock()
            && clock.funded_offers_for(driver.incarnation()).await
        {
            features.extend(["transact", FUNDED_APPOINTMENT, TRANSACT_REQUIRES, TIMED_TRANSACT, RELATIVE_VALID_TIME]);
        }
        self.check_managed_appointment_attachment(call, &database, appointment.expires_at())?;
        Ok(proto::CapabilitiesResponse {
            api: super::API.into(),
            features: features.into_iter().map(str::to_owned).collect(),
            limits: Some(proto::Limits {
                max_page_rows: 0, max_change_groups: 0, max_wait_millis: 0, min_key_bytes: 32, max_key_bytes: 32,
            }),
            server_time_micros: 0,
        }.encode_to_vec())
    }

    pub(super) async fn managed_appointment_status(&self, call: &Call, key: &[u8]) -> Result<Vec<u8>, Status> {
        let (database, appointment) = self.managed_appointment_attachment(call)?;
        let driver = database.serving_driver().ok_or_else(denied)?;
        let receipt = driver.managed_appointment_status(&appointment, key, &call.cancellation).await.map_err(error::anomaly)?;
        self.check_managed_appointment_attachment(call, &database, appointment.expires_at())?;
        Ok(terminal_for(Some(appointment.namespace()), receipt)?.encode_to_vec())
    }

    pub(super) async fn prepare_funded_appointment(
        &self, call: &Call, request: proto::PrepareFundedAppointmentRequest,
    ) -> Result<Vec<u8>, Status> {
        let predecessor = request.predecessor.ok_or_else(|| error::incorrect("Missing appointment predecessor"))?;
        let predecessor = Basis::from_bytes(&predecessor.value).map_err(error::anomaly)?;
        let version = request.receipt_absent.ok_or_else(|| error::incorrect("Missing Original receipt point"))?;
        let absent = transact::decode_guard(
            proto::Guard { guard: Some(proto::guard::Guard::Version(version)) }, call.memory(),
        )?;
        let intent = digest(&request.intent_sha256)?;
        let request = Transact::prepare_funded_appointment(request.key, absent, Timestamp(request.before_micros))
            .map_err(error::anomaly)?;
        let command = AppointmentCommand::Prepare { predecessor, booking: *uuid::Uuid::new_v4().as_bytes(), intent };
        let (receipt, owner) = self.order_funded_appointment(call, request, command).await?;
        Ok(wire(evidence(Some(owner), &receipt)?)?.encode_to_vec())
    }

    pub(super) async fn activate_funded_appointment(
        &self, call: &Call, request: proto::ActivateFundedAppointmentRequest,
    ) -> Result<Vec<u8>, Status> {
        let transaction = request.transaction.ok_or_else(|| error::incorrect("Missing appointment activation transaction"))?;
        let transaction = transact::request(transaction, call.memory())?;
        let command = AppointmentCommand::Activate { booking: booking(&request.booking)? };
        let (receipt, owner) = self.order_funded_appointment(call, transaction, command).await?;
        Ok(wire(evidence(Some(owner), &receipt)?)?.encode_to_vec())
    }

    pub(super) async fn consume_funded_appointment(
        &self, call: &Call, request: proto::TransactRequest, used: proto::FundedAppointmentUse,
    ) -> Result<Vec<u8>, Status> {
        let transaction = transact::request(request, call.memory())?;
        let command = AppointmentCommand::Consume { booking: booking(&used.booking)?, intent: digest(&used.intent_sha256)? };
        let (receipt, owner) = self.order_funded_appointment(call, transaction, command).await?;
        Ok(terminal_for(Some(owner), receipt)?.encode_to_vec())
    }

    async fn order_funded_appointment(
        &self, call: &Call, mut request: Transact, command: AppointmentCommand,
    ) -> Result<(Receipt, [u8; 32]), Status> {
        if is_managed(call) {
            let (database, appointment) = self.managed_appointment_attachment(call)?;
            let driver = database.serving_driver().ok_or_else(denied)?;
            let clock = self.0.node.funded_offer_clock().ok_or_else(|| error::unsupported(FUNDED_APPOINTMENT))?;
            let (owner, expiry) = (appointment.namespace(), appointment.expires_at());
            request.request_memory = Some(call.request_reservation());
            let receipt = driver.transact_managed_appointment(request, appointment, command, clock, &call.cancellation)
                .await.map_err(error::anomaly)?;
            self.check_managed_appointment_attachment(call, &database, expiry)?;
            return Ok((receipt, owner));
        }
        let owner = owner(call).ok_or_else(|| error::unsupported(FUNDED_APPOINTMENT))?;
        let database = self.wire_attachment(call, DatabaseAccess::Write).await?;
        let driver = database.serving_driver().filter(|_| !database.read_only())
            .ok_or_else(|| error::unsupported(FUNDED_APPOINTMENT))?;
        if !driver.supports_funded_appointment().await { return Err(error::unsupported(FUNDED_APPOINTMENT)); }
        let clock = self.0.node.funded_offer_clock().ok_or_else(|| error::unsupported(FUNDED_APPOINTMENT))?;
        request.check_appointment_shape(command).map_err(error::anomaly)?;
        request.request_memory = Some(call.request_reservation());
        let funding = FundingSubmission { command: FundingCommand::Appointment(command), owner, clock };
        let receipt = driver.transact_funded(request, None, funding, &call.cancellation).await.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        Ok((receipt, owner))
    }
}

fn booking(value: &[u8]) -> Result<[u8; 16], Status> {
    value.try_into().ok().filter(|value| *value != [0; 16])
        .ok_or_else(|| error::incorrect("Invalid funded appointment booking"))
}

fn digest(value: &[u8]) -> Result<[u8; 32], Status> {
    value.try_into().ok().filter(|value| *value != [0; 32])
        .ok_or_else(|| error::incorrect("Invalid appointment intent digest"))
}

fn evidence(owner: Option<[u8; 32]>, receipt: &Receipt) -> Result<FundedReceipt, Status> {
    receipt.decision_clock.filter(|offer| {
        offer.is_appointment() && owner.is_some_and(|owner| offer.belongs_to(owner))
    }).ok_or_else(|| error::not_found("rtdb/unknown-transaction", "No matching Original appointment has committed"))
}

fn wire(offer: FundedReceipt) -> Result<proto::FundedAppointment, Status> {
    let cut = offer.appointment_cut().ok_or_else(|| error::unsupported(FUNDED_APPOINTMENT))?;
    let journal = offer.appointment_journal_cutoff().ok_or_else(|| error::unsupported(FUNDED_APPOINTMENT))?;
    Ok(proto::FundedAppointment {
        booking: offer.booking().to_vec(),
        basis: Some(cut.basis.into()),
        reading_micros: offer.reading().0,
        decision_before_micros: offer.before().0,
        journal_before_micros: journal.before().0,
        certificate_sha256: journal.certificate_sha256().to_vec(),
    })
}

pub(super) fn terminal(call: &Call, receipt: Receipt) -> Result<proto::Receipt, Status> {
    terminal_for(owner(call), receipt)
}

fn terminal_for(owner: Option<[u8; 32]>, receipt: Receipt) -> Result<proto::Receipt, Status> {
    let offer = wire(evidence(owner, &receipt)?)?;
    let mut receipt = transact::receipt(receipt);
    receipt.funded_appointment = Some(offer);
    Ok(receipt)
}
