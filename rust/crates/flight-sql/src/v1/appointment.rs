use prost::Message;
use rtdb_node::serving::FundingSubmission;
use rtdb_tx::transact::{AppointmentCommand, FUNDED_APPOINTMENT, FundedReceipt, FundingCommand, Receipt, Transact};
use rtdb_types::{Timestamp, rtdb_v1::Basis};
use tonic::Status;

use super::{Call, DatabaseAccess, Service, error, funding_owner, proto, transact};

pub(super) const PREPARE: &str = "rtdb.v1.PrepareFundedAppointment";
pub(super) const ACTIVATE: &str = "rtdb.v1.ActivateFundedAppointment";

pub(super) fn owner(call: &Call) -> Option<[u8; 32]> {
    let principal = &call.session.caller.principal;
    if principal.label_scope().is_some() || principal.workload_grant().is_some() { return None; }
    funding_owner(call)
}

impl Service {
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
        let receipt = self.order_funded_appointment(call, request, command).await?;
        Ok(wire(evidence(call, &receipt)?)?.encode_to_vec())
    }

    pub(super) async fn activate_funded_appointment(
        &self, call: &Call, request: proto::ActivateFundedAppointmentRequest,
    ) -> Result<Vec<u8>, Status> {
        let transaction = request.transaction.ok_or_else(|| error::incorrect("Missing appointment activation transaction"))?;
        let transaction = transact::request(transaction, call.memory())?;
        let command = AppointmentCommand::Activate { booking: booking(&request.booking)? };
        let receipt = self.order_funded_appointment(call, transaction, command).await?;
        Ok(wire(evidence(call, &receipt)?)?.encode_to_vec())
    }

    pub(super) async fn consume_funded_appointment(
        &self, call: &Call, request: proto::TransactRequest, used: proto::FundedAppointmentUse,
    ) -> Result<Vec<u8>, Status> {
        let transaction = transact::request(request, call.memory())?;
        let command = AppointmentCommand::Consume { booking: booking(&used.booking)?, intent: digest(&used.intent_sha256)? };
        let receipt = self.order_funded_appointment(call, transaction, command).await?;
        Ok(terminal(call, receipt)?.encode_to_vec())
    }

    async fn order_funded_appointment(
        &self, call: &Call, mut request: Transact, command: AppointmentCommand,
    ) -> Result<Receipt, Status> {
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
        Ok(receipt)
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

fn evidence(call: &Call, receipt: &Receipt) -> Result<FundedReceipt, Status> {
    receipt.decision_clock.filter(|offer| {
        offer.is_appointment() && owner(call).is_some_and(|owner| offer.belongs_to(owner))
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
    let offer = wire(evidence(call, &receipt)?)?;
    let mut receipt = transact::receipt(receipt);
    receipt.funded_appointment = Some(offer);
    Ok(receipt)
}
