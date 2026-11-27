use std::sync::Arc;

use prost::Message;
use rtdb_node::Database;
use rtdb_tx::transact::{FUNDED_APPOINTMENT, FundedReceipt, Receipt, workload};
use rtdb_types::Timestamp;
use tonic::Status;

use super::{Call, Service, error, funding_owner, proto, transact};

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
            || call.session.fresh_replica
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
        driver.managed_appointment_ready(&appointment).await.map_err(error::anomaly)?;
        self.check_managed_appointment_attachment(call, &database, appointment.expires_at())?;
        Ok(proto::CapabilitiesResponse {
            api: super::API.into(),
            features: vec!["tx-status".into()],
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
