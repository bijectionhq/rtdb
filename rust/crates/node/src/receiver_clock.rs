use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use rtdb_crypt::receiver_clock::VerifiedReceiverCertificate;
use rtdb_tx::{DecisionClock, DecisionObservation, FundedCutoff, transact::funding_unsupported};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::Basis};

pub struct CertifiedDecisionClock {
    receiver: Arc<dyn DecisionClock>,
    certificate: VerifiedReceiverCertificate,
    domain: [u8; 16],
}

impl CertifiedDecisionClock {
    pub fn new(
        receiver: Arc<dyn DecisionClock>, certificate: VerifiedReceiverCertificate, domain: [u8; 16],
    ) -> Result<Self> {
        if domain == [0; 16] { return Err(funding_unsupported()); }
        Ok(Self { receiver, certificate, domain })
    }
}

#[async_trait]
impl DecisionClock for CertifiedDecisionClock {
    async fn observe(&self) -> Result<DecisionObservation> {
        let sample = tokio::time::timeout(Duration::from_secs(1), self.receiver.observe()).await
            .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.clock/unavailable",
                "No fresh receiver clock sample is installed"))??;
        if sample.domain != self.domain || sample.epoch == 0 || sample.sequence < sample.epoch || sample.instant.0 <= 0 {
            return Err(funding_unsupported());
        }
        Ok(sample)
    }

    fn supports_funded_offers(&self) -> bool { true }

    async fn funded_offers_for(&self, incarnation: [u8; 16]) -> bool {
        incarnation == self.certificate.source()
            && self.observe().await.is_ok_and(|sample| self.certificate.live_at(sample.instant))
    }

    fn funded_certificate(&self, original: Basis, receiver: DecisionObservation) -> Option<[u8; 32]> {
        (original.incarnation() == self.certificate.source() && receiver.domain == self.domain
            && self.certificate.live_at(receiver.instant))
            .then(|| self.certificate.digest())
    }

    async fn authorize_offer(&self, predecessor: Basis, receiver: DecisionObservation, before: Timestamp) -> Result<FundedCutoff> {
        if self.funded_certificate(predecessor, receiver).is_none() { return Err(funding_unsupported()); }
        let (decision_before, journal) = self.certificate.cutoffs(receiver.instant, before)?;
        Ok(FundedCutoff { decision_before, journal })
    }
}
