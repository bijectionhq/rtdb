use async_trait::async_trait;
use rtdb_index::branches::{BranchIndex, CustodyGroupId, CustodyRelease};
use rtdb_tx::transact::{Op, Outcome, Transact, Window};
use rtdb_types::{Anomaly, AnomalyCategory, Result, rtdb_v1::Basis};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::ServingDriver;
use crate::Clock;

pub use rtdb_index::branches::{
    CustodyCarrierAuthority, CustodyReleasePhase, CustodyReleaseRegistration, CustodyResponsePart,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CarrierObservation {
    Live,
    Disposed { evidence_sha256: [u8; 32] },
}

/// The installed adapter binds the original source to the exact database attachment and verifies
/// the original writer, response registration and admitted carrier at a current source barrier.
/// Disposed requires a positive durable transport terminal.
#[async_trait]
pub trait CustodyCarrierJournal: Send + Sync {
    fn authority(&self, attachment: &dyn ServingDriver) -> Result<CustodyCarrierAuthority>;
    async fn observe(
        &self,
        registration: CustodyReleaseRegistration,
        cancellation: &CancellationToken,
    ) -> Result<CarrierObservation>;
}

pub struct ResponseRelease<'a> {
    pub driver: &'a dyn ServingDriver,
    pub original: &'a dyn CustodyCarrierJournal,
    pub clock: &'a dyn Clock,
    pub cancellation: &'a CancellationToken,
}

impl ResponseRelease<'_> {
    /// A caller persists these coordinates at the original recovery registry before staging.
    pub async fn prepare(
        &self,
        registration: CustodyReleaseRegistration,
    ) -> Result<CustodyReleaseRegistration> {
        let index = self.current(registration.authority).await?;
        index.check_custody_release_input(registration)?;
        require_live_group(&index, registration)?;
        registration.require_live_time(self.clock.now())?;
        Ok(registration)
    }

    pub async fn stage(&self, registration: CustodyReleaseRegistration) -> Result<()> {
        self.require_live_registration(registration).await?;
        self.order(registration, CustodyReleasePhase::Private)
            .await?;
        self.check_live(registration, false).await
    }

    pub async fn promote(&self, registration: CustodyReleaseRegistration) -> Result<()> {
        self.require_live_registration(registration).await?;
        self.order(registration, CustodyReleasePhase::Promoted)
            .await?;
        self.check(registration).await
    }

    pub async fn check(&self, registration: CustodyReleaseRegistration) -> Result<()> {
        self.check_live(registration, true).await
    }

    pub async fn cancel(
        &self,
        registration: CustodyReleaseRegistration,
    ) -> Result<CustodyReleasePhase> {
        let prior = self
            .current(registration.authority)
            .await?
            .custody_release_phase(registration)?;
        if prior == Some(CustodyReleasePhase::Cancelled) {
            return Ok(CustodyReleasePhase::Cancelled);
        }
        self.require_live_registration(registration).await?;
        self.order(registration, CustodyReleasePhase::Cancelled)
            .await?;
        Ok(CustodyReleasePhase::Cancelled)
    }

    pub async fn dispose(
        &self,
        registration: CustodyReleaseRegistration,
    ) -> Result<CustodyReleasePhase> {
        if let Some(terminal) = self
            .current(registration.authority)
            .await?
            .custody_release_phase(registration)?
            .filter(|phase| phase.terminal())
        {
            return Ok(terminal);
        }
        let CarrierObservation::Disposed { evidence_sha256 } = self.observe(registration).await?
        else {
            return Err(release_unavailable());
        };
        let terminal = CustodyReleasePhase::Disposed { evidence_sha256 };
        match self.order(registration, terminal).await {
            Ok(()) => Ok(terminal),
            Err(error) if error.code == "rtdb/custody-release" => self
                .current(registration.authority)
                .await?
                .custody_release_phase(registration)?
                .filter(|phase| phase.terminal())
                .ok_or(error),
            Err(error) => Err(error),
        }
    }

    pub async fn drained(
        &self,
        authority: CustodyCarrierAuthority,
        group: CustodyGroupId,
        revoked_at: Basis,
        inventory_sha256: [u8; 32],
    ) -> Result<bool> {
        let index = self.current(authority).await?;
        let inventory = index
            .custody_inventory(group)
            .ok_or_else(release_unavailable)?;
        if inventory.revoked_at != Some(revoked_at) || inventory.sha256 != inventory_sha256 {
            return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb/custody-inventory-changed",
                "Drain requires the exact ordered revocation inventory",
            ));
        }
        Ok(index.custody_releases_drained(group))
    }

    async fn check_live(
        &self,
        registration: CustodyReleaseRegistration,
        promoted: bool,
    ) -> Result<()> {
        self.require_live_registration(registration).await?;
        let index = self.current(registration.authority).await?;
        require_live_group(&index, registration)?;
        match index.custody_release_phase(registration)? {
            Some(CustodyReleasePhase::Promoted) => (),
            Some(CustodyReleasePhase::Private) if !promoted => (),
            _ => return Err(release_unavailable()),
        }
        self.driver
            .check_custody_delivery(&[registration.group].into(), self.cancellation)
            .await?;
        self.require_authority(registration.authority)?;
        registration.require_live_time(self.clock.now())
    }

    async fn current(&self, authority: CustodyCarrierAuthority) -> Result<BranchIndex> {
        self.require_authority(authority)?;
        let index = self.driver.current_custody(self.cancellation).await?;
        self.require_authority(authority)?;
        Ok(index)
    }

    fn require_authority(&self, authority: CustodyCarrierAuthority) -> Result<()> {
        if self.cancellation.is_cancelled()
            || self.original.authority(self.driver)? != authority
            || self.driver.incarnation() != authority.incarnation
        {
            return Err(release_unavailable());
        }
        Ok(())
    }

    async fn observe(
        &self,
        registration: CustodyReleaseRegistration,
    ) -> Result<CarrierObservation> {
        self.require_authority(registration.authority)?;
        let observation = self
            .original
            .observe(registration, self.cancellation)
            .await?;
        self.require_authority(registration.authority)?;
        Ok(observation)
    }

    async fn require_live_registration(
        &self,
        registration: CustodyReleaseRegistration,
    ) -> Result<()> {
        if self.observe(registration).await? == CarrierObservation::Live {
            Ok(())
        } else {
            Err(release_unavailable())
        }
    }

    async fn order(
        &self,
        registration: CustodyReleaseRegistration,
        phase: CustodyReleasePhase,
    ) -> Result<()> {
        self.require_authority(registration.authority)?;
        let discriminator = match phase {
            CustodyReleasePhase::Private => 0,
            CustodyReleasePhase::Promoted => 1,
            CustodyReleasePhase::Cancelled => 2,
            CustodyReleasePhase::Disposed { .. } => 3,
        };
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/custody-response-phase\0");
        hash.update(registration.attempt());
        hash.update([discriminator]);
        let receipt = self
            .driver
            .transact(
                Transact {
                    key: hash.finalize().to_vec(),
                    require: Vec::new(),
                    window: Window::default(),
                    guards: Vec::new(),
                    ops: vec![Op::CustodyRelease(CustodyRelease {
                        registration,
                        phase,
                    })],
                    otherwise: Vec::new(),
                    requires: Vec::new(),
                    custody_request_digest: None,
                    request_memory: None,
                },
                self.cancellation,
            )
            .await?;
        match receipt.outcome {
            Outcome::Then => Ok(()),
            Outcome::Refused { error } => Err(error),
            Outcome::Otherwise { .. } => Err(Anomaly::fault(
                "rtdb/custody-release",
                "Response release has no alternative arm",
            )),
        }
    }
}

fn require_live_group(index: &BranchIndex, registration: CustodyReleaseRegistration) -> Result<()> {
    if index.custody_group_live(registration.group)? {
        Ok(())
    } else {
        Err(rtdb_crypt::erased_under_view())
    }
}

fn release_unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/custody-release-authority",
        "The original response release authority is unavailable",
    )
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU64,
        sync::atomic::{AtomicBool, Ordering},
    };

    use rtdb_tx::transact::{
        CustodyPayload, CustodyPlacement, CustodyPlacementCommitments, ROW_CUSTODY, Receipt,
        Resolution, Row, TransactState, TransactionRecord,
    };
    use rtdb_types::{
        EntityId, StructFields, TableRef, Timestamp,
        rtdb_v1::{At, ClaimSealer, Layer, MAIN_BRANCH},
    };
    use tokio::sync::Mutex;

    use super::*;
    use crate::serving::{ChangePage, Changes, QuerySource, QueryView};

    const INCARNATION: [u8; 16] = [1; 16];
    const GROUP: CustodyGroupId = [2; 16];

    struct TestClock;
    impl Clock for TestClock {
        fn now(&self) -> Timestamp {
            Timestamp(10)
        }
    }

    struct Driver {
        state: Mutex<TransactState>,
        lose_reply: AtomicBool,
    }

    #[async_trait]
    impl ServingDriver for Driver {
        fn incarnation(&self) -> [u8; 16] {
            INCARNATION
        }
        fn features(&self) -> &[&str] {
            &[]
        }

        async fn current_custody(&self, _: &CancellationToken) -> Result<BranchIndex> {
            Ok(self.state.lock().await.index().clone())
        }

        async fn check_custody_delivery(
            &self,
            groups: &std::collections::BTreeSet<CustodyGroupId>,
            cancellation: &CancellationToken,
        ) -> Result<()> {
            let index = self.current_custody(cancellation).await?;
            for &group in groups {
                if !index.custody_group_live(group)? {
                    return Err(rtdb_crypt::erased_under_view());
                }
            }
            Ok(())
        }

        async fn transact(&self, request: Transact, _: &CancellationToken) -> Result<Receipt> {
            let mut state = self.state.lock().await;
            let position = state.index().head().basis.position() + 1;
            let claims = ClaimSealer::new(INCARNATION, [3; 32]);
            let receipt = match state.resolve(
                &request,
                position,
                TestClock.now(),
                &[ROW_CUSTODY],
                &claims,
            )? {
                Resolution::Answer(receipt) => receipt,
                Resolution::Append(append) => {
                    let bytes = append.record().encode()?;
                    state
                        .apply(position, TransactionRecord::decode(&bytes)?)?
                        .unwrap()
                }
            };
            if self.lose_reply.swap(false, Ordering::SeqCst) {
                return Err(Anomaly::new(
                    AnomalyCategory::Interrupted,
                    "test/lost-reply",
                    "Lost the committed reply",
                ));
            }
            Ok(receipt)
        }

        async fn query(
            &self,
            _: QuerySource,
            _: bool,
            _: Timestamp,
            _: &CancellationToken,
        ) -> Result<QueryView> {
            unreachable!()
        }
        async fn changes(&self, _: Changes, _: &CancellationToken) -> Result<ChangePage> {
            unreachable!()
        }
        async fn tx_status(&self, _: &[u8], _: &CancellationToken) -> Result<Receipt> {
            unreachable!()
        }
    }

    struct Carrier<'a> {
        driver: &'a Driver,
        identity: CustodyCarrierAuthority,
        observation: Mutex<Option<(CustodyReleaseRegistration, CarrierObservation)>>,
    }

    #[async_trait]
    impl CustodyCarrierJournal for Carrier<'_> {
        fn authority(&self, attachment: &dyn ServingDriver) -> Result<CustodyCarrierAuthority> {
            if !std::ptr::addr_eq(attachment, self.driver as &dyn ServingDriver) {
                return Err(release_unavailable());
            }
            Ok(self.identity)
        }

        async fn observe(
            &self,
            registration: CustodyReleaseRegistration,
            _: &CancellationToken,
        ) -> Result<CarrierObservation> {
            self.observation
                .lock()
                .await
                .as_ref()
                .filter(|(admitted, _)| *admitted == registration)
                .map(|(_, observed)| *observed)
                .ok_or_else(release_unavailable)
        }
    }

    fn request(key: u8, ops: Vec<Op>) -> Transact {
        Transact {
            key: vec![key; 16],
            require: Vec::new(),
            window: Window::default(),
            guards: Vec::new(),
            ops,
            otherwise: Vec::new(),
            requires: Vec::new(),
            custody_request_digest: None,
            request_memory: None,
        }
    }

    #[tokio::test]
    async fn response_terminal_recovers_after_fence_lost_reply_and_registry_removal() {
        let driver = Driver {
            state: Mutex::new(TransactState::new(INCARNATION)),
            lose_reply: AtomicBool::new(false),
        };
        let cancellation = CancellationToken::new();
        let row = Row {
            id: EntityId::Integer(1),
            valid: None,
            cells: StructFields::new(),
            custody_group: Some(GROUP),
            protected_columns: vec!["carrier".into()],
            custody_payload: Some(CustodyPayload {
                binding: [3; 32],
                envelope: vec![1],
            }),
        };
        let placed = driver
            .transact(
                request(
                    1,
                    vec![
                        Op::CreateCustodyGroup { group: GROUP },
                        Op::PlaceCustodyRow {
                            branch: MAIN_BRANCH.into(),
                            table: TableRef::new("public", "documents"),
                            row,
                            placement: CustodyPlacement {
                                placement_id: [4; 16],
                                reservation: None,
                                carrier_column: "carrier".into(),
                                link_column: None,
                                commitments: Some(CustodyPlacementCommitments {
                                    carrier_sha256: [5; 32],
                                    link_sha256: None,
                                }),
                            },
                        },
                    ],
                ),
                &cancellation,
            )
            .await
            .unwrap();
        assert_eq!(placed.outcome, Outcome::Then);
        let index = driver.current_custody(&cancellation).await.unwrap();
        let view = index
            .view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(10))
            .unwrap();
        let occurrence = view
            .custody_placements()
            .next()
            .unwrap()
            .occurrence
            .unwrap()
            .commitment(GROUP);
        let registration = CustodyReleaseRegistration {
            authority: CustodyCarrierAuthority {
                incarnation: INCARNATION,
                source_history: [6; 32],
                installation: [7; 16],
                registry: [8; 32],
                original_writer: [9; 32],
            },
            group: GROUP,
            original: occurrence,
            occurrence,
            expires_at: Timestamp(50),
            part: CustodyResponsePart {
                transport_history: [10; 16],
                producer: [11; 16],
                consumer: [12; 16],
                response: [13; 16],
                ordinal: NonZeroU64::MIN,
            },
        };
        let carrier = Carrier {
            driver: &driver,
            identity: registration.authority,
            observation: Mutex::new(None),
        };
        let release = ResponseRelease {
            driver: &driver,
            original: &carrier,
            clock: &TestClock,
            cancellation: &cancellation,
        };
        assert_eq!(release.prepare(registration).await.unwrap(), registration);
        assert!(release.stage(registration).await.is_err());
        *carrier.observation.lock().await = Some((registration, CarrierObservation::Live));
        release.stage(registration).await.unwrap();
        release.promote(registration).await.unwrap();
        let inventory_sha256 = index.custody_inventory(GROUP).unwrap().sha256;
        let fenced = driver
            .transact(
                request(
                    2,
                    vec![Op::RevokeCustodyGroup {
                        group: GROUP,
                        inventory_sha256,
                    }],
                ),
                &cancellation,
            )
            .await
            .unwrap();
        assert_eq!(fenced.outcome, Outcome::Then);
        assert!(
            !release
                .drained(
                    registration.authority,
                    GROUP,
                    fenced.basis,
                    inventory_sha256
                )
                .await
                .unwrap()
        );
        assert!(
            release
                .drained(registration.authority, GROUP, fenced.basis, [42; 32])
                .await
                .is_err()
        );
        assert!(release.check(registration).await.is_err());
        assert!(release.cancel(registration).await.is_err());
        assert!(release.dispose(registration).await.is_err());
        assert!(
            !driver
                .current_custody(&cancellation)
                .await
                .unwrap()
                .custody_releases_drained(GROUP)
        );

        *carrier.observation.lock().await = Some((
            registration,
            CarrierObservation::Disposed {
                evidence_sha256: [14; 32],
            },
        ));
        driver.lose_reply.store(true, Ordering::SeqCst);
        assert_eq!(
            release.dispose(registration).await.unwrap_err().code,
            "test/lost-reply"
        );
        *carrier.observation.lock().await = None;
        {
            let mut state = driver.state.lock().await;
            let checkpoint = state.encode_checkpoint().unwrap();
            *state = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
        }
        assert_eq!(
            release.dispose(registration).await.unwrap(),
            CustodyReleasePhase::Disposed {
                evidence_sha256: [14; 32]
            }
        );
        assert!(
            driver
                .current_custody(&cancellation)
                .await
                .unwrap()
                .custody_releases_drained(GROUP)
        );
        assert!(
            release
                .dispose(CustodyReleaseRegistration {
                    expires_at: Timestamp(51),
                    ..registration
                })
                .await
                .is_err()
        );
        assert!(
            release
                .dispose(CustodyReleaseRegistration {
                    occurrence: [15; 32],
                    ..registration
                })
                .await
                .is_err()
        );
    }
}
