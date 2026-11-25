use async_trait::async_trait;
use rtdb_index::branches::{BranchIndex, CustodyGroupId, CustodyRelease};
use rtdb_tx::transact::{CustodyWriterFence, Outcome, Receipt, TrustedCustodyCommand, TrustedCustodyRelease};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::Basis};
use std::collections::BTreeSet;
use tokio_util::sync::CancellationToken;

use super::ServingDriver;
use crate::Clock;

mod mounted;
mod original;
pub(crate) use mounted::CustodyProviderSlot;
pub use mounted::{
    CapturedCustodyRank, CustodyContributors, CustodyProviderInstallation, CustodyProviderMount,
    CustodyResponseOwner, MountedResponseRelease, NativeAuthoring, NativeAuthoringLine, NativeAuthoringRewrite, NativeLandingOrder, NativeLandingReview, NativeLandingWitness,
    RegisteredCustodyResponse, VerifiedNativeLanding,
    custody_frame_commitment,
};
pub use original::{
    AttachedCustodyCarrierJournal, OriginalCarrierObservations, OriginalCarrierReceipt,
    InheritedContributor, InheritedFamily, InheritedFamilyPoint, OriginalReceipt, OriginalWriterPoint,
    original_writer_commitment, same_original_installation,
};

pub use rtdb_index::branches::{
    CustodyCarrierAuthority, CustodyReleasePhase, CustodyReleaseRegistration, CustodyResponseAdmission, CustodyResponsePart,
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
    async fn writer_fence(&self, cancellation: &CancellationToken) -> Result<CustodyWriterFence>;
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

    pub async fn check_all(
        &self,
        expected: &BTreeSet<(CustodyGroupId, [u8; 32])>,
        registrations: &[CustodyReleaseRegistration],
    ) -> Result<()> {
        self.check_contributors(expected, registrations, None).await
            .map(drop)
    }

    async fn check_contributors(
        &self,
        expected: &CustodyContributors,
        registrations: &[CustodyReleaseRegistration],
        generation: Option<u64>,
    ) -> Result<CustodyWriterFence> {
        if expected.len() > 512
            || expected.len() != registrations.len()
            || registrations.iter().any(|registration| {
                !expected.contains(&(registration.group, registration.occurrence))
            })
        {
            return Err(release_unavailable());
        }
        self.check_registrations(registrations, true, generation)
            .await
    }

    async fn check_live(
        &self,
        registration: CustodyReleaseRegistration,
        promoted: bool,
    ) -> Result<()> {
        self.check_registrations(&[registration], promoted, None)
            .await.map(drop)
    }

    async fn check_registrations(
        &self,
        registrations: &[CustodyReleaseRegistration],
        promoted: bool,
        generation: Option<u64>,
    ) -> Result<CustodyWriterFence> {
        let Some(first) = registrations.first() else {
            return Err(release_unavailable());
        };
        if registrations.len() > 512 {
            return Err(release_unavailable());
        }
        let mut groups = BTreeSet::new();
        let mut attempts = BTreeSet::new();
        for &registration in registrations {
            if registration.authority != first.authority
                || registration.part != first.part
                || registration.expires_at != first.expires_at
                || !attempts.insert(registration.attempt())
            {
                return Err(release_unavailable());
            }
            self.require_live_registration(registration).await?;
            groups.insert(registration.group);
        }
        let (index, writer) = self.current_with_writer(first.authority).await?;
        for &registration in registrations {
            require_live_group(&index, registration)?;
            match index.custody_release_phase(registration)? {
                Some(CustodyReleasePhase::Promoted) => (),
                Some(CustodyReleasePhase::Private) if !promoted => (),
                _ => return Err(release_unavailable()),
            }
        }
        self.driver
            .check_custody_release_delivery(&groups, &writer, generation, self.cancellation)
            .await?;
        self.require_authority(first.authority)?;
        first.require_live_time(self.clock.now())?;
        Ok(writer)
    }

    async fn current(&self, authority: CustodyCarrierAuthority) -> Result<BranchIndex> {
        self.current_with_writer(authority)
            .await
            .map(|(index, _)| index)
    }

    async fn current_with_writer(
        &self,
        authority: CustodyCarrierAuthority,
    ) -> Result<(BranchIndex, CustodyWriterFence)> {
        self.require_authority(authority)?;
        let writer = self.original.writer_fence(self.cancellation).await?;
        let index = self.driver.current_custody(self.cancellation).await?;
        self.require_authority(authority)?;
        if !writer.matches(&index, self.clock.now())? {
            return Err(release_unavailable());
        }
        Ok((index, writer))
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
        let writer = self.original.writer_fence(self.cancellation).await?;
        self.require_authority(registration.authority)?;
        let command = TrustedCustodyCommand::Phase(TrustedCustodyRelease {
            release: CustodyRelease {
                registration,
                phase,
            },
            writer,
        });
        self.order_command(command).await.map(drop)
    }

    async fn order_command(&self, command: TrustedCustodyCommand) -> Result<Receipt> {
        let receipt = self
            .driver
            .submit_custody_release(command, self.cancellation)
            .await?;
        match receipt.outcome {
            Outcome::Then => Ok(receipt),
            Outcome::Refused { error } => Err(error),
            Outcome::Otherwise { .. } => Err(Anomaly::fault(
                "rtdb/custody-release",
                "Response release has no alternative arm",
            )),
        }
    }
}

pub(crate) fn check_release_cut(
    index: &BranchIndex,
    writer: &CustodyWriterFence,
    groups: &BTreeSet<CustodyGroupId>,
    now: Timestamp,
) -> Result<()> {
    if !writer.matches(index, now)? {
        return Err(release_unavailable());
    }
    for &group in groups {
        if !index.custody_group_live(group)? {
            return Err(rtdb_crypt::erased_under_view());
        }
    }
    Ok(())
}

fn require_live_group(index: &BranchIndex, registration: CustodyReleaseRegistration) -> Result<()> {
    if index.custody_group_live(registration.group)? {
        Ok(())
    } else {
        Err(rtdb_crypt::erased_under_view())
    }
}

pub(crate) fn release_unavailable() -> Anomaly {
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
        Cell, CustodyPayload, CustodyPlacement, CustodyPlacementCommitments, Op, ROW_CUSTODY,
        Resolution, Row, TRANSACT_REQUIRES, Transact, TransactState, TransactionRecord, ValidTime,
        Window,
    };
    use rtdb_types::{
        EntityId, Interval, StructFields, TableRef, Timestamp, Value,
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

        async fn submit_custody_release(
            &self,
            command: TrustedCustodyCommand,
            cancellation: &CancellationToken,
        ) -> Result<Receipt> {
            self.transact(command.into_request()?, cancellation).await
        }

        async fn check_custody_release_delivery(
            &self,
            groups: &BTreeSet<CustodyGroupId>,
            writer: &CustodyWriterFence,
            generation: Option<u64>,
            cancellation: &CancellationToken,
        ) -> Result<()> {
            if generation == Some(7) {
                return Err(rtdb_crypt::erased_under_view());
            }
            let index = self.current_custody(cancellation).await?;
            if !writer.matches(&index, index.head().system_time)? {
                return Err(release_unavailable());
            }
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
                &[ROW_CUSTODY, TRANSACT_REQUIRES],
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
        async fn tx_status(&self, key: &[u8], _: &CancellationToken) -> Result<Receipt> {
            self.state.lock().await.tx_status(key)
        }

        async fn original_custody_receipt(&self, key: &[u8], _: Basis, cancellation: &CancellationToken) -> Result<Receipt> {
            self.tx_status(key, cancellation).await
        }
    }

    struct Carrier {
        writer: OriginalReceipt,
        observation: Mutex<Option<OriginalCarrierReceipt>>,
    }

    #[async_trait]
    impl OriginalCarrierObservations for Carrier {
        async fn confirmed_writer(
            &self,
            _: CustodyCarrierAuthority,
            _: &CancellationToken,
        ) -> Result<OriginalReceipt> {
            Ok(self.writer.clone())
        }

        async fn registered_response(
            &self,
            _: CustodyCarrierAuthority,
            _: CustodyResponsePart,
            _: &CancellationToken,
        ) -> Result<RegisteredCustodyResponse> {
            Err(release_unavailable())
        }

        async fn registered_carrier(
            &self,
            registration: CustodyReleaseRegistration,
            _: &CancellationToken,
        ) -> Result<OriginalCarrierReceipt> {
            self.observation
                .lock()
                .await
                .as_ref()
                .filter(|observed| observed.registration == registration)
                .cloned()
                .ok_or_else(release_unavailable)
        }
    }

    fn witness(receipt: &Receipt) -> OriginalReceipt {
        OriginalReceipt {
            key: receipt.key.clone(),
            basis: receipt.basis,
            system_time: receipt.system_time,
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
        let writer = OriginalWriterPoint {
            table: TableRef::new("public", "installation"),
            id: EntityId::Integer(1),
            valid_at: Timestamp(0),
        };
        let writer_receipt = driver
            .transact(
                request(
                    1,
                    vec![Op::Put {
                        branch: MAIN_BRANCH.into(),
                        table: writer.table.clone(),
                        if_absent: false,
                        rows: vec![Row {
                            id: writer.id.clone(),
                            valid: Some(ValidTime::Literal(Interval::ALL)),
                            cells: StructFields::from([(
                                "writer".into(),
                                Cell::Value(Value::I64(1)),
                            )]),
                            custody_group: None,
                            custody_payload: None,
                            protected_columns: Vec::new(),
                        }],
                    }],
                ),
                &cancellation,
            )
            .await
            .unwrap();
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
                    2,
                    vec![
                        Op::CreateCustodyGroup { group: GROUP },
                        Op::PlaceCustodyRow {
                            branch: MAIN_BRANCH.into(),
                            table: TableRef::new("public", "documents"),
                            row,
                            placement: CustodyPlacement {
                                placement_id: [4; 16],
                                reservation: None,
                                derived: None,
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
                original_writer: original_writer_commitment([7; 16], &writer_receipt.key),
            },
            group: GROUP,
            original: occurrence,
            occurrence,
            carrier_receipt_sha256: witness(&placed).commitment(),
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
            writer: witness(&writer_receipt),
            observation: Mutex::new(None),
        };
        let original =
            AttachedCustodyCarrierJournal::new(&driver, registration.authority, writer, &carrier)
                .unwrap();
        let release = ResponseRelease {
            driver: &driver,
            original: &original,
            clock: &TestClock,
            cancellation: &cancellation,
        };
        assert_eq!(release.prepare(registration).await.unwrap(), registration);
        assert!(release.stage(registration).await.is_err());
        *carrier.observation.lock().await = Some(OriginalCarrierReceipt {
            registration,
            receipt: witness(&placed),
            observation: CarrierObservation::Live,
        });
        let wrong_carrier = OriginalCarrierReceipt {
            registration,
            receipt: witness(&writer_receipt),
            observation: CarrierObservation::Live,
        };
        let prior = carrier.observation.lock().await.replace(wrong_carrier);
        assert!(release.stage(registration).await.is_err());
        *carrier.observation.lock().await = prior;
        release.stage(registration).await.unwrap();
        release.promote(registration).await.unwrap();
        let expected = [(GROUP, occurrence)].into();
        release.check_all(&expected, &[registration]).await.unwrap();
        assert_eq!(
            release
                .check_contributors(&expected, &[registration], Some(7))
                .await
                .unwrap_err(),
            rtdb_crypt::erased_under_view()
        );
        let incomplete = [(GROUP, occurrence), ([99; 16], [99; 32])].into();
        assert!(
            release
                .check_all(&incomplete, &[registration])
                .await
                .is_err()
        );
        let inventory_sha256 = index.custody_inventory(GROUP).unwrap().sha256;
        let fenced = driver
            .transact(
                request(
                    3,
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

        *carrier.observation.lock().await = Some(OriginalCarrierReceipt {
            registration,
            receipt: witness(&placed),
            observation: CarrierObservation::Disposed {
                evidence_sha256: [14; 32],
            },
        });
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
