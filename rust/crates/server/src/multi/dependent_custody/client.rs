use std::collections::BTreeMap;

use rtdb_types::rtdb_v1::{
    NativeCheckpointMember, NativeDependentCustodyDisposition, NativeDependentCustodyEnrollment,
    NativeDependentSourceGeneration,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::*;
use crate::multi::NativeDependentCustodyAuthority;

pub(super) struct Connection(provider_http::Connection);

impl Connection {
    pub(super) fn from_profile(profile: provider_http::Profile) -> Result<Self> {
        provider_http::Connection::from_profile(profile, MAX_BYTES, DEADLINE)
            .map(Self)
            .map_err(transport_error)
    }

    pub(super) async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        request: &impl Serialize,
    ) -> Result<T> {
        self.0
            .post(path, request, VERSION)
            .await
            .map_err(transport_error)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ObserverProfile {
    source_incarnation: Uuid,
    destination_incarnation: Uuid,
    owner: [u8; 32],
    application_scope: [u8; 32],
    #[serde(default)]
    inherited_reads: bool,
    connection: provider_http::Profile,
}

pub(super) struct Observer(BTreeMap<[u8; 16], Commissioned>);

struct Commissioned {
    source: [u8; 16],
    owner: [u8; 32],
    application_scope: [u8; 32],
    inherited_reads: bool,
    connection: Connection,
}

impl Observer {
    pub(super) fn from_profiles(profiles: Vec<ObserverProfile>) -> Result<Self> {
        let mut commissioned = BTreeMap::new();
        for profile in profiles {
            if profile.source_incarnation.is_nil()
                || profile.destination_incarnation.is_nil()
                || profile.source_incarnation == profile.destination_incarnation
                || profile.owner == [0; 32]
                || profile.application_scope == [0; 32]
            {
                return Err(invalid());
            }
            let record = Commissioned {
                source: *profile.source_incarnation.as_bytes(),
                owner: profile.owner,
                application_scope: profile.application_scope,
                inherited_reads: profile.inherited_reads,
                connection: Connection::from_profile(profile.connection)?,
            };
            if commissioned
                .insert(*profile.destination_incarnation.as_bytes(), record)
                .is_some()
            {
                return Err(invalid());
            }
        }
        Ok(Self(commissioned))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn select(&self, scope: NativeDependentCustodyScope) -> Result<&Commissioned> {
        self.0
            .get(&scope.reservation.target.incarnation)
            .filter(|profile| profile.source == scope.reservation.source.basis.incarnation())
            .ok_or_else(pending)
    }

    async fn enrollment(
        &self,
        scope: NativeDependentCustodyScope,
        native: &[NativeCheckpointMember],
        source_generation: Option<NativeDependentSourceGeneration>,
        cancellation: &CancellationToken,
    ) -> Result<NativeDependentCustodyEnrollment> {
        let profile = self.select(scope)?;
        if profile.inherited_reads != source_generation.is_some() {
            return Err(refused());
        }
        let request = ObservationRequest {
            version: VERSION,
            observation: Observation::Enrollment {
                scope,
                native,
                source_generation,
            },
        };
        let result = cancellation
            .run_until_cancelled(profile.connection.post(OBSERVE, &request))
            .await
            .ok_or_else(pending)??;
        match result {
            ObservationResult::Enrollment { enrollment }
                if enrollment.scope == scope
                    && enrollment.owner == profile.owner
                    && enrollment.application_scope == profile.application_scope =>
            {
                Ok(enrollment)
            }
            ObservationResult::Pending => Err(pending()),
            _ => Err(refused()),
        }
    }

    async fn observe_inherited(
        &self,
        proof: &InheritedReadProof,
        observation: Observation<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObservationResult> {
        let profile = self.select(proof.read.scope)?;
        if (!profile.inherited_reads
            && !matches!(&observation, Observation::InheritedDisposition { .. }))
            || profile.owner != proof.owner
            || profile.application_scope != proof.application_scope
        {
            return Err(refused());
        }
        let request = ObservationRequest {
            version: VERSION,
            observation,
        };
        cancellation
            .run_until_cancelled(profile.connection.post(OBSERVE, &request))
            .await
            .ok_or_else(pending)?
    }
}

#[derive(Serialize)]
struct ObservationRequest<'a> {
    version: u32,
    observation: Observation<'a>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Observation<'a> {
    Enrollment {
        scope: NativeDependentCustodyScope,
        native: &'a [NativeCheckpointMember],
        #[serde(skip_serializing_if = "Option::is_none")]
        source_generation: Option<NativeDependentSourceGeneration>,
    },
    Disposition {
        enrollment: &'a NativeDependentCustodyEnrollment,
        fence: &'a NativeCustodyFence,
    },
    InheritedRegistration {
        release: &'a InheritedRelease,
    },
    InheritedAdmission {
        admission: &'a InheritedAdmission,
    },
    InheritedDisposition {
        admission: &'a InheritedAdmission,
    },
    InheritedEnrollment {
        proof: &'a InheritedReadProof,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ObservationResult {
    Pending,
    Enrollment {
        enrollment: NativeDependentCustodyEnrollment,
    },
    Disposition {
        disposition: NativeDependentCustodyDisposition,
    },
    InheritedRegistration {
        registration: InheritedRegistration,
    },
    InheritedAdmission {
        registration: InheritedAdmissionRegistration,
    },
    InheritedDisposition {
        disposition: InheritedDisposition,
    },
    InheritedEnrollment {
        enrollment: rtdb_types::rtdb_v1::DependentCustodyReceipt,
    },
}

#[async_trait::async_trait]
impl NativeDependentCustodyAuthority for Observer {
    fn enrollment_uses_source_generation(&self, scope: NativeDependentCustodyScope) -> bool {
        self.select(scope)
            .is_ok_and(|profile| profile.inherited_reads)
    }

    async fn observe_enrollment(
        &self,
        scope: NativeDependentCustodyScope,
        native: &[NativeCheckpointMember],
        cancellation: &CancellationToken,
    ) -> Result<NativeDependentCustodyEnrollment> {
        self.enrollment(scope, native, None, cancellation).await
    }

    async fn observe_enrollment_in_generation(
        &self,
        scope: NativeDependentCustodyScope,
        native: &[NativeCheckpointMember],
        generation: NativeDependentSourceGeneration,
        cancellation: &CancellationToken,
    ) -> Result<NativeDependentCustodyEnrollment> {
        self.enrollment(scope, native, Some(generation), cancellation)
            .await
    }

    async fn observe_disposition(
        &self,
        enrollment: &NativeDependentCustodyEnrollment,
        fence: &NativeCustodyFence,
        cancellation: &CancellationToken,
    ) -> Result<NativeDependentCustodyDisposition> {
        let profile = self.select(enrollment.scope)?;
        if profile.owner != enrollment.owner
            || profile.application_scope != enrollment.application_scope
        {
            return Err(refused());
        }
        let request = ObservationRequest {
            version: VERSION,
            observation: Observation::Disposition { enrollment, fence },
        };
        let result = cancellation
            .run_until_cancelled(profile.connection.post(OBSERVE, &request))
            .await
            .ok_or_else(pending)??;
        match result {
            ObservationResult::Disposition { disposition }
                if disposition.scope == enrollment.scope
                    && disposition.enrollment == enrollment.receipt
                    && &disposition.fence == fence =>
            {
                Ok(disposition)
            }
            ObservationResult::Pending => Err(pending()),
            _ => Err(refused()),
        }
    }

    async fn observe_inherited_registration(
        &self,
        release: &InheritedRelease,
        cancellation: &CancellationToken,
    ) -> Result<InheritedRegistration> {
        let result = self
            .observe_inherited(
                &release.proof,
                Observation::InheritedRegistration { release },
                cancellation,
            )
            .await?;
        match result {
            ObservationResult::InheritedRegistration { registration } => Ok(registration),
            ObservationResult::Pending => Err(pending()),
            _ => Err(refused()),
        }
    }

    async fn observe_inherited_admission(
        &self,
        admission: &InheritedAdmission,
        cancellation: &CancellationToken,
    ) -> Result<InheritedAdmissionRegistration> {
        let result = self
            .observe_inherited(
                &admission.release.proof,
                Observation::InheritedAdmission { admission },
                cancellation,
            )
            .await?;
        match result {
            ObservationResult::InheritedAdmission { registration } => Ok(registration),
            ObservationResult::Pending => Err(pending()),
            _ => Err(refused()),
        }
    }

    async fn observe_inherited_enrollment(
        &self,
        proof: &InheritedReadProof,
        cancellation: &CancellationToken,
    ) -> Result<rtdb_types::rtdb_v1::DependentCustodyReceipt> {
        let result = self
            .observe_inherited(
                proof,
                Observation::InheritedEnrollment { proof },
                cancellation,
            )
            .await?;
        match result {
            ObservationResult::InheritedEnrollment { enrollment } => Ok(enrollment),
            ObservationResult::Pending => Err(pending()),
            _ => Err(refused()),
        }
    }

    async fn observe_inherited_disposition(
        &self,
        admission: &InheritedAdmission,
        cancellation: &CancellationToken,
    ) -> Result<InheritedDisposition> {
        let result = self
            .observe_inherited(
                &admission.release.proof,
                Observation::InheritedDisposition { admission },
                cancellation,
            )
            .await?;
        match result {
            ObservationResult::InheritedDisposition { disposition } => Ok(disposition),
            ObservationResult::Pending => Err(pending()),
            _ => Err(refused()),
        }
    }
}
