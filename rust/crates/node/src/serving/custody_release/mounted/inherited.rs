use rtdb_index::branches::CustodyPayload;
use rtdb_types::Value;

use super::*;

impl MountedResponseRelease<'_> {
    pub async fn require_inherited_family(
        &self,
        point: &InheritedFamilyPoint,
        family: &InheritedFamily,
    ) -> Result<()> {
        let release = self.release();
        self.while_mounted(async {
            release.require_authority(point.authority)?;
            family.require_live(self.clock.now())?;
            let observed = self
                .owner
                .provider
                .installation
                .observations
                .inherited_family(point, family, self.cancellation)
                .await?;
            if observed.0 != *point
                || observed.1 != *family
                || self.original_writer_fence().await?.written != point.original_writer
            {
                return Err(release_unavailable());
            }
            family.require_live(self.clock.now())
        })
        .await?;
        self.owner
            .with_live_mount(|| release.require_authority(point.authority))
    }

    pub async fn open_inherited_carrier(
        &self,
        point: &InheritedFamilyPoint,
        family: &InheritedFamily,
        payload: &CustodyPayload,
        envelope_sha256: [u8; 32],
    ) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        self.require_inherited_family(point, family).await?;
        self.while_mounted(async {
            let selected = point
                .contributors
                .iter()
                .find(|member| member.occurrence == point.read.occurrence)
                .ok_or_else(release_unavailable)?;
            if selected.placement != point.read.placement
                || <[u8; 32]>::from(Sha256::digest(&payload.envelope)) != envelope_sha256
            {
                return Err(release_unavailable());
            }
            let vault = self.owner.database.token_authority().await?.custody_vault();
            let cells =
                crate::serving::custody::open(&vault, point.read.scope.group, payload).await?;
            let mut carrier = None;
            let mut link = None;
            let mut remaining = 128_usize * 1024;
            for (_, value) in cells {
                let (Value::Binary(bytes) | Value::FixedSizeBinary(bytes)) = value else {
                    return Err(release_unavailable());
                };
                let digest: [u8; 32] = Sha256::digest(&bytes).into();
                remaining = remaining
                    .checked_sub(bytes.len())
                    .ok_or_else(release_unavailable)?;
                if bytes.is_empty() {
                    return Err(release_unavailable());
                }
                if digest == selected.carrier_sha256 && carrier.is_none() {
                    carrier = Some(bytes);
                } else if Some(digest) == selected.link_sha256 && link.is_none() {
                    link = Some(bytes);
                } else {
                    return Err(release_unavailable());
                }
            }
            if link.is_some() != selected.link_sha256.is_some()
                || selected.link_sha256 == Some(selected.carrier_sha256)
            {
                return Err(release_unavailable());
            }
            Ok((carrier.ok_or_else(release_unavailable)?, link))
        })
        .await
    }

    pub async fn dispose_inherited(
        &self,
        registration: CustodyReleaseRegistration,
        evidence_sha256: [u8; 32],
    ) -> Result<()> {
        let release = self.release();
        let authority = release.original.authority(release.driver)?;
        if evidence_sha256 == [0; 32]
            || !same_original_installation(authority, registration.authority)
        {
            return Err(release_unavailable());
        }
        self.while_mounted(async {
            let terminal = CustodyReleasePhase::Disposed { evidence_sha256 };
            if let Some(phase) = release
                .current(authority)
                .await?
                .custody_release_phase(registration)?
                .filter(|phase| phase.terminal())
            {
                return if phase == terminal {
                    Ok(())
                } else {
                    Err(release_unavailable())
                };
            }
            let observed = self
                .owner
                .provider
                .installation
                .observations
                .inherited_disposed_carrier(authority, registration, self.cancellation)
                .await?;
            let expected = CarrierObservation::Disposed { evidence_sha256 };
            if self
                .journal
                .confirm_carrier_observation(registration, observed, self.cancellation)
                .await?
                != expected
            {
                return Err(release_unavailable());
            }
            let writer = self.original_writer_fence().await?;
            let command = TrustedCustodyCommand::Phase(TrustedCustodyRelease {
                release: CustodyRelease {
                    registration,
                    phase: terminal,
                },
                writer,
            });
            if let Err(error) = release.order_command(command).await
                && release
                    .current(authority)
                    .await?
                    .custody_release_phase(registration)?
                    != Some(terminal)
            {
                return Err(error);
            }
            Ok(())
        })
        .await?;
        self.owner
            .with_live_mount(|| release.require_authority(authority))
    }
}
