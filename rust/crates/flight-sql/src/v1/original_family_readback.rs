use std::{collections::BTreeSet, fmt::Write, sync::Arc};

use arrow_flight as flight;
use prost::Message;
use rtdb_auth::{
    OriginalFamilyReadbackGrant, OriginalFamilyReadbackProjection as Projection,
    VerifiedWorkloadGrant,
};
use rtdb_index::branches::{
    BranchCut, BranchEvent, BranchIndex, BranchLineage, BranchView, CustodyPlacementEntry, RowImage,
};
use rtdb_node::{Database, SystemClock};
use rtdb_tx::transact::{
    CustodyWriterFence, Outcome, ROW_CUSTODY,
    workload::{self, Applied, Operation, Point, Reply},
};
use rtdb_types::{
    EntityId, TableRef, Timestamp, Value,
    rtdb_v1::{At, Basis, Layer, MAIN_BRANCH},
};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use tonic::{Response, Status};

use crate::service::{Call, Output, Service};

use super::{
    custody::{CustodyProviderRoute, OriginalFamilyReadbackLayout},
    error,
    original_family_landing::installed_native_authority,
    proto,
};

pub(crate) const ACTION: &str = "rtdb.v1.ManagedOriginalFamilyReadback";
const FEATURE: &str = "row-custody.managed-original-family-readback";
const MAX_REQUEST_BYTES: usize = 1024;
const MAX_REPLY_BYTES: usize = 128 * 1024;
const MAX_BODY_BYTES: usize = 48 * 1024;
const MAX_DIGEST_CELL_BYTES: usize = 8 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Scope {},
    Lineage {
        basis: String,
    },
    Registry {
        basis: String,
        location: RegistryLocation,
    },
    Carrier {
        basis: String,
    },
    Placement {},
    Mapping {},
    Settlement {},
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum RegistryLocation {
    Main,
    Named,
}

struct Readback<'a> {
    grant: &'a VerifiedWorkloadGrant,
    scope: &'a OriginalFamilyReadbackGrant,
    point: &'a Point,
    route: &'a CustodyProviderRoute,
    layout: &'a OriginalFamilyReadbackLayout,
    database: Arc<Database>,
    writer: CustodyWriterFence,
    expires_at: Timestamp,
    created: Basis,
}

pub(crate) fn is_readback(call: &Call) -> bool {
    call.session
        .caller
        .principal
        .workload_grant()
        .is_some_and(VerifiedWorkloadGrant::is_original_family_readback)
}

impl Service {
    pub(crate) async fn check_family_readback_handshake(&self, call: &Call) -> Result<(), Status> {
        let readback = self.open_family_readback(call).await?;
        let index = readback.capture(self, call).await?;
        readback.revalidate_delivery(self, call, &index).await
    }

    pub(super) fn family_readback_action(
        &self,
        call: Call,
        action: flight::Action,
    ) -> Result<Response<Output<flight::Result>>, Status> {
        if call.minted || !is_readback(&call) {
            return Err(call.error(denied()));
        }
        let capabilities = action.r#type == super::CAPABILITIES;
        let request = if capabilities {
            super::decode::<proto::CapabilitiesRequest>(action.body)
                .map_err(|status| call.error(status))?;
            Request::Scope {}
        } else {
            if action.body.len() > MAX_REQUEST_BYTES {
                return Err(call.error(invalid()));
            }
            serde_json::from_slice(&action.body).map_err(|_| call.error(invalid()))?
        };
        let headers = call.response(())?.metadata().clone();
        let service = self.clone();
        let stream: Output<flight::Result> = Box::pin(async_stream::stream! {
            let result = service.deliver_family_readback(&call, request, capabilities).await;
            yield result.map_err(|status| call.error(status));
        });
        let mut response = Response::new(stream);
        *response.metadata_mut() = headers;
        Ok(response)
    }

    async fn open_family_readback<'a>(&'a self, call: &'a Call) -> Result<Readback<'a>, Status> {
        let catalog = self.0.node.catalog();
        let caller = &call.session.caller;
        if !caller.verified_credential
            || !catalog.is_managed()
            || self.follower_only()
            || call.session.fresh_replica
        {
            return Err(denied());
        }
        let grant = caller.principal.workload_grant().ok_or_else(denied)?;
        let route = self
            .0
            .config
            .custody_providers
            .get(&call.database)
            .ok_or_else(denied)?
            .as_ref();
        if route.owner().is_none()
            || caller.principal.user_id() != route.owner()
            || route.family_vault.is_none()
            || route.family_line_registry.is_none()
        {
            return Err(denied());
        }
        let layout = route.family_readback_layout.as_ref().ok_or_else(denied)?;
        let (scope, point, expires_at) = grant
            .original_family_readback(&call.database, route.authority(), self.0.node.now())
            .map_err(error::anomaly)?;
        if scope
            .carrier
            .as_ref()
            .is_some_and(|point| point.table != layout.carrier)
        {
            return Err(denied());
        }
        let database = catalog.database(&call.database).map_err(error::anomaly)?;
        if !call.session.holds_attachment(&database)
            || !installed_native_authority(&database)
            || !database
                .serving_driver()
                .is_some_and(|driver| driver.features().contains(&ROW_CUSTODY))
        {
            return Err(denied());
        }
        self.check_attachment(call, &database)?;
        let owner = route.acquire(self, call, &database).await?;
        let writer = owner
            .response_release(&SystemClock, &call.cancellation)
            .map_err(error::anomaly)?
            .original_writer_fence()
            .await
            .map_err(error::anomaly)?;
        if writer.table != point.table
            || writer.id != point.id
            || writer.valid_at != point.valid_at
            || writer.written.to_bytes() != scope.line.writer.basis
        {
            return Err(denied());
        }
        let created = Basis::from_bytes(&scope.line.created).map_err(error::anomaly)?;
        Ok(Readback {
            grant,
            scope,
            point,
            route,
            layout,
            database,
            writer,
            expires_at,
            created,
        })
    }

    async fn deliver_family_readback(
        &self,
        call: &Call,
        request: Request,
        capabilities: bool,
    ) -> Result<flight::Result, Status> {
        let readback = self.open_family_readback(call).await?;
        let index = readback.capture(self, call).await?;
        let (value, protected) = readback
            .project(&index, call, request, self.0.node.now())
            .await?;
        let bytes = if capabilities {
            proto::CapabilitiesResponse {
                api: super::API.into(),
                features: vec![FEATURE.into()],
                limits: None,
                server_time_micros: self.0.node.now().0,
            }
            .encode_to_vec()
        } else {
            serde_json::to_vec(&value).map_err(|_| denied())?
        };
        if bytes.len() > MAX_REPLY_BYTES {
            return Err(denied());
        }
        let charge = call.reserve(bytes.len()).map_err(error::map_status)?;
        if protected {
            let driver = readback.database.serving_driver().ok_or_else(denied)?;
            driver
                .check_custody_release_delivery(
                    &BTreeSet::from([readback.scope.line.group]),
                    &readback.writer,
                    None,
                    &call.cancellation,
                )
                .await
                .map_err(error::anomaly)?;
        }
        readback.revalidate_delivery(self, call, &index).await?;
        Ok(flight::Result {
            body: crate::wire::retain(bytes, charge),
        })
    }
}

impl Readback<'_> {
    async fn revalidate_delivery(
        &self,
        service: &Service,
        call: &Call,
        index: &BranchIndex,
    ) -> Result<(), Status> {
        let owner = self.route.acquire(service, call, &self.database).await?;
        owner
            .response_release(&SystemClock, &call.cancellation)
            .map_err(error::anomaly)?
            .require_original_writer()
            .await
            .map_err(error::anomaly)?;
        self.check_family(index).await?;
        if self.current_head(call).await? != index.head() {
            return Err(error::unavailable(
                "Managed Original readback head advanced during delivery",
            ));
        }
        service.check_attachment(call, &self.database)?;
        let now = service.0.node.now();
        self.require_line_current(index, now)?;
        if call.session.closed() || now >= self.expires_at {
            return Err(denied());
        }
        Ok(())
    }

    async fn current_head(&self, call: &Call) -> Result<BranchCut, Status> {
        let driver = self.database.serving_driver().ok_or_else(denied)?;
        let reply = driver
            .managed_original_family(
                workload::Request {
                    point: self.point.clone(),
                    namespace: self.grant.receipt_namespace(),
                    expires_at: self.expires_at,
                    operation: Operation::ReadRoot {
                        fence: self.scope.line.fence.clone(),
                        writer: self.scope.line.writer.clone(),
                    },
                },
                &call.cancellation,
            )
            .await
            .map_err(error::anomaly)?;
        let Reply::ReadCut(cut) = reply else {
            return Err(denied());
        };
        Ok(cut)
    }

    async fn capture(&self, service: &Service, call: &Call) -> Result<BranchIndex, Status> {
        let head = self.current_head(call).await?;
        let index = self
            .database
            .serving_driver()
            .ok_or_else(denied)?
            .current_custody(&call.cancellation)
            .await
            .map_err(error::anomaly)?;
        if index.head() != head {
            return Err(error::unavailable(
                "Managed Original readback head advanced during admission",
            ));
        }
        if index.has_visibility_history()
            || index.custody_revocation(self.scope.line.group).is_some()
        {
            return Err(denied());
        }
        self.require_line_current(&index, service.0.node.now())?;
        self.check_family(&index).await?;
        let registry = self.registry().ok_or_else(denied)?;
        let creation = index
            .view(
                MAIN_BRANCH,
                At::Exactly(self.created),
                Layer::Merged,
                service.0.node.now(),
            )
            .map_err(error::anomaly)?;
        let id = EntityId::String(self.scope.line.branch.clone());
        let event = exact_point(&creation, registry, &id, Timestamp(0))?.ok_or_else(denied)?;
        if event.written_basis() != Some(self.created) {
            return Err(denied());
        }
        self.registry_identity(clear_image(event)?)?;
        service.check_attachment(call, &self.database)?;
        Ok(index)
    }

    async fn check_family(&self, index: &BranchIndex) -> Result<(), Status> {
        let live = index
            .custody_group_live(self.scope.line.group)
            .unwrap_or(false);
        self.route
            .family_vault
            .as_ref()
            .ok_or_else(denied)?
            .check_readback(
                self.scope,
                self.registry().ok_or_else(denied)?,
                &self.layout.admission,
                live,
            )
            .await
            .map_err(error::anomaly)
    }

    fn require_line_current(&self, index: &BranchIndex, now: Timestamp) -> Result<(), Status> {
        let line = self.lineage_at(index, index.head().basis)?;
        if line.dropped.is_none() && line.expires_at.is_some_and(|until| now >= until) {
            return Err(denied());
        }
        Ok(())
    }

    fn lineage_at(&self, index: &BranchIndex, at: Basis) -> Result<BranchLineage, Status> {
        index
            .lineage_at(&self.scope.line.branch, at)
            .filter(|line| {
                line.created == self.created
                    && line.writable
                    && line
                        .base
                        .as_ref()
                        .is_some_and(|(parent, _)| parent == MAIN_BRANCH)
            })
            .ok_or_else(denied)
    }

    fn registry(&self) -> Option<&TableRef> {
        self.route.family_line_registry.as_ref()
    }

    fn registry_identity(&self, image: &RowImage) -> Result<(), Status> {
        for (name, expected) in [
            ("created_basis", self.scope.line.created.as_slice()),
            ("line_generation", self.scope.generation.as_slice()),
            ("creation_key", self.scope.creation_key.as_slice()),
        ] {
            if binary(image.cells.get(name)) != Some(expected) {
                return Err(denied());
            }
        }
        Ok(())
    }

    async fn project(
        &self,
        index: &BranchIndex,
        call: &Call,
        request: Request,
        now: Timestamp,
    ) -> Result<(Json, bool), Status> {
        match request {
            Request::Scope {} => Ok((
                json!({
                    "database": self.grant.database(),
                    "incarnation": encode_hex(&self.scope.line.authority.incarnation),
                    "branch": self.scope.line.branch,
                    "created_basis": encode_basis(self.created),
                    "line_generation": encode_hex(&self.scope.generation),
                    "family": encode_hex(&self.scope.line.family),
                    "group": encode_hex(&self.scope.line.group),
                    "expires_at_micros": self.expires_at.0,
                    "observation_basis": self.scope.observation.as_ref().map(|basis| encode_hex(basis)),
                    "creation_key": encode_hex(&self.scope.creation_key),
                    "carrier": self.scope.carrier.as_ref().map(|point| json!({
                        "id": text_identity(&point.id),
                        "valid_at_micros": point.valid_at.0,
                    })),
                    "witness": self.scope.witness.as_ref().map(|witness| json!({
                        "namespace": encode_hex(&witness.receipt.namespace),
                        "key": encode_hex(&witness.receipt.key),
                        "basis": encode_hex(&witness.receipt.basis),
                        "system_time_micros": witness.receipt.system_time_micros,
                        "placement": encode_hex(&witness.placement),
                    })),
                }),
                false,
            )),
            Request::Lineage { basis } => {
                let cut = self.observed_cut(index, &basis, call).await?;
                let line = self.lineage_at(index, cut.basis)?;
                let (parent, basis) = line.base.ok_or_else(denied)?;
                let base = index.exact(basis).map_err(error::anomaly)?;
                Ok((
                    json!({
                        "cut": cut_json(cut),
                        "branch": line.branch,
                        "base_branch": parent,
                        "base_basis": encode_basis(base.basis),
                        "base_system_time_micros": base.system_time.0,
                        "created_basis": encode_basis(line.created),
                        "writable": line.writable,
                        "expires_at_micros": line.expires_at.map(|at| at.0),
                        "dropped_basis": line.dropped.map(encode_basis),
                    }),
                    false,
                ))
            }
            Request::Registry { basis, location } => {
                let cut = self.observed_cut(index, &basis, call).await?;
                let branch = match location {
                    RegistryLocation::Main => MAIN_BRANCH,
                    RegistryLocation::Named => self.scope.line.branch.as_str(),
                };
                let view = index
                    .view(branch, At::Exactly(cut.basis), Layer::Merged, now)
                    .map_err(error::anomaly)?;
                let id = EntityId::String(self.scope.line.branch.clone());
                let event = exact_point(
                    &view,
                    self.registry().ok_or_else(denied)?,
                    &id,
                    Timestamp(0),
                )?
                .ok_or_else(denied)?;
                let image = clear_image(event)?;
                self.registry_identity(image)?;
                if branch == MAIN_BRANCH
                    && self.scope.witness.as_ref().is_some_and(|witness| {
                        witness.receipt.basis == cut.basis.to_bytes()
                            && matches!(witness.projection, Projection::Landing { .. })
                    })
                    && (event.written_basis() != Some(cut.basis)
                        || binary(image.cells.get("dropped_basis"))
                            != Some(cut.basis.to_bytes().as_slice()))
                {
                    return Err(denied());
                }
                Ok((
                    json!({ "cut": cut_json(cut), "point": coordinates(event)?, "cells": registry_cells(image)? }),
                    false,
                ))
            }
            Request::Carrier { basis } => {
                let cut = self.observed_cut(index, &basis, call).await?;
                let point = self.scope.carrier.as_ref().ok_or_else(denied)?;
                let view = index
                    .view(
                        &self.scope.line.branch,
                        At::Exactly(cut.basis),
                        Layer::Merged,
                        now,
                    )
                    .map_err(error::anomaly)?;
                let event = exact_point(&view, &self.layout.carrier, &point.id, point.valid_at)?;
                let Some(event) = event else {
                    return Ok((
                        json!({
                            "cut": cut_json(cut), "state": "absent", "id": text_identity(&point.id),
                            "valid_at_micros": point.valid_at.0,
                        }),
                        false,
                    ));
                };
                let (group, selected) = view
                    .custody_event_contributor(&self.layout.carrier, event)
                    .ok_or_else(denied)?;
                if group != self.scope.line.group || event.cut.basis < self.created {
                    return Err(denied());
                }
                let (_, occurrence) = index
                    .custody_read_occurrence(cut.basis, group, selected)
                    .map_err(error::anomaly)?;
                if occurrence.target.branch != self.scope.line.branch {
                    return Err(denied());
                }
                Ok((
                    json!({
                        "cut": cut_json(cut), "state": "written", "point": coordinates(event)?,
                        "valid_at_micros": point.valid_at.0,
                    }),
                    true,
                ))
            }
            request @ (Request::Placement {} | Request::Mapping {} | Request::Settlement {}) => {
                let (cut, entry, projection) = self.witness(index, call).await?;
                let value = match request {
                    Request::Placement {} => catalog(cut, entry),
                    Request::Mapping {} => {
                        self.member_mapping(index, cut, entry, projection, now)?
                    }
                    Request::Settlement {} => self.settlement(index, cut, projection, now)?,
                    _ => return Err(denied()),
                };
                Ok((value, true))
            }
        }
    }

    async fn observed_cut(
        &self,
        index: &BranchIndex,
        basis: &str,
        call: &Call,
    ) -> Result<BranchCut, Status> {
        let basis = decode_basis(basis)?;
        if basis == self.scope.line.created || self.scope.observation == Some(basis) {
            return index
                .exact(Basis::from_bytes(&basis).map_err(error::anomaly)?)
                .map_err(error::anomaly);
        }
        if !self
            .scope
            .witness
            .as_ref()
            .is_some_and(|witness| witness.receipt.basis == basis)
        {
            return Err(denied());
        }
        let (cut, _, _) = self.witness(index, call).await?;
        Ok(cut)
    }

    async fn witness<'a>(
        &'a self,
        index: &'a BranchIndex,
        call: &Call,
    ) -> Result<(BranchCut, CustodyPlacementEntry<'a>, &'a Projection), Status> {
        let witness = self.scope.witness.as_ref().ok_or_else(denied)?;
        let applied = &witness.receipt;
        let basis = Basis::from_bytes(&applied.basis).map_err(error::anomaly)?;
        let cut = index.exact(basis).map_err(error::anomaly)?;
        self.confirm_positive_receipt(call, cut, applied).await?;
        let group = self.scope.line.group;
        let entry = index
            .custody_placement_at(group, witness.placement, basis)
            .map_err(error::anomaly)?
            .ok_or_else(denied)?;
        let occurrence = entry.occurrence.filter(|occurrence| {
            occurrence.written == basis
                && occurrence.receipt_key == applied.key
                && occurrence.target.table == self.layout.carrier
        });
        match &witness.projection {
            Projection::Author { .. } => {
                let occurrence = occurrence
                    .filter(|occurrence| occurrence.target.branch == self.scope.line.branch)
                    .ok_or_else(denied)?;
                index
                    .custody_read_occurrence(basis, group, occurrence.commitment(group))
                    .map_err(error::anomaly)?;
            }
            Projection::Reserve {} => {
                if entry.enrolled != basis
                    || entry.reservation.is_none()
                    || entry.occurrence.is_some()
                {
                    return Err(denied());
                }
                let (reservation, at) = index
                    .custody_reservation_at(group, witness.placement, basis)
                    .map_err(error::anomaly)?;
                let matches = match entry.reviewed {
                    Some((reviewed, _)) => {
                        reviewed.source.branch == self.scope.line.branch
                            && reviewed.source.basis >= self.created
                            && reviewed.receipt_key == applied.key
                            && reservation.target.branch == MAIN_BRANCH
                    }
                    None => reservation.target.branch == self.scope.line.branch,
                };
                if !matches || at != basis || reservation.target.table != self.layout.carrier {
                    return Err(denied());
                }
            }
            Projection::Landing { .. } => {
                let occurrence = occurrence
                    .filter(|occurrence| occurrence.target.branch == MAIN_BRANCH)
                    .ok_or_else(denied)?;
                let (reviewed, reservation) = entry.reviewed.ok_or_else(denied)?;
                if reservation.target.table != self.layout.carrier
                    || reviewed.source.branch != self.scope.line.branch
                    || reviewed.source.basis < self.created
                    || self.lineage_at(index, basis)?.dropped != Some(basis)
                    || !index
                        .matches_reviewed_activation(group, witness.placement, basis, &applied.key)
                        .map_err(error::anomaly)?
                {
                    return Err(denied());
                }
                index
                    .custody_read_occurrence(basis, group, occurrence.commitment(group))
                    .map_err(error::anomaly)?;
            }
        }
        Ok((cut, entry, &witness.projection))
    }

    async fn confirm_positive_receipt(
        &self,
        call: &Call,
        cut: BranchCut,
        applied: &Applied,
    ) -> Result<(), Status> {
        let driver = self.database.serving_driver().ok_or_else(denied)?;
        let reply = driver
            .managed_original_family(
                workload::Request {
                    point: self.point.clone(),
                    namespace: applied.namespace,
                    expires_at: self.expires_at,
                    operation: Operation::Status {
                        key: applied.key.clone(),
                        family: workload::Family::OriginalFamilyWrite,
                    },
                },
                &call.cancellation,
            )
            .await
            .map_err(error::anomaly)?;
        if !matches!(reply, Reply::Receipt(receipt) if receipt.outcome == Outcome::Then
            && receipt.key == applied.key && receipt.basis == cut.basis && receipt.system_time == cut.system_time
            && cut.system_time.0 == applied.system_time_micros)
        {
            return Err(denied());
        }
        Ok(())
    }

    fn member_mapping(
        &self,
        index: &BranchIndex,
        cut: BranchCut,
        entry: CustodyPlacementEntry<'_>,
        projection: &Projection,
        now: Timestamp,
    ) -> Result<Json, Status> {
        let table = &self.layout.member_mapping;
        let id = EntityId::String(format!(
            "{}:{}",
            encode_hex(&entry.group),
            encode_hex(&entry.placement_id)
        ));
        let (event, digest) = match projection {
            Projection::Author {
                mapping_sha256: Some(digest),
                ..
            } => (
                index
                    .receipt_written_local_point(
                        &self.scope.line.branch,
                        self.created,
                        cut.basis,
                        table,
                        &id,
                    )
                    .map_err(error::anomaly)?
                    .ok_or_else(denied)?,
                digest,
            ),
            Projection::Landing {
                mapping_sha256: Some(digest),
                ..
            } => {
                let view = index
                    .view(MAIN_BRANCH, At::Exactly(cut.basis), Layer::Merged, now)
                    .map_err(error::anomaly)?;
                (co_written(&view, table, &id, cut.system_time)?, digest)
            }
            _ => return Err(denied()),
        };
        let body = committed_body(event, "body", digest)?;
        Ok(json!({ "cut": cut_json(cut), "point": coordinates(event)?, "body": encode_hex(body) }))
    }

    fn settlement(
        &self,
        index: &BranchIndex,
        cut: BranchCut,
        projection: &Projection,
        now: Timestamp,
    ) -> Result<Json, Status> {
        let view = index
            .view(MAIN_BRANCH, At::Exactly(cut.basis), Layer::Merged, now)
            .map_err(error::anomaly)?;
        let admission = &self.layout.admission;
        let point = |table: &TableRef, id: &str, valid| {
            co_written(&view, table, &EntityId::String(id.to_owned()), valid)
        };
        let at_zero = Timestamp(0);
        match projection {
            Projection::Author {
                settlement: Some(witnesses),
                ..
            } => {
                let mut result = serde_json::Map::new();
                for (name, id) in [
                    ("command", &witnesses.command),
                    ("head", &witnesses.head),
                    ("dispatch", &witnesses.dispatch),
                    ("pending", &witnesses.pending),
                ] {
                    let event = point(admission, id, at_zero)?;
                    result.insert(name.into(), digest_point(event, &["admission"])?);
                }
                let business = point(
                    &self.layout.business_receipt,
                    &witnesses.business_receipt,
                    at_zero,
                )?;
                let execution = point(&self.layout.execution, &witnesses.execution_slot, at_zero)?;
                result.insert(
                    "business_receipt".into(),
                    digest_point(business, &["outcome"])?,
                );
                result.insert(
                    "execution_slot".into(),
                    digest_point(execution, &["marker", "capsule"])?,
                );
                Ok(json!({ "cut": cut_json(cut), "witnesses": result }))
            }
            Projection::Landing {
                settlement: Some(witnesses),
                ..
            } => {
                let receipt = point(admission, &witnesses.receipt, at_zero)?;
                let proof = point(admission, &witnesses.proof, at_zero)?;
                let body = committed_body(receipt, "admission", &witnesses.body_sha256)?;
                if committed_body(proof, "admission", &witnesses.body_sha256)? != body {
                    return Err(denied());
                }
                let publication = point(&self.layout.publication, "head", cut.system_time)?;
                let root_version = point(&self.layout.root_version, "head", cut.system_time)?;
                clear_image(publication)?;
                clear_image(root_version)?;
                Ok(json!({
                    "cut": cut_json(cut),
                    "body": encode_hex(body),
                    "receipt": coordinates(receipt)?,
                    "proof": coordinates(proof)?,
                    "publication": coordinates(publication)?,
                    "root_version": coordinates(root_version)?,
                }))
            }
            _ => Err(denied()),
        }
    }
}

pub(super) fn verify_replay_author(
    index: &BranchIndex,
    layout: &OriginalFamilyReadbackLayout,
    review: &rtdb_node::serving::custody_release::NativeLandingReview,
    created: Basis,
    placement: [u8; 16],
    evidence: &super::original_family_landing::private_replay::AuthorEvidence,
    written: Basis,
    system_time_micros: i64,
    now: Timestamp,
) -> Result<(), Status> {
    use super::original_family_landing::wire::nonzero;
    let cut = index.exact(written).map_err(error::anomaly)?;
    let entry = index.custody_placement_at(review.group, placement, written)
        .map_err(error::anomaly)?.ok_or_else(denied)?;
    if cut.system_time.0 != system_time_micros || !entry.occurrence.is_some_and(|occurrence| {
        occurrence.written == written && occurrence.target.table == layout.carrier
            && occurrence.target.branch == review.cut.branch
    }) {
        return Err(denied());
    }
    let mapping_id = EntityId::String(format!("{}:{}", encode_hex(&review.group), encode_hex(&placement)));
    let mapping = index.receipt_written_local_point(
        &review.cut.branch, created, written, &layout.member_mapping, &mapping_id,
    ).map_err(error::anomaly)?.ok_or_else(denied)?;
    committed_body(mapping, "body", &nonzero(&evidence.mapping_sha256)?)?;
    let view = index.view(MAIN_BRANCH, At::Exactly(written), Layer::Merged, now).map_err(error::anomaly)?;
    for (table, id, columns) in [
        (&layout.admission, &evidence.command, &[("admission", &evidence.command_sha256)][..]),
        (&layout.business_receipt, &evidence.business_receipt, &[("outcome", &evidence.business_receipt_sha256)][..]),
        (&layout.execution, &evidence.execution_slot, &[("marker", &evidence.marker_sha256), ("capsule", &evidence.capsule_sha256)][..]),
    ] {
        if !(1..=1024).contains(&id.len()) || id.chars().any(char::is_control) {
            return Err(denied());
        }
        let event = co_written(&view, table, &EntityId::String(id.clone()), Timestamp(0))?;
        if event.cut.system_time != cut.system_time
            || event.valid.lower() != Timestamp(0) || event.valid.upper() != Timestamp::MAX
        {
            return Err(denied());
        }
        for (column, expected) in columns {
            if cell_digest(event, column)? != nonzero(expected)? {
                return Err(denied());
            }
        }
    }
    Ok(())
}

fn exact_point<'a>(
    view: &BranchView<'a>,
    table: &TableRef,
    id: &EntityId,
    valid_at: Timestamp,
) -> Result<Option<&'a BranchEvent>, Status> {
    if view
        .table(table)
        .is_none_or(|definition| definition.visibility.is_some())
        || view.has_erased_point(table, id)
    {
        return Err(denied());
    }
    Ok(view.row(table, id, valid_at))
}

fn co_written<'a>(
    view: &BranchView<'a>,
    table: &TableRef,
    id: &EntityId,
    at: Timestamp,
) -> Result<&'a BranchEvent, Status> {
    exact_point(view, table, id, at)?
        .filter(|event| event.written_basis() == Some(view.cut().basis))
        .ok_or_else(denied)
}

fn catalog(cut: BranchCut, entry: CustodyPlacementEntry<'_>) -> Json {
    let occurrence = entry.occurrence;
    let mut cells = json!({
        "id": encode_hex(&entry.placement_id),
        "group_id": encode_hex(&entry.group),
        "enrolled_basis": encode_basis(entry.enrolled),
        "reservation_sha256": entry.reservation.map(|digest| encode_hex(&digest)),
        "occurrence_sha256": occurrence.map(|row| encode_hex(&row.commitment(entry.group))),
        "carrier_sha256": occurrence.map(|row| encode_hex(&row.carrier_sha256)),
        "written_basis": occurrence.map(|row| encode_basis(row.written)),
        "ordinal": occurrence.map(|row| row.ordinal),
    });
    if let Some((reviewed, reservation)) = entry.reviewed {
        cells["reviewed"] = json!({
            "branch": reviewed.source.branch,
            "basis": encode_basis(reviewed.source.basis),
            "valid_at_micros": reviewed.source.valid_at.0,
            "inventory_sha256": encode_hex(&reviewed.source.inventory_sha256),
            "original_sha256": encode_hex(&reservation.original),
            "source_sha256": encode_hex(&reservation.source),
            "review_sha256": encode_hex(&reservation.review_sha256),
            "carrier_sha256": encode_hex(&reservation.carrier_sha256),
            "link_sha256": encode_hex(&reservation.link_sha256),
            "link_id": encode_hex(&reservation.link),
            "target_table": format!("{}.{}", reservation.target.table.schema, reservation.target.table.table),
            "target_id": encode_hex(&reservation.target.id.handle_bytes()),
            "reserve_key": encode_hex(&reviewed.receipt_key),
            "activation_key": occurrence.map(|row| encode_hex(&row.receipt_key)),
            "envelope_sha256": occurrence.map(|row| encode_hex(&row.envelope_sha256)),
            "written_valid_from_micros": occurrence.map(|row| row.target.valid.lower().0),
            "written_valid_to_micros": occurrence.map(|row| row.target.valid.upper().0),
        });
    }
    json!({ "cut": cut_json(cut), "placement": cells })
}

fn committed_body<'a>(
    event: &'a BranchEvent,
    column: &str,
    digest: &[u8; 32],
) -> Result<&'a [u8], Status> {
    let value = binary(clear_image(event)?.cells.get(column)).ok_or_else(denied)?;
    if value.len() > MAX_BODY_BYTES || <[u8; 32]>::from(Sha256::digest(value)) != *digest {
        return Err(denied());
    }
    Ok(value)
}

fn digest_point(event: &BranchEvent, columns: &[&str]) -> Result<Json, Status> {
    let mut digests = serde_json::Map::new();
    for &column in columns {
        digests.insert(column.into(), json!(encode_hex(&cell_digest(event, column)?)));
    }
    Ok(json!({ "point": coordinates(event)?, "sha256": digests }))
}

fn cell_digest(event: &BranchEvent, column: &str) -> Result<[u8; 32], Status> {
    let image = clear_image(event)?;
    let value = binary(image.cells.get(column))
        .filter(|bytes| bytes.len() <= MAX_DIGEST_CELL_BYTES)
        .ok_or_else(denied)?;
    Ok(Sha256::digest(value).into())
}

fn clear_image(event: &BranchEvent) -> Result<&RowImage, Status> {
    event
        .image
        .as_deref()
        .filter(|image| image.custody_group.is_none() && image.custody_payload.is_none())
        .ok_or_else(denied)
}

fn registry_cells(image: &RowImage) -> Result<Json, Status> {
    let mut cells = serde_json::Map::new();
    for (name, value) in [
        ("frame", registry_binary(image, "frame", 32)?),
        ("parent", registry_binary(image, "parent", 16)?),
        ("base_source", registry_binary(image, "base_source", 16)?),
        (
            "created_basis",
            registry_binary(image, "created_basis", 24)?,
        ),
        (
            "dropped_basis",
            registry_binary(image, "dropped_basis", 24)?,
        ),
        (
            "line_generation",
            registry_binary(image, "line_generation", 16)?,
        ),
        (
            "creation_key",
            binary(image.cells.get("creation_key")).map(encode_hex),
        ),
    ] {
        cells.insert(name.into(), json!(value));
    }
    for name in [
        "parent_micros",
        "base_clock",
        "initial_clock",
        "clock",
        "expires",
    ] {
        let Some(Value::I64(value)) = image.cells.get(name) else {
            return Err(denied());
        };
        cells.insert(name.into(), json!(value));
    }
    let Some(Value::Bool(writable)) = image.cells.get("writable") else {
        return Err(denied());
    };
    cells.insert("writable".into(), json!(writable));
    for name in ["retained_owner", "proposal_owner"] {
        let value = match image.cells.get(name) {
            None | Some(Value::Null | Value::Undefined) => Json::Null,
            Some(Value::Utf8(value)) if value.len() <= 1024 => json!(value),
            _ => return Err(denied()),
        };
        cells.insert(name.into(), value);
    }
    Ok(Json::Object(cells))
}

fn registry_binary(image: &RowImage, name: &str, width: usize) -> Result<Option<String>, Status> {
    match image.cells.get(name) {
        None | Some(Value::Null | Value::Undefined) => Ok(None),
        value => binary(value)
            .filter(|bytes| bytes.len() == width)
            .map(encode_hex)
            .map(Some)
            .ok_or_else(denied),
    }
}

fn binary(value: Option<&Value>) -> Option<&[u8]> {
    match value? {
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => Some(bytes),
        _ => None,
    }
}

fn text_identity(id: &EntityId) -> Option<&str> {
    match id {
        EntityId::String(text) => Some(text),
        _ => None,
    }
}

fn coordinates(event: &BranchEvent) -> Result<Json, Status> {
    let EntityId::String(id) = &event.id else {
        return Err(denied());
    };
    Ok(json!({
        "id": id,
        "written_basis": encode_basis(event.written_basis().ok_or_else(denied)?),
        "system_time_micros": event.cut.system_time.0,
        "valid_from_micros": event.valid.lower().0,
        "valid_to_micros": event.valid.upper().0,
    }))
}

fn cut_json(cut: BranchCut) -> Json {
    json!({ "basis": encode_basis(cut.basis), "system_time_micros": cut.system_time.0 })
}

fn encode_basis(basis: Basis) -> String {
    encode_hex(&basis.to_bytes())
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(result, "{byte:02x}").expect("write to string");
    }
    result
}

fn decode_basis(value: &str) -> Result<[u8; Basis::WIDTH], Status> {
    if value.len() != 2 * Basis::WIDTH
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid());
    }
    let mut bytes = [0; Basis::WIDTH];
    for (idx, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[2 * idx..2 * idx + 2], 16).map_err(|_| invalid())?;
    }
    Ok(bytes)
}

fn invalid() -> Status {
    error::incorrect("Invalid managed Original family readback request")
}

fn denied() -> Status {
    Status::permission_denied(
        "Managed Original family readback requires its exact commissioned witness",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mounted_layout_requires_distinct_bounded_application_relations() {
        let relation = |name| TableRef::new("app", name);
        let writer = relation("writer");
        let registry = relation("registry");
        let layout = OriginalFamilyReadbackLayout {
            carrier: relation("carrier"),
            member_mapping: relation("mapping"),
            admission: relation("admission"),
            business_receipt: relation("business_receipt"),
            execution: relation("execution"),
            publication: relation("publication"),
            root_version: relation("root_version"),
        };
        assert!(layout.validate(&writer, &registry).is_ok());
        for table in [
            layout.carrier.clone(),
            writer.clone(),
            registry.clone(),
            TableRef::new("xt", "placements"),
            TableRef::new("PG_CATALOG", "relations"),
            TableRef::new("information_schema", "tables"),
            TableRef::new("rtdb_owned_v1", "slots"),
            TableRef::new("", "bare_name"),
            TableRef::new("app.schema", "ambiguous_name"),
            TableRef::new("app", "x".repeat(256)),
        ] {
            let invalid = OriginalFamilyReadbackLayout {
                execution: table,
                ..layout.clone()
            };
            assert!(invalid.validate(&writer, &registry).is_err());
        }
    }

    #[test]
    fn request_fields_cannot_select_other_branches_tables_or_receipts() {
        assert!(serde_json::from_str::<Request>(r#"{"op":"placement"}"#).is_ok());
        assert!(serde_json::from_str::<Request>(r#"{"op":"mapping","key":"other"}"#).is_err());
        assert!(
            serde_json::from_str::<Request>(r#"{"op":"carrier","basis":"cut","id":"other"}"#)
                .is_err()
        );
        assert!(
            serde_json::from_str::<Request>(
                r#"{"op":"registry","basis":"cut","location":"other"}"#
            )
            .is_err()
        );
        assert!(serde_json::from_str::<Request>(r#"{"op":"query","sql":"SELECT 1"}"#).is_err());
    }
}
