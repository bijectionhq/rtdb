use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use ordered_logkeeper::ordered::{
    Checkpoint, Command, Custody, Cut, Enrollment, Fingerprint, Limits, Outcome, Placement,
    Principal, Refusal, Request, RequestId, Window,
    daemon::{
        ApplicationRoute, Client, ClientPin, ClientRole, Credentials, DaemonClient, Endpoint,
        EndpointAddress, InstalledGroup, ManagementReply, ManagementRequest, PublishedRoute,
        RouteBinding, VoterRoute,
    },
    service::wire::{Operation, Reply},
};
use rtdb_directory::{DeploymentRecord, Directory, now_ms};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::Deserialize;
use uuid::Uuid;

use super::{
    Authority, InstalledRoute,
    hosted::{GatewayPlacement, WorkerEndpoint},
};
use crate::{
    deployment::CellStorage,
    keys::{JournalKey, KeyService},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledCell {
    pub cell: String,
    ca: PathBuf,
    voters: Vec<Voter>,
    limits: Limits,
    pub archive_root: String,
    pub keys: String,
    pub custody_manifest: Option<PathBuf>,
    maximum_prefix_bytes: usize,
    maximum_application_frame: usize,
    #[serde(default = "exchange_ms")]
    exchange_ms: u64,
    administrator_pin: Fingerprint,
    route_reader_pin: Fingerprint,
    custodian_pin: Option<Fingerprint>,
    workers: Vec<Worker>,
    #[serde(default)]
    credentials: CredentialFiles,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Voter {
    voter: Uuid,
    zone: String,
    endpoint: String,
    server_name: String,
    certificate: Fingerprint,
    receipt_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Worker {
    worker: Uuid,
    certificate: Fingerprint,
    control: String,
    pgwire: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialFiles {
    administrator: Option<PemFiles>,
    route_reader: Option<PemFiles>,
    worker: Option<PemFiles>,
    custodian: Option<PemFiles>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PemFiles {
    certificate: PathBuf,
    key: PathBuf,
}

#[derive(Clone, Copy)]
pub enum Role {
    Administrator,
    RouteReader,
    Worker,
    Custodian,
}

fn exchange_ms() -> u64 {
    2_000
}

impl InstalledCell {
    pub fn read(path: &Path) -> Result<Arc<Self>> {
        let mut cell: Self = super::hosted::read_manifest(path)?;
        let mut identities = BTreeSet::new();
        let mut sites = BTreeSet::new();
        let mut voter_ids = BTreeSet::new();
        let mut zones = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        identities.insert(cell.administrator_pin);
        if cell.cell.is_empty()
            || cell.exchange_ms == 0
            || cell.voters.len() < 3
            || cell.voters.len() % 2 == 0
            || cell.workers.is_empty()
            || !identities.insert(cell.route_reader_pin)
            || cell
                .custodian_pin
                .is_some_and(|pin| !identities.insert(pin))
            || cell.workers.iter().any(|worker| {
                worker.worker.is_nil()
                    || worker.control.is_empty()
                    || worker.pgwire.is_empty()
                    || !sites.insert(worker.worker)
                    || !identities.insert(worker.certificate)
            })
            || cell.voters.iter().any(|voter| {
                voter.voter.is_nil()
                    || voter.zone.is_empty()
                    || !voter_ids.insert(voter.voter)
                    || !zones.insert(&voter.zone)
                    || !endpoints.insert(voter.endpoint.to_ascii_lowercase())
                    || !identities.insert(voter.certificate)
            })
        {
            return Err(invalid(
                "The installed cell needs distinct voter and client identities and worker endpoints",
            ));
        }
        if cell.workers.len() >= cell.limits.enrolled_workers {
            return Err(invalid(
                "The installed worker slots must leave room for one more enrollment",
            ));
        }
        cell.voter_routes()?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        cell.custody_manifest = cell.custody_manifest.map(|path| parent.join(path));
        Ok(Arc::new(cell))
    }

    pub fn credentials(&self, role: Role) -> Result<Credentials> {
        let (files, prefix) = match role {
            Role::Administrator => (
                &self.credentials.administrator,
                "RTDB_ORDERED_ADMINISTRATOR",
            ),
            Role::RouteReader => (&self.credentials.route_reader, "RTDB_ORDERED_ROUTE_READER"),
            Role::Worker => (&self.credentials.worker, "RTDB_ORDERED_WORKER"),
            Role::Custodian => (&self.credentials.custodian, "RTDB_ORDERED_CUSTODIAN"),
        };
        let (certificate, key) = match files {
            Some(files) => (files.certificate.clone(), files.key.clone()),
            None => {
                let credential = |suffix| {
                    std::env::var_os(format!("{prefix}_{suffix}"))
                        .filter(|path| !path.is_empty())
                        .map(PathBuf::from)
                        .ok_or_else(|| invalid("The installed cell credential is missing"))
                };
                (credential("CERTIFICATE")?, credential("KEY")?)
            }
        };
        let credentials = Credentials {
            certificate,
            key,
            roots: self.ca.clone(),
        };
        let pin = credentials.certificate_pin().map_err(journal_error)?;
        let allowed = match role {
            Role::Administrator => pin == self.administrator_pin,
            Role::RouteReader => pin == self.route_reader_pin,
            Role::Worker => self.workers.iter().any(|worker| pin == worker.certificate),
            Role::Custodian => Some(pin) == self.custodian_pin,
        };
        if !allowed {
            return Err(invalid(
                "The installed cell credential has a different role",
            ));
        }
        Ok(credentials)
    }

    pub(crate) fn archive(&self) -> Result<CellStorage> {
        CellStorage::parse(&self.archive_root)
    }

    fn voter_routes(&self) -> Result<Vec<VoterRoute>> {
        self.voters
            .iter()
            .map(|voter| {
                if voter.server_name.is_empty()
                    || voter
                        .endpoint
                        .rsplit_once(':')
                        .and_then(|(host, port)| {
                            (!host.is_empty())
                                .then(|| port.parse::<std::num::NonZeroU16>().ok())
                                .flatten()
                        })
                        .is_none()
                {
                    return Err(invalid(
                        "The installed cell needs a TLS name and host:port for each voter",
                    ));
                }
                let key = voter.receipt_key.as_bytes();
                if key.len() != 64 {
                    return Err(invalid(
                        "The voter receipt key must contain 32 hexadecimal bytes",
                    ));
                }
                let mut receipt_key = [0; 32];
                for (byte, pair) in receipt_key.iter_mut().zip(key.chunks_exact(2)) {
                    *byte = std::str::from_utf8(pair)
                        .ok()
                        .and_then(|p| u8::from_str_radix(p, 16).ok())
                        .ok_or_else(|| {
                            invalid("The voter receipt key must contain 32 hexadecimal bytes")
                        })?;
                }
                let address = voter
                    .endpoint
                    .parse()
                    .map(EndpointAddress::Socket)
                    .unwrap_or_else(|_| EndpointAddress::Host(voter.endpoint.clone()));
                Ok(VoterRoute {
                    voter: voter.voter,
                    zone: voter.zone.clone(),
                    receipt_key,
                    endpoint: Endpoint {
                        address,
                        server_name: voter.server_name.clone(),
                        certificate: voter.certificate,
                    },
                })
            })
            .collect()
    }

    fn enrollments(&self) -> impl Iterator<Item = (&Worker, Enrollment)> {
        self.workers.iter().enumerate().map(|(index, worker)| {
            (
                worker,
                Enrollment {
                    worker: worker.worker,
                    serial: index as u64 + 1,
                },
            )
        })
    }

    pub(crate) fn worker_attachment(
        &self,
        route: ApplicationRoute,
    ) -> Result<(InstalledRoute, Vec<WorkerEndpoint>)> {
        let credentials = self.credentials(Role::Worker)?;
        let pin = credentials.certificate_pin().map_err(journal_error)?;
        let own = self
            .enrollments()
            .find(|(worker, _)| worker.certificate == pin)
            .map(|(_, enrollment)| enrollment)
            .ok_or_else(|| invalid("The worker is outside the installed cell"))?;
        Ok((
            installed_route(route, Authority::Worker { enrollment: own }, credentials),
            self.worker_endpoints(),
        ))
    }

    fn worker_endpoints(&self) -> Vec<WorkerEndpoint> {
        self.enrollments()
            .map(|(worker, enrollment)| WorkerEndpoint {
                enrollment,
                control: worker.control.clone(),
                pgwire: worker.pgwire.clone(),
            })
            .collect()
    }

    fn group(&self, deployment: Uuid) -> Result<InstalledGroup> {
        let placement = Placement {
            deployment,
            incarnation: derived_uuid(deployment, b"incarnation"),
            group: derived_uuid(deployment, b"group"),
            generation: 1,
        };
        let initial_checkpoint = Checkpoint {
            cut: Cut {
                next_lsn: 0,
                history: derive(deployment, b"history"),
            },
            root: [0; 32],
            manifest: [0; 32],
            custody: Custody {
                allocation: derived_uuid(deployment, b"checkpoint"),
                revision: 1,
                provider: [0; 32],
            },
        };
        let bytes = self
            .limits
            .retained_data_bytes
            .checked_sub(self.limits.maintenance_bytes)
            .filter(|bytes| *bytes > 0)
            .ok_or_else(|| {
                invalid("The installed journal needs funded ordinary and maintenance windows")
            })?;
        let initial_window = Window {
            allocation: derived_uuid(deployment, b"window"),
            serial: 1,
            bytes,
        };
        let mut clients = vec![
            ClientPin {
                certificate: self.administrator_pin,
                role: ClientRole::Administrator,
            },
            ClientPin {
                certificate: self.route_reader_pin,
                role: ClientRole::RouteReader,
            },
        ];
        if let Some(certificate) = self.custodian_pin {
            clients.push(ClientPin {
                certificate,
                role: ClientRole::Custodian,
            });
        }
        clients.extend(self.enrollments().map(|(worker, enrollment)| ClientPin {
            certificate: worker.certificate,
            role: ClientRole::Worker { enrollment },
        }));
        Ok(InstalledGroup {
            placement,
            limits: self.limits,
            initial_checkpoint,
            initial_window,
            clients,
            voters: self.voter_routes()?,
            maximum_prefix_bytes: self.maximum_prefix_bytes,
            maximum_application_frame: self.maximum_application_frame,
            predecessor: None,
            predecessor_keys: Vec::new(),
            retained_barrier_keys: Vec::new(),
        })
    }

    pub fn access(self: &Arc<Self>, role: Role) -> Result<CellAccess> {
        let credentials = self.credentials(role)?;
        let daemon = DaemonClient::new(&credentials, Duration::from_millis(self.exchange_ms))
            .map_err(journal_error)?;
        let voters = self
            .voter_routes()?
            .into_iter()
            .map(|voter| voter.endpoint)
            .collect();
        Ok(CellAccess {
            cell: self.clone(),
            credentials,
            daemon,
            voters,
        })
    }
}

pub struct CellAccess {
    cell: Arc<InstalledCell>,
    credentials: Credentials,
    daemon: DaemonClient,
    voters: Vec<Endpoint>,
}

impl CellAccess {
    async fn majority(&self, request: ManagementRequest) -> Result<ManagementReply> {
        let required = self.voters.len() / 2 + 1;
        let mut replies = Vec::<(ManagementReply, usize)>::new();
        let mut pending: futures::stream::FuturesUnordered<_> = self
            .voters
            .iter()
            .map(|voter| self.daemon.call(voter, &request))
            .collect();
        use futures::StreamExt;
        while let Some(result) = pending.next().await {
            if let Ok(reply) = result {
                if let Some((_, count)) = replies.iter_mut().find(|(prior, _)| *prior == reply) {
                    *count += 1;
                    if *count == required {
                        return Ok(reply);
                    }
                } else {
                    replies.push((reply, 1));
                }
            }
        }
        Err(unavailable(
            "The installed cell did not return a matching voter quorum",
        ))
    }

    async fn change(&self, request: ManagementRequest) -> Result<ManagementReply> {
        // Let every reachable voter finish even when a majority has already
        // responded, so the next voter failure can leave a discoverable route.
        let responses = futures::future::join_all(
            self.voters
                .iter()
                .map(|voter| self.daemon.call(voter, &request)),
        )
        .await;
        let replies: Vec<_> = responses
            .into_iter()
            .filter_map(std::result::Result::ok)
            .collect();
        let index = replies
            .iter()
            .position(|reply| {
                replies.iter().filter(|other| *other == reply).count() > self.voters.len() / 2
            })
            .ok_or_else(|| {
                unavailable("A matching majority did not persist the installed cell change")
            })?;
        Ok(replies.into_iter().nth(index).unwrap())
    }

    async fn journal_query(&self, request: ManagementRequest) -> Result<ManagementReply> {
        for voter in &self.voters {
            match self.daemon.call(voter, &request).await {
                Ok(
                    reply @ (ManagementReply::Enrollment(_)
                    | ManagementReply::RetirementCheckpoint(_)),
                ) => return Ok(reply),
                Ok(ManagementReply::Forbidden) => return Err(refused(ManagementReply::Forbidden)),
                _ => {}
            }
        }
        Err(unavailable(
            "No voter coordinator recovered the installed group",
        ))
    }

    pub async fn by_deployment(&self, deployment: Uuid) -> Result<Option<PublishedRoute>> {
        match self
            .majority(ManagementRequest::RouteByDeployment { deployment })
            .await?
        {
            ManagementReply::Published(route) if route.route.placement.deployment == deployment => {
                Ok(Some(route))
            }
            ManagementReply::Absent => Ok(None),
            reply => Err(refused(reply)),
        }
    }

    pub async fn by_name(&self, name: &str) -> Result<Option<PublishedRoute>> {
        match self
            .majority(ManagementRequest::RouteByName {
                name: name.to_owned(),
            })
            .await?
        {
            ManagementReply::Published(route) if route.binding.name == name => Ok(Some(route)),
            ManagementReply::Absent => Ok(None),
            reply => Err(refused(reply)),
        }
    }

    pub(crate) async fn custody_journal(
        &self,
        deployment: Uuid,
    ) -> Result<Option<Arc<super::InstalledJournal>>> {
        self.by_deployment(deployment)
            .await?
            .map(|published| {
                super::InstalledJournal::open(installed_route(
                    published.route,
                    Authority::Custody,
                    self.credentials.clone(),
                ))
            })
            .transpose()
    }

    pub(crate) async fn gateway_placement(&self, name: &str) -> Result<Option<GatewayPlacement>> {
        let Some(published) = self.by_name(name).await? else {
            return Ok(None);
        };
        let deployment = published.route.placement.deployment;
        let journal = super::InstalledJournal::open(installed_route(
            published.route,
            Authority::Routing,
            self.credentials.clone(),
        ))?;
        Ok(Some(GatewayPlacement {
            deployment,
            journal,
            endpoints: self
                .cell
                .worker_endpoints()
                .into_iter()
                .map(|worker| (worker.enrollment, worker))
                .collect(),
        }))
    }

    pub async fn install(
        &self,
        directory: &dyn Directory,
        keys: &dyn KeyService,
        deployment: Uuid,
        name: &str,
        organization: &str,
    ) -> Result<()> {
        let binding = RouteBinding {
            name: name.into(),
            organization: organization.into(),
        };
        if name.is_empty()
            || name.len() > 255
            || name.contains('@')
            || organization.is_empty()
            || organization.len() > 255
            || deployment.is_nil()
        {
            return Err(invalid(
                "The installed deployment needs a UUID, name and organization",
            ));
        }
        if self
            .retired(deployment, name, organization)
            .await?
            .is_some()
        {
            return Err(conflict());
        }
        if self
            .by_deployment(deployment)
            .await?
            .is_some_and(|existing| existing.binding != binding)
        {
            return Err(conflict());
        }
        if directory.deployment_by_name(name).await?.is_some()
            || self
                .by_name(name)
                .await?
                .is_some_and(|existing| existing.route.placement.deployment != deployment)
        {
            return Err(conflict());
        }
        let group = self.cell.group(deployment)?;
        let placement = group.placement;
        let response = self
            .change(ManagementRequest::InstallGroup {
                group: Box::new(group),
                expected_absent: true,
            })
            .await?;
        let ManagementReply::Installed(route) = response else {
            return Err(refused(response));
        };
        if route.placement != placement {
            return Err(conflict());
        }
        directory.reserve_installed_journal(deployment).await?;
        let record = deployment_record(name, organization, deployment);
        keys.key(&record.key_id()).await?;
        keys.provision_journal_key(JournalKey::new(placement, 1))
            .await?;
        self.enroll(&route).await?;
        match self
            .change(ManagementRequest::PublishRoute {
                placement,
                binding: binding.clone(),
            })
            .await?
        {
            ManagementReply::Published(published)
                if published.binding == binding && published.route == route =>
            {
                Ok(())
            }
            reply => Err(refused(reply)),
        }
    }

    async fn enroll(&self, route: &ApplicationRoute) -> Result<()> {
        let admin = Client::new(route.clone(), &self.credentials).map_err(journal_error)?;
        for (_, expected) in self.cell.enrollments() {
            let selected = self
                .journal_query(ManagementRequest::Enrollment {
                    placement: route.placement,
                    worker: expected.worker,
                })
                .await?;
            match selected {
                ManagementReply::Enrollment(Some(existing)) if existing == expected => continue,
                ManagementReply::Enrollment(None) => {}
                other => return Err(refused(other)),
            }
            match execute(
                &admin,
                Command::Enroll {
                    worker: expected.worker,
                    expected: None,
                },
            )
            .await?
            {
                Ok(Outcome::Enrolled(enrolled)) if enrolled == expected => {}
                Err(Refusal::Stale) => {
                    if self
                        .journal_query(ManagementRequest::Enrollment {
                            placement: route.placement,
                            worker: expected.worker,
                        })
                        .await?
                        != ManagementReply::Enrollment(Some(expected))
                    {
                        return Err(conflict());
                    }
                }
                _ => return Err(conflict()),
            }
        }
        Ok(())
    }

    pub async fn retire(
        &self,
        deployment: Uuid,
        name: &str,
        organization: &str,
    ) -> Result<Option<u64>> {
        let binding = RouteBinding {
            name: name.into(),
            organization: organization.into(),
        };
        let retirement = self
            .majority(ManagementRequest::RetirementStatus { deployment })
            .await;
        if let Ok(ManagementReply::RetirementStatus { route, .. }) = retirement {
            return if route.binding == binding && route.route.placement.deployment == deployment {
                Ok(Some(route.route.placement.generation))
            } else {
                Err(conflict())
            };
        }
        let Some(published) = self.by_deployment(deployment).await? else {
            return Ok(None);
        };
        if published.binding != binding {
            return Err(conflict());
        }
        let placement = published.route.placement;
        let admin = Client::new(published.route, &self.credentials).map_err(journal_error)?;
        let checkpoint = match admin.call(Operation::Route).await {
            Ok(Reply::Route(view)) => {
                if view.checkpoint.cut != view.head || view.preparing.is_some() {
                    return Ok(None);
                }
                for (_, enrollment) in self.cell.enrollments() {
                    match execute(&admin, Command::Withdraw { enrollment }).await? {
                        Ok(Outcome::Withdrawn) | Err(Refusal::Stale) => {}
                        _ => return Ok(None),
                    }
                }
                view.checkpoint
            }
            Ok(Reply::Refused(Refusal::CohortSealed)) | Err(_) => {
                match self
                    .journal_query(ManagementRequest::RetirementCheckpoint { placement })
                    .await?
                {
                    ManagementReply::RetirementCheckpoint(checkpoint) => checkpoint,
                    _ => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        let request = ManagementRequest::RetireGroup {
            placement,
            final_checkpoint: checkpoint,
        };
        let mut retired = 0;
        let mut deferred = 0;
        for voter in &self.voters {
            match self.daemon.call(voter, &request).await {
                Ok(ManagementReply::Retired(exact)) if exact == checkpoint => retired += 1,
                Ok(ManagementReply::Deferred) => deferred += 1,
                _ => {}
            }
        }
        if retired > self.voters.len() / 2 {
            Ok(Some(placement.generation))
        } else if deferred > self.voters.len() / 2 {
            Ok(None)
        } else {
            Err(unavailable(
                "A matching majority did not persist group retirement",
            ))
        }
    }

    pub async fn retired(
        &self,
        deployment: Uuid,
        name: &str,
        organization: &str,
    ) -> Result<Option<u64>> {
        match self
            .majority(ManagementRequest::RetirementStatus { deployment })
            .await?
        {
            ManagementReply::RetirementStatus { route, .. }
                if route.route.placement.deployment == deployment
                    && route.binding.name == name
                    && route.binding.organization == organization =>
            {
                Ok(Some(route.route.placement.generation))
            }
            ManagementReply::RetirementStatus { .. } => Err(conflict()),
            ManagementReply::Absent => Ok(None),
            reply => Err(refused(reply)),
        }
    }
}

async fn execute(
    client: &Client,
    command: Command,
) -> Result<std::result::Result<Outcome, Refusal>> {
    let Reply::Route(route) = client.call(Operation::Route).await.map_err(journal_error)? else {
        return Err(unavailable(
            "The installed journal is not accepting administrative commands",
        ));
    };
    let id = RequestId::fresh(route.position, route.retry_positions)
        .map_err(|_| unavailable("The installed journal could not issue an invocation"))?;
    let principal = Principal::Administrator;
    let request = Request {
        placement: route.placement,
        principal,
        id,
        command,
    };
    match client
        .call(Operation::Execute(request))
        .await
        .map_err(journal_error)?
    {
        Reply::Decision(decision)
            if decision.receipt.request == id
                && decision.receipt.principal == principal
                && decision.receipt.placement == route.placement =>
        {
            Ok(decision.receipt.outcome)
        }
        Reply::Refused(refusal) => Ok(Err(refusal)),
        _ => Err(unavailable(
            "The installed journal did not acknowledge the administrative command",
        )),
    }
}

pub(crate) fn deployment_record(
    name: &str,
    organization: &str,
    deployment: Uuid,
) -> DeploymentRecord {
    let mut record = DeploymentRecord::new(name, organization, Vec::new(), now_ms());
    record.id = deployment.to_string();
    record.lineage = record.id.clone();
    record.linearizable_reads = true;
    record
}

fn installed_route(
    route: ApplicationRoute,
    authority: Authority,
    credentials: Credentials,
) -> InstalledRoute {
    InstalledRoute {
        incarnation: 1,
        log_epoch: 0,
        authority,
        route,
        credentials,
    }
}

fn derive(deployment: Uuid, purpose: &[u8]) -> Fingerprint {
    use ring::digest::{Context, SHA256};
    let mut context = Context::new(&SHA256);
    context.update(b"rtdb.multi/runtime-install/v1/");
    context.update(purpose);
    context.update(deployment.as_bytes());
    context.finish().as_ref().try_into().expect("SHA-256 width")
}

fn derived_uuid(deployment: Uuid, purpose: &[u8]) -> Uuid {
    let mut bytes: [u8; 16] = derive(deployment, purpose)[..16].try_into().unwrap();
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/installed-cell", message.to_owned())
}
fn unavailable(message: &str) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/installed-cell",
        message.to_owned(),
    )
}
fn conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.multi/installed-cell",
        "The installed cell has a different deployment binding",
    )
}
fn journal_error(error: impl std::fmt::Display) -> Anomaly {
    unavailable(&error.to_string())
}
fn refused(reply: ManagementReply) -> Anomaly {
    match reply {
        ManagementReply::Conflict => conflict(),
        ManagementReply::Forbidden => Anomaly::new(
            AnomalyCategory::Forbidden,
            "rtdb.multi/installed-cell",
            "The credential cannot make this installed-cell request",
        ),
        _ => unavailable("The installed cell refused the requested transition"),
    }
}
