use super::*;
use rtdb_types::{MemoryReservation, Result, java_hash_map::Admission};
use super::snapshot::ClusterProfile;

/// One already resolved source InetSocketAddress endpoint in the represented UTF8 hostname domain.
/// `host` is its getHostString value; this input performs no resolution or hostname normalization.
#[derive(Clone, Copy)]
pub struct ResolvedEndpoint<'a> { pub host: &'a str, pub port: u16 }
struct Endpoint { host: Text, port: u16 }
/// Immutable admitted resolved addresses; clones retain their original endpoint owners and budget domain.
#[derive(Clone)]
pub struct BootstrapAddresses { endpoints: Shared<Vector<Endpoint>>, domain: Arc<MemoryBudget> }

/// Synchronous byte admission; providers retain each returned lease through its corresponding state/scratch allocation's final release.
/// The borrowed authority itself must not be retained.
pub type ShuffleAdmission<'a> = dyn FnMut(usize) -> Result<Arc<MemoryReservation>> + 'a;
/// Qualified synchronous observations of Collections.shuffle's default Random operations.
/// Retained generator state remains caller-owned; its native allocations and scratch require admission.
/// A missing or exhausted profile returns its original error; no native random substitute is selected.
pub trait JavaShuffleObservations {
    /// Acquires or initializes the source default Random, even for an empty or singleton list.
    fn begin(&mut self, admit: &mut ShuffleAdmission<'_>) -> Result<()>;
    /// Observes the complete source Random.nextInt(bound), including its internal rejection draws.
    /// Returns an integer in [0, bound); the source shuffle requests decreasing signed bounds through two.
    fn next_int(&mut self, bound: i32, admit: &mut ShuffleAdmission<'_>) -> Result<i32>;
}
/// Rejects Cluster shuffle construction without a qualified source random profile.
pub struct MissingShuffleObservations;
impl JavaShuffleObservations for MissingShuffleObservations {
    fn begin(&mut self, _: &mut ShuffleAdmission<'_>) -> Result<()> { Err(missing_shuffle()) }
    fn next_int(&mut self, _: i32, _: &mut ShuffleAdmission<'_>) -> Result<i32> { Err(missing_shuffle()) }
}
fn missing_shuffle() -> Anomaly { Anomaly::unsupported("rtdb/kafka-java-shuffle-required", "A qualified Java Collections.shuffle observation is required") }

pub(super) fn shuffle_nodes(nodes: &mut Vector<Shared<Node>>, budget: &mut Budget, shuffle: &mut dyn JavaShuffleObservations) -> Outcome<()> {
    let mut admit = |bytes| budget.reserve(bytes);
    shuffle.begin(&mut admit)?;
    for bound in (2..=nodes.len()).rev() {
        let bound = i32::try_from(bound).map_err(|_| owned::overflow())?;
        let selected = shuffle.next_int(bound, &mut admit)?;
        if !(0..bound).contains(&selected) {
            return Err(Anomaly::unsupported("rtdb/kafka-java-shuffle-range", "Java shuffle observation is outside its requested bound").into());
        }
        nodes.as_mut_slice().swap(bound as usize - 1, selected as usize);
    }
    Ok(())
}

impl BootstrapAddresses {
    pub fn len(&self) -> usize { self.endpoints.len() }
    pub fn is_empty(&self) -> bool { self.endpoints.len() == 0 }
    /// Borrows the retained resolved endpoint sequence in its original source order.
    pub fn endpoints(&self) -> impl ExactSizeIterator<Item = ResolvedEndpoint<'_>> {
        self.endpoints.as_slice().iter().map(|endpoint| ResolvedEndpoint { host: endpoint.host.as_str(), port: endpoint.port })
    }
    /// Compares the retained resolved-list owner independently of endpoint equality.
    pub fn same_object(&self, other: &Self) -> bool { self.endpoints.same_object(&other.endpoints) }
}

impl ProducerMetadata {
    /// Prepares native ownership for an already resolved source address list before calling bootstrap.
    /// Hostnames must equal source getHostString values within the represented UTF8 domain; ports use the source address domain.
    /// The endpoint count must fit `i32::MAX`.
    /// Admission precedes endpoint copies and their shared owner, with the producer's native capability failure policy.
    pub fn prepare_bootstrap_addresses(&mut self, source: &[ResolvedEndpoint<'_>]) -> Outcome<BootstrapAddresses> {
        self.usable()?;
        let result = (|| {
            if source.len() > i32::MAX as usize { return Err(Anomaly::unsupported("rtdb/kafka-bootstrap-list-size", "Bootstrap address lists require a signed int length").into()); }
            let mut endpoints = Vector::with_capacity(source.len(), &mut self.budget)?;
            for endpoint in source { endpoints.push(Endpoint { host: Text::from_raw(endpoint.host.as_bytes(), &mut self.budget)?, port: endpoint.port }, &mut self.budget)?; }
            Ok(BootstrapAddresses { endpoints: Shared::new(endpoints, &mut self.budget)?, domain: Arc::clone(&self.budget.0) })
        })();
        self.completed(result)
    }

    /// Publishes a bootstrap generation and then retains its already prepared source addresses.
    /// The full-update flag and wrapping update version change before snapshot construction, including construction failures.
    /// Addresses must have been prepared with the same shared `MemoryBudget` instance.
    /// Shuffle and source identity authorities are borrowed only for this call.
    /// Existing topic, epoch, counter, error and backoff state is preserved apart from that source refresh prefix.
    pub fn bootstrap(&mut self, addresses: BootstrapAddresses, shuffle: &mut impl JavaShuffleObservations, identity: &mut impl IdentityWitness) -> Outcome<()> {
        self.usable()?;
        self.refresh.bootstrap();
        let result = (|| {
            if !Arc::ptr_eq(&addresses.domain, &self.budget.0) { return Err(Anomaly::unsupported("rtdb/kafka-bootstrap-budget-domain", "Bootstrap addresses belong to another admission domain").into()); }
            let snapshot = Snapshot::bootstrap(&addresses, &mut self.budget, shuffle, identity)?;
            self.snapshot = snapshot;
            self.bootstrap_addresses = Some(addresses);
            Ok(())
        })();
        self.completed(result)
    }

    /// Regenerates bootstrap metadata from the retained resolved list without resolving it again.
    /// Missing addresses fail after the source refresh prefix; previously published snapshots remain readable.
    pub fn rebootstrap(&mut self, shuffle: &mut impl JavaShuffleObservations, identity: &mut impl IdentityWitness) -> Outcome<()> {
        self.usable()?;
        let Some(addresses) = self.bootstrap_addresses.clone() else { self.refresh.bootstrap(); return Err(Failure::MissingBootstrapAddresses); };
        self.bootstrap(addresses, shuffle, identity)
    }

    /// Retains the last successfully installed resolved-list owner independently of the current snapshot.
    pub fn bootstrap_addresses(&self) -> Option<BootstrapAddresses> { self.bootstrap_addresses.clone() }
}

impl Snapshot {
    fn bootstrap(addresses: &BootstrapAddresses, budget: &mut Budget, shuffle: &mut impl JavaShuffleObservations, identity: &mut impl IdentityWitness) -> Outcome<Self> {
        let mut nodes = Map::new(None)?;
        let mut id = -1i32;
        for endpoint in addresses.endpoints.as_slice() {
            let node = Shared::new(Node { id, host: endpoint.host.clone(), port: i32::from(endpoint.port), rack: None }, budget)?;
            nodes.put(id, node, &mut Ops(identity), budget)?;
            id = id.wrapping_sub(1);
        }
        let nodes = Shared::new(nodes.finish(budget)?, budget)?;
        let mut cluster_nodes = Vector::with_capacity(addresses.len(), budget)?;
        let mut id = -1i32;
        for endpoint in addresses.endpoints.as_slice() {
            cluster_nodes.push(Shared::new(Node { id, host: endpoint.host.clone(), port: i32::from(endpoint.port), rack: None }, budget)?, budget)?;
            id = id.wrapping_sub(1);
        }
        shuffle_nodes(&mut cluster_nodes, budget, shuffle)?;
        let input = Input { nodes, cluster_id: None, controller: -1, partitions: Vector::new(), sets: Sets::new()?, topic_ids: Map::new(None)? };
        Self::build_with_cluster(input, budget, identity, |_, _, _| Ok(ClusterProfile::Bootstrap { nodes: cluster_nodes }))
    }
}
