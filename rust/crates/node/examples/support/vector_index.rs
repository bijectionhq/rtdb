use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use rtdb_storage::{LocalStore, ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_types::{Iid, Value};

#[derive(Clone, Copy, Debug)]
pub(super) enum Filter {
    All,
    RandomBelow(u32),
    ClusterBelow(u32),
}

impl Filter {
    pub(super) fn accepts(self, random: u32, cluster: u32) -> bool {
        match self {
            Self::All => true,
            Self::RandomBelow(ceiling) => random < ceiling,
            Self::ClusterBelow(ceiling) => cluster < ceiling,
        }
    }

    pub(super) fn sql(self) -> String {
        match self {
            Self::All => "TRUE".into(),
            Self::RandomBelow(ceiling) => format!("random_group < {ceiling}"),
            Self::ClusterBelow(ceiling) => format!("cluster_group < {ceiling}"),
        }
    }
}

pub(super) fn groups(id: usize, cluster: usize) -> (u32, u32) {
    let mut hash = (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    hash ^= hash >> 32;
    ((hash % 1000) as u32, cluster_group(cluster))
}

fn cluster_group(cluster: usize) -> u32 {
    ((cluster as u64).wrapping_mul(0x7FEB_352D) % 1000) as u32
}

pub(super) struct Embeddings {
    pub(super) vectors: Vec<f32>,
    pub(super) queries: Vec<f32>,
    pub(super) centroids: Vec<f32>,
    pub(super) clusters: Vec<usize>,
    pub(super) dimensions: usize,
}

impl Embeddings {
    pub(super) fn read(path: &Path) -> Self {
        let f32_file = |name| {
            let bytes = std::fs::read(path.join(name)).unwrap();
            assert_eq!(bytes.len() % 4, 0);
            bytes.chunks_exact(4).map(|value| f32::from_le_bytes(value.try_into().unwrap())).collect::<Vec<_>>()
        };
        let vectors = f32_file("vectors.f32");
        let queries = f32_file("queries.f32");
        let centroids = f32_file("centroids.f32");
        let instances = std::fs::read(path.join("clusters.u32")).unwrap();
        let dimensions: usize = std::fs::read_to_string(path.join("dimensions")).unwrap().trim().parse().unwrap();
        assert!(dimensions > 0);
        assert_eq!(instances.len(), vectors.len() / dimensions * 4);
        assert!([vectors.len(), queries.len(), centroids.len()].into_iter().all(|len| len > 0 && len % dimensions == 0));
        let clusters: Vec<_> = instances.chunks_exact(4).map(|value| u32::from_le_bytes(value.try_into().unwrap()) as usize).collect();
        assert!(clusters.iter().all(|id| *id < centroids.len() / dimensions));
        assert!(vectors.iter().chain(&queries).chain(&centroids).all(|value| value.is_finite()));
        Self { vectors, queries, centroids, clusters, dimensions }
    }

    pub(super) fn vector(&self, id: usize) -> &[f32] {
        &self.vectors[id * self.dimensions..(id + 1) * self.dimensions]
    }
}

pub(super) struct VectorIndex {
    pub(super) store: Arc<LocalStore>,
    pub(super) centroids: Vec<f32>,
    pub(super) bytes: u64,
    dimensions: usize,
    ranges: Vec<Range<u64>>,
    ids: Vec<(Iid, i64)>,
    object: ObjectKey,
}

pub(super) struct Candidates {
    pub(super) ids: Vec<i64>,
    pub(super) requested_ranges: usize,
    pub(super) requested_bytes: u64,
    pub(super) object_reads: usize,
    pub(super) object_bytes: u64,
    pub(super) scored: usize,
}

impl VectorIndex {
    pub(super) async fn build(data: &Embeddings, scratch: &Path) -> Self {
        let mut members = vec![Vec::new(); data.centroids.len() / data.dimensions];
        for (id, &cluster) in data.clusters.iter().enumerate() {
            members[cluster].push(id);
        }
        let stage = scratch.join("vectors.bin");
        let mut out = BufWriter::new(File::create(&stage).unwrap());
        let mut ranges = Vec::with_capacity(members.len());
        let mut offset = 0u64;
        let stride = (4 + data.dimensions * 4) as u64;
        for members in members {
            let end = offset + stride * members.len() as u64;
            ranges.push(offset..end);
            offset = end;
            for id in members {
                out.write_all(&u32::try_from(id).unwrap().to_le_bytes()).unwrap();
                for value in data.vector(id) {
                    out.write_all(&value.to_le_bytes()).unwrap();
                }
            }
        }
        out.flush().unwrap();
        drop(out);
        let store = Arc::new(LocalStore::open(scratch.join("objects")).unwrap());
        let object = ObjectKey::new("ann/vectors.bin").unwrap();
        store.put_file(&object, &stage).await.unwrap();
        std::fs::remove_file(stage).unwrap();
        let mut ids: Vec<_> = (0..data.clusters.len() as i64)
            .map(|id| (Value::I64(id).entity_id().unwrap().iid(), id))
            .collect();
        ids.sort_unstable_by_key(|entry| entry.0);
        Self { store, centroids: data.centroids.clone(), bytes: offset, dimensions: data.dimensions, ranges, ids, object }
    }

    pub(super) async fn warm_cache(&self) -> BufferPool {
        let cache = BufferPool::new(self.store.clone(), 512 << 20);
        cache.get(&self.object).await.unwrap();
        cache
    }

    pub(super) fn indexed_id(&self, iid: Iid) -> Option<i64> {
        let slot = self.ids.binary_search_by_key(&iid, |entry| entry.0).ok()?;
        Some(self.ids[slot].1)
    }

    pub(super) fn lookup_bytes(&self) -> usize {
        self.ids.capacity() * std::mem::size_of::<(Iid, i64)>()
    }

    pub(super) async fn candidates(
        &self,
        cache: &BufferPool,
        query: &[f32],
        filter: Filter,
        probes: usize,
        limit: usize,
        changed: &BTreeSet<i64>,
    ) -> Candidates {
        let mut groups: Vec<_> = self.centroids.chunks_exact(self.dimensions).enumerate()
            .filter(|(id, _)| !self.ranges[*id].is_empty())
            .filter(|(id, _)| !matches!(filter, Filter::ClusterBelow(end) if cluster_group(*id) >= end))
            .map(|(id, centroid)| (id, -query.iter().zip(centroid).map(|(left, right)| *left * right).sum::<f32>()))
            .collect();
        groups.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let selected = &groups[..probes.min(groups.len())];
        let requested_bytes = selected.iter().map(|(id, _)| self.ranges[*id].end - self.ranges[*id].start).sum();
        let cached = cache.holds(&self.object);
        let mut held = BinaryHeap::new();
        let mut scored = 0;
        let mut reads = futures::stream::iter(selected.iter().map(|&(id, _)| async move {
            cache.get_range(&self.object, self.ranges[id].clone()).await.map(|bytes| (id, bytes))
        })).buffer_unordered(8);
        while let Some((cluster, bytes)) = reads.try_next().await.unwrap() {
            for row in bytes.as_slice().chunks_exact(4 + self.dimensions * 4) {
                let id = u32::from_le_bytes(row[..4].try_into().unwrap()) as usize;
                if !changed.is_empty() && changed.contains(&(id as i64)) { continue; }
                let (random, group) = self::groups(id, cluster);
                if !filter.accepts(random, group) { continue; }
                let (mut dot, mut norm) = (0.0, 0.0);
                for (bytes, query) in row[4..].chunks_exact(4).zip(query) {
                    let value = f32::from_le_bytes(bytes.try_into().unwrap());
                    dot += value * query;
                    norm += value * value;
                }
                let candidate = Neighbor(-dot / norm.sqrt(), id as i64);
                scored += 1;
                if held.len() < limit {
                    held.push(candidate);
                } else if held.peek().is_some_and(|worst| candidate < *worst) {
                    held.pop();
                    held.push(candidate);
                }
            }
        }
        Candidates { ids: held.into_iter().map(|candidate| candidate.1).collect(), requested_ranges: selected.len(), requested_bytes,
            object_reads: if cached { 0 } else { selected.len() },
            object_bytes: if cached { 0 } else { requested_bytes },
            scored }
    }
}

struct Neighbor(f32, i64);
impl PartialEq for Neighbor { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for Neighbor {}
impl PartialOrd for Neighbor { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl Ord for Neighbor { fn cmp(&self, other: &Self) -> Ordering { self.0.total_cmp(&other.0).then_with(|| self.1.cmp(&other.1)) } }
