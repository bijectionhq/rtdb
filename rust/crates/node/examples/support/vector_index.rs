use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap};
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array, builder::FixedSizeBinaryBuilder};
use arrow_buffer::Buffer;
use arrow_ipc::{reader::StreamDecoder, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use rtdb_storage::{LocalStore, ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_types::{EntityId, Iid};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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
    pub(super) vectors: Float32Array,
    pub(super) queries: Float32Array,
    pub(super) centroids: Float32Array,
    pub(super) clusters: UInt32Array,
    pub(super) dimensions: usize,
}

fn read_batch(mut stream: Buffer) -> RecordBatch {
    let mut decoder = StreamDecoder::new().with_require_alignment(true);
    let batch = decoder.decode(&mut stream).unwrap().expect("embedding record batch");
    assert!(decoder.decode(&mut stream).unwrap().is_none());
    decoder.finish().unwrap();
    batch
}

impl Embeddings {
    pub(super) fn read(path: &Path) -> Self {
        let open = |name| read_batch(Buffer::from_vec(std::fs::read(path.join(name)).unwrap()));
        let read = |batch: &RecordBatch| {
            let vectors = batch.column_by_name("embedding").unwrap().as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let components = vectors.values().as_any().downcast_ref::<Float32Array>().unwrap();
            assert!(vectors.value_length() > 0 && vectors.len() > 0 && vectors.null_count() == 0 && components.null_count() == 0);
            assert!(components.values().iter().all(|value| value.is_finite()));
            (vectors.value_length() as usize, components.clone())
        };
        let input = open("vectors.arrow");
        let (dimensions, vectors) = read(&input);
        let (query_width, queries) = read(&open("queries.arrow"));
        let (centroid_width, centroids) = read(&open("centroids.arrow"));
        assert_eq!(query_width, dimensions);
        assert_eq!(centroid_width, dimensions);
        let clusters = input.column_by_name("cluster").unwrap().as_any().downcast_ref::<UInt32Array>().unwrap().clone();
        assert_eq!(clusters.null_count(), 0);
        assert!(clusters.values().iter().all(|id| (*id as usize) < centroids.len() / dimensions));
        Self { vectors, queries, centroids, clusters, dimensions }
    }

    pub(super) fn vector(&self, id: usize) -> &[f32] {
        &self.vectors.values()[id * self.dimensions..(id + 1) * self.dimensions]
    }
}

pub(super) struct VectorIndex {
    pub(super) store: Arc<LocalStore>,
    pub(super) centroids: Float32Array,
    pub(super) bytes: u64,
    dimensions: usize,
    ranges: Vec<Range<u64>>,
    object: ObjectKey,
}

pub(super) struct Candidates {
    pub(super) rows: Vec<(i64, Iid)>,
    pub(super) requested_ranges: usize,
    pub(super) requested_bytes: u64,
    pub(super) object_reads: usize,
    pub(super) object_bytes: u64,
    pub(super) scored: usize,
}

impl VectorIndex {
    pub(super) async fn build(data: &Embeddings, scratch: &Path) -> Self {
        let mut members = vec![Vec::new(); data.centroids.len() / data.dimensions];
        for (id, &cluster) in data.clusters.values().iter().enumerate() {
            members[cluster as usize].push(id);
        }
        let stage = scratch.join("vectors.bin");
        let mut out = BufWriter::new(File::create(&stage).unwrap());
        let item = Arc::new(Field::new("item", DataType::Float32, false));
        let width = i32::try_from(data.dimensions).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("_iid", DataType::FixedSizeBinary(16), false),
            Field::new("embedding", DataType::FixedSizeList(Arc::clone(&item), width), false),
        ]));
        let mut ranges = Vec::with_capacity(members.len());
        let mut offset = 0u64;
        for members in members {
            if members.is_empty() {
                ranges.push(offset..offset);
                continue;
            }
            let padding = ((64 - offset % 64) % 64) as usize;
            out.write_all(&[0; 64][..padding]).unwrap();
            let start = offset + padding as u64;
            let ids = UInt32Array::from_iter_values(members.iter().map(|id| u32::try_from(*id).unwrap()));
            let mut iids = FixedSizeBinaryBuilder::with_capacity(members.len(), 16);
            for id in &members {
                iids.append_value(EntityId::Integer(*id as i64).iid().0).unwrap();
            }
            let child = Float32Array::from_iter_values(members.iter().flat_map(|id| data.vector(*id).iter().copied()));
            let vectors = FixedSizeListArray::try_new(Arc::clone(&item), width, Arc::new(child), None).unwrap();
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(iids.finish()), Arc::new(vectors)]).unwrap();
            let mut writer = StreamWriter::try_new(&mut out, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            drop(writer);
            offset = out.stream_position().unwrap();
            ranges.push(start..offset);
        }
        out.flush().unwrap();
        drop(out);
        let store = Arc::new(LocalStore::open(scratch.join("objects")).unwrap());
        let object = ObjectKey::new("ann/vectors.bin").unwrap();
        store.put_file(&object, &stage).await.unwrap();
        std::fs::remove_file(stage).unwrap();
        Self { store, centroids: data.centroids.clone(), bytes: offset, dimensions: data.dimensions, ranges, object }
    }

    pub(super) async fn warm_cache(&self) -> BufferPool {
        let cache = BufferPool::new(self.store.clone(), 512 << 20);
        cache.get(&self.object).await.unwrap();
        cache
    }

    pub(super) async fn candidates(
        &self,
        cache: &BufferPool,
        query: &[f32],
        filter: Filter,
        probes: usize,
        limit: usize,
        changed: &BTreeSet<Iid>,
    ) -> Candidates {
        let mut groups: Vec<_> = self.centroids.values().chunks_exact(self.dimensions).enumerate()
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
            let batch = read_batch(Buffer::from(Bytes::from_owner(bytes)));
            let ids = batch.column(0).as_any().downcast_ref::<UInt32Array>().unwrap();
            let iids = batch.column(1).as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            let list = batch.column(2).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let vectors = list.values().as_any().downcast_ref::<Float32Array>().unwrap();
            for (row, (&id, vector)) in ids.values().iter().zip(vectors.values().chunks_exact(self.dimensions)).enumerate() {
                let id = id as usize;
                let (random, group) = self::groups(id, cluster);
                if !filter.accepts(random, group) { continue; }
                let iid = Iid::from_slice(iids.value(row)).unwrap();
                if changed.contains(&iid) { continue; }
                let (mut dot, mut norm) = (0.0, 0.0);
                for (&value, query) in vector.iter().zip(query) {
                    dot += value * query;
                    norm += value * value;
                }
                let candidate = Neighbor(-dot / norm.sqrt(), id as i64, iid);
                scored += 1;
                if held.len() < limit {
                    held.push(candidate);
                } else if held.peek().is_some_and(|worst| candidate < *worst) {
                    held.pop();
                    held.push(candidate);
                }
            }
        }
        Candidates { rows: held.into_iter().map(|candidate| (candidate.1, candidate.2)).collect(), requested_ranges: selected.len(), requested_bytes,
            object_reads: if cached { 0 } else { selected.len() },
            object_bytes: if cached { 0 } else { requested_bytes },
            scored }
    }
}

struct Neighbor(f32, i64, Iid);
impl PartialEq for Neighbor { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for Neighbor {}
impl PartialOrd for Neighbor { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl Ord for Neighbor { fn cmp(&self, other: &Self) -> Ordering { self.0.total_cmp(&other.0).then_with(|| self.1.cmp(&other.1)) } }
