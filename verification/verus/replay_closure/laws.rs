use crate::{inputs::*, production::*, prost_replay::*};
use vstd::prelude::*;

verus! {

pub fn checkpoint_enumeration_matches_base_keys(
    catalog: &BlockCatalog, tables: &Tables, key: u64,
) -> (out: Result<BTreeSet>)
    ensures out matches Ok(actual) ==> (actual@.contains(key) <==> baseKeys(catalog, tables).contains(key)),
{
    checkpoint_dependencies(catalog, tables)
}

pub fn tail_enumeration_matches_record_keys(
    base: ReplayBase, checkpoint: Option<&BlockCatalog>, suffix: Vec<&ReplicaEnvelope>,
    payloads: &PayloadSource, catalogs: &CatalogSource, key: u64,
) -> (out: Result<BTreeSet>)
    requires base.installed == match checkpoint { Some(catalog) => Some(catalog.block), None => None },
    ensures out matches Ok(actual) ==> (actual@.contains(key) <==>
        tailKeys(base, checkpoint, suffix@, payloads, catalogs).contains(key)),
{
    tail_dependencies(base, checkpoint, suffix, payloads, catalogs)
}

pub fn adopted_catalog_contributes_its_entire_base(
    catalog: &BlockCatalog, tables: &Tables, keys: &mut BTreeSet, index: i64, key: u64,
) -> (out: Result<()>)
    ensures out is Ok ==> (final(keys)@.contains(key) <==> (old(keys)@.contains(key) || baseKeys(catalog, tables).contains(key))),
{
    adopt_catalog(catalog, tables, keys, index)?;
    assert(keys@.contains(rootKey(index)));
    Ok(())
}

pub fn typed_body_contributes_exactly_its_files(
    base: ReplayBase, body: &ReplicaBody, slugs: &Slugs, keys: &mut BTreeSet, key: u64,
) -> (out: Result<()>)
    ensures out is Ok ==> (final(keys)@.contains(key) <==> (old(keys)@.contains(key) || bodyFileKeys(base, body, slugs.entries@).contains(key))),
{
    record_dependency_keys(base, body, slugs, keys)
}

pub fn oversized_record_contributes_the_payload_and_the_decoded_body<'a>(
    envelope: &'a ReplicaEnvelope, base: ReplayBase, keys: &mut BTreeSet, bodies: &mut Vec<Cow<'a>>,
    source: &PayloadSource, slugs: &Slugs, key: u64,
) -> (out: Result<()>)
    ensures out is Ok ==> (final(keys)@.contains(key) <==> (old(keys)@.contains(key)
        || payloadKeys(envelope).contains(key) || bodyFileKeys(base, &decodedBody(envelope, source), slugs.entries@).contains(key))),
{
    resolve_record(envelope, base, keys, bodies, source)?;
    let body = match &bodies[bodies.len() - 1] {
        Cow::Borrowed(body) => *body,
        Cow::Owned(body) => body,
    };
    record_dependency_keys(base, body, slugs, keys)
}

}
