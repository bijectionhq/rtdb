use crate::{CatalogCut, Hold, Instant, RequestId, catalog::catalog_matches_cut, production::*, query::{empty_catalog_matches_cut, query_snapshot}};
use vstd::prelude::*;

verus! {

fn read_covers_acknowledgement(hold: Hold, acknowledged: u64)
    requires acknowledged < hold.through.next_lsn,
{
    let cut = barrier_cut(hold);
    let offset = barrier_offset(cut.next_lsn);
    if let Some(offset) = offset {
        assert(acknowledged as int <= offset as int);
    }
}

fn exact_read_cut(hold: Hold) {
    let cut = barrier_cut(hold);
    let next_lsn = hold.through.next_lsn;
    let offset = barrier_offset(cut.next_lsn);
    assert(cut.history == hold.through.history && match offset {
        Some(offset) => offset as int + 1 == next_lsn as int,
        None => next_lsn as int - 1 > i64::MAX,
    });
}

fn uses_selected_cut(published: CatalogCut, requested: CatalogCut) {
    let captured = catalog_matches_cut(published, requested);
    assert(captured == (published == requested));
}

fn empty_cut_requires_an_untracked_catalog(catalog_cut: Option<u64>, tx_basis: Option<u64>) {
    let captured = empty_catalog_matches_cut(catalog_cut, tx_basis);
    assert(captured == (catalog_cut is None && tx_basis is None));
}

fn query_uses_read(captured: u64, current: u64) {
    let out = query_snapshot(Some(captured), || current);
    assert(out == captured);
}

fn fresh_barrier_matches_invocation(actual: RequestId, requested: RequestId) {
    let accepted = barrier_matches_invocation(actual, requested);
    assert(accepted == (actual == requested));
}

fn release_preserves_the_retry_horizon(position: u64, expires_at: u64) {
    let settled = read_release_settled(position, expires_at);
    assert(settled == (expires_at <= position));
}

fn read_lease_expires_at_its_deadline(now: Instant, deadline: Instant) {
    let admitted = read_lease_admits(now, deadline);
    assert(admitted == (now < deadline));
}

}
