use crate::certified_checkpoint::{matches_catalog, matches_table};
use vstd::prelude::*;

verus! {

pub fn certified_catalog_binds_selected_table(
    chosen_root: [u8; 32],
    loaded_root: Option<[u8; 32]>,
    has_manifest: bool,
    chosen_table: Option<[u8; 32]>,
    loaded_table: [u8; 32],
) -> (admitted: bool)
    ensures admitted <==> has_manifest
        && (exists|root: [u8; 32]| loaded_root == Some(root) && root@ == chosen_root@)
        && (exists|table: [u8; 32]| chosen_table == Some(table) && table@ == loaded_table@),
{
    matches_catalog(chosen_root, loaded_root, true, has_manifest)
        && matches_table(chosen_table, loaded_table)
}

}
