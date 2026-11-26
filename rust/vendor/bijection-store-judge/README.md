# Vendored Bijection `fact_store::persistence::judge`

`src/judge.rs` is a verbatim copy of Bijection's
`crates/fact_store/src/persistence/judge.rs` and is this crate's root. Its
SHA-256 (`5010d6fb02ae6aaff5ae37417ed816d2cc09b0f3dd5973ab127754e3b6ea92e3`) is
pinned in both repositories, so RTDB's `rtdb.store.v1.Append` resolver
(`TransactState::resolve_store`) and Bijection's reference store decide ties,
regression and the window through the same bytes (T7, V9 and V9′).

The file calls one item of Bijection's `store_read` crate outside its
`cfg(verus_keep_ghost)` annotations, `order::le_bytes` (`[u8]`'s `<=`), which
`store-read-order/` provides under the crate name `store_read`.

To update it, copy the Bijection file over `src/judge.rs` byte for byte and
change the SHA-256 pin in both repositories in paired commits.
