# Bijection's store read core

Licensed under the Functional Source License 1.1 with the Apache 2.0 future
license. See [LICENSE.md](LICENSE.md).

`store_read`, `kernel_text` and `geometry` are verbatim copies of Bijection's
crates of the same names at the commit `VENDOR.json` records, kept side by
side as in Bijection's `crates/` so `store_read/build.rs` computes the same
`RANK_DIGEST` from the same files. Only the `Cargo.toml` files are RTDB's
(explicit versions instead of Bijection's workspace dependencies).

`rtdb.store.v1.Rank` runs `store_read::rank` over the store's ordered
entries. RTDB adds no ranking semantics. A request for other ranking code is
refused (`rtdb.store/rank-version`), and the server refuses to start when the
compiled digest differs from `RANK_DIGEST` here.

Never edit these files here. Change them in Bijection, copy `src/` (and
`store_read/build.rs`, `store_read/pins.txt`) over, and update `VENDOR.json`
and `RANK_DIGEST` in the paired commit.
