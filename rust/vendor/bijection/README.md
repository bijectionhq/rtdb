# Bijection's store read core

Licensed under the Functional Source License 1.1 with the Apache 2.0 future
license. See [LICENSE.md](LICENSE.md).

`VENDOR.json` records the Bijection base for `store_read`, `kernel_text` and
`geometry` and the RTDB commits that backport changes to that base.
The crates sit side by side so `store_read/build.rs` computes the digest over
the vendored sources. Their manifests use explicit versions for dependencies
that Bijection's manifests inherit from the workspace.

`rtdb.store.v1.Rank` runs `store_read::rank` over the store's ordered
entries. RTDB adds no ranking semantics. A request for other ranking code is
refused (`rtdb.store/rank-version`), and the server refuses to start when the
compiled digest differs from `RANK_DIGEST` here.

Never edit these files here. Change them in Bijection, copy `src/` (and
`store_read/build.rs`, `store_read/pins.txt`) over, and update `VENDOR.json`
and `RANK_DIGEST` in the paired commit.
