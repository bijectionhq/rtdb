# bijection-store-derive

Bijection's pure derived-key maintenance function for `rtdb.store.v1.Append` (T3).
`src/lib.rs` is a verbatim copy of Bijection's `crates/store_read/src/derive.rs`.
Bijection defines what a derived line means; RTDB only runs it at the ordering point.
Change it in Bijection and copy it here in the paired commit; never edit it here.
