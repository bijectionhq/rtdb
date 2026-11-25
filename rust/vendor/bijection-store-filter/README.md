# bijection-store-filter

Bijection's pushed-down value filter for `rtdb.store.v1.Read`.
`src/lib.rs` is a verbatim copy of Bijection's `crates/fact_store/src/persistence/filter.rs`.
Bijection defines what a filter means; RTDB only runs it next to the data.
Change it in Bijection and copy it here in the paired commit; never edit it here.
