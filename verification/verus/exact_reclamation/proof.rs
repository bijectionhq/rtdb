#![feature(proc_macro_hygiene)]

extern crate self as rtdb_format;

#[path = "../../../rust/crates/format/src/publication_identity.rs"]
mod publication_identity;
mod catalog {
    pub use crate::publication_identity::same_publication_digest;
}

#[path = "../../../rust/crates/multi/src/reclamation_decision.rs"]
mod production;
#[path = "../../../rust/crates/s3/src/versions/retirement_decision.rs"]
mod provider_decision;
mod laws;

fn main() {}
