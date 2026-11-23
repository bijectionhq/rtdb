#![feature(proc_macro_hygiene)]

mod laws;
#[path = "../../../rust/crates/replication/src/promotion.rs"]
mod promotion;
#[path = "../../../rust/crates/types/src/term.rs"]
mod term_types;

pub struct Anomaly;
pub type Result<T> = std::result::Result<T, Anomaly>;

impl Anomaly {
    pub fn fault(_code: &str, _message: &str) -> Self { Self }
}

#[path = "../../../rust/crates/replication/src/admission_step.rs"]
mod production;

fn main() {}
