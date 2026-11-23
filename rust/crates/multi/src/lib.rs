//! RTDB multi: many deployments on one shared, autoscaling cell (`allium/rtdb-multi.allium`).
//!
//! A deployment is one RTDB database, held by one node under a directory-allocated epoch, with its logs on a logkeeper ensemble.
//! This crate wires the directory, the logkeepers and the engine together: node agents claim and open deployments, gateways route clients to their holders.

pub mod admin;
pub mod agent;
pub mod archive;
pub mod capability;
mod catalog_readers;
pub mod cells;
mod checkpoint_copy_budget;
mod checkpoint_retention;
pub mod cipher;
pub mod control;
pub mod deployment;
pub mod flight_gateway;
pub mod fragments;
pub mod gateway;
pub mod install;
pub mod keys;
pub mod lease;
pub mod load;
pub mod logs;
pub mod journal;
mod ordered_index_storage;
pub mod packing;
pub mod read_width;
mod replay_deadline;
mod member_probe;
mod replay_admission;
pub mod recovery;
mod recovery_holds;
pub mod reference;
pub mod process;
pub mod relay;
mod replica_retention;
mod hosted_recovery;
pub mod session;
pub mod store;
