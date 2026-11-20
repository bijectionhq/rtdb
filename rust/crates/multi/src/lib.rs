//! RTDB multi: many deployments on one shared, autoscaling cell (`allium/rtdb-multi.allium`).
//!
//! A deployment is one RTDB database, held by one node under a directory-allocated epoch, with its logs on a logkeeper ensemble.
//! This crate wires the directory, the logkeepers and the engine together: node agents claim and open deployments, gateways route clients to their holders.

pub mod admin;
pub mod agent;
pub mod cipher;
pub mod control;
pub mod deployment;
pub mod gateway;
pub mod keys;
pub mod lease;
pub mod logs;
pub mod process;
pub mod session;
pub mod store;
