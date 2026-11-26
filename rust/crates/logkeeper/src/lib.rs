//! The logkeeper: a log server holding many named streams, and a quorum client presenting one stream as an `rtdb_storage::log::Log`.
//!
//! A stream's writer holds an epoch.
//! It seals a write quorum of the stream's servers at that epoch, recovers the committed history onto them, and only then appends; a server accepts an append only from the writer it is synced to at its promised epoch, so a deposed writer cannot commit.
//! An entry is committed once a write quorum holds it durably; readers see committed entries only.

mod attestation;
mod client;
mod decision;
mod history;
mod protocol;
mod local;
mod server;

pub use decision::{DecisionCertificate, DecisionInstallation, DecisionPrefix, DecisionScope, DecisionStatement, DecisionVote, Handoff, HandoffCertificate, HandoffCheckpoint, StableDecision};
pub use client::{ActiveLogletReadIndex, CertifiedRangeReader, CopyFrom, JournalSyncObservation, Loglet, QuorumLog, RetainedAudit, StreamConfig, audit_retained, bootstrap_committed_page, certify_committed_prefix, certify_committed_range, certify_observed_entry, copy_certified_range, copy_retained, drop_stream, open_reader, open_reader_on, open_writer, open_writer_on, open_writer_on_checkpoint, original_sync_observations, retain_original_entry_digest, retire, seal, seal_recover_bounds, seal_recover_committed, timely_original_sync_upper, trim, trim_all, verify_certified_range};
pub use protocol::{Entry as ArchivedReplicaEntry, TimedJournalOrigin};
pub use local::LocalListener;
pub use rtdb_types::JournalEndpoint;
pub use server::{AuthorityListener, ConnectionLimits, JournalDigest, PreparedJournal, ServerOptions, inspect_retained, inspect_retained_with_prefix, serve};

/// Protocol fixtures and scripted stream journal faults for simulation.
#[cfg(feature = "simulation")]
pub mod simulation {
    pub use crate::client::recovery::{Holding, RepairPlan, plan_repair, winner};
    pub use crate::journal_faults::{Fault, FaultJournal};
    pub use crate::protocol::{Accepted, Entries, Entry, EpochRun, Failed, Refused, ResponseBody, Sealed};
    pub use crate::server::state::{Decision, Event, Held, StreamState};
}

#[cfg(feature = "simulation")]
mod journal_faults;
