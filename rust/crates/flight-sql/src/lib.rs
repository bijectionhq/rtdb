mod auth;
mod audience_seat;
mod config;
mod error;
mod execute;
mod actions;
mod upload;
mod lease;
mod metadata;
mod protocol;
mod server;
mod service;
mod state;
mod ticket;
mod wire;
#[cfg(feature = "multi")]
mod follower;
#[cfg(feature = "multi")]
pub use follower::NativeFollowerAssignment;

pub use config::{AUDIENCE_FLIGHT_HEADER, AUDIENCE_SEAT_HEADER, FORWARDED_PEER_HEADER, AudienceFlightConfig, AudienceSeat, ClassicFlightExclusion, ClassicRetainedAdmission, ClassicRetainedRead, FlightConfig, FlightPeerVerifier, HostedControlConfig};
pub use config::{AUDIENCE_GATEWAY_TRANSPORT_BYTES, AUDIENCE_H2_CONNECTION_WINDOW, AUDIENCE_H2_STREAM_WINDOW, AUDIENCE_H2_STREAMS};
pub use actions::classic_retained_action;
pub use server::{FlightEgress, FlightEgressIo, ServerDriver, ServerHandle, ServerStatus};

mod owned_actions;
mod owned_landing_wire;

mod owned_world;
mod owned_family;
pub use owned_family::{FamilyReleaseInputWire, FamilyReleaseRegistrationWire, FamilyReleaseRequest, FamilyDerivedRow, FamilyFenceReply, FamilyOccurrenceWire, FamilyReply, FamilyRequest, FamilyReservationWire, FamilyStage, RetiredFamilyRequest, RetiredFamilyOperation, RetiredWorldRankRequest, WorldRowAuthoring};
pub use owned_world::{FrozenWorldWinner, OwnedWorldHost, OwnedWorldRead, PreparedReplayParent, PreparedWorldOverlay, RetainedReplayWorld, WorldCall, WorldIdentityPage, WorldParentVersion, WorldParentVersions, WorldPointResult, WorldPointRow};

/// Private Flight gateway request marker and response cutoff.
pub const GATEWAY_ANSWER_DEADLINE_HEADER: &str = "x-rtdb-answer-deadline";

mod v1;
#[cfg(feature = "multi")]
mod admin;
#[cfg(feature = "multi")]
pub use admin::{DeploymentAdminAction, DeploymentAdminAuthority};
#[cfg(feature = "multi")]
pub mod admin_client;
pub use v1::custody::{CustodyProviderRoute, OriginalFamilyReadbackLayout};
pub use v1::{OWNED_HOSTED_FEATURE, RETIRED_ORIGINAL_FAMILY_FEATURE, RETIRED_ORIGINAL_FAMILY_RECOVERY_FEATURE};

pub mod rtdb {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/rtdb.v1.rs"));
    }
    pub mod store {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/rtdb.store.v1.rs"));
        }
    }
    pub mod admin {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/rtdb.admin.v1.rs"));
        }
    }
}
