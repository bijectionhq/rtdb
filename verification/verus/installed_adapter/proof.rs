#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/journal/adapter_command.rs"]
mod production;
#[path = "../../../rust/crates/multi/src/admin/holder_handoff.rs"]
mod holder_handoff;
mod laws;

mod uuid_model {
    use vstd::prelude::*;

    #[derive(Clone, Copy)]
    pub struct Uuid(pub u128);

    impl Uuid {
        #[verus_spec(out => ensures out == self.0)]
        pub fn as_u128(&self) -> u128 {
            self.0
        }
    }
}

use uuid_model::Uuid;
pub type Bytes = Vec<u8>;
pub type ObjectName = [u8; 32];

#[derive(Clone, Copy)]
pub struct Placement {
    pub deployment: Uuid,
    pub incarnation: Uuid,
    pub group: Uuid,
    pub generation: u64,
}

#[derive(Clone, Copy)]
pub struct Enrollment {
    pub worker: Uuid,
    pub serial: u64,
}

#[derive(Clone, Copy)]
pub struct Grant {
    pub enrollment: Enrollment,
    pub epoch: u64,
    pub key_generation: u64,
}

#[derive(Clone, Copy)]
pub struct RequestId {
    pub nonce: Uuid,
    pub expires_at: u64,
}

#[derive(Clone, Copy)]
pub enum Principal {
    Administrator,
    Worker(Enrollment),
    Custodian,
    RouteReader,
}

pub enum AppendClass {
    Data,
    Maintenance,
}

pub enum Command {
    Barrier,
    ProtectRange { from: u64, through: u64 },
    ReleaseRead { read: RequestId },
    Append {
        class: AppendClass,
        grant: Grant,
        expected_next: u64,
        timestamp: i64,
        payload: Bytes,
        references: Option<Vec<ObjectName>>,
    },
    Lease { grant: Grant },
    DisposeExpired { enrollment: Enrollment },
}

pub struct Request {
    pub placement: Placement,
    pub principal: Principal,
    pub id: RequestId,
    pub command: Command,
}

fn main() {}
