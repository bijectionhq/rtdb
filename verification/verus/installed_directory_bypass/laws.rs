use crate::node::{directory_move_park_allowed, directory_route_allowed, pressure_candidate};
use crate::production::{memory_allows_insert, memory_allows_reservation};
use crate::production::dynamo::reservation_fields;
use vstd::prelude::*;

verus! {

pub open spec fn dynamo_allows_write(fields: Seq<Seq<char>>, expected: Option<u64>, current: Option<u64>) -> bool {
    match expected {
        None => !fields.contains("id"@),
        Some(version) => fields.contains("version"@) && current == Some(version),
    }
}

fn reserved_identity_refuses_directory_insert(replacing_stage: bool, exists: bool) {
    let allowed = memory_allows_insert(true, replacing_stage, exists);
    assert(!allowed);
}

fn directory_record_refuses_reservation() {
    let allowed = memory_allows_reservation(true);
    assert(!allowed);
}

fn dynamo_reservation_refuses_directory_write(expected: Option<u64>, current: Option<u64>) {
    let fields = reservation_fields();
    proof! {
        reveal_strlit("id");
        reveal_strlit("version");
        reveal_strlit("installed_journal");
    }
    assert(!dynamo_allows_write(seq![fields@[0]@, fields@[1]@], expected, current));
}

fn installed_name_refuses_directory_route() {
    let allowed = directory_route_allowed(true);
    assert(!allowed);
}

fn installed_name_refuses_directory_move_park() {
    let allowed = directory_move_park_allowed(true, true);
    assert(!allowed);
}

fn journal_holding_is_never_a_pressure_candidate(protected: bool, resident: bool) {
    let candidate = pressure_candidate(false, protected, resident);
    assert(!candidate);
}

}
