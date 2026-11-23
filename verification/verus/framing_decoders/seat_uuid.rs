use vstd::prelude::*;

verus! {

pub struct Uuid { pub value: u128 }

pub open spec fn non_nil(attempt: Uuid) -> bool { attempt.value != 0 }
pub uninterp spec fn canonical_uuid(attempt: Uuid) -> Seq<u8>;

#[verifier::external_body]
pub fn parse_attempt(bytes: &[u8], start: usize) -> (out: Option<(Uuid, [u8; 36])>)
    ensures
        out matches Some((attempt, text)) ==> text@ == canonical_uuid(attempt) && non_nil(attempt),
        forall|attempt: Uuid| start <= bytes@.len()
            && bytes@.skip(start as int) == #[trigger] canonical_uuid(attempt) && non_nil(attempt)
            ==> (out matches Some((decoded, text)) && decoded == attempt && text@ == canonical_uuid(attempt)),
{ unimplemented!() }

}
