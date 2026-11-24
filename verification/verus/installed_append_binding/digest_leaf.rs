use vstd::prelude::*;

verus! {

pub uninterp spec fn sha256(bytes: Seq<u8>) -> Seq<u8>;

#[verifier::external_body]
pub fn digest_matches(expected: &[u8], bytes: &[u8]) -> (matches: bool)
    ensures matches == (expected@ == sha256(bytes@)),
{ unimplemented!() }

}
