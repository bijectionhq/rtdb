use crate::production::valid_object_version;
use vstd::prelude::*;

verus! {

// LAW S3-VERSION-PRESENCE.
pub fn an_empty_version_refuses(version: &[u8])
    requires version@.len() == 0,
{
    let accepted = valid_object_version(version);
    assert(!accepted);
}

// LAW S3-VERSION-IMMUTABILITY.
pub fn a_null_version_refuses(version: &[u8])
    requires version@ == seq![110u8, 117u8, 108u8, 108u8],
{
    let accepted = valid_object_version(version);
    assert(!accepted);
}

// LAW S3-VERSION-OPACITY.
pub fn other_nonempty_versions_are_accepted(version: &[u8])
    requires version@.len() > 0, version@ != seq![110u8, 117u8, 108u8, 108u8],
{
    let accepted = valid_object_version(version);
    proof {
        if version@.len() == 4 && version@[0] == 110 && version@[1] == 117 && version@[2] == 108 && version@[3] == 108 {
            assert(version@ =~= seq![110u8, 117u8, 108u8, 108u8]);
        }
    }
    assert(accepted);
}

}
