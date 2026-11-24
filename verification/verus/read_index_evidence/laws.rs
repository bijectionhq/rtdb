use crate::production::valid_reply;
use vstd::prelude::*;

verus! {
    pub fn a_prior_invocation_cannot_vote() {
        let accepted = valid_reply(2, 1, 42, 41);
        assert(!accepted);
    }

    pub fn exposure_cannot_lead_decision_installation() {
        let accepted = valid_reply(1, 1, 41, 42);
        assert(!accepted);
    }

    pub fn a_matching_reply_may_report_partial_installation() {
        let accepted = valid_reply(1, 1, 42, 41);
        assert(accepted);
    }
}
