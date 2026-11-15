use rtdb_replication::{TermFence, TermPosition, Verdict};
use rtdb_types::{Term, TermSequence};

#[test]
fn term_seven_duplicate_then_gap_voids_delayed_records_until_a_new_claim() {
    let mut fence = TermFence::from_boundary(Term(6), None).unwrap();
    assert!(fence.admit(Term(7), Some(TermSequence(0))).unwrap().opens_term());
    assert_eq!(fence.admit(Term(7), Some(TermSequence(1))).unwrap().verdict, Verdict::Apply);
    assert_eq!(fence.admit(Term(7), Some(TermSequence(1))).unwrap().verdict, Verdict::Discarded);
    assert_eq!(fence.admit(Term(7), Some(TermSequence(3))).unwrap().verdict, Verdict::Voided);
    assert_eq!(fence.admit(Term(7), Some(TermSequence(2))).unwrap().verdict, Verdict::Discarded);
    assert_eq!(fence.position(), TermPosition::Voided);
    assert!(fence.admit(Term(8), Some(TermSequence(0))).unwrap().opens_term());
    assert_eq!(fence.admit(Term(8), Some(TermSequence(1))).unwrap().verdict, Verdict::Apply);
}

#[test]
fn a_losing_claim_neither_applies_nor_voids_the_winning_term() {
    let mut fence = TermFence::from_boundary(Term(1), Some(TermSequence(1))).unwrap();
    assert_eq!(fence.admit(Term(1), Some(TermSequence(0))).unwrap().verdict, Verdict::Discarded);
    assert_eq!(fence.admit(Term(1), None).unwrap().verdict, Verdict::Discarded);
    assert_eq!(fence.admit(Term(1), Some(TermSequence(2))).unwrap().verdict, Verdict::Apply);
}

#[test]
fn boundary_sequence_seeds_the_next_admissible_record() {
    let mut fence = TermFence::from_boundary(Term(3), Some(TermSequence(6))).unwrap();
    assert_eq!(fence.admit(Term(3), Some(TermSequence(8))).unwrap().verdict, Verdict::Voided);
    assert_eq!(fence.admit(Term(3), Some(TermSequence(7))).unwrap().verdict, Verdict::Discarded);
}

#[test]
fn a_new_term_without_its_claim_is_voided_immediately() {
    let mut fence = TermFence::from_boundary(Term(3), None).unwrap();
    assert_eq!(fence.admit(Term(4), Some(TermSequence(1))).unwrap().verdict, Verdict::Voided);
    assert_eq!(fence.highest_term(), Term(4));
    assert_eq!(fence.admit(Term(4), Some(TermSequence(0))).unwrap().verdict, Verdict::Discarded);
}

#[test]
fn a_term_won_by_an_unsequenced_claim_remains_unchecked() {
    let mut fence = TermFence::from_boundary(Term(1), Some(TermSequence(6))).unwrap();
    assert!(fence.admit(Term(2), None).unwrap().opens_term());
    for sequence in [Some(TermSequence(5)), None, Some(TermSequence(2)), Some(TermSequence(0))] {
        assert_eq!(fence.admit(Term(2), sequence).unwrap().verdict, Verdict::Apply);
    }
}

#[test]
fn a_legacy_boundary_takes_the_first_nonclaim_position_as_given() {
    let mut fence = TermFence::from_boundary(Term(3), None).unwrap();
    assert_eq!(fence.position(), TermPosition::Unknown);
    assert_eq!(fence.admit(Term(3), Some(TermSequence(0))).unwrap().verdict, Verdict::Discarded);
    assert_eq!(fence.position(), TermPosition::Unknown);
    assert_eq!(fence.admit(Term(3), Some(TermSequence(10))).unwrap().verdict, Verdict::Apply);
    assert_eq!(fence.admit(Term(3), Some(TermSequence(12))).unwrap().verdict, Verdict::Voided);
}

#[test]
fn a_losing_claim_after_a_legacy_boundary_leaves_the_mode_to_the_next_record() {
    let mut fence = TermFence::from_boundary(Term(3), None).unwrap();
    let claim = fence.admit(Term(3), Some(TermSequence(0))).unwrap();
    assert_eq!(claim.verdict, Verdict::Discarded);
    assert!(!claim.opens_term());
    assert_eq!(fence.position(), TermPosition::Unknown);
    assert_eq!(fence.admit(Term(3), None).unwrap().verdict, Verdict::Apply);
    assert_eq!(fence.position(), TermPosition::Unsequenced);
    assert_eq!(fence.admit(Term(3), Some(TermSequence(10))).unwrap().verdict, Verdict::Apply);
}

#[test]
fn absent_and_explicit_zero_boundary_positions_have_different_modes() {
    let unsequenced = TermFence::from_boundary(Term(0), None).unwrap();
    let unknown = TermFence::from_boundary(Term(3), None).unwrap();
    let sequenced = TermFence::from_boundary(Term(3), Some(TermSequence(0))).unwrap();
    assert_eq!(unsequenced.position(), TermPosition::Unsequenced);
    assert_eq!(unknown.position(), TermPosition::Unknown);
    assert_eq!(sequenced.position(), TermPosition::Expecting(TermSequence(1)));
}

#[test]
fn a_higher_unsequenced_term_reopens_a_voided_term() {
    let mut fence = TermFence::from_boundary(Term(3), Some(TermSequence(6))).unwrap();
    assert_eq!(fence.admit(Term(3), Some(TermSequence(8))).unwrap().verdict, Verdict::Voided);
    for sequence in [None, Some(TermSequence(0)), Some(TermSequence(7)), Some(TermSequence(9))] {
        assert_eq!(fence.admit(Term(3), sequence).unwrap().verdict, Verdict::Discarded);
    }
    let claim = fence.admit(Term(4), None).unwrap();
    assert!(claim.opens_term());
    assert_eq!(claim.previous_term, Term(3));
    assert_eq!(claim.highest_term, Term(4));
    assert_eq!(fence.position(), TermPosition::Unsequenced);
    assert_eq!(fence.admit(Term(4), Some(TermSequence(9))).unwrap().verdict, Verdict::Apply);
}

#[test]
fn admission_survives_role_changes_and_never_lowers_the_fence() {
    let mut reader_fence = TermFence::from_boundary(Term(1), Some(TermSequence(0))).unwrap();
    reader_fence.admit(Term(2), Some(TermSequence(0))).unwrap();
    let previous = reader_fence.clone();
    assert_eq!(reader_fence.admit(Term(1), Some(TermSequence(1))).unwrap().verdict, Verdict::Fenced);
    assert_eq!(reader_fence, previous);
    assert_eq!(reader_fence.claim_term().unwrap(), Term(3));
}

#[test]
fn sequence_exhaustion_returns_an_error_without_publishing_a_wrapped_position() {
    let mut fence = TermFence::from_boundary(Term(3), Some(TermSequence(i64::MAX - 1))).unwrap();
    let before = fence.clone();
    assert!(fence.admit(Term(3), Some(TermSequence(i64::MAX))).is_err());
    assert_eq!(fence, before);
}
