import Std

namespace RTDB.SearchWindows

def windowOf (document : Nat) : Nat := document / 128

structure Posting where
  document : Nat
  frequency : Nat
  length : Nat

structure Impact where
  frequency : Nat
  length : Nat
  deriving DecidableEq

def summarize : List Posting → Option Impact
  | [] => none
  | posting :: rest =>
    some (match summarize rest with
      | none => ⟨posting.frequency, posting.length⟩
      | some impact => ⟨max posting.frequency impact.frequency, min posting.length impact.length⟩)

theorem summary_bounds_every_posting (postings : List Posting) (posting : Posting)
    (found : posting ∈ postings) :
    ∃ impact, summarize postings = some impact ∧
      posting.frequency ≤ impact.frequency ∧ impact.length ≤ posting.length := by
  induction postings with
  | nil => simp at found
  | cons head tail ih =>
    rcases List.mem_cons.1 found with same | later
    · subst posting
      cases recorded : summarize tail with
      | none => exact ⟨⟨head.frequency, head.length⟩, by simp [summarize, recorded], Nat.le_refl _, Nat.le_refl _⟩
      | some impact =>
        exact ⟨⟨max head.frequency impact.frequency, min head.length impact.length⟩,
          by simp [summarize, recorded], Nat.le_max_left _ _, Nat.min_le_left _ _⟩
    · obtain ⟨impact, recorded, frequency, length⟩ := ih later
      exact ⟨⟨max head.frequency impact.frequency, min head.length impact.length⟩,
        by simp [summarize, recorded], Nat.le_trans frequency (Nat.le_max_right _ _),
        Nat.le_trans (Nat.min_le_right _ _) length⟩

def impactAt (postings : List Posting) (window : Nat) : Option Impact :=
  summarize (postings.filter (fun posting => decide (windowOf posting.document = window)))

theorem assigned_impact_bounds_posting (postings : List Posting) (posting : Posting)
    (found : posting ∈ postings) :
    ∃ impact, impactAt postings (windowOf posting.document) = some impact ∧
      posting.frequency ≤ impact.frequency ∧ impact.length ≤ posting.length := by
  apply summary_bounds_every_posting
  simpa using found

-- Each term's window list can come from cached metadata or from the postings iterator.
-- Membership in the window intersection is a necessary condition for a match.
def candidateWindows (windows : Nat → List Nat) (first : Nat) (rest : List Nat) : List Nat :=
  (windows first).filter (fun window => rest.all (fun term => (windows term).contains window))

theorem intersection_keeps_every_matching_document
    (windows : Nat → List Nat) (postings : Nat → List Posting)
    (first : Nat) (rest : List Nat) (doc : Nat)
    (summaryCovers : ∀ term posting, posting ∈ postings term → windowOf posting.document ∈ windows term)
    (matchesTerms : ∀ term ∈ first :: rest, ∃ posting ∈ postings term, posting.document = doc) :
    windowOf doc ∈ candidateWindows windows first rest := by
  have inWindow (term : Nat) (member : term ∈ first :: rest) : windowOf doc ∈ windows term := by
    obtain ⟨posting, found, identifies⟩ := matchesTerms term member
    simpa [identifies] using summaryCovers term posting found
  apply List.mem_filter.2
  refine ⟨inWindow first (List.mem_cons_self ..), List.all_eq_true.2 ?_⟩
  intro term member
  simpa using inWindow term (List.mem_cons_of_mem _ member)

def maxWord : Nat := 4_294_967_295

def windowStart (window : Nat) : Nat := min (window * 128) maxWord

def windowEnd (window : Nat) : Nat := min (min (window + 1) maxWord * 128) maxWord

theorem scanning_interval_matches_assignment (doc window : Nat) (readable : doc < maxWord) :
    windowOf doc = window ↔ windowStart window ≤ doc ∧ doc < windowEnd window := by
  simp only [windowOf, windowStart, windowEnd, maxWord, Nat.min_def] at *
  split <;> split <;> split <;> omega

theorem summary_combines_extrema_from_different_postings :
    summarize [⟨0, 9, 10⟩, ⟨1, 3, 5⟩] = some ⟨9, 5⟩ := by decide

theorem intersection_keeps_a_shared_window :
    1 ∈ candidateWindows (fun _ => [1]) 0 [1] := by decide

theorem a_window_reads_its_first_document :
    windowStart 1 ≤ 128 ∧ 128 < windowEnd 1 := by decide

#print axioms summary_bounds_every_posting
#print axioms assigned_impact_bounds_posting
#print axioms intersection_keeps_every_matching_document
#print axioms scanning_interval_matches_assignment
#print axioms summary_combines_extrema_from_different_postings
#print axioms intersection_keeps_a_shared_window
#print axioms a_window_reads_its_first_document

end RTDB.SearchWindows
