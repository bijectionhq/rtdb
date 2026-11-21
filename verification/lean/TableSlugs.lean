import Std

namespace RTDB.TableSlugs

def escapedComponent (name : String) : List Char :=
  name.toList.map (fun char => if char = '.' ∨ char = '/' then '$' else char)

def legacySlug (schema table : String) : List Char :=
  escapedComponent schema ++ '$' :: escapedComponent table

theorem escaped_names_can_share_a_slug :
    "a.b" ≠ "a$b" ∧ legacySlug "public" "a.b" = legacySlug "public" "a$b" := by
  decide

def firstFree [DecidableEq Slug] (used candidates : List Slug) : Option Slug :=
  candidates.find? (fun slug => decide (slug ∉ used))

theorem a_selected_slug_is_unclaimed [DecidableEq Slug] (used candidates : List Slug) (slug : Slug)
    (selected : firstFree used candidates = some slug) : slug ∉ used := by
  have fresh := List.find?_some selected
  simpa using fresh

theorem enough_distinct_candidates_supply_a_slug [DecidableEq Slug] (used candidates : List Slug)
    (distinct : candidates.Nodup) (enough : used.length < candidates.length) :
    (firstFree used candidates).isSome := by
  cases selected : firstFree used candidates with
  | some slug => rfl
  | none =>
    have exhausted := List.find?_eq_none.mp selected
    have subset : candidates ⊆ used := by
      intro candidate present
      simpa using exhausted candidate present
    exact (Nat.not_le_of_gt enough (distinct.length_le_of_subset subset)).elim

def suffixed (base : String) (suffix : Nat) : String := (base ++ "~") ++ suffix.repr

theorem different_suffix_numbers_give_different_slugs (base : String) {left right : Nat}
    (equal : suffixed base left = suffixed base right) : left = right :=
  Nat.repr_injective ((String.append_right_inj (base ++ "~")).mp equal)

private theorem suffixed_distinct (base : String) (numbers : List Nat) (distinct : numbers.Nodup) :
    (numbers.map (suffixed base)).Nodup := by
  induction numbers with
  | nil => simp
  | cons number rest ih =>
    obtain ⟨fresh, distinctRest⟩ := List.nodup_cons.mp distinct
    apply List.nodup_cons.mpr
    refine ⟨?_, ih distinctRest⟩
    intro member
    obtain ⟨other, inRest, same⟩ := List.mem_map.mp member
    exact fresh (different_suffix_numbers_give_different_slugs base same ▸ inRest)

theorem first_sufficient_suffixes_always_supply_a_slug (base : String) (used : List String) :
    (firstFree used ((List.range' 1 (used.length + 1)).map (suffixed base))).isSome := by
  apply enough_distinct_candidates_supply_a_slug
  · exact suffixed_distinct base _ (List.nodup_range' 1)
  · simp

def AliasFree (records : List (Name × Slug)) : Prop := (records.map Prod.snd).Nodup

theorem registration_preserves_recorded_slugs [DecidableEq Slug] (records : List (Name × Slug))
    (name : Name) (slug : Slug) (candidates : List Slug) (separate : AliasFree records)
    (selected : firstFree (records.map Prod.snd) candidates = some slug) :
    AliasFree ((name, slug) :: records) ∧ ∀ item ∈ records, item ∈ (name, slug) :: records := by
  have unclaimed := a_selected_slug_is_unclaimed _ _ _ selected
  exact ⟨List.nodup_cons.mpr ⟨unclaimed, separate⟩,
    fun _ present => List.mem_cons_of_mem _ present⟩

#print axioms escaped_names_can_share_a_slug
#print axioms a_selected_slug_is_unclaimed
#print axioms enough_distinct_candidates_supply_a_slug
#print axioms different_suffix_numbers_give_different_slugs
#print axioms first_sufficient_suffixes_always_supply_a_slug
#print axioms registration_preserves_recorded_slugs

end RTDB.TableSlugs
