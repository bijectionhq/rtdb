import Std

namespace RTDB.Recovery

structure Stream where
  deployment : Nat
  generation : Nat
  deriving DecidableEq

structure Pack where
  writerEpoch : Nat
  boundary : Nat
  deriving DecidableEq

def update (f : Stream → α) (stream : Stream) (value : α) : Stream → α :=
  fun key => if key = stream then value else f key

structure Catalog where
  epoch : Stream → Nat
  uploaded : Stream → Pack → Prop
  published : Stream → Option Pack
  inherited : Stream → Option (Stream × Pack)
  trimmed : Stream → Nat

def empty : Catalog where
  epoch := fun _ => 0
  uploaded := fun _ _ => False
  published := fun _ => none
  inherited := fun _ => none
  trimmed := fun _ => 0

inductive Step (durablePrefix : Stream → Nat → Prop) : Catalog → Catalog → Prop where
  | advance (s : Catalog) (stream : Stream) (epoch : Nat)
      (newer : s.epoch stream < epoch) :
      Step durablePrefix s {s with epoch := update s.epoch stream epoch}
  | upload (s : Catalog) (stream : Stream) (pack : Pack)
      (complete : durablePrefix stream pack.boundary) :
      Step durablePrefix s
        {s with uploaded := fun key candidate =>
          (key = stream ∧ candidate = pack) ∨ s.uploaded key candidate}
  | publish (s : Catalog) (stream : Stream) (pack : Pack)
      (current : pack.writerEpoch = s.epoch stream)
      (uploaded : s.uploaded stream pack)
      (forward : ∀ old, s.published stream = some old → old.boundary ≤ pack.boundary) :
      Step durablePrefix s {s with published := update s.published stream (some pack)}
  | inherit (s : Catalog) (child parent : Stream) (pack : Pack)
      (distinct : child ≠ parent)
      (source : s.published parent = some pack) :
      Step durablePrefix s {s with inherited := update s.inherited child (some (parent, pack))}
  | trim (s : Catalog) (stream : Stream) (through : Nat) (pack : Pack)
      (published : s.published stream = some pack)
      (forward : s.trimmed stream ≤ through)
      (keepsBoundary : through < pack.boundary) :
      Step durablePrefix s {s with trimmed := update s.trimmed stream through}

def Valid (durablePrefix : Stream → Nat → Prop) (s : Catalog) : Prop :=
  (∀ stream pack, s.uploaded stream pack → durablePrefix stream pack.boundary) ∧
  (∀ stream pack, s.published stream = some pack → s.uploaded stream pack) ∧
  (∀ stream, s.trimmed stream = 0 ∨
    ∃ pack, s.published stream = some pack ∧ s.trimmed stream < pack.boundary)

theorem valid_empty : Valid durablePrefix empty := by
  exact ⟨fun _ _ h => False.elim h, by simp [empty], fun _ => Or.inl rfl⟩

theorem valid_step (before : Valid durablePrefix s)
    (step : Step durablePrefix s t) : Valid durablePrefix t := by
  obtain ⟨packs, publishedPacks, trims⟩ := before
  cases step with
  | advance => exact ⟨packs, publishedPacks, trims⟩
  | inherit => exact ⟨packs, publishedPacks, trims⟩
  | upload stream pack complete =>
      refine ⟨?_, ?_, trims⟩
      · intro key candidate evidence
        rcases evidence with ⟨rfl, rfl⟩ | alreadyUploaded
        · exact complete
        · exact packs key candidate alreadyUploaded
      · intro key candidate evidence
        exact Or.inr (publishedPacks key candidate evidence)
  | publish stream pack current uploaded forward =>
      refine ⟨packs, ?_, ?_⟩
      · intro key candidate evidence
        by_cases same : key = stream
        · subst key
          simp [update] at evidence
          subst candidate
          exact uploaded
        · exact publishedPacks key candidate (by simpa [update, same] using evidence)
      · intro key
        rcases trims key with untouched | ⟨old, wasPublished, within⟩
        · exact Or.inl untouched
        · right
          by_cases same : key = stream
          · subst key
            exact ⟨pack, by simp [update], Nat.lt_of_lt_of_le within (forward old wasPublished)⟩
          · exact ⟨old, by simpa [update, same] using wasPublished, within⟩
  | trim stream through pack published forward keepsBoundary =>
      refine ⟨packs, publishedPacks, ?_⟩
      intro key
      by_cases same : key = stream
      · subst key
        exact Or.inr ⟨pack, published, by simpa [update] using keepsBoundary⟩
      · simpa [update, same] using trims key

inductive Reachable (durablePrefix : Stream → Nat → Prop) : Catalog → Prop where
  | init : Reachable durablePrefix empty
  | next : Reachable durablePrefix s → Step durablePrefix s t → Reachable durablePrefix t

theorem reachable_valid (trace : Reachable durablePrefix s) : Valid durablePrefix s := by
  induction trace with
  | init => exact valid_empty
  | next _ step ih => exact valid_step ih step

theorem trimmed_prefix_has_durable_same_stream_checkpoint
    (trace : Reachable durablePrefix s) (nonempty : s.trimmed stream ≠ 0) :
    ∃ pack, s.published stream = some pack ∧
      durablePrefix stream pack.boundary ∧ s.trimmed stream < pack.boundary := by
  obtain ⟨packs, publishedPacks, trims⟩ := reachable_valid trace
  rcases trims stream with untouched | ⟨pack, published, within⟩
  · exact False.elim (nonempty untouched)
  · exact ⟨pack, published, packs stream pack (publishedPacks stream pack published), within⟩

theorem inherited_checkpoint_cannot_authorize_trim
    (trace : Reachable durablePrefix s) (noOwnCheckpoint : s.published stream = none) :
    s.trimmed stream = 0 := by
  rcases (reachable_valid trace).2.2 stream with untouched | ⟨pack, published, _⟩
  · exact untouched
  · rw [noOwnCheckpoint] at published
    contradiction

#print axioms trimmed_prefix_has_durable_same_stream_checkpoint
#print axioms inherited_checkpoint_cannot_authorize_trim

end RTDB.Recovery
